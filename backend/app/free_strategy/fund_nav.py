"""ETF unit-net-value loading for native free strategies."""
from __future__ import annotations

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import polars as pl

logger = logging.getLogger(__name__)

_NAV_PATTERN = re.compile(r"var\s+Data_netWorthTrend\s*=\s*(\[[^;]*\])", re.DOTALL)
_CHINA_TIMEZONE = timezone(timedelta(hours=8))
# QDII NAVs are commonly published one or two Chinese trading sessions late.
MAX_NAV_DISCLOSURE_LAG_TRADING_DAYS = 2


def _row_date(row: Any) -> date | None:
    if isinstance(row, dict):
        value = row.get("date") or row.get("timestamp")
    else:
        value = getattr(row, "date", None) or getattr(row, "timestamp", None)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def align_disclosed_nav(
    nav_rows: list[dict[str, Any]],
    market_rows: list[Any],
    required_date: date | datetime | str,
    *,
    max_lag_trading_days: int = MAX_NAV_DISCLOSURE_LAG_TRADING_DAYS,
) -> tuple[dict[str, Any], Any, int] | None:
    """Pair the latest disclosed NAV with the market close from the same day."""
    required = (
        required_date.date()
        if isinstance(required_date, datetime)
        else required_date
        if isinstance(required_date, date)
        else date.fromisoformat(str(required_date)[:10])
    )
    market_by_date = {
        day: row
        for row in market_rows
        if (day := _row_date(row)) is not None and day <= required
    }
    if required not in market_by_date:
        return None
    market_dates = sorted(market_by_date)
    candidates: list[tuple[date, dict[str, Any]]] = []
    for row in nav_rows:
        day = _row_date(row)
        try:
            value = float(row.get("value"))
        except (AttributeError, TypeError, ValueError):
            continue
        if day is not None and day in market_by_date and day <= required and value > 0:
            candidates.append((day, {"date": day.isoformat(), "value": value}))
    if not candidates:
        return None
    nav_date, nav_row = max(candidates, key=lambda item: item[0])
    lag = sum(nav_date < day <= required for day in market_dates)
    if lag > max_lag_trading_days:
        return None
    return nav_row, market_by_date[nav_date], lag


def _fund_nav_path(data_dir: Path, symbol: str) -> Path:
    return data_dir / "fund_nav" / f"symbol={symbol}" / "part.parquet"


def _fetch_fund_nav(symbol: str) -> list[dict[str, Any]]:
    code = symbol.split(".", 1)[0]
    response: httpx.Response | None = None
    for attempt in range(3):
        try:
            response = httpx.get(
                f"https://fund.eastmoney.com/pingzhongdata/{code}.js",
                params={"v": datetime.now().strftime("%Y%m%d")},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
        except httpx.TransportError:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise
        if response.status_code < 400:
            break
        if attempt < 2 and response.status_code in {429, 514}:
            time.sleep(0.5 * (attempt + 1))
            continue
        response.raise_for_status()
    assert response is not None
    response.raise_for_status()
    match = _NAV_PATTERN.search(response.text.lstrip("\ufeff"))
    if match is None:
        return []
    values = json.loads(match.group(1))
    return [
        {
            "symbol": symbol,
            "date": datetime.fromtimestamp(float(item["x"]) / 1000, _CHINA_TIMEZONE).date(),
            "unit_net_value": float(item["y"]),
            "date_timezone": "Asia/Shanghai",
        }
        for item in values
        if item.get("x") is not None and item.get("y") is not None
    ]


def _read_cached_nav(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        frame = pl.read_parquet(path)
        if "date_timezone" not in frame.columns:
            frame = frame.with_columns((pl.col("date") + pl.duration(days=1)).alias("date"))
        return frame.select("symbol", "date", "unit_net_value").to_dicts()
    except Exception as exc:  # noqa: BLE001
        logger.warning("基金净值缓存读取失败 %s: %s", path, exc)
        return []


def _merge_nav_rows(
    symbol: str,
    cached: list[dict[str, Any]],
    fetched: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_date: dict[date, float] = {}
    for row in [*cached, *fetched]:
        day = row.get("date")
        value = row.get("unit_net_value")
        if not isinstance(day, date) or value is None:
            continue
        by_date[day] = float(value)
    return [
        {
            "symbol": symbol,
            "date": day,
            "unit_net_value": by_date[day],
            "date_timezone": "Asia/Shanghai",
        }
        for day in sorted(by_date)
    ]


def _write_nav_cache(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        pl.DataFrame(rows).select(
            "symbol", "date", "unit_net_value", "date_timezone",
        ).sort("date").write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_fund_nav_history(
    data_dir: Path,
    engine: Any,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, Any]:
    values: dict[str, list[dict[str, Any]]] = {}
    refresh: list[str] = []
    refresh_failed: set[str] = set()
    for symbol in symbols:
        path = _fund_nav_path(data_dir, symbol)
        cached = _read_cached_nav(path)
        if cached:
            values[symbol] = cached
        actual_date = max((row["date"] for row in cached), default=None)
        if actual_date is None or actual_date < end:
            refresh.append(symbol)

    with ThreadPoolExecutor(max_workers=min(4, max(1, len(refresh)))) as pool:
        futures = {pool.submit(_fetch_fund_nav, symbol): symbol for symbol in refresh}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                rows = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("基金净值拉取失败 %s: %s", symbol, exc)
                rows = []
            if not rows:
                refresh_failed.add(symbol)
                continue
            merged = _merge_nav_rows(symbol, values.get(symbol, []), rows)
            values[symbol] = merged
            path = _fund_nav_path(data_dir, symbol)
            try:
                _write_nav_cache(path, merged)
            except Exception as exc:  # noqa: BLE001
                logger.warning("基金净值缓存写入失败 %s: %s", symbol, exc)

    visible: dict[str, dict[date, float]] = {}
    cutoff_start = start - timedelta(days=90)
    for symbol, rows in values.items():
        selected = {
            row["date"]: float(row["unit_net_value"])
            for row in rows
            if cutoff_start <= row["date"] <= end
        }
        if selected:
            visible[symbol] = selected
    engine.set_extra_history("unit_net_value", visible)
    symbol_freshness: dict[str, dict[str, str | None]] = {}
    for symbol in symbols:
        actual_date = max(
            (row["date"] for row in values.get(symbol, [])),
            default=None,
        )
        if actual_date is not None and actual_date >= end:
            status = "fresh"
        elif symbol in refresh_failed:
            status = "unavailable"
        else:
            status = "stale"
        symbol_freshness[symbol] = {
            "required_date": end.isoformat(),
            "actual_date": actual_date.isoformat() if actual_date is not None else None,
            "freshness_status": status,
        }
    from app.services.fund_nav_schema import write_fund_nav_schema_registry

    write_fund_nav_schema_registry(data_dir)
    return {
        "provider": "eastmoney.fund",
        "requested_symbols": len(symbols),
        "available_symbols": len(visible),
        "missing_symbols": sorted(set(symbols) - set(visible)),
        "stale_symbols": sorted(
            symbol for symbol, item in symbol_freshness.items()
            if item["freshness_status"] == "stale"
        ),
        "unavailable_symbols": sorted(
            symbol for symbol, item in symbol_freshness.items()
            if item["freshness_status"] == "unavailable"
        ),
        "symbol_freshness": symbol_freshness,
        "rows": sum(len(rows) for rows in visible.values()),
    }


def prepare_fund_nav_data(
    repo: Any,
    engine: Any,
    start: date,
    end: date,
) -> dict[str, Any]:
    nav: dict[str, Any] = {
        "provider": "eastmoney.fund",
        "mode": "lazy",
        "requested_symbols": 0,
        "available_symbols": 0,
        "missing_symbols": [],
        "stale_symbols": [],
        "unavailable_symbols": [],
        "symbol_freshness": {},
        "rows": 0,
    }
    attempted_through: dict[str, date] = {}
    freshness: dict[str, dict[str, str | None]] = {}

    def load_nav(info: str, symbols: list[str], load_start: date, load_end: date) -> None:
        if info != "unit_net_value":
            return
        requested = {
            str(symbol).strip().upper()
            for symbol in [*symbols, *engine.universe]
            if str(symbol).strip()
        }
        pending = sorted(
            symbol for symbol in requested
            if attempted_through.get(symbol, date.min) < load_end
        )
        if not pending:
            return
        result = load_fund_nav_history(
            repo.store.data_dir,
            engine,
            pending,
            load_start,
            load_end,
        )
        for symbol in pending:
            attempted_through[symbol] = load_end
        freshness.update(result.get("symbol_freshness", {}))
        visible = engine.extra_history.get("unit_net_value", {})
        nav.update({
            "requested_symbols": len(attempted_through),
            "available_symbols": len(set(attempted_through) & set(visible)),
            "missing_symbols": sorted(set(attempted_through) - set(visible)),
            "stale_symbols": sorted(
                symbol for symbol, item in freshness.items()
                if item.get("freshness_status") == "stale"
            ),
            "unavailable_symbols": sorted(
                symbol for symbol, item in freshness.items()
                if item.get("freshness_status") == "unavailable"
            ),
            "symbol_freshness": dict(freshness),
            "rows": sum(len(visible.get(symbol, {})) for symbol in attempted_through),
        })

    engine.set_extra_history_loader(load_nav)
    return nav
