"""ETF unit-net-value loading for native free strategies."""
from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import polars as pl

logger = logging.getLogger(__name__)

_NAV_PATTERN = re.compile(r"var\s+Data_netWorthTrend\s*=\s*(\[[^;]*\])", re.DOTALL)
_CHINA_TIMEZONE = timezone(timedelta(hours=8))


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


def load_fund_nav_history(
    data_dir: Path,
    engine: Any,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, Any]:
    values: dict[str, list[dict[str, Any]]] = {}
    missing = []
    for symbol in symbols:
        path = _fund_nav_path(data_dir, symbol)
        cached = _read_cached_nav(path)
        if cached:
            values[symbol] = cached
        else:
            missing.append(symbol)

    with ThreadPoolExecutor(max_workers=min(4, max(1, len(missing)))) as pool:
        futures = {pool.submit(_fetch_fund_nav, symbol): symbol for symbol in missing}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                rows = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("基金净值拉取失败 %s: %s", symbol, exc)
                rows = []
            if not rows:
                continue
            values[symbol] = rows
            path = _fund_nav_path(data_dir, symbol)
            path.parent.mkdir(parents=True, exist_ok=True)
            pl.DataFrame(rows).sort("date").write_parquet(path)

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
    return {
        "provider": "eastmoney.fund",
        "requested_symbols": len(symbols),
        "available_symbols": len(visible),
        "missing_symbols": sorted(set(symbols) - set(visible)),
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
        "rows": 0,
    }
    attempted: set[str] = set()

    def load_nav(info: str, symbols: list[str], load_start: date, load_end: date) -> None:
        if info != "unit_net_value":
            return
        pending = sorted(set(symbols) - attempted)
        if not pending:
            return
        attempted.update(pending)
        load_fund_nav_history(
            repo.store.data_dir,
            engine,
            pending,
            load_start,
            engine.run_end or load_end,
        )
        visible = engine.extra_history.get("unit_net_value", {})
        nav.update({
            "requested_symbols": len(attempted),
            "available_symbols": len(set(attempted) & set(visible)),
            "missing_symbols": sorted(attempted - set(visible)),
            "rows": sum(len(visible.get(symbol, {})) for symbol in attempted),
        })

    engine.set_extra_history_loader(load_nav)
    return nav
