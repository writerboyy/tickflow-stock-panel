"""Read-only EasyTDX corroboration for rejected TickFlow index bars."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
import logging
import math
from typing import Any

import polars as pl


_FIELDS = ("open", "high", "low", "close", "volume", "amount")
_PRICE_FIELDS = {"open", "high", "low", "close"}
_MAX_REASONABLE_INDEX_AMOUNT = 1e15
_FetchIndexDaily = Callable[[list[str], date, date], pl.DataFrame]
logger = logging.getLogger(__name__)


def _market_for_symbol(symbol: str):
    from easy_tdx.models.enums import Market

    if symbol.endswith(".SH"):
        return Market.SH
    if symbol.endswith(".SZ"):
        return Market.SZ
    if symbol.endswith(".BJ"):
        return Market.BJ
    raise ValueError(f"unsupported index symbol: {symbol}")


def _normalize_easy_tdx_bars(frame: Any, symbol: str) -> pl.DataFrame:
    if frame is None or len(frame) == 0:
        return pl.DataFrame()
    if isinstance(frame, pl.DataFrame):
        result = frame
    elif hasattr(frame, "reset_index"):
        result = pl.from_pandas(frame.reset_index(drop=True))
    else:
        result = pl.DataFrame(frame)
    if "vol" in result.columns and "volume" not in result.columns:
        result = result.rename({"vol": "volume"})
    required = {"date", *_FIELDS}
    missing = sorted(required - set(result.columns))
    if missing:
        raise ValueError(f"EasyTDX index bars missing fields: {', '.join(missing)}")
    return (
        result.select(
            pl.lit(symbol).alias("symbol"),
            pl.col("date").cast(pl.String).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=True),
            *(pl.col(field).cast(pl.Float64, strict=True) for field in _FIELDS),
        )
        .unique(subset=["symbol", "date"], keep="last")
        .sort(["symbol", "date"])
    )


def fetch_easy_tdx_index_daily(
    symbols: list[str],
    start_date: date,
    end_date: date,
    *,
    timeout: float = 8.0,
    page_size: int = 800,
    max_pages: int = 10,
    max_hosts: int = 8,
) -> pl.DataFrame:
    """Fetch index daily bars without persisting or invoking collectors."""
    from easy_tdx import TdxClient
    from easy_tdx.config import get_best_host, get_known_hosts, get_port, save_best_host
    from easy_tdx.models.enums import KlineCategory

    parts: list[pl.DataFrame] = []
    hosts = list(dict.fromkeys([get_best_host(), *get_known_hosts()]))[:max_hosts]
    for symbol in sorted(set(symbols)):
        symbol_parts: list[pl.DataFrame] = []
        for host in hosts:
            try:
                with TdxClient(
                    host,
                    get_port(),
                    timeout=min(timeout, 3.0),
                    auto_reconnect=False,
                    heartbeat_interval=0,
                ) as client:
                    market = _market_for_symbol(symbol)
                    code = symbol.split(".", 1)[0]
                    for page in range(max_pages):
                        frame = client.get_index_bars(
                            market,
                            code,
                            KlineCategory.DAY,
                            page * page_size,
                            page_size,
                        )
                        normalized = _normalize_easy_tdx_bars(frame, symbol)
                        if normalized.is_empty():
                            break
                        symbol_parts.append(normalized.filter(pl.col("date") <= end_date))
                        if (
                            normalized["date"].min() <= start_date
                            or normalized.height < page_size
                        ):
                            break
                if symbol_parts:
                    save_best_host(host)
                    parts.extend(symbol_parts)
                    break
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "EasyTDX 指数 %s 节点 %s 核验失败 (%s)",
                    symbol,
                    host,
                    type(exc).__name__,
                )
        if not symbol_parts:
            logger.warning("EasyTDX 指数 %s 在所有节点均无可用数据", symbol)
    if not parts:
        return pl.DataFrame()
    return (
        pl.concat(parts, how="diagonal_relaxed")
        .filter(pl.col("date").is_between(start_date, end_date))
        .unique(subset=["symbol", "date"], keep="last")
        .sort(["symbol", "date"])
    )


def _anomalies(row: dict[str, Any]) -> list[str]:
    anomalies: list[str] = []
    for field in _FIELDS:
        value = row.get(field)
        if value is None or not math.isfinite(float(value)):
            anomalies.append(f"{field}_not_finite")
    for field in _PRICE_FIELDS:
        value = row.get(field)
        if value is not None and math.isfinite(float(value)) and float(value) <= 0:
            anomalies.append(f"{field}_not_positive")
    volume = row.get("volume")
    amount = row.get("amount")
    if volume is not None and math.isfinite(float(volume)) and float(volume) < 0:
        anomalies.append("volume_negative")
    if amount is not None and math.isfinite(float(amount)):
        if float(amount) < 0:
            anomalies.append("amount_negative")
        elif float(amount) >= _MAX_REASONABLE_INDEX_AMOUNT:
            anomalies.append("amount_overflow")
    values = {
        field: float(row[field])
        for field in _PRICE_FIELDS
        if row.get(field) is not None and math.isfinite(float(row[field]))
    }
    if len(values) == len(_PRICE_FIELDS):
        if values["high"] < max(values.values()):
            anomalies.append("high_below_ohlc")
        if values["low"] > min(values.values()):
            anomalies.append("low_above_ohlc")
    return sorted(set(anomalies))


def _same_value(field: str, left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    left_value = float(left)
    right_value = float(right)
    if not math.isfinite(left_value) or not math.isfinite(right_value):
        return False
    if field in _PRICE_FIELDS:
        return math.isclose(left_value, right_value, rel_tol=1e-5, abs_tol=0.02)
    return math.isclose(left_value, right_value, rel_tol=1e-4, abs_tol=1.0)


def crosscheck_index_daily(
    tickflow_rows: pl.DataFrame,
    *,
    fetcher: _FetchIndexDaily = fetch_easy_tdx_index_daily,
) -> dict[str, Any]:
    """Corroborate rejected rows while keeping TickFlow publication fail-closed."""
    if tickflow_rows.is_empty():
        return {
            "status": "no_anomalies",
            "source": "easy_tdx",
            "requested_rows": 0,
            "matched_rows": 0,
            "status_counts": {},
            "rows": [],
        }
    required = {"symbol", "date", *_FIELDS}
    missing = sorted(required - set(tickflow_rows.columns))
    if missing:
        raise ValueError(f"TickFlow index bars missing fields: {', '.join(missing)}")
    source = tickflow_rows.select("symbol", "date", *_FIELDS).sort(["symbol", "date"])
    symbols = source["symbol"].unique().sort().to_list()
    start_date = source["date"].min()
    end_date = source["date"].max()
    try:
        tdx = fetcher(symbols, start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "unavailable",
            "source": "easy_tdx",
            "requested_rows": source.height,
            "matched_rows": 0,
            "error_code": type(exc).__name__,
            "status_counts": {"tdx_unavailable": source.height},
            "rows": [],
        }

    tdx_by_key = {
        (str(row["symbol"]), row["date"]): row
        for row in tdx.select("symbol", "date", *_FIELDS).iter_rows(named=True)
    } if not tdx.is_empty() else {}
    results: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for tickflow in source.iter_rows(named=True):
        key = str(tickflow["symbol"]), tickflow["date"]
        other = tdx_by_key.get(key)
        tickflow_anomalies = _anomalies(tickflow)
        if other is None:
            status = "tdx_unavailable"
            tdx_anomalies: list[str] = []
            differing_fields: list[str] = []
        else:
            tdx_anomalies = _anomalies(other)
            invalid_fields = {value.split("_", 1)[0] for value in tickflow_anomalies}
            comparable_fields = [field for field in _FIELDS if field not in invalid_fields]
            differing_fields = [
                field
                for field in comparable_fields
                if not _same_value(field, tickflow.get(field), other.get(field))
            ]
            if tdx_anomalies:
                status = "both_anomalous"
            elif differing_fields:
                status = "source_conflict"
            else:
                status = "tickflow_anomaly_confirmed"
        counts[status] = counts.get(status, 0) + 1
        results.append({
            "symbol": key[0],
            "date": key[1].isoformat(),
            "status": status,
            "tickflow_anomalies": tickflow_anomalies,
            "tdx_anomalies": tdx_anomalies,
            "differing_valid_fields": differing_fields,
            "tickflow": {field: tickflow.get(field) for field in _FIELDS},
            "easy_tdx": (
                {field: other.get(field) for field in _FIELDS}
                if other is not None
                else None
            ),
        })
    matched = sum(1 for row in results if row["easy_tdx"] is not None)
    return {
        "status": "complete" if matched == source.height else "partial",
        "source": "easy_tdx",
        "requested_rows": source.height,
        "matched_rows": matched,
        "status_counts": dict(sorted(counts.items())),
        "rows": results,
    }


def crosscheck_summary(result: dict[str, Any]) -> str:
    counts = result.get("status_counts") or {}
    detail = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    return (
        f"status={result.get('status', 'unknown')}, "
        f"matched={result.get('matched_rows', 0)}/{result.get('requested_rows', 0)}"
        + (f", {detail}" if detail else "")
    )
