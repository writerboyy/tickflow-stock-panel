"""Normalize provider responses into internal Polars schemas."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from app.indicators.pipeline import filter_halt_days

DAILY_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "quote_ts"]
ADJ_FACTOR_COLS = ["symbol", "trade_date", "ex_factor"]
INSTRUMENT_COLS = ["symbol", "name", "code", "exchange", "asset_type", "source"]
TICK_COLS = [
    "symbol", "datetime", "last_price", "close", "open", "high", "low",
    "prev_close", "volume", "amount", "limit_up", "limit_down", "suspended",
    "source", "sequence", "trade_id", "source_order",
]

_CN_TZ = ZoneInfo("Asia/Shanghai")


def to_polars(data) -> pl.DataFrame:
    if data is None:
        return pl.DataFrame()
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, dict):
        rows: list[dict] = []
        for sym, values in data.items():
            for item in values or []:
                row = dict(item or {})
                row.setdefault("symbol", sym)
                rows.append(row)
        return pl.DataFrame(rows) if rows else pl.DataFrame()
    if hasattr(data, "reset_index"):
        return pl.from_pandas(data.reset_index())
    try:
        return pl.DataFrame(data)
    except Exception:  # noqa: BLE001
        return pl.DataFrame()


def normalize_daily(data, default_symbol: str | None = None, source: str = "tickflow") -> pl.DataFrame:  # noqa: ARG001
    df = to_polars(data)
    if df.is_empty():
        return df
    rename_map = {
        "ts_code": "symbol",
        "trade_date": "date",
        "datetime": "date",
        "vol": "volume",
        "amt": "amount",
        "timestamp": "quote_ts",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})
    if "symbol" not in df.columns and default_symbol:
        df = df.with_columns(pl.lit(default_symbol).alias("symbol"))
    if "date" in df.columns and df.schema["date"] != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date, strict=False))
    # quote_ts: 毫秒级行情时间戳, 用于盘后校验/量比折算。保留为 Int64, 缺失则置 null。
    if "quote_ts" in df.columns:
        df = df.with_columns(pl.col("quote_ts").cast(pl.Int64, strict=False))
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    df = filter_halt_days(df)
    keep = [c for c in DAILY_COLS if c in df.columns]
    return df.select(keep) if keep else pl.DataFrame()


def normalize_adj_factors(data, source: str = "tickflow") -> pl.DataFrame:  # noqa: ARG001
    df = to_polars(data)
    if df.is_empty():
        return df
    rename_map = {
        "timestamp": "trade_date",
        "date": "trade_date",
        "adj_factor": "ex_factor",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})
    if "trade_date" in df.columns:
        if df.schema["trade_date"] in {pl.Int64, pl.Int32, pl.UInt64, pl.UInt32, pl.Float64, pl.Float32}:
            # 毫秒时间戳 → 北京墙钟日期 (直接 from_epoch().dt.date() 是 UTC 日期,
            # 除权事件时间戳为北京零点 = UTC 前一日 16:00, 会整体早一天)。
            df = df.with_columns(
                pl.from_epoch(pl.col("trade_date").cast(pl.Int64), time_unit="ms")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone("Asia/Shanghai")
                .dt.replace_time_zone(None)
                .dt.date()
                .alias("trade_date")
            )
        else:
            df = df.with_columns(pl.col("trade_date").cast(pl.Date, strict=False))
    if "ex_factor" in df.columns:
        df = df.with_columns(pl.col("ex_factor").cast(pl.Float64, strict=False))
    keep = [c for c in ADJ_FACTOR_COLS if c in df.columns]
    if len(keep) != len(ADJ_FACTOR_COLS):
        return pl.DataFrame()
    return (
        df.select(keep)
        .drop_nulls()
        .filter(pl.col("ex_factor").is_finite() & (pl.col("ex_factor") > 0))
    )


def normalize_instruments(rows: list[dict], asset_type: str, source: str = "tickflow") -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    out: list[dict] = []
    for item in rows:
        symbol = item.get("symbol")
        if not symbol:
            continue
        out.append({
            "symbol": str(symbol),
            "name": item.get("name") or str(symbol),
            "code": item.get("code") or str(symbol).split(".")[0],
            "exchange": item.get("exchange"),
            "asset_type": asset_type,
            "source": source,
        })
    if not out:
        return pl.DataFrame()
    return pl.DataFrame(out).select(INSTRUMENT_COLS).unique(subset=["symbol"], keep="last").sort("symbol")


def _tick_records(data: Any, default_symbol: str | None = None) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, pl.DataFrame):
        return [
            {**row, **({"symbol": default_symbol} if default_symbol and not row.get("symbol") else {})}
            for row in data.to_dicts()
        ]
    if hasattr(data, "reset_index"):
        return _tick_records(data.reset_index().to_dict("records"), default_symbol)
    if isinstance(data, (list, tuple)):
        rows: list[dict[str, Any]] = []
        for item in data:
            rows.extend(_tick_records(item, default_symbol))
        return rows
    if not isinstance(data, dict):
        return []
    if data.get("__bigqmt_type__") == "DataFrame":
        records = data.get("records") or []
        if isinstance(records, dict):
            try:
                records = pl.DataFrame(records).to_dicts()
            except Exception:  # noqa: BLE001
                return []
        return _tick_records(records, default_symbol)
    if isinstance(data.get("records"), list):
        return _tick_records(data["records"], default_symbol)
    known = {
        "symbol", "stock_code", "stockCode", "code", "time", "datetime",
        "timestamp", "lastPrice", "last_price", "price",
    }
    if known.intersection(data):
        row = dict(data)
        if default_symbol and not any(row.get(key) for key in ("symbol", "stock_code", "stockCode", "code")):
            row["symbol"] = default_symbol
        return [row]
    rows = []
    for symbol, values in data.items():
        rows.extend(_tick_records(values, str(symbol)))
    return rows


def _field(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    lowered = {str(key).replace("_", "").lower(): value for key, value in row.items()}
    for name in names:
        value = lowered.get(name.replace("_", "").lower())
        if value not in (None, ""):
            return value
    return None


def _tick_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number >= 1_000_000_000_000:
            parsed = datetime.fromtimestamp(number / 1000, timezone.utc)
        elif number >= 1_000_000_000:
            parsed = datetime.fromtimestamp(number, timezone.utc)
        else:
            return None
    else:
        text = str(value or "").strip()
        if not text:
            return None
        parsed = None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
        for fmt in (
            "%Y%m%d%H%M%S.%f", "%Y%m%d%H%M%S%f", "%Y%m%d%H%M%S",
            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
        ):
            if parsed is not None:
                break
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
        if parsed is None:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_CN_TZ).replace(tzinfo=None)
    return parsed


def _finite_float(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and result not in {float("inf"), float("-inf")} else default


def _positive_float(value: Any, default: float | None = None) -> float | None:
    result = _finite_float(value, default)
    return result if result is not None and result > 0 else default


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _tick_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "suspended"}


def normalize_tick(
    data: Any,
    *,
    default_symbol: str | None = None,
    source: str,
) -> pl.DataFrame:
    """Normalize vendor tick responses while preserving source event order."""
    normalized: list[dict[str, Any]] = []
    for source_order, row in enumerate(_tick_records(data, default_symbol)):
        symbol = _field(row, "symbol", "stock_code", "stockCode", "code") or default_symbol
        timestamp = _tick_datetime(_field(row, "datetime", "time", "timestamp", "timetag"))
        price = _finite_float(_field(row, "last_price", "lastPrice", "price", "close"))
        if not symbol or timestamp is None or price is None or price <= 0:
            continue
        open_price = _positive_float(_field(row, "open"), price)
        high = _positive_float(_field(row, "high"), price)
        low = _positive_float(_field(row, "low"), price)
        normalized.append({
            "symbol": str(symbol).strip().upper(),
            "datetime": timestamp,
            "last_price": price,
            "close": price,
            "open": open_price,
            "high": high,
            "low": low,
            "prev_close": _positive_float(_field(row, "prev_close", "lastClose", "preClose")),
            "volume": _finite_float(_field(row, "volume", "vol"), 0.0),
            "amount": _finite_float(_field(row, "amount", "turnover"), 0.0),
            "limit_up": _positive_float(_field(row, "limit_up", "upperLimit", "upStopPrice")),
            "limit_down": _positive_float(_field(row, "limit_down", "lowerLimit", "downStopPrice")),
            "suspended": _tick_bool(_field(row, "suspended", "isSuspended")),
            "source": source,
            "sequence": _optional_text(_field(row, "sequence", "seq", "tickSequence")),
            "trade_id": _optional_text(_field(row, "trade_id", "tradeId", "transaction_id")),
            "source_order": source_order,
        })
    if not normalized:
        return pl.DataFrame()
    return pl.DataFrame(normalized).select(TICK_COLS).with_columns(
        pl.col("symbol", "source", "sequence", "trade_id").cast(pl.String, strict=False),
        pl.col("datetime").cast(pl.Datetime("us"), strict=False),
        pl.col(
            "last_price", "close", "open", "high", "low", "prev_close",
            "volume", "amount", "limit_up", "limit_down",
        ).cast(pl.Float64, strict=False),
        pl.col("suspended").cast(pl.Boolean, strict=False),
        pl.col("source_order").cast(pl.Int64, strict=False),
    )
