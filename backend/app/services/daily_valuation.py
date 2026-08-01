"""Persist point-in-time daily valuation metrics derived from local data."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
from typing import Any
import uuid

import polars as pl

from app.services.source_snapshot import capture_source_snapshot


TABLE_NAME = "valuation_daily"
SCHEMA_VERSION = 1
_BATCH_DAYS = 32

VALUATION_DAILY_SCHEMA: dict[str, pl.DataType] = {
    "symbol": pl.String,
    "date": pl.Date,
    "raw_close": pl.Float64,
    "total_shares": pl.Float64,
    "float_shares": pl.Float64,
    "market_cap": pl.Float64,
    "float_market_cap": pl.Float64,
    "float_share_ratio": pl.Float64,
    "income_announce_date": pl.Date,
    "income_period_end": pl.Date,
    "net_income_ttm": pl.Float64,
    "revenue_ttm": pl.Float64,
    "balance_announce_date": pl.Date,
    "balance_period_end": pl.Date,
    "equity_attributable": pl.Float64,
    "cash_flow_announce_date": pl.Date,
    "cash_flow_period_end": pl.Date,
    "operating_cash_flow_ttm": pl.Float64,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "pcf_ttm": pl.Float64,
}


def _empty_frame(schema: Mapping[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=dict(schema))


def _as_date_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    dtype = frame.schema[column]
    if dtype == pl.String:
        return pl.col(column).str.to_date(strict=False)
    return pl.col(column).cast(pl.Date, strict=False)


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _normalize_financial_events(
    frame: pl.DataFrame,
    value_columns: Iterable[str],
) -> pl.DataFrame:
    value_columns = list(value_columns)
    required = {"symbol", "period_end", "announce_date", *value_columns}
    if frame.is_empty() or not required <= set(frame.columns):
        return pl.DataFrame()
    normalized = (
        frame
        .select(
            pl.col("symbol").cast(pl.String),
            _as_date_expr(frame, "period_end").alias("period_end"),
            _as_date_expr(frame, "announce_date").alias("announce_date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).alias(column)
                for column in value_columns
            ],
        )
        .filter(
            pl.col("symbol").is_not_null()
            & pl.col("period_end").is_not_null()
            & pl.col("announce_date").is_not_null()
            & (pl.col("period_end") <= pl.col("announce_date"))
        )
        .unique(maintain_order=False)
    )
    key = ["symbol", "period_end", "announce_date"]
    conflicts = (
        normalized.group_by(key).len().filter(pl.col("len") > 1).sort(key)
    )
    if not conflicts.is_empty():
        sample = ", ".join(
            "/".join(str(row[column]) for column in key)
            for row in conflicts.head(8).iter_rows(named=True)
        )
        raise ValueError(
            "财务表存在同键不同值且缺少可验证的 revision/update 元数据；"
            f"拒绝按文件顺序选择记录: {sample}"
        )
    return normalized.sort(["symbol", "announce_date", "period_end"])


def _ttm_value(
    values_by_period: dict[date, dict[str, float | None]],
    period: date,
    column: str,
) -> float | None:
    current = values_by_period[period].get(column)
    if current is None:
        return None
    if (period.month, period.day) == (12, 31):
        return current
    if (period.month, period.day) not in {(3, 31), (6, 30), (9, 30)}:
        return None
    previous_annual = values_by_period.get(date(period.year - 1, 12, 31), {}).get(column)
    previous_same = values_by_period.get(
        date(period.year - 1, period.month, period.day),
        {},
    ).get(column)
    if previous_annual is None or previous_same is None:
        return None
    return current + previous_annual - previous_same


def build_ttm_events(
    frame: pl.DataFrame,
    value_map: Mapping[str, str],
    *,
    prefix: str,
) -> pl.DataFrame:
    """Build PIT TTM snapshots from cumulative A-share statement rows."""
    output_schema = {
        "symbol": pl.String,
        f"{prefix}_announce_date": pl.Date,
        f"{prefix}_period_end": pl.Date,
        **{target: pl.Float64 for target in value_map.values()},
    }
    normalized = _normalize_financial_events(frame, value_map)
    if normalized.is_empty():
        return _empty_frame(output_schema)

    output: list[dict[str, Any]] = []
    current_symbol: str | None = None
    symbol_rows: list[dict[str, Any]] = []

    def flush_symbol(symbol: str, rows: list[dict[str, Any]]) -> None:
        values_by_period: dict[date, dict[str, float | None]] = {}
        latest_period: date | None = None
        index = 0
        while index < len(rows):
            announcement = rows[index]["announce_date"]
            while index < len(rows) and rows[index]["announce_date"] == announcement:
                row = rows[index]
                period = row["period_end"]
                values_by_period[period] = {
                    column: _finite(row.get(column)) for column in value_map
                }
                latest_period = period if latest_period is None else max(latest_period, period)
                index += 1
            if latest_period is None:
                continue
            item: dict[str, Any] = {
                "symbol": symbol,
                f"{prefix}_announce_date": announcement,
                f"{prefix}_period_end": latest_period,
            }
            for source, target in value_map.items():
                item[target] = _ttm_value(values_by_period, latest_period, source)
            output.append(item)

    for row in normalized.iter_rows(named=True):
        symbol = str(row["symbol"])
        if current_symbol is not None and symbol != current_symbol:
            flush_symbol(current_symbol, symbol_rows)
            symbol_rows = []
        current_symbol = symbol
        symbol_rows.append(row)
    if current_symbol is not None:
        flush_symbol(current_symbol, symbol_rows)

    return pl.DataFrame(output, schema=output_schema, strict=False).sort(
        ["symbol", f"{prefix}_announce_date"]
    )


def _build_latest_events(
    frame: pl.DataFrame,
    value_map: Mapping[str, str],
    *,
    prefix: str,
) -> pl.DataFrame:
    output_schema = {
        "symbol": pl.String,
        f"{prefix}_announce_date": pl.Date,
        f"{prefix}_period_end": pl.Date,
        **{target: pl.Float64 for target in value_map.values()},
    }
    normalized = _normalize_financial_events(frame, value_map)
    if normalized.is_empty():
        return _empty_frame(output_schema)

    output: list[dict[str, Any]] = []
    for symbol_frame in normalized.partition_by("symbol", maintain_order=True):
        symbol = str(symbol_frame["symbol"][0])
        values_by_period: dict[date, dict[str, float | None]] = {}
        latest_period: date | None = None
        rows = symbol_frame.to_dicts()
        index = 0
        while index < len(rows):
            announcement = rows[index]["announce_date"]
            while index < len(rows) and rows[index]["announce_date"] == announcement:
                row = rows[index]
                period = row["period_end"]
                values_by_period[period] = {
                    column: _finite(row.get(column)) for column in value_map
                }
                latest_period = period if latest_period is None else max(latest_period, period)
                index += 1
            if latest_period is None:
                continue
            item: dict[str, Any] = {
                "symbol": symbol,
                f"{prefix}_announce_date": announcement,
                f"{prefix}_period_end": latest_period,
            }
            for source, target in value_map.items():
                item[target] = values_by_period[latest_period].get(source)
            output.append(item)

    return pl.DataFrame(output, schema=output_schema, strict=False).sort(
        ["symbol", f"{prefix}_announce_date"]
    )


def _read_financial(data_dir: Path, table: str) -> pl.DataFrame:
    path = data_dir / "financials" / table / "part.parquet"
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _join_events(base: pl.DataFrame, events: pl.DataFrame, prefix: str) -> pl.DataFrame:
    if events.is_empty():
        return base
    return (
        base
        .sort(["symbol", "date"])
        .join_asof(
            events,
            left_on="date",
            right_on=f"{prefix}_announce_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
    )


def _positive_ratio(numerator: str, denominator: str, output: str) -> pl.Expr:
    return (
        pl.when(
            pl.col(numerator).is_finite()
            & (pl.col(numerator) > 0)
            & pl.col(denominator).is_finite()
            & (pl.col(denominator) > 0)
        )
        .then(pl.col(numerator) / pl.col(denominator))
        .otherwise(None)
        .alias(output)
    )


def _valuation_rows(base: pl.DataFrame, event_frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    required = {"symbol", "date", "raw_close", "total_shares", "float_shares"}
    if base.is_empty() or not required <= set(base.columns):
        return _empty_frame(VALUATION_DAILY_SCHEMA)
    result = base.select(
        pl.col("symbol").cast(pl.String),
        pl.col("date").cast(pl.Date, strict=False),
        pl.col("raw_close").cast(pl.Float64, strict=False),
        pl.col("total_shares").cast(pl.Float64, strict=False),
        pl.col("float_shares").cast(pl.Float64, strict=False),
    )
    for prefix, events in event_frames.items():
        result = _join_events(result, events, prefix)
    result = result.with_columns(
        _positive_ratio("raw_close", "raw_close", "_valid_price"),
        _positive_ratio("total_shares", "total_shares", "_valid_total_shares"),
        _positive_ratio("float_shares", "float_shares", "_valid_float_shares"),
    ).with_columns(
        pl.when(pl.col("_valid_price").is_not_null() & pl.col("_valid_total_shares").is_not_null())
        .then(pl.col("raw_close") * pl.col("total_shares"))
        .otherwise(None)
        .alias("market_cap"),
        pl.when(pl.col("_valid_price").is_not_null() & pl.col("_valid_float_shares").is_not_null())
        .then(pl.col("raw_close") * pl.col("float_shares"))
        .otherwise(None)
        .alias("float_market_cap"),
        _positive_ratio("float_shares", "total_shares", "float_share_ratio"),
    ).drop("_valid_price", "_valid_total_shares", "_valid_float_shares")

    for column, dtype in VALUATION_DAILY_SCHEMA.items():
        if column not in result.columns:
            result = result.with_columns(pl.lit(None, dtype=dtype).alias(column))
    result = result.with_columns(
        _positive_ratio("market_cap", "net_income_ttm", "pe_ttm"),
        _positive_ratio("market_cap", "equity_attributable", "pb"),
        _positive_ratio("market_cap", "revenue_ttm", "ps_ttm"),
        _positive_ratio("market_cap", "operating_cash_flow_ttm", "pcf_ttm"),
    )
    return result.select(
        [pl.col(column).cast(dtype, strict=False) for column, dtype in VALUATION_DAILY_SCHEMA.items()]
    )


def _partition_dates(directory: Path) -> list[date]:
    dates: list[date] = []
    if not directory.exists():
        return dates
    for child in directory.glob("date=*"):
        try:
            dates.append(date.fromisoformat(child.name.removeprefix("date=")))
        except ValueError:
            continue
    return sorted(dates)


def load_daily_valuation_metadata(data_dir: Path) -> dict[str, Any]:
    path = Path(data_dir) / TABLE_NAME / "metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _replace_directory(staging: Path, target: Path) -> None:
    if not target.exists():
        os.replace(staging, target)
        return
    backup = target.with_name(f".{target.name}.{uuid.uuid4().hex}.backup")
    os.replace(target, backup)
    try:
        os.replace(staging, target)
    except Exception:
        os.replace(backup, target)
        raise
    shutil.rmtree(backup)


def build_daily_valuation(
    data_dir: Path,
    dates: Iterable[date] | None = None,
) -> dict[str, int]:
    """Build the complete table, or atomically upsert selected trading days."""
    data_dir = Path(data_dir)
    prior_metadata = load_daily_valuation_metadata(data_dir)
    enriched_dir = data_dir / "kline_daily_enriched"
    available_dates = _partition_dates(enriched_dir)
    requested = None if dates is None else set(dates)
    selected_dates = [day for day in available_dates if requested is None or day in requested]
    if not selected_dates:
        return {"rows": 0, "trading_days": 0}

    income_events = build_ttm_events(
        _read_financial(data_dir, "income"),
        {
            "net_income_attributable": "net_income_ttm",
            "revenue": "revenue_ttm",
        },
        prefix="income",
    )
    balance_events = _build_latest_events(
        _read_financial(data_dir, "balance_sheet"),
        {"equity_attributable": "equity_attributable"},
        prefix="balance",
    )
    cash_flow_events = build_ttm_events(
        _read_financial(data_dir, "cash_flow"),
        {"net_operating_cash_flow": "operating_cash_flow_ttm"},
        prefix="cash_flow",
    )
    event_frames = {
        "income": income_events,
        "balance": balance_events,
        "cash_flow": cash_flow_events,
    }

    target = data_dir / TABLE_NAME
    full_rebuild = requested is None
    output_dir = (
        data_dir / f".{TABLE_NAME}.{uuid.uuid4().hex}.tmp"
        if full_rebuild
        else target
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    symbols_seen: set[str] = set()
    previous_rows = 0
    existing_metadata: dict[str, Any] = {}
    if not full_rebuild:
        metadata_path = target / "metadata.json"
        if metadata_path.exists():
            try:
                existing_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                existing_metadata = {}
        for day in selected_dates:
            existing_path = target / f"date={day.isoformat()}" / "part.parquet"
            if existing_path.exists():
                previous_rows += pl.scan_parquet(existing_path).select(pl.len()).collect().item()
    try:
        for offset in range(0, len(selected_dates), _BATCH_DAYS):
            batch = selected_dates[offset : offset + _BATCH_DAYS]
            frames = [
                pl.read_parquet(enriched_dir / f"date={day.isoformat()}" / "part.parquet")
                for day in batch
            ]
            values = _valuation_rows(pl.concat(frames, how="diagonal_relaxed"), event_frames)
            symbols_seen.update(values["symbol"].drop_nulls().to_list())
            for key, partition in values.partition_by("date", as_dict=True).items():
                day = key[0] if isinstance(key, tuple) else key
                path = output_dir / f"date={day.isoformat()}" / "part.parquet"
                _atomic_write_parquet(partition.sort("symbol"), path)
                rows_written += partition.height

        if rows_written == 0:
            raise ValueError("valuation_daily 无可写入记录，请先生成含历史股本的 enriched 数据")
        stored_dates = _partition_dates(output_dir)
        total_rows = (
            rows_written
            if full_rebuild
            else int(existing_metadata.get("rows") or 0) - previous_rows + rows_written
        )
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows": total_rows,
            "symbols": max(int(existing_metadata.get("symbols") or 0), len(symbols_seen)),
            "trading_days": len(stored_dates),
            "earliest_date": stored_dates[0].isoformat(),
            "latest_date": stored_dates[-1].isoformat(),
            "source_tables": [
                "kline_daily_enriched",
                "financials/shares",
                "financials/income",
                "financials/balance_sheet",
                "financials/cash_flow",
            ],
            "source_snapshots": capture_source_snapshot(
                data_dir,
                [
                    "kline_daily_enriched",
                    "financials/shares",
                    "financials/income",
                    "financials/balance_sheet",
                    "financials/cash_flow",
                ],
                previous=prior_metadata.get("source_snapshots"),
            ),
        }
        metadata_path = output_dir / "metadata.json"
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        if full_rebuild:
            _replace_directory(output_dir, target)
    except Exception:
        if full_rebuild and output_dir.exists():
            shutil.rmtree(output_dir)
        raise
    return {"rows": rows_written, "trading_days": len(selected_dates)}


def sync_missing_daily_valuation(data_dir: Path) -> dict[str, int]:
    data_dir = Path(data_dir)
    source_dates = set(_partition_dates(data_dir / "kline_daily_enriched"))
    target_dates = set(_partition_dates(data_dir / TABLE_NAME))
    return build_daily_valuation(data_dir, sorted(source_dates - target_dates))


def assert_daily_valuation_coverage(data_dir: Path, start: date, end: date) -> None:
    data_dir = Path(data_dir)
    enriched_dates = _partition_dates(data_dir / "kline_daily_enriched")
    valuation_dates = set(_partition_dates(data_dir / TABLE_NAME))
    previous_dates = [day for day in enriched_dates if day < start]
    required = {
        day for day in enriched_dates
        if start <= day <= end
    }
    if previous_dates:
        required.add(previous_dates[-1])
    missing = sorted(required - valuation_dates)
    if not required or missing:
        sample = ", ".join(day.isoformat() for day in missing[:5]) or "无可用估值日期"
        raise ValueError(
            "绩优小市值回测需要完整的 valuation_daily 日度估值表；"
            f"缺失: {sample}。请先重建日度估值。"
        )


def load_latest_market_caps(
    data_dir: Path,
    symbols: Iterable[str],
    cutoff: date,
) -> dict[str, float]:
    symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
    directory = Path(data_dir) / TABLE_NAME
    if not symbols or not directory.exists():
        return {}
    try:
        frame = (
            pl.scan_parquet(str(directory / "**" / "*.parquet"), missing_columns="insert")
            .filter(pl.col("symbol").is_in(symbols) & (pl.col("date") <= cutoff))
            .select("symbol", "date", "market_cap")
            .filter(pl.col("market_cap").is_finite() & (pl.col("market_cap") > 0))
            .sort(["symbol", "date"])
            .group_by("symbol", maintain_order=True)
            .tail(1)
            .collect()
        )
    except Exception:
        return {}
    return {
        str(symbol): float(market_cap)
        for symbol, market_cap in frame.select("symbol", "market_cap").iter_rows()
    }
