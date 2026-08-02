"""Published table contracts for HiThink supplemental snapshots.

These tables are intentionally not registered as primary TickFlow market data.
They are auditable reference facts that can be consumed by backtests only when
the caller accepts their provenance: snapshot_frozen or observed.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from app.services.ingestion_manifest import stable_content_hash


SOURCE = "hithink"
INDEX_CONSTITUENTS_TABLE = "index_constituents_snapshots"
THS_SECTOR_CONSTITUENTS_TABLE = "ths_sector_constituents_snapshots"
INSTRUMENT_LIFECYCLE_TABLE = "instrument_lifecycle_observed"
PARSER_VERSION = "hithink_snapshot_v1"


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null", "<na>"} else text


def _table_root(data_dir: Path, table: str) -> Path:
    return Path(data_dir) / "pit_reference" / SOURCE / table


def partition_path(data_dir: Path, table: str, snapshot_date: date) -> Path:
    return _table_root(data_dir, table) / f"snapshot_date={snapshot_date.isoformat()}" / "part.parquet"


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("item") or []
    if not isinstance(items, list):
        raise ValueError("HiThink payload item must be a list")
    return [item for item in items if isinstance(item, dict)]


def normalize_index_constituents(
    *,
    index_symbol: str,
    payload: dict[str, Any],
    snapshot_date: date,
    index_name: str = "",
) -> pl.DataFrame:
    items = _items(payload)
    source_timestamp = payload.get("timestamp")
    snapshot_hash = stable_content_hash({
        "dataset": INDEX_CONSTITUENTS_TABLE,
        "index_symbol": index_symbol,
        "source_timestamp": source_timestamp,
        "items": items,
    })
    rows = []
    seen: set[str] = set()
    for item in items:
        member_symbol = _text(item.get("thscode")).upper()
        if not member_symbol or member_symbol in seen:
            continue
        seen.add(member_symbol)
        rows.append({
            "index_symbol": _text(index_symbol).upper(),
            "index_name": _text(index_name),
            "member_symbol": member_symbol,
            "member_code": _text(item.get("ticker")),
            "member_name": _text(item.get("name")),
            "snapshot_date": snapshot_date,
            "source_timestamp": source_timestamp,
            "source": SOURCE,
            "provenance": "snapshot_frozen",
            "snapshot_hash": snapshot_hash,
        })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).select([
        pl.col("index_symbol").cast(pl.String),
        pl.col("index_name").cast(pl.String),
        pl.col("member_symbol").cast(pl.String),
        pl.col("member_code").cast(pl.String),
        pl.col("member_name").cast(pl.String),
        pl.col("snapshot_date").cast(pl.Date),
        pl.col("source_timestamp").cast(pl.Int64, strict=False),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("snapshot_hash").cast(pl.String),
    ]).sort(["index_symbol", "member_symbol"])


def normalize_sector_constituents(
    *,
    sector_symbol: str,
    sector_name: str,
    sector_tag: str,
    payload: dict[str, Any],
    snapshot_date: date,
) -> pl.DataFrame:
    items = _items(payload)
    source_timestamp = payload.get("timestamp")
    snapshot_hash = stable_content_hash({
        "dataset": THS_SECTOR_CONSTITUENTS_TABLE,
        "sector_symbol": sector_symbol,
        "source_timestamp": source_timestamp,
        "items": items,
    })
    rows = []
    seen: set[str] = set()
    for item in items:
        member_symbol = _text(item.get("thscode")).upper()
        if not member_symbol or member_symbol in seen:
            continue
        seen.add(member_symbol)
        rows.append({
            "sector_symbol": _text(sector_symbol).upper(),
            "sector_name": _text(sector_name),
            "sector_tag": _text(sector_tag).lower(),
            "member_symbol": member_symbol,
            "member_code": _text(item.get("ticker")),
            "member_name": _text(item.get("name")),
            "snapshot_date": snapshot_date,
            "source_timestamp": source_timestamp,
            "source": SOURCE,
            "provenance": "snapshot_frozen",
            "snapshot_hash": snapshot_hash,
        })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).select([
        pl.col("sector_symbol").cast(pl.String),
        pl.col("sector_name").cast(pl.String),
        pl.col("sector_tag").cast(pl.String),
        pl.col("member_symbol").cast(pl.String),
        pl.col("member_code").cast(pl.String),
        pl.col("member_name").cast(pl.String),
        pl.col("snapshot_date").cast(pl.Date),
        pl.col("source_timestamp").cast(pl.Int64, strict=False),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("snapshot_hash").cast(pl.String),
    ]).sort(["sector_tag", "sector_symbol", "member_symbol"])


def normalize_lifecycle_observed(
    *,
    current_tickers: Iterable[dict[str, Any]],
    daily_rows: pl.DataFrame,
    observed_as_of: date,
    source_timestamp: int | None = None,
) -> pl.DataFrame:
    current: dict[str, dict[str, Any]] = {}
    for item in current_tickers:
        symbol = _text(item.get("thscode")).upper()
        if not symbol:
            continue
        current[symbol] = {
            "symbol": symbol,
            "name": _text(item.get("name")),
            "exchange": _text(item.get("exchange")),
            "asset_type": _text(item.get("asset_type")),
        }

    history: dict[str, tuple[date | None, date | None]] = {}
    if not daily_rows.is_empty():
        symbol_col = "symbol" if "symbol" in daily_rows.columns else "thscode"
        date_col = "date" if "date" in daily_rows.columns else "trade_date"
        required = {symbol_col, date_col}
        if not required.issubset(set(daily_rows.columns)):
            raise ValueError("daily_rows must include symbol/thscode and date/trade_date")
        normalized = daily_rows.select([
            pl.col(symbol_col).cast(pl.String).str.to_uppercase().alias("symbol"),
            pl.col(date_col).cast(pl.Date, strict=False).alias("date"),
        ]).drop_nulls()
        if not normalized.is_empty():
            grouped = normalized.group_by("symbol").agg([
                pl.col("date").min().alias("first_trade_date"),
                pl.col("date").max().alias("last_trade_date"),
            ])
            history = {
                row["symbol"]: (row["first_trade_date"], row["last_trade_date"])
                for row in grouped.iter_rows(named=True)
            }

    rows = []
    for symbol in sorted(set(current) | set(history)):
        ticker = current.get(symbol, {})
        first_trade_date, last_trade_date = history.get(symbol, (None, None))
        is_current = symbol in current
        has_history = symbol in history
        rows.append({
            "symbol": symbol,
            "name": _text(ticker.get("name")),
            "exchange": _text(ticker.get("exchange")),
            "asset_type": _text(ticker.get("asset_type") or "a-share"),
            "first_trade_date": first_trade_date,
            "last_trade_date": last_trade_date,
            "is_currently_listed": is_current,
            "observed_delisted": bool(has_history and not is_current),
            "status_confidence": (
                "current_snapshot_with_history"
                if is_current and has_history
                else "current_snapshot"
                if is_current
                else "observed_history_only"
            ),
            "source": SOURCE,
            "observed_as_of": observed_as_of,
            "source_timestamp": source_timestamp,
        })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).select([
        pl.col("symbol").cast(pl.String),
        pl.col("name").cast(pl.String),
        pl.col("exchange").cast(pl.String),
        pl.col("asset_type").cast(pl.String),
        pl.col("first_trade_date").cast(pl.Date),
        pl.col("last_trade_date").cast(pl.Date),
        pl.col("is_currently_listed").cast(pl.Boolean),
        pl.col("observed_delisted").cast(pl.Boolean),
        pl.col("status_confidence").cast(pl.String),
        pl.col("source").cast(pl.String),
        pl.col("observed_as_of").cast(pl.Date),
        pl.col("source_timestamp").cast(pl.Int64, strict=False),
    ]).sort("symbol")


def publish_snapshot(data_dir: Path, table: str, snapshot_date: date, frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    path = partition_path(data_dir, table, snapshot_date)
    _atomic_write_parquet(frame, path)
    return frame.height


def read_latest_snapshot(data_dir: Path, table: str) -> pl.DataFrame:
    root = _table_root(data_dir, table)
    partitions = sorted(root.glob("snapshot_date=*/part.parquet"))
    if not partitions:
        return pl.DataFrame()
    return pl.read_parquet(partitions[-1])

