"""BaoStock stock-basic lifecycle supplement.

BaoStock query_stock_basic exposes listing and delisting dates. Those dates are
useful lifecycle evidence for recent full-A research windows, but they do not
include delisting decision, delisting-period, or reason fields. This collector
therefore publishes partial lifecycle events and preserves existing exchange/PIT
rows from other sources.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from app.data_providers.baostock_provider import BaoStockProvider
from app.plugins.pit_history.storage import (
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    PARSER_VERSION as PIT_HISTORY_PARSER_VERSION,
    SOURCE as PIT_HISTORY_SOURCE,
    normalize_instrument_lifecycle_events,
    publish_history_table,
    read_history_table,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)

SOURCE = "baostock"
PARSER_VERSION = "baostock_instrument_lifecycle_v1"
DEFAULT_LOOKBACK_YEARS = 5


def lookback_start(end_date: date, years: int = DEFAULT_LOOKBACK_YEARS) -> date:
    if years <= 0:
        raise ValueError("years must be positive")
    try:
        return end_date.replace(year=end_date.year - years)
    except ValueError:
        return end_date.replace(month=2, day=28, year=end_date.year - years)


def _date_value(value: object) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _overlaps_window(row: dict[str, Any], *, start_date: date, end_date: date) -> bool:
    listed = _date_value(row.get("list_date") or row.get("listing_date"))
    delisted = _date_value(row.get("delist_date"))
    if listed is not None and listed > end_date:
        return False
    if delisted is not None and delisted < start_date:
        return False
    return listed is not None or delisted is not None


def _lifecycle_input_rows(
    stock_basic: pl.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in stock_basic.to_dicts() if not stock_basic.is_empty() else []:
        if not _overlaps_window(row, start_date=start_date, end_date=end_date):
            continue
        rows.append({
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "exchange": row.get("exchange"),
            "listed_date": row.get("list_date") or row.get("listing_date"),
            "delisted_date": row.get("delist_date"),
        })
    return rows


def _merge_lifecycle(existing: pl.DataFrame, supplement: pl.DataFrame) -> pl.DataFrame:
    if supplement.is_empty():
        return existing
    frames: list[pl.DataFrame] = []
    if not existing.is_empty():
        if "source" in existing.columns:
            frames.append(existing.filter(pl.col("source") != SOURCE))
        else:
            frames.append(existing)
    frames.append(supplement)
    merged = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
    return (
        merged.unique(subset=["symbol", "event_type", "event_date", "source"], keep="last")
        .sort(["symbol", "event_date", "event_type", "source"])
    )


class BaoStockInstrumentLifecycleCollector:
    def __init__(
        self,
        data_dir: Path,
        bs_module: Any | None = None,
        *,
        provider: BaoStockProvider | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.provider = provider or BaoStockProvider(bs_module=bs_module)

    def collect_stock_lifecycle(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        years: int = DEFAULT_LOOKBACK_YEARS,
    ) -> dict[str, Any]:
        end_date = end_date or date.today()
        start_date = start_date or lookback_start(end_date, years)
        if start_date > end_date:
            raise ValueError("start_date must be on or before end_date")

        stock_basic = self.provider.get_instruments("stock")
        raw_rows = stock_basic.to_dicts() if not stock_basic.is_empty() else []
        lifecycle_rows = _lifecycle_input_rows(
            stock_basic,
            start_date=start_date,
            end_date=end_date,
        )
        frame = normalize_instrument_lifecycle_events(
            lifecycle_rows,
            source=SOURCE,
            provenance="historical_event",
        )
        existing = read_history_table(self.data_dir, INSTRUMENT_LIFECYCLE_EVENTS_TABLE)
        merged = _merge_lifecycle(existing, frame)
        total_rows = (
            publish_history_table(self.data_dir, INSTRUMENT_LIFECYCLE_EVENTS_TABLE, merged)
            if not merged.is_empty()
            else 0
        )

        logical_snapshot = (
            f"baostock_lifecycle_{start_date.isoformat()}_{end_date.isoformat()}"
        )
        _, source_hash = archive_source_payload(
            self.data_dir,
            PIT_HISTORY_SOURCE,
            INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
            logical_snapshot,
            SOURCE,
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "raw_rows": raw_rows,
                "lifecycle_rows": lifecycle_rows,
            },
            parser_version=PARSER_VERSION,
        )
        update_ingestion_manifest(
            self.data_dir,
            PIT_HISTORY_SOURCE,
            INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
            logical_snapshot,
            status="published" if frame.height else "valid_empty",
            parser_version=PARSER_VERSION,
            base_parser_version=PIT_HISTORY_PARSER_VERSION,
            schema_version=1,
            source_content_hash=source_hash,
            content_hash=stable_content_hash(frame.to_dicts()) if frame.height else None,
            published_rows=frame.height,
            total_table_rows=total_rows,
            provenance="historical_event",
            upstream_source=SOURCE,
            empty_reason=None if frame.height else "source_empty",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return {
            "source_rows": len(raw_rows),
            "candidate_rows": len(lifecycle_rows),
            "published_rows": frame.height,
            "total_table_rows": total_rows,
            "start_date": start_date,
            "end_date": end_date,
        }
