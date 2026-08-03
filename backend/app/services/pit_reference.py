"""PIT reference status and snapshot maintenance.

The data page and daily pipeline use BaoStock candidate snapshots and lifecycle
events. Historical PIT builders and the optional HiThink collector remain
available as explicit offline/manual tools.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from app.plugins.hithink.client import HiThinkAuthError, HiThinkClient
from app.plugins.hithink.collector import HiThinkSnapshotCollector
from app.plugins.hithink.storage import (
    INDEX_CONSTITUENTS_TABLE,
    INSTRUMENT_LIFECYCLE_TABLE,
    THS_SECTOR_CONSTITUENTS_TABLE,
)
from app.plugins.baostock.index_candidates import (
    BaoStockIndexCandidateCollector,
    INDEX_CONSTITUENT_CANDIDATES_TABLE,
    SOURCE as BAOSTOCK_SOURCE,
)
from app.plugins.baostock.instrument_lifecycle import (
    DEFAULT_LOOKBACK_YEARS as BAOSTOCK_LIFECYCLE_LOOKBACK_YEARS,
    BaoStockInstrumentLifecycleCollector,
)
from app.plugins.pit_history.storage import (
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    SOURCE as PIT_HISTORY_SOURCE,
    summarize_lifecycle_completeness,
    table_path,
)

logger = logging.getLogger(__name__)

DEFAULT_INDEX_NAMES = {"000300.SH": "沪深300"}
DEFAULT_SECTOR_TAGS = ("industry",)

_HISTORY_TABLES: dict[str, dict[str, Any]] = {
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE: {
        "label": "BaoStock 股票生命周期",
        "symbol_col": "symbol",
        "start_col": "event_date",
        "end_col": None,
        "source": BAOSTOCK_SOURCE,
    },
}

_SNAPSHOT_TABLES: dict[str, dict[str, Any]] = {
    INDEX_CONSTITUENT_CANDIDATES_TABLE: {
        "label": "BaoStock 沪深300候选快照",
        "symbol_col": "member_symbol",
        "source": BAOSTOCK_SOURCE,
    },
}


def _date_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _latest_manifest(data_dir: Path, source: str, table: str) -> dict[str, Any] | None:
    root = Path(data_dir) / "ext_data" / "_ingestion" / source / table
    manifests = sorted(root.glob("*.json"))
    if not manifests:
        return None
    path = manifests[-1]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("invalid ingestion manifest: %s", path)
        return {"logical_snapshot": path.stem, "status": "invalid_manifest"}
    if not isinstance(payload, dict):
        return {"logical_snapshot": path.stem, "status": "invalid_manifest"}
    return {
        "logical_snapshot": payload.get("logical_snapshot") or path.stem,
        "status": payload.get("status"),
        "published_rows": int(payload.get("published_rows") or 0),
        "provenance": payload.get("provenance"),
        "empty_reason": payload.get("empty_reason"),
    }


def _provenance_counts(frame: pl.DataFrame) -> dict[str, int]:
    if frame.is_empty() or "provenance" not in frame.columns:
        return {}
    return {
        str(row["provenance"]): int(row["count"])
        for row in frame.group_by("provenance").len(name="count").iter_rows(named=True)
    }


def _decorate_history_status(
    table: str,
    status: dict[str, Any],
    frame: pl.DataFrame | None = None,
) -> dict[str, Any]:
    frame = frame if frame is not None else pl.DataFrame()
    if table == INSTRUMENT_LIFECYCLE_EVENTS_TABLE:
        status["lifecycle_completeness"] = summarize_lifecycle_completeness(frame)
    return status


def _history_table_status(data_dir: Path, table: str, meta: dict[str, Any]) -> dict[str, Any]:
    path = table_path(data_dir, table)
    if not path.exists():
        return _decorate_history_status(table, {
            "label": meta["label"],
            "rows": 0,
            "earliest_date": None,
            "latest_date": None,
            "symbols_covered": 0,
            "path_exists": False,
            "manifest": _latest_manifest(data_dir, PIT_HISTORY_SOURCE, table),
        })

    frame = pl.read_parquet(path)
    source = meta.get("source")
    if source and "source" in frame.columns:
        frame = frame.filter(pl.col("source") == source)
    if frame.is_empty():
        return _decorate_history_status(table, {
            "label": meta["label"],
            "rows": 0,
            "earliest_date": None,
            "latest_date": None,
            "symbols_covered": 0,
            "path_exists": True,
            "manifest": _latest_manifest(data_dir, PIT_HISTORY_SOURCE, table),
        }, frame)

    start_col = str(meta["start_col"])
    end_col = meta.get("end_col")
    latest_expr = (
        pl.max_horizontal(pl.col(start_col), pl.col(str(end_col)).fill_null(pl.col(start_col)))
        if end_col and end_col in frame.columns
        else pl.col(start_col)
    )
    row = frame.select(
        pl.len().alias("rows"),
        pl.col(start_col).min().alias("earliest_date"),
        latest_expr.max().alias("latest_date"),
        pl.col(str(meta["symbol_col"])).n_unique().alias("symbols_covered"),
    ).to_dicts()[0]

    sources = []
    if "source" in frame.columns:
        sources = sorted(str(v) for v in frame["source"].drop_nulls().unique().to_list())

    return _decorate_history_status(table, {
        "label": meta["label"],
        "rows": int(row["rows"] or 0),
        "earliest_date": _date_text(row["earliest_date"]),
        "latest_date": _date_text(row["latest_date"]),
        "symbols_covered": int(row["symbols_covered"] or 0),
        "path_exists": True,
        "sources": sources,
        "provenance_counts": _provenance_counts(frame),
        "manifest": _latest_manifest(data_dir, PIT_HISTORY_SOURCE, table),
    }, frame)


def _snapshot_root(data_dir: Path, table: str, source: str) -> Path:
    return Path(data_dir) / "pit_reference" / source / table


def _snapshot_table_status(data_dir: Path, table: str, meta: dict[str, Any]) -> dict[str, Any]:
    source = str(meta.get("source") or BAOSTOCK_SOURCE)
    root = _snapshot_root(data_dir, table, source)
    partitions = sorted(root.glob("snapshot_date=*/part.parquet"))
    if not partitions:
        return {
            "label": meta["label"],
            "source": source,
            "rows": 0,
            "latest_snapshot_date": None,
            "earliest_snapshot_date": None,
            "snapshots": 0,
            "symbols_covered": 0,
            "manifest": _latest_manifest(data_dir, source, table),
            **_snapshot_quality(table),
        }

    latest = partitions[-1]
    frame = pl.read_parquet(latest)
    snapshot_dates = [path.parent.name.split("=", 1)[1] for path in partitions]
    symbol_col = str(meta["symbol_col"])
    return {
        "label": meta["label"],
        "source": source,
        "rows": frame.height,
        "latest_snapshot_date": snapshot_dates[-1],
        "earliest_snapshot_date": snapshot_dates[0],
        "snapshots": len(partitions),
        "symbols_covered": int(frame[symbol_col].n_unique()) if symbol_col in frame.columns else 0,
        "provenance_counts": _provenance_counts(frame),
        "manifest": _latest_manifest(data_dir, source, table),
        **_snapshot_quality(table),
    }


def _snapshot_quality(table: str) -> dict[str, Any]:
    if table != INDEX_CONSTITUENT_CANDIDATES_TABLE:
        return {}
    return {
        "candidate_source": {
            "strict_backtest_usable": False,
            "message": (
                "BaoStock dated constituents are candidate snapshots; do not use them "
                "as strict PIT intervals without separate effective-from/to evidence"
            ),
        }
    }


def get_status(data_dir: Path) -> dict[str, Any]:
    data_dir = Path(data_dir)
    history = {
        table: _history_table_status(data_dir, table, meta)
        for table, meta in _HISTORY_TABLES.items()
    }
    snapshots = {
        table: _snapshot_table_status(data_dir, table, meta)
        for table, meta in _SNAPSHOT_TABLES.items()
    }
    history_rows = sum(int(item["rows"]) for item in history.values())
    snapshot_rows = sum(int(item["rows"]) for item in snapshots.values())
    history_dates = [
        value
        for item in history.values()
        for value in (item.get("earliest_date"), item.get("latest_date"))
        if value
    ]
    latest_snapshots = [
        item["latest_snapshot_date"]
        for item in snapshots.values()
        if item.get("latest_snapshot_date")
    ]
    return {
        "history": history,
        "snapshots": snapshots,
        "summary": {
            "source": BAOSTOCK_SOURCE,
            "history_rows": history_rows,
            "snapshot_rows": snapshot_rows,
            "rows": history_rows + snapshot_rows,
            "earliest_date": min(history_dates) if history_dates else None,
            "latest_date": max(history_dates) if history_dates else None,
            "latest_snapshot_date": max(latest_snapshots) if latest_snapshots else None,
            "strict_index_membership_usable": False,
        },
    }


def _daily_rows(data_dir: Path) -> pl.DataFrame:
    root = Path(data_dir) / "kline_daily"
    files = sorted(root.glob("**/*.parquet"))
    if not files:
        return pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date})
    return pl.scan_parquet([str(path) for path in files]).select(["symbol", "date"]).collect()


def sync_hithink_snapshots(
    data_dir: Path,
    *,
    snapshot_date: date | None = None,
    collector: HiThinkSnapshotCollector | None = None,
    sector_limit: int | None = None,
) -> dict[str, Any]:
    snapshot_date = snapshot_date or date.today()
    data_dir = Path(data_dir)
    if collector is None:
        client = HiThinkClient()
        try:
            client._api_key()
        except HiThinkAuthError as exc:
            return {
                "status": "skipped",
                "reason": "missing_hithink_api_key",
                "message": str(exc),
                "snapshot_date": snapshot_date.isoformat(),
                "tables": {},
                "published_rows": 0,
            }
        collector = HiThinkSnapshotCollector(data_dir, client=client)

    tables: dict[str, int] = {}
    errors: list[str] = []

    try:
        tables[INDEX_CONSTITUENTS_TABLE] = collector.collect_index_constituents(
            DEFAULT_INDEX_NAMES.keys(),
            snapshot_date=snapshot_date,
            index_names=DEFAULT_INDEX_NAMES,
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{INDEX_CONSTITUENTS_TABLE}: {exc}")

    try:
        tables[THS_SECTOR_CONSTITUENTS_TABLE] = collector.collect_sector_constituents(
            DEFAULT_SECTOR_TAGS,
            snapshot_date=snapshot_date,
            sector_limit=sector_limit,
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{THS_SECTOR_CONSTITUENTS_TABLE}: {exc}")

    try:
        tables[INSTRUMENT_LIFECYCLE_TABLE] = collector.collect_lifecycle_observed(
            observed_as_of=snapshot_date,
            daily_rows=_daily_rows(data_dir),
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{INSTRUMENT_LIFECYCLE_TABLE}: {exc}")

    status = "failed" if errors else "published"
    return {
        "status": status,
        "snapshot_date": snapshot_date.isoformat(),
        "tables": tables,
        "published_rows": sum(tables.values()),
        "errors": errors,
    }


def sync_baostock_index_candidates(
    data_dir: Path,
    *,
    snapshot_dates: Iterable[date] | None = None,
    collector: BaoStockIndexCandidateCollector | None = None,
) -> dict[str, Any]:
    dates = tuple(snapshot_dates or (date.today(),))
    data_dir = Path(data_dir)
    collector = collector or BaoStockIndexCandidateCollector(data_dir)
    try:
        rows = collector.collect_hs300_snapshots(dates)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "source": BAOSTOCK_SOURCE,
            "tables": {},
            "published_rows": 0,
            "errors": [f"{INDEX_CONSTITUENT_CANDIDATES_TABLE}: {exc}"],
        }
    return {
        "status": "published",
        "source": BAOSTOCK_SOURCE,
        "snapshot_dates": [item.isoformat() for item in dates],
        "tables": {INDEX_CONSTITUENT_CANDIDATES_TABLE: rows},
        "published_rows": rows,
        "errors": [],
    }


def sync_baostock_reference(
    data_dir: Path,
    *,
    snapshot_date: date | None = None,
    years: int = BAOSTOCK_LIFECYCLE_LOOKBACK_YEARS,
) -> dict[str, Any]:
    """Sync the BaoStock-only reference datasets used by the data page."""
    snapshot_date = snapshot_date or date.today()
    candidate_result = sync_baostock_index_candidates(
        data_dir,
        snapshot_dates=(snapshot_date,),
    )
    lifecycle_result = sync_baostock_lifecycle(
        data_dir,
        end_date=snapshot_date,
        years=years,
    )

    errors = [
        *(f"index candidates: {item}" for item in candidate_result.get("errors") or []),
        *(f"lifecycle: {item}" for item in lifecycle_result.get("errors") or []),
    ]
    tables = {
        **(candidate_result.get("tables") or {}),
        **(lifecycle_result.get("tables") or {}),
    }
    candidate_rows = int(candidate_result.get("published_rows") or 0)
    lifecycle_rows = int(lifecycle_result.get("published_rows") or 0)
    return {
        "status": "failed" if errors else "published",
        "source": BAOSTOCK_SOURCE,
        "snapshot_date": snapshot_date.isoformat(),
        "tables": tables,
        "published_rows": candidate_rows + lifecycle_rows,
        "index_candidate_rows": candidate_rows,
        "lifecycle_rows": lifecycle_rows,
        "instrument_appended_symbols": int(
            lifecycle_result.get("instrument_appended_symbols") or 0
        ),
        "errors": errors,
    }


def sync_baostock_lifecycle(
    data_dir: Path,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    years: int = BAOSTOCK_LIFECYCLE_LOOKBACK_YEARS,
    collector: BaoStockInstrumentLifecycleCollector | None = None,
) -> dict[str, Any]:
    data_dir = Path(data_dir)
    collector = collector or BaoStockInstrumentLifecycleCollector(data_dir)
    try:
        result = collector.collect_stock_lifecycle(
            start_date=start_date,
            end_date=end_date,
            years=years,
        )
        from app.services import instrument_sync

        instrument_result = instrument_sync.apply_lifecycle_supplement(data_dir)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "source": BAOSTOCK_SOURCE,
            "tables": {},
            "published_rows": 0,
            "errors": [f"{INSTRUMENT_LIFECYCLE_EVENTS_TABLE}: {exc}"],
        }
    return {
        "status": "published",
        "source": BAOSTOCK_SOURCE,
        "start_date": result["start_date"].isoformat(),
        "end_date": result["end_date"].isoformat(),
        "years": years,
        "tables": {INSTRUMENT_LIFECYCLE_EVENTS_TABLE: result["published_rows"]},
        "published_rows": result["published_rows"],
        "source_rows": result["source_rows"],
        "candidate_rows": result["candidate_rows"],
        "total_table_rows": result["total_table_rows"],
        "instrument_rows": instrument_result["rows"],
        "instrument_matched_symbols": instrument_result["matched_symbols"],
        "instrument_appended_symbols": instrument_result["appended_symbols"],
        "errors": [],
    }
