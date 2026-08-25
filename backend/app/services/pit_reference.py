"""PIT reference status and canonical index-membership maintenance."""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from app.plugins.hithink.client import HiThinkAuthError, HiThinkClient
from app.plugins.hithink.collector import HiThinkSnapshotCollector
from app.plugins.baostock.index_candidates import (
    BaoStockIndexMembershipCollector,
    SOURCE as BAOSTOCK_SOURCE,
    derive_csi800,
)
from app.plugins.baostock.instrument_lifecycle import (
    DEFAULT_LOOKBACK_YEARS as BAOSTOCK_LIFECYCLE_LOOKBACK_YEARS,
    BaoStockInstrumentLifecycleCollector,
)
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    SOURCE as PIT_HISTORY_SOURCE,
    merge_index_membership_history,
    summarize_lifecycle_completeness,
    table_path,
    validate_index_membership_history,
)

logger = logging.getLogger(__name__)

DEFAULT_INDEX_NAMES = {
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000906.SH": "中证800",
    "000852.SH": "中证1000",
}
BAOSTOCK_CROSSCHECK_INDICES = ("000300.SH", "000905.SH")

_HISTORY_TABLES: dict[str, dict[str, Any]] = {
    INDEX_MEMBERSHIP_HISTORY_TABLE: {
        "label": "指数逐日成分历史",
        "symbol_col": "member_symbol",
        "start_col": "snapshot_date",
        "end_col": None,
        "source": None,
    },
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE: {
        "label": "BaoStock 股票生命周期",
        "symbol_col": "symbol",
        "start_col": "event_date",
        "end_col": None,
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
    elif table == INDEX_MEMBERSHIP_HISTORY_TABLE:
        status["membership_validation"] = validate_index_membership_history(frame)
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


def get_status(data_dir: Path) -> dict[str, Any]:
    data_dir = Path(data_dir)
    history = {
        table: _history_table_status(data_dir, table, meta)
        for table, meta in _HISTORY_TABLES.items()
    }
    snapshots: dict[str, Any] = {}
    history_rows = sum(int(item["rows"]) for item in history.values())
    snapshot_rows = sum(int(item["rows"]) for item in snapshots.values())
    history_dates = [
        value
        for item in history.values()
        for value in (item.get("earliest_date"), item.get("latest_date"))
        if value
    ]
    index_history = history.get(INDEX_MEMBERSHIP_HISTORY_TABLE, {})
    membership_validation = index_history.get("membership_validation") or {}
    return {
        "history": history,
        "snapshots": snapshots,
        "summary": {
            "source": "canonical",
            "historical_default_source": "baostock",
            "daily_snapshot_primary_source": "hithink",
            "history_rows": history_rows,
            "snapshot_rows": snapshot_rows,
            "rows": history_rows + snapshot_rows,
            "earliest_date": min(history_dates) if history_dates else None,
            "latest_date": max(history_dates) if history_dates else None,
            "latest_snapshot_date": index_history.get("latest_date"),
            "strict_index_membership_usable": bool(membership_validation.get("usable")),
        },
    }


def _compare_membership_sources(primary: pl.DataFrame, crosscheck: pl.DataFrame) -> int:
    checked = 0
    for snapshot in crosscheck.partition_by(
        ["index_symbol", "snapshot_date"], maintain_order=True
    ):
        index_symbol = str(snapshot["index_symbol"][0])
        snapshot_date = snapshot["snapshot_date"][0]
        primary_snapshot = primary.filter(
            (pl.col("index_symbol") == index_symbol)
            & (pl.col("snapshot_date") == snapshot_date)
        )
        primary_members = set(primary_snapshot["member_symbol"].to_list())
        crosscheck_members = set(snapshot["member_symbol"].to_list())
        if primary_members != crosscheck_members:
            raise ValueError(
                f"provider conflict for {index_symbol} on {snapshot_date}: "
                f"hithink_only={sorted(primary_members - crosscheck_members)[:20]} "
                f"baostock_only={sorted(crosscheck_members - primary_members)[:20]}"
            )
        checked += 1
    return checked


def sync_index_membership_snapshots(
    data_dir: Path,
    *,
    snapshot_date: date | None = None,
    hithink_collector: HiThinkSnapshotCollector | None = None,
    baostock_collector: BaoStockIndexMembershipCollector | None = None,
) -> dict[str, Any]:
    snapshot_date = snapshot_date or date.today()
    data_dir = Path(data_dir)
    hithink_error: str | None = None
    baostock_error: str | None = None
    hithink_frame = pl.DataFrame()
    baostock_frame = pl.DataFrame()

    if hithink_collector is None:
        client = HiThinkClient()
        try:
            client._api_key()
        except HiThinkAuthError as exc:
            hithink_error = str(exc)
        else:
            hithink_collector = HiThinkSnapshotCollector(data_dir, client=client)
    if hithink_collector is not None:
        try:
            hithink_frame = hithink_collector.fetch_index_constituents(
                DEFAULT_INDEX_NAMES,
                snapshot_date=snapshot_date,
                index_names=DEFAULT_INDEX_NAMES,
            )
        except Exception as exc:  # noqa: BLE001
            hithink_error = str(exc)

    baostock_collector = baostock_collector or BaoStockIndexMembershipCollector(data_dir)
    try:
        baostock_frame = baostock_collector.fetch_index_snapshots(
            BAOSTOCK_CROSSCHECK_INDICES,
            snapshot_dates=(snapshot_date,),
            index_names=DEFAULT_INDEX_NAMES,
        )
    except Exception as exc:  # noqa: BLE001
        baostock_error = str(exc)

    crosschecked_snapshots = 0
    source = "hithink"
    if not hithink_frame.is_empty():
        if not baostock_frame.is_empty():
            try:
                crosschecked_snapshots = _compare_membership_sources(
                    hithink_frame, baostock_frame
                )
            except ValueError as exc:
                return {
                    "status": "failed",
                    "source": "hithink",
                    "snapshot_date": snapshot_date.isoformat(),
                    "tables": {},
                    "published_rows": 0,
                    "index_membership_rows": 0,
                    "crosschecked_snapshots": 0,
                    "errors": [str(exc)],
                }
        incoming = hithink_frame
    elif not baostock_frame.is_empty():
        source = "baostock_fallback"
        incoming = baostock_frame
        if set(incoming["index_symbol"].unique().to_list()) == set(
            BAOSTOCK_CROSSCHECK_INDICES
        ):
            incoming = pl.concat(
                [incoming, derive_csi800(incoming)], how="diagonal_relaxed"
            )
    else:
        errors = [
            item
            for item in (
                f"hithink: {hithink_error}" if hithink_error else None,
                f"baostock: {baostock_error}" if baostock_error else None,
            )
            if item
        ]
        return {
            "status": "failed",
            "source": "unavailable",
            "snapshot_date": snapshot_date.isoformat(),
            "tables": {},
            "published_rows": 0,
            "index_membership_rows": 0,
            "crosschecked_snapshots": 0,
            "errors": errors or ["no index membership source returned data"],
        }

    try:
        result = merge_index_membership_history(data_dir, incoming)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "source": source,
            "snapshot_date": snapshot_date.isoformat(),
            "tables": {},
            "published_rows": 0,
            "index_membership_rows": 0,
            "crosschecked_snapshots": crosschecked_snapshots,
            "errors": [str(exc)],
        }
    warnings = [
        item
        for item in (
            f"hithink: {hithink_error}" if hithink_error else None,
            f"baostock crosscheck: {baostock_error}" if baostock_error else None,
        )
        if item
    ]
    added_rows = int(result["added_rows"])
    return {
        "status": "published",
        "source": source,
        "snapshot_date": snapshot_date.isoformat(),
        "tables": {INDEX_MEMBERSHIP_HISTORY_TABLE: added_rows},
        "published_rows": added_rows,
        "index_membership_rows": added_rows,
        "crosschecked_snapshots": crosschecked_snapshots,
        "warnings": warnings,
        "errors": [],
    }


def sync_pit_reference(
    data_dir: Path,
    *,
    snapshot_date: date | None = None,
    years: int = BAOSTOCK_LIFECYCLE_LOOKBACK_YEARS,
) -> dict[str, Any]:
    snapshot_date = snapshot_date or date.today()
    membership_result = sync_index_membership_snapshots(
        data_dir,
        snapshot_date=snapshot_date,
    )
    lifecycle_result = sync_baostock_lifecycle(
        data_dir,
        end_date=snapshot_date,
        years=years,
    )
    errors = [
        *(f"index membership: {item}" for item in membership_result.get("errors") or []),
        *(f"lifecycle: {item}" for item in lifecycle_result.get("errors") or []),
    ]
    membership_rows = int(membership_result.get("published_rows") or 0)
    lifecycle_rows = int(lifecycle_result.get("published_rows") or 0)
    return {
        "status": "failed" if errors else "published",
        "source": str(membership_result.get("source") or "unavailable"),
        "snapshot_date": snapshot_date.isoformat(),
        "tables": {
            **(membership_result.get("tables") or {}),
            **(lifecycle_result.get("tables") or {}),
        },
        "published_rows": membership_rows + lifecycle_rows,
        "index_membership_rows": membership_rows,
        "crosschecked_snapshots": int(
            membership_result.get("crosschecked_snapshots") or 0
        ),
        "lifecycle_rows": lifecycle_rows,
        "instrument_appended_symbols": int(
            lifecycle_result.get("instrument_appended_symbols") or 0
        ),
        "warnings": membership_result.get("warnings") or [],
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
