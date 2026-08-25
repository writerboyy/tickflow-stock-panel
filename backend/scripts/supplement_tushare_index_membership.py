"""Temporarily supplement canonical index history with Tushare monthly snapshots."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date
import json
from pathlib import Path
from typing import Any

import polars as pl

from app.config import settings
from app.plugins.baostock.index_candidates import derive_csi800
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    merge_index_membership_history,
    normalize_index_membership_history,
    read_history_table,
    validate_index_membership_history,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)
from app.services.tushare_history import TushareProxyClient, load_tushare_key
from scripts.backfill_index_membership_history import backup_canonical_table


SOURCE = "tushare_proxy"
PARSER_VERSION = "tushare_index_weight_snapshot_v1"
DEFAULT_INDEX = "000852.SH"
DERIVED_SOURCE = "baostock+tushare_proxy"


def half_year_windows(start_date: date, end_date: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    for year in range(start_date.year, end_date.year + 1):
        for first, last in ((date(year, 1, 1), date(year, 6, 30)), (date(year, 7, 1), date(year, 12, 31))):
            window_start = max(start_date, first)
            window_end = min(end_date, last)
            if window_start <= window_end:
                windows.append((window_start, window_end))
    return windows


def fetch_monthly_snapshots(
    data_dir: Path,
    *,
    index_symbol: str,
    start_date: date,
    end_date: date,
    client: Any,
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    for window_start, window_end in half_year_windows(start_date, end_date):
        response = client.request(
            "index_weight",
            {
                "index_code": index_symbol,
                "start_date": window_start.strftime("%Y%m%d"),
                "end_date": window_end.strftime("%Y%m%d"),
            },
        )
        archive_source_payload(
            data_dir,
            SOURCE,
            "index_weight",
            f"{index_symbol}_{start_date.isoformat()}_{end_date.isoformat()}",
            f"{window_start.isoformat()}_{window_end.isoformat()}",
            response.raw,
            parser_version=PARSER_VERSION,
        )
        for row in response.rows:
            trade_date = row.get("trade_date")
            rows.append(
                {
                    **row,
                    "index_symbol": row.get("index_code") or index_symbol,
                    "member_symbol": row.get("con_code"),
                    "snapshot_date": trade_date,
                    "source_update_date": trade_date,
                    "provenance": "monthly_weight_snapshot",
                }
            )
    frame = normalize_index_membership_history(rows, source=SOURCE)
    if frame.is_empty():
        raise ValueError(f"Tushare index_weight returned no rows for {index_symbol}")
    validation = validate_index_membership_history(frame, index_symbol=index_symbol)
    invalid_snapshot_dates = list(validation.get("invalid_snapshot_dates") or [])
    if invalid_snapshot_dates:
        invalid_dates = [item["snapshot_date"] for item in invalid_snapshot_dates]
        frame = frame.filter(~pl.col("snapshot_date").cast(pl.String).is_in(invalid_dates))
    if frame.is_empty():
        raise ValueError(f"Tushare monthly snapshots failed strict validation: {validation}")
    validation = validate_index_membership_history(frame, index_symbol=index_symbol)
    if not validation["usable"]:
        raise ValueError(f"Tushare monthly snapshots failed strict validation: {validation}")
    return frame, invalid_snapshot_dates


def supplement_tushare_index_membership(
    data_dir: Path,
    *,
    indices: Iterable[str],
    start_date: date,
    end_date: date,
    client: Any | None = None,
) -> dict[str, Any]:
    data_dir = Path(data_dir)
    owned_client = client is None
    if client is None:
        key = load_tushare_key(data_dir=data_dir)
        if not key:
            raise ValueError("missing configured Tushare proxy key")
        client = TushareProxyClient(key, timeout=60, attempts=4, direct=True)
    try:
        fetched = [
            fetch_monthly_snapshots(
                data_dir,
                index_symbol=index_symbol,
                start_date=start_date,
                end_date=end_date,
                client=client,
            )
            for index_symbol in indices
        ]
    finally:
        if owned_client:
            client.close()
    frames = [item[0] for item in fetched]
    skipped_invalid_dates = [
        item
        for fetched_frame in fetched
        for item in fetched_frame[1]
    ]
    incoming = pl.concat(frames, how="diagonal_relaxed").sort(
        ["index_symbol", "snapshot_date", "member_symbol"]
    )
    result = merge_index_membership_history(data_dir, incoming)
    derived_rows = 0
    source_dates = (
        incoming.filter(pl.col("index_symbol").is_in(["000300.SH", "000905.SH"]))
        .select("snapshot_date")
        .unique()["snapshot_date"]
        .to_list()
    )
    if source_dates:
        canonical = read_history_table(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
        derivation_source = canonical.filter(
            pl.col("index_symbol").is_in(["000300.SH", "000905.SH"])
            & pl.col("snapshot_date").is_in(source_dates)
        )
        complete_dates = (
            derivation_source.group_by("snapshot_date")
            .agg(pl.col("index_symbol").n_unique().alias("source_indices"))
            .filter(pl.col("source_indices") == 2)["snapshot_date"]
            .to_list()
        )
        existing_csi800_dates = set(
            canonical.filter(pl.col("index_symbol") == "000906.SH")[
                "snapshot_date"
            ].to_list()
        )
        derive_dates = [item for item in complete_dates if item not in existing_csi800_dates]
        if derive_dates:
            derived = derive_csi800(
                derivation_source.filter(pl.col("snapshot_date").is_in(derive_dates)),
                source_name=DERIVED_SOURCE,
            )
            derived_result = merge_index_membership_history(data_dir, derived)
            derived_rows = int(derived_result["added_rows"])
            result["added_rows"] = int(result["added_rows"]) + derived_rows
            result["published_rows"] = int(derived_result["published_rows"])
            result["total_rows"] = int(derived_result["total_rows"])
            result["validation"] = derived_result["validation"]
    result["derived_csi800_rows"] = derived_rows
    result["skipped_invalid_snapshot_dates"] = skipped_invalid_dates
    update_ingestion_manifest(
        data_dir,
        SOURCE,
        INDEX_MEMBERSHIP_HISTORY_TABLE,
        f"monthly_{start_date.isoformat()}_{end_date.isoformat()}",
        status="published",
        parser_version=PARSER_VERSION,
        schema_version=1,
        source_content_hash=stable_content_hash(incoming.to_dicts()),
        content_hash=stable_content_hash(incoming.to_dicts()),
        published_rows=int(result["added_rows"]),
        provenance="monthly_weight_snapshot",
    )
    return result


def _date_arg(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--start-date", type=_date_arg, default=date(2010, 1, 1))
    parser.add_argument("--end-date", type=_date_arg, default=date.today())
    parser.add_argument("--indices", default=DEFAULT_INDEX)
    args = parser.parse_args(argv)
    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    indices = tuple(item.strip().upper() for item in args.indices.split(",") if item.strip())
    if not indices:
        parser.error("--indices must include at least one index")

    backup = backup_canonical_table(args.data_dir)
    result = supplement_tushare_index_membership(
        args.data_dir,
        indices=indices,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    result["backup"] = str(backup) if backup else None
    result["source_scope"] = "temporary_monthly_snapshot_supplement"
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
