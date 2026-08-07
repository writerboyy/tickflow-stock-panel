"""Collect HiThink supplemental snapshots into PIT reference tables."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from app.config import settings
from app.plugins.hithink.collector import HiThinkSnapshotCollector


def _daily_rows(data_dir: Path) -> pl.DataFrame:
    root = data_dir / "kline_daily"
    files = sorted(root.glob("**/*.parquet"))
    if not files:
        return pl.DataFrame()
    return pl.scan_parquet([str(path) for path in files]).select(["symbol", "date"]).collect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    parser.add_argument(
        "--indices",
        default="000300.SH,000905.SH,000906.SH,000852.SH",
        help="Comma-separated standard index thscodes",
    )
    parser.add_argument(
        "--sector-tags",
        default="",
        help="Comma-separated THS catalog tags: industry,cn_concept,region,tszs",
    )
    parser.add_argument("--sector-limit", type=int, default=None)
    parser.add_argument("--lifecycle", action="store_true")
    args = parser.parse_args()

    snapshot_date = date.fromisoformat(args.snapshot_date)
    collector = HiThinkSnapshotCollector(args.data_dir)

    rows = 0
    indices = [item.strip() for item in args.indices.split(",") if item.strip()]
    if indices:
        rows += collector.collect_index_constituents(indices, snapshot_date=snapshot_date)

    tags = [item.strip() for item in args.sector_tags.split(",") if item.strip()]
    if tags:
        rows += collector.collect_sector_constituents(
            tags,
            snapshot_date=snapshot_date,
            sector_limit=args.sector_limit,
        )

    if args.lifecycle:
        rows += collector.collect_lifecycle_observed(
            observed_as_of=snapshot_date,
            daily_rows=_daily_rows(args.data_dir),
        )

    print(f"published_rows={rows}")


if __name__ == "__main__":
    main()
