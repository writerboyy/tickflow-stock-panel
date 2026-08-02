"""Collect BaoStock HS300 constituent candidate snapshots.

This script publishes dated candidate snapshots under
pit_reference/baostock/index_constituent_candidates. It never writes the strict
pit_reference/history/index_membership_events table.
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.config import settings
from app.plugins.baostock.index_candidates import (
    INDEX_CONSTITUENT_CANDIDATES_TABLE,
    BaoStockIndexCandidateCollector,
    partition_path,
)


def local_trading_dates(data_dir: Path, start_date: date, end_date: date) -> list[date]:
    root = Path(data_dir) / "kline_daily"
    files = sorted(root.glob("**/*.parquet"))
    if not files:
        return []
    frame = (
        pl.scan_parquet([str(path) for path in files])
        .select(pl.col("date").cast(pl.Date).alias("date"))
        .filter(pl.col("date").is_between(start_date, end_date))
        .unique()
        .sort("date")
        .collect()
    )
    return [item for item in frame["date"].to_list() if item is not None]


def weekday_dates(start_date: date, end_date: date) -> list[date]:
    current = start_date
    dates: list[date] = []
    while current <= end_date:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def candidate_dates(
    data_dir: Path,
    *,
    start_date: date,
    end_date: date,
    weekday_fallback: bool,
) -> tuple[list[date], str]:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    dates = local_trading_dates(data_dir, start_date, end_date)
    if dates:
        return dates, "local_trading_dates"
    if weekday_fallback:
        return weekday_dates(start_date, end_date), "weekday_fallback"
    return [], "none"


def query_dates(data_dir: Path, dates: Iterable[date], *, refresh_existing: bool) -> list[date]:
    if refresh_existing:
        return list(dates)
    return [
        item
        for item in dates
        if not partition_path(data_dir, INDEX_CONSTITUENT_CANDIDATES_TABLE, item).exists()
    ]


def _date_arg(value: str) -> date:
    return date.fromisoformat(value)


def _range_label(dates: Iterable[date]) -> str:
    ordered = list(dates)
    if not ordered:
        return "none"
    return f"{ordered[0].isoformat()}..{ordered[-1].isoformat()}"


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--start-date", type=_date_arg)
    parser.add_argument("--end-date", type=_date_arg, default=date.today())
    parser.add_argument(
        "--weekday-fallback",
        action="store_true",
        help="Use weekdays when local kline_daily trading dates are unavailable",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Seconds to sleep between BaoStock date queries",
    )
    parser.add_argument("--max-dates", type=int)
    parser.add_argument(
        "--proxy-url",
        help="HTTP proxy URL for BaoStock raw TCP CONNECT, e.g. http://127.0.0.1:7890",
    )
    parser.add_argument(
        "--force-proxy",
        action="store_true",
        help="Use BaoStock HTTP CONNECT proxy without trying direct TCP first",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-query dates whose candidate snapshot partition already exists",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.years <= 0:
        parser.error("--years must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be non-negative")
    if args.max_dates is not None and args.max_dates <= 0:
        parser.error("--max-dates must be positive")
    if args.proxy_url:
        os.environ["BAOSTOCK_PROXY_URL"] = args.proxy_url
    if args.force_proxy:
        os.environ["BAOSTOCK_FORCE_PROXY"] = "1"

    end_date = args.end_date
    start_date = args.start_date or (end_date - timedelta(days=365 * args.years))
    all_dates, source = candidate_dates(
        args.data_dir,
        start_date=start_date,
        end_date=end_date,
        weekday_fallback=args.weekday_fallback,
    )
    if not all_dates:
        print(
            "candidate_dates=0 source=none "
            "message=no local trading dates; pass --weekday-fallback to use weekdays"
        )
        return 2
    dates = query_dates(args.data_dir, all_dates, refresh_existing=args.refresh_existing)
    skipped_existing = len(all_dates) - len(dates)
    if args.max_dates is not None:
        dates = dates[: args.max_dates]
    if not dates:
        print(
            f"candidate_dates={len(all_dates)} query_dates=0 source={source} "
            f"skipped_existing={skipped_existing} message=all candidate snapshots exist"
        )
        return 0
    if args.dry_run:
        print(
            f"candidate_dates={len(all_dates)} query_dates={len(dates)} source={source} "
            f"skipped_existing={skipped_existing} range={_range_label(dates)} dry_run=true"
        )
        return 0

    collector = BaoStockIndexCandidateCollector(
        args.data_dir,
        timeout=args.timeout,
        query_delay_seconds=args.sleep_seconds,
    )
    rows = collector.collect_hs300_snapshots(dates)
    print(
        f"published_rows={rows} candidate_dates={len(all_dates)} query_dates={len(dates)} "
        f"source={source} skipped_existing={skipped_existing} range={_range_label(dates)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
