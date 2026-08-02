"""Collect BaoStock HS300 constituent candidate snapshots.

This script publishes dated candidate snapshots under
pit_reference/baostock/index_constituent_candidates. It never writes the strict
pit_reference/history/index_membership_events table.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.config import settings
from app.plugins.baostock.index_candidates import BaoStockIndexCandidateCollector


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
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.years <= 0:
        parser.error("--years must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    end_date = args.end_date
    start_date = args.start_date or (end_date - timedelta(days=365 * args.years))
    dates, source = candidate_dates(
        args.data_dir,
        start_date=start_date,
        end_date=end_date,
        weekday_fallback=args.weekday_fallback,
    )
    if not dates:
        print(
            "candidate_dates=0 source=none "
            "message=no local trading dates; pass --weekday-fallback to use weekdays"
        )
        return 2
    if args.dry_run:
        print(
            f"candidate_dates={len(dates)} source={source} "
            f"range={_range_label(dates)} dry_run=true"
        )
        return 0

    collector = BaoStockIndexCandidateCollector(args.data_dir, timeout=args.timeout)
    rows = collector.collect_hs300_snapshots(dates)
    print(
        f"published_rows={rows} candidate_dates={len(dates)} "
        f"source={source} range={_range_label(dates)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
