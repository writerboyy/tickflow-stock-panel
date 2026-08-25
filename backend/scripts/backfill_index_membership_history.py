"""Backfill canonical CSI index membership history from BaoStock."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date, datetime
import json
from pathlib import Path
import shutil

import polars as pl

from app.config import settings
from app.plugins.baostock.index_candidates import BaoStockIndexMembershipCollector
from app.plugins.pit_history.storage import INDEX_MEMBERSHIP_HISTORY_TABLE, table_path


def local_trading_dates(
    data_dir: Path,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[date]:
    root = Path(data_dir) / "kline_daily"
    files = sorted(root.glob("**/*.parquet"))
    if not files:
        return []
    date_expr = pl.col("date").cast(pl.Date)
    query = pl.scan_parquet([str(path) for path in files], extra_columns="ignore").select(
        date_expr.alias("date")
    )
    if start_date is not None:
        query = query.filter(pl.col("date") >= start_date)
    if end_date is not None:
        query = query.filter(pl.col("date") <= end_date)
    frame = query.unique().sort("date").collect()
    return [item for item in frame["date"].to_list() if item is not None]


def backup_canonical_table(data_dir: Path) -> Path | None:
    source = table_path(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
    if not source.exists():
        return None
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    target = (
        Path(data_dir)
        / "_backups"
        / "index_membership_history"
        / timestamp
        / "part.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=False)
    shutil.copy2(source, target)
    return target


def _date_arg(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--start-date", type=_date_arg)
    parser.add_argument("--end-date", type=_date_arg)
    parser.add_argument(
        "--indices",
        default="000300.SH,000905.SH",
        help="BaoStock historical indices; CSI 800 is derived when 300 and 500 are both selected",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be non-negative")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    indices = tuple(item.strip().upper() for item in args.indices.split(",") if item.strip())
    if not indices:
        parser.error("--indices must include at least one index")

    dates = local_trading_dates(
        args.data_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if not dates:
        print("trading_dates=0 message=no local kline_daily trading dates")
        return 2
    if args.dry_run:
        print(
            json.dumps(
                {
                    "trading_dates": len(dates),
                    "date_start": dates[0].isoformat(),
                    "date_end": dates[-1].isoformat(),
                    "indices": indices,
                    "derive_csi800": {"000300.SH", "000905.SH"}.issubset(indices),
                    "dry_run": True,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    backup = backup_canonical_table(args.data_dir)
    collector = BaoStockIndexMembershipCollector(
        args.data_dir,
        timeout=args.timeout,
        query_delay_seconds=args.sleep_seconds,
    )
    result = collector.collect_historical_membership(
        trading_dates=dates,
        indices=indices,
    )
    result.update(
        {
            "date_start": dates[0].isoformat(),
            "date_end": dates[-1].isoformat(),
            "indices_requested": list(indices),
            "backup": str(backup) if backup else None,
        }
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
