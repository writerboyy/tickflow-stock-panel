#!/usr/bin/env python3
"""Cross-check rejected TickFlow index bars against all configured reference sources."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

import polars as pl

from app.services.index_consensus import crosscheck_index_daily_consensus
from app.services.index_sync import IndexDailyQualityError, _validate_index_daily


def _invalid_rows(frame: pl.DataFrame) -> pl.DataFrame:
    try:
        _validate_index_daily(frame)
    except IndexDailyQualityError as exc:
        return exc.invalid_rows
    return pl.DataFrame()


def main() -> int:
    parser = argparse.ArgumentParser(description="使用全部备用源只读核验 TickFlow 异常指数日线")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    pattern = args.data_dir.resolve() / "kline_index_daily" / "**" / "*.parquet"
    frame = pl.scan_parquet(str(pattern), hive_partitioning=True)
    if args.symbols:
        frame = frame.filter(pl.col("symbol").is_in(sorted(set(args.symbols))))
    if args.start_date:
        frame = frame.filter(pl.col("date") >= args.start_date)
    if args.end_date:
        frame = frame.filter(pl.col("date") <= args.end_date)
    result = crosscheck_index_daily_consensus(_invalid_rows(frame.collect()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return 0 if result["status"] in {"complete", "no_anomalies"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
