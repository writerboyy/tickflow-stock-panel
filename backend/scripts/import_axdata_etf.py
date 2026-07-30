#!/usr/bin/env python3
"""Import ETF history from AxData through the production repair service."""
from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from app.services.etf_data_repair import (
    _daily_frame,
    _dividend_factors,
    _minute_frame,
    import_symbol,
)

__all__ = ["_daily_frame", "_dividend_factors", "_minute_frame", "import_symbol"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import missing ETF history from a local AxData service")
    parser.add_argument("symbol", help="TickFlow symbol, for example 161226.SZ")
    parser.add_argument("--name", default=None, help="Instrument display name")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data")
    parser.add_argument("--axdata-url", default="http://127.0.0.1:8666")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--replace-minute",
        action="store_true",
        help="Replace existing minute rows with AxData raw prices adjusted by imported dividends",
    )
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must not be after --end")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    daily_count, minute_count = import_symbol(
        symbol=args.symbol.upper(), name=args.name or args.symbol.upper(),
        start=args.start, end=args.end, data_dir=args.data_dir.resolve(),
        axdata_url=args.axdata_url, workers=args.workers, retries=args.retries,
        replace_minute=args.replace_minute,
    )
    logging.info("import complete: daily=%d minute=%d", daily_count, minute_count)


if __name__ == "__main__":
    main()
