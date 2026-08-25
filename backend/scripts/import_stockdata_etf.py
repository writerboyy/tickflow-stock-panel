#!/usr/bin/env python3
"""Audit and optionally publish local StockData ETF parquet files."""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from app.config import settings
from app.services.stockdata_etf_import import (
    StockDataEtfImportBlocked,
    StockDataEtfImportConfig,
    run_stockdata_etf_import,
)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit and fill ETF daily/minute gaps from a local StockData directory"
    )
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--start", type=_date, default=date(2019, 1, 1))
    parser.add_argument("--end", type=_date, default=date.today())
    parser.add_argument("--run-id")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="stage, validate, and atomically publish; default is read-only audit",
    )
    args = parser.parse_args(argv)
    try:
        result = run_stockdata_etf_import(
            StockDataEtfImportConfig(
                source_dir=args.source_dir,
                data_dir=args.data_dir,
                start=args.start,
                end=args.end,
                run_id=args.run_id,
                publish=args.publish,
            ),
            progress=lambda message: print(message, flush=True),
        )
    except (StockDataEtfImportBlocked, ValueError) as exc:
        parser.exit(1, f"StockData ETF import blocked: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
