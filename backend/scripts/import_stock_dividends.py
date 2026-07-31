#!/usr/bin/env python3
"""Import a validated XDXR snapshot into TickFlow corporate actions."""
from __future__ import annotations

import argparse
from pathlib import Path

from app.services.stock_dividends import import_xdxr_cash_dividends


def main() -> None:
    parser = argparse.ArgumentParser(description="Import stock XDXR cash dividends")
    parser.add_argument("source", type=Path, help="XDXR parquet snapshot")
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data")
    args = parser.parse_args()
    if not args.source.is_file():
        parser.error(f"source does not exist: {args.source}")
    rows = import_xdxr_cash_dividends(args.source, args.data_dir.resolve())
    print(f"imported {rows} stock cash-dividend events")


if __name__ == "__main__":
    main()
