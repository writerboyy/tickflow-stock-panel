#!/usr/bin/env python3
"""Validate or apply exact-duplicate cleanup for local financial parquet tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.financial_repair import FINANCIAL_REPAIR_TABLES, repair_financial_tables


def main() -> int:
    parser = argparse.ArgumentParser(description="去除财务 Parquet 的精确重复行，冲突 revision 保持 fail-closed")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--table",
        action="append",
        choices=FINANCIAL_REPAIR_TABLES,
        help="只修复指定财务表；可重复传入。默认检查全部财务表。",
    )
    args = parser.parse_args()
    tables = tuple(args.table) if args.table else FINANCIAL_REPAIR_TABLES
    result = repair_financial_tables(args.data_dir.resolve(), tables=tables, apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
