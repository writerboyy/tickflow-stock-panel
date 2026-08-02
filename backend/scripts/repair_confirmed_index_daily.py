#!/usr/bin/env python3
"""Validate or publish evidence-confirmed index daily repairs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.index_repair import repair_confirmed_index_daily


def main() -> int:
    parser = argparse.ArgumentParser(description="影子修复已由辅助源确认的指数日线异常字段")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--corroboration", type=Path)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--expected-dual-source-rows", type=int, required=True)
    parser.add_argument("--expected-remaining-rows", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = repair_confirmed_index_daily(
        args.data_dir.resolve(),
        args.evidence.resolve(),
        corroboration_path=args.corroboration.resolve() if args.corroboration else None,
        expected_rows=args.expected_rows,
        expected_dual_source_rows=args.expected_dual_source_rows,
        expected_remaining_rows=args.expected_remaining_rows,
        apply=args.apply,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
