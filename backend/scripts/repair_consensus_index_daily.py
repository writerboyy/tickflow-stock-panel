#!/usr/bin/env python3
"""Validate or publish multi-source-consensus index daily repairs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.index_repair import repair_consensus_index_daily


def main() -> int:
    parser = argparse.ArgumentParser(description="影子修复已由多源逐字段共识确认的指数日线")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--expected-remaining-rows", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = repair_consensus_index_daily(
        args.data_dir.resolve(),
        args.evidence.resolve(),
        expected_rows=args.expected_rows,
        expected_remaining_rows=args.expected_remaining_rows,
        apply=args.apply,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
