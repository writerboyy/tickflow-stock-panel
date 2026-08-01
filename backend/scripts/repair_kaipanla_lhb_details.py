#!/usr/bin/env python3
"""Validate or publish the Kaipanla LHB detail primary-key repair."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.plugins.kaipanla.lhb_detail_repair import repair_lhb_details


def main() -> int:
    parser = argparse.ArgumentParser(description="影子重建开盘啦龙虎榜席位明细")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = repair_lhb_details(args.data_dir.resolve(), apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
