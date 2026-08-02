#!/usr/bin/env python3
"""Write explicit read-only evidence for conflicting financial rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.financial_crosscheck import crosscheck_financial_conflicts


def main() -> int:
    parser = argparse.ArgumentParser(description="使用 A-Stock-Data/新浪只读核验财务同键冲突")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = crosscheck_financial_conflicts(args.data_dir.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return 0 if result["status"] == "no_conflicts" else 1


if __name__ == "__main__":
    raise SystemExit(main())
