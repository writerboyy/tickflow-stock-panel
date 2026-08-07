#!/usr/bin/env python3
"""Strictly compare TickFlow execution records with a JoinQuant export."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.free_strategy.parity import compare_executions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path, help="TickFlow result.json")
    parser.add_argument("reference", type=Path, help="聚宽 GB18030 transaction.csv/zip")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--diagnose-alignment", action="store_true")
    parser.add_argument("--max-differences", type=int, default=30)
    args = parser.parse_args()
    report = compare_executions(
        args.result,
        args.reference,
        diagnose_alignment=args.diagnose_alignment,
    )
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    print(f"{report['status'].upper()}: {len(report['differences'])} differences")
    for item in report["differences"][:args.max_differences]:
        print(
            f"第 {item['index']} 笔 {item['field']}: "
            f"TickFlow={item['tickflow']}, 聚宽={item['joinquant']}"
        )
    if report["status"] == "passed":
        return 0
    return 1 if report["status"] == "failed" else 2


if __name__ == "__main__":
    sys.exit(main())
