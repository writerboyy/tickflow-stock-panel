#!/usr/bin/env python3
"""Emit machine-readable external ingestion health without collecting data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.ingestion_health import summarize_ingestion_health


def main() -> int:
    parser = argparse.ArgumentParser(description="检查外部数据入库 manifest 健康状态")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--source", action="append", dest="sources")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-data", action="store_true")
    args = parser.parse_args()
    result = summarize_ingestion_health(
        args.data_dir.resolve(),
        sources=set(args.sources) if args.sources else None,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if result["status"] == "unhealthy":
        return 1
    if args.require_data and result["status"] == "no_data":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
