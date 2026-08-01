from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.plugins.kaipanla.replay import replay_archives


def main() -> int:
    parser = argparse.ArgumentParser(description="只读回放并对账开盘啦原始归档")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-parquet-compare", action="store_true")
    args = parser.parse_args()

    result = replay_archives(
        args.data_dir,
        compare_parquet=not args.no_parquet_compare,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
