#!/usr/bin/env python3
"""Stage or publish the auditable P0 historical TickFlow backfill."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from app.services.p0_backfill import DATASETS, BackfillConfig, BackfillBlocked, run_p0_backfill


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _split_symbols(values: list[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    return tuple(symbol.strip() for value in values for symbol in value.split(",") if symbol.strip())


def _read_symbols_file(path: Path | None) -> tuple[str, ...] | None:
    if path is None:
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    return tuple(line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="在 shadow 目录中补齐 P0 历史数据；默认只 staging，必须显式 --publish 才替换 canonical 数据。"
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--start", type=_date, required=True)
    parser.add_argument("--end", type=_date, required=True)
    parser.add_argument(
        "--datasets",
        default=",".join(DATASETS),
        help=f"逗号分隔的数据集：{','.join(DATASETS)}",
    )
    parser.add_argument("--symbols", action="append", help="逗号分隔的标的；可重复传入")
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--rpm", type=int)
    parser.add_argument("--run-id")
    parser.add_argument("--publish", action="store_true", help="通过 manifest 校验后原子替换 canonical 数据")
    args = parser.parse_args()

    explicit_symbols = _split_symbols(args.symbols)
    file_symbols = _read_symbols_file(args.symbols_file)
    if explicit_symbols is not None and file_symbols is not None:
        parser.error("--symbols 与 --symbols-file 不能同时使用")
    datasets = tuple(item.strip() for item in args.datasets.split(",") if item.strip())
    config = BackfillConfig(
        data_dir=args.data_dir,
        start=args.start,
        end=args.end,
        datasets=datasets,
        symbols=explicit_symbols or file_symbols,
        max_symbols=args.max_symbols,
        batch_size=args.batch_size,
        rpm=args.rpm,
        run_id=args.run_id,
        publish=args.publish,
    )
    try:
        result = run_p0_backfill(config)
    except (BackfillBlocked, ValueError) as exc:
        parser.exit(1, f"P0 backfill blocked: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
