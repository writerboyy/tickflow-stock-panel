"""生成 09:24:57 最后 3 秒采集用的竞价重点池

数据来源：
  1. 最近交易日的涨停股（ext_kpl_limitup）—— 次日最可能出现竞价抢筹
  2. --extra 指定的自定义池（可选）

注意：data/pools/CN_Equity_A.parquet 是全市场池（约 5555 只），
不可作为重点池来源——那会让 092457 退化成 56 个批次，远超 3 秒窗口。

用法:
    ./backend/.venv/bin/python backend/scripts/build_auction_focus.py
    ./backend/.venv/bin/python backend/scripts/build_auction_focus.py --date 2026-08-27
    ./backend/.venv/bin/python backend/scripts/build_auction_focus.py --max 500
    ./backend/.venv/bin/python backend/scripts/build_auction_focus.py --extra data/pools/my_pool.parquet

输出: data/instruments/auction_focus.parquet（仅 symbol 列）
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
LIMITUP_GLOB = "data/ext_data/ext_kpl_limitup/timeseries/date=*/part.parquet"


def latest_limitup() -> tuple[date, pl.DataFrame] | None:
    files = sorted(Path(PROJECT_ROOT).glob(LIMITUP_GLOB))
    if not files:
        return None
    path = files[-1]
    day = date.fromisoformat(path.parent.name.removeprefix("date="))
    return day, pl.read_parquet(path)


def extra_symbols(path: Path) -> list[str]:
    """读取 --extra 指定的自定义池（需含 symbol 列）。"""
    frame = pl.read_parquet(path)
    if "symbol" not in frame.columns:
        sys.exit(f"自定义池缺少 symbol 列: {path}")
    return [str(v).strip().upper() for v in frame["symbol"].drop_nulls().to_list()]


def main() -> None:
    ap = argparse.ArgumentParser(description="生成竞价重点池 auction_focus.parquet")
    ap.add_argument("--date", help="取该日涨停股，默认最新分区")
    ap.add_argument(
        "--max", type=int, default=300,
        help="重点池上限（默认 300，约 3 个采集批次，可稳在 3 秒窗口内）",
    )
    ap.add_argument("--extra", help="追加的自定义池 parquet 路径（需含 symbol 列）")
    args = ap.parse_args()

    symbols: list[str] = []
    if args.date:
        path = PROJECT_ROOT / f"data/ext_data/ext_kpl_limitup/timeseries/date={args.date}/part.parquet"
        if not path.exists():
            sys.exit(f"缺少涨停分区: {path}")
        frame = pl.read_parquet(path)
        src_day = args.date
    else:
        found = latest_limitup()
        if found is None:
            sys.exit("未找到 ext_kpl_limitup 任何分区")
        src_day, frame = found
        src_day = src_day.isoformat()

    if "symbol" in frame.columns:
        symbols.extend(str(v).strip().upper() for v in frame["symbol"].drop_nulls().to_list())
    print(f"涨停股({src_day}): {len(symbols)} 只")

    if args.extra:
        extra = extra_symbols(Path(args.extra))
        print(f"自定义池({args.extra}): {len(extra)} 条（去重前）")
        symbols.extend(extra)

    # 只保留 instruments 维表内的在市股票，避免扶摇拒绝未知标的
    inst_path = DATA_DIR / "instruments" / "instruments.parquet"
    if inst_path.exists():
        inst = pl.read_parquet(inst_path)
        if "status" in inst.columns:
            inst = inst.filter(pl.col("status").cast(pl.String).str.to_lowercase() == "active")
        valid = {str(v).strip().upper() for v in inst["symbol"].drop_nulls().to_list()}
        before = len(set(symbols))
        symbols = [s for s in symbols if s in valid]
        print(f"过滤非在市标的: {before} -> {len(set(symbols))}")

    symbols = sorted(set(symbols))
    if len(symbols) > args.max:
        print(f"超过上限 {args.max}，截断")
        symbols = symbols[: args.max]

    if not symbols:
        sys.exit("重点池为空，不生成文件")

    out = DATA_DIR / "instruments" / "auction_focus.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(out)
    batches = (len(symbols) + 99) // 100
    print(f"已写入 {out}: {len(symbols)} 只 -> {batches} 个采集批次")


if __name__ == "__main__":
    main()
