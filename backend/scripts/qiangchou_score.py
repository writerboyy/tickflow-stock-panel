"""竞价抢筹强度打分（基于扶摇集合竞价表 ext_fuyao_auction）

公式（由 2026-08-28 榜单反推 + 全市场约束校验）:
    抢筹强度 = 竞价换手率^1.5 × 竞价涨幅^0.7

校验结果:
  - 榜单 13 只票全部落在全市场 top 94（样本 5490 只）
  - 13 只内部相对顺序 10/12 与榜单一致

用法:
    python qiangchou_score.py                      # 最新交易日
    python qiangchou_score.py --date 2026-08-28    # 指定日期
    python qiangchou_score.py --top 30             # 输出前 30
    python qiangchou_score.py --checkpoint 0925    # 指定竞价时点
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

# backend/scripts/x.py -> parents[0]=scripts, [1]=backend, [2]=项目根
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data" / "ext_data" / "ext_fuyao_auction" / "timeseries"

ALPHA = 0.7   # 涨幅指数
BETA = 1.5    # 换手指数


def partition(d: date) -> Path:
    return DATA_ROOT / f"date={d.isoformat()}" / "part.parquet"


def latest_date() -> date:
    dirs = sorted(p.name for p in DATA_ROOT.iterdir() if p.is_dir() and p.name.startswith("date="))
    if not dirs:
        raise SystemExit("未找到任何竞价数据分区")
    return date.fromisoformat(dirs[-1].removeprefix("date="))


def score(d: date, checkpoint: str, top: int,
          exclude_st: bool, max_pct: float | None) -> pl.DataFrame:
    path = partition(d)
    if not path.exists():
        raise SystemExit(f"缺少 {d} 的竞价数据: {path}")

    df = pl.read_parquet(path).filter(pl.col("checkpoint") == checkpoint)
    df = df.filter(
        pl.col("auction_turnover_pct").is_not_null()
        & pl.col("auction_pct").is_not_null()
        & (pl.col("auction_pct") > 0)
    )
    if exclude_st:
        df = df.filter(~pl.col("name").str.contains("ST"))
    if max_pct is not None:
        df = df.filter(pl.col("auction_pct") < max_pct)

    return (
        df.with_columns(
            (pl.col("auction_turnover_pct") ** BETA
             * pl.col("auction_pct") ** ALPHA).alias("qiangchou_score")
        )
        .with_columns(pl.col("qiangchou_score").rank("ordinal", descending=True).alias("rank"))
        .sort("rank")
        .head(top)
        .select([
            "rank", "code", "name", "qiangchou_score",
            "auction_pct", "auction_turnover_pct", "auction_amount",
            "auction_unmatched", "auction_volume_ratio",
        ])
        .with_columns(pl.col("auction_amount").truediv(1e4).round(0).cast(pl.Int64).alias("竞价额_万"))
        .select([
            "rank", "code", "name", "qiangchou_score",
            "auction_pct", "auction_turnover_pct", "竞价额_万",
            "auction_unmatched", "auction_volume_ratio",
        ])
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="竞价抢筹强度打分")
    ap.add_argument("--date", help="交易日 YYYY-MM-DD，默认最新")
    ap.add_argument("--checkpoint", default="0925", help="竞价时点: 0915/0920/0925/1457/1500")
    ap.add_argument("--top", type=int, default=30, help="输出前 N 名")
    ap.add_argument("--exclude-st", action="store_true", help="排除 ST 股")
    ap.add_argument("--max-pct", type=float, help="排除涨幅超过该值的（过滤新股/涨停）")
    args = ap.parse_args()

    d = date.fromisoformat(args.date) if args.date else latest_date()
    result = score(d, args.checkpoint, args.top, args.exclude_st, args.max_pct)

    print(f"交易日 {d}  时点 {args.checkpoint}  "
          f"公式: 换手^{BETA} × 涨幅^{ALPHA}")
    print(result)


if __name__ == "__main__":
    main()
