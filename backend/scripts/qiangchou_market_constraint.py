"""用全市场约束反推抢筹强度：

截图里的 13 只是全市场按「抢筹强度」排序后的 top 13（强度 3.93~6.32）。
因此正确的公式必须同时满足：
  1) 这 13 只在全市场排名靠前（top 区间）
  2) 这 13 只内部的相对顺序 == 截图的顺序

本脚本对候选公式同时检验这两个约束。
"""
from __future__ import annotations

import polars as pl

DATA = "/Users/jiangbo/workspace/量化/付费量化/tickflow-stock-panel/data/ext_data/ext_fuyao_auction/timeseries/date=2026-08-28/part.parquet"

QD = {
    "300311": 6.32, "301629": 6.04, "301591": 4.86, "300456": 4.83,
    "300189": 4.82, "301205": 4.79, "300378": 4.36, "301309": 4.30,
    "300615": 4.21, "002396": 4.11, "301373": 4.09, "688410": 4.06,
    "300798": 3.93,
}
CODES = list(QD)


def build() -> pl.DataFrame:
    df = pl.read_parquet(DATA)
    a = df.filter(pl.col("checkpoint") == "0925")
    b = df.filter(pl.col("checkpoint") == "0920").select(
        ["code", "auction_amount", "auction_unmatched", "auction_pct"]
    ).rename({"auction_amount": "amt_b", "auction_unmatched": "unm_b", "auction_pct": "pct_b"})

    j = a.join(b, on="code", how="left")
    return j.filter(
        pl.col("auction_amount").is_not_null()
        & pl.col("auction_turnover_pct").is_not_null()
    ).with_columns([
        (pl.col("auction_amount") - pl.col("amt_b")).alias("amt_delta"),
        (pl.col("auction_unmatched") - pl.col("unm_b")).alias("unm_delta"),
        (pl.col("auction_pct") - pl.col("pct_b")).alias("pct_delta"),
        (pl.col("auction_unmatched").abs() * pl.col("auction_price")
         / pl.col("auction_amount").replace(0, None) * 100).alias("unm_over_amt"),
    ]).with_columns([
        pl.col("auction_amount").log().alias("log_amt"),
        pl.col("auction_turnover_pct").log1p().alias("log_turn"),
        pl.col("auction_pct").abs().log1p().alias("log_pct"),
    ])


CANDIDATES = {
    "竞价换手率": "auction_turnover_pct",
    "竞价涨幅": "auction_pct",
    "竞价额对数": "log_amt",
    "未匹配量/竞价额": "unm_over_amt",
    "未匹配量增量": "unm_delta",
    "竞价额增量": "amt_delta",
    "换手×涨幅": None,
    "换手×√涨幅": None,
    "换手×ln(1+涨幅)": None,
    "换手×√涨幅×量比": None,
    "量比×涨幅": None,
    "竞价量比": "auction_volume_ratio",
}


def main() -> None:
    j = build()
    total = j.height
    print(f"全市场 0925 有效样本: {total}")

    j = j.with_columns([
        (pl.col("auction_turnover_pct") * pl.col("auction_pct")).alias("turn_x_pct"),
        (pl.col("auction_turnover_pct") * pl.col("auction_pct").abs().sqrt()).alias("turn_x_sqrtpct"),
        (pl.col("auction_turnover_pct") * pl.col("auction_pct").abs().log1p()).alias("turn_x_logpct"),
        (pl.col("auction_turnover_pct") * pl.col("auction_pct").abs().sqrt()
         * pl.col("auction_volume_ratio").fill_null(1)).alias("turn_sqrtpct_vr"),
        (pl.col("auction_volume_ratio").fill_null(1) * pl.col("auction_pct")).alias("vr_x_pct"),
    ])

    expr_map = {
        "竞价换手率": "auction_turnover_pct",
        "竞价涨幅": "auction_pct",
        "竞价额对数": "log_amt",
        "未匹配量/竞价额": "unm_over_amt",
        "未匹配量增量": "unm_delta",
        "竞价额增量": "amt_delta",
        "竞价量比": "auction_volume_ratio",
        "换手×涨幅": "turn_x_pct",
        "换手×√涨幅": "turn_x_sqrtpct",
        "换手×ln(1+涨幅)": "turn_x_logpct",
        "换手×√涨幅×量比": "turn_sqrtpct_vr",
        "量比×涨幅": "vr_x_pct",
    }

    # 目标顺序（按截图强度降序）
    target_order = [c for c, _ in sorted(QD.items(), key=lambda kv: -kv[1])]

    rows = []
    for label, col in expr_map.items():
        sub = j.filter(pl.col(col).is_not_null()).with_columns(
            pl.col(col).rank("ordinal", descending=True).alias("rank")
        )
        n = sub.height
        ranks = {}
        for c in CODES:
            r = sub.filter(pl.col("code") == c).select("rank")
            ranks[c] = r.item() if r.height else None
        got = [ranks[c] for c in CODES]
        valid = [r for r in got if r is not None]
        # 约束1：这13只在全市场的最差排名（越小越好）
        worst = max(valid) if valid else None
        # 约束2：13只内部顺序与截图顺序的 Spearman
        ordered = [ranks[c] for c in target_order]
        # 计算单调性：顺序完全正确的相邻对数
        correct = sum(1 for i in range(len(ordered) - 1)
                      if ordered[i] is not None and ordered[i + 1] is not None
                      and ordered[i] < ordered[i + 1])
        rows.append({
            "候选公式": label,
            "有效样本": n,
            "13只最差排名": worst,
            "13只最好排名": min(valid) if valid else None,
            "内部顺序正确/12": f"{correct}/12",
        })

    out = pl.DataFrame(rows)
    print("\n" + "=" * 90)
    print("约束检验：这 13 只在全市场 5555 只票中应排前列，且内部顺序与截图一致")
    print("=" * 90)
    print(out)


if __name__ == "__main__":
    main()
