"""用扶摇竞价表(ext_fuyao_auction)反推截图「抢筹强度」

数据源锁定：截图 = 2026-08-28 扶摇 0925 时点
  - 截图涨幅 == auction_pct_0925  (13/13 精确吻合)
  - 截图竞额 == auction_amount_0925 (13/13 精确吻合)
"""
from __future__ import annotations

import polars as pl

DATA = "/Users/jiangbo/workspace/量化/付费量化/tickflow-stock-panel/data/ext_data/ext_fuyao_auction/timeseries/date=2026-08-28/part.parquet"

# 截图原始抢筹强度
QD = {
    "300311": 6.32, "301629": 6.04, "301591": 4.86, "300456": 4.83,
    "300189": 4.82, "301205": 4.79, "300378": 4.36, "301309": 4.30,
    "300615": 4.21, "002396": 4.11, "301373": 4.09, "688410": 4.06,
    "300798": 3.93,
}


def load() -> pl.DataFrame:
    df = pl.read_parquet(DATA)
    qdf = pl.DataFrame({"code": list(QD), "qd": [QD[c] for c in QD]})

    def at(cp: str, suffix: str) -> pl.DataFrame:
        cols = ["code", "auction_price", "auction_pct", "auction_volume",
                "auction_amount", "auction_unmatched", "auction_turnover_pct",
                "auction_volume_ratio", "float_market_cap", "pre_close_price"]
        return df.filter(pl.col("checkpoint") == cp).select(cols).rename(
            {c: f"{c}_{suffix}" for c in cols if c != "code"}
        )

    base = df.filter(pl.col("checkpoint") == "0925").select(["code", "name"])
    out = base.join(at("0925", "a"), on="code", how="left")
    out = out.join(at("0920", "b"), on="code", how="left")
    out = out.join(qdf, on="code", how="inner")
    return out


def derive(j: pl.DataFrame) -> pl.DataFrame:
    return j.with_columns([
        # 最后 5 分钟（09:20 -> 09:25）资金突袭增量
        (pl.col("auction_amount_a") - pl.col("auction_amount_b")).alias("amt_delta"),
        (pl.col("auction_pct_a") - pl.col("auction_pct_b")).alias("pct_delta"),
        (pl.col("auction_unmatched_a") - pl.col("auction_unmatched_b")).alias("unm_delta"),
        # 未匹配量的金额化（未匹配量 × 竞价价）
        (pl.col("auction_unmatched_a") * pl.col("auction_price_a")).alias("unm_amount"),
        # 未匹配量占竞价成交额比
        (pl.col("auction_unmatched_a") * pl.col("auction_price_a")
         / pl.col("auction_amount_a") * 100).alias("unm_pct_of_amt"),
        # 最后5分钟增量占0925总额比
        ((pl.col("auction_amount_a") - pl.col("auction_amount_b"))
         / pl.col("auction_amount_a") * 100).alias("delta_ratio"),
        pl.col("auction_amount_a").log().alias("log_amt"),
        pl.col("auction_unmatched_a").abs().log1p().alias("log_abs_unm"),
    ])


CANDIDATES = [
    "auction_pct_a", "auction_amount_a", "auction_unmatched_a",
    "auction_turnover_pct_a", "auction_volume_ratio_a",
    "amt_delta", "pct_delta", "unm_delta", "unm_amount",
    "unm_pct_of_amt", "delta_ratio", "log_amt", "log_abs_unm",
]


def main() -> None:
    j = derive(load())
    print("=" * 100)
    print("0925 时点明细（按截图强度降序）")
    print("=" * 100)
    print(j.sort("qd", descending=True).select([
        "name", "qd", "auction_pct_a", "auction_amount_a", "auction_unmatched_a",
        "auction_turnover_pct_a", "auction_volume_ratio_a", "amt_delta", "pct_delta",
    ]).with_columns(pl.col("auction_amount_a").truediv(1e4).round(1).alias("竞价额(万)"))
      .with_columns(pl.col("amt_delta").truediv(1e4).round(1).alias("增量(万)"))
      .select(["name", "qd", "auction_pct_a", "竞价额(万)", "auction_unmatched_a",
               "auction_turnover_pct_a", "auction_volume_ratio_a", "增量(万)", "pct_delta"]))

    print("\n" + "=" * 100)
    print("各字段 vs 截图抢筹强度：相关性")
    print("=" * 100)
    rows = []
    for c in CANDIDATES:
        s = j.select([c, "qd"]).drop_nulls()
        if s.height < 8:
            continue
        sp = s.select(pl.corr("qd", c, method="spearman")).item()
        pe = s.select(pl.corr("qd", c, method="pearson")).item()
        rows.append({"字段": c, "Spearman": round(sp, 4), "Pearson": round(pe, 4), "n": s.height})
    print(pl.DataFrame(rows).sort("Spearman", descending=True, nulls_last=True))


if __name__ == "__main__":
    main()
