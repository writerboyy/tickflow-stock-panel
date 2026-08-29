"""最终网格搜索：把未匹配量纳入公式

强度 = 竞价换手率^β × 竞价涨幅^α × (|未匹配量变化|+1)^γ
约束：截图 13 只在全市场排名靠前 + 内部顺序一致
"""
from __future__ import annotations

import itertools
import math

import polars as pl

DATA = "/Users/jiangbo/workspace/量化/付费量化/tickflow-stock-panel/data/ext_data/ext_fuyao_auction/timeseries/date=2026-08-28/part.parquet"

QD = {
    "300311": 6.32, "301629": 6.04, "301591": 4.86, "300456": 4.83,
    "300189": 4.82, "301205": 4.79, "300378": 4.36, "301309": 4.30,
    "300615": 4.21, "002396": 4.11, "301373": 4.09, "688410": 4.06,
    "300798": 3.93,
}
TARGET = [c for c, _ in sorted(QD.items(), key=lambda kv: -kv[1])]


def load() -> pl.DataFrame:
    df = pl.read_parquet(DATA)
    a = df.filter(pl.col("checkpoint") == "0925").select(
        ["code", "name", "auction_pct", "auction_turnover_pct", "auction_unmatched"])
    b = df.filter(pl.col("checkpoint") == "0920").select(
        ["code", "auction_unmatched"]).rename({"auction_unmatched": "unm_b"})
    j = a.join(b, on="code", how="left")
    return j.filter(
        pl.col("auction_turnover_pct").is_not_null()
        & pl.col("auction_pct").is_not_null()
        & (pl.col("auction_pct") > 0)
    ).with_columns(
        (pl.col("auction_unmatched") - pl.col("unm_b").fill_null(0)).alias("unm_delta")
    ).with_columns(
        (pl.col("unm_delta").abs() + 1).alias("abs_unm")
    )


def evaluate(df: pl.DataFrame, alpha: float, beta: float, gamma: float) -> dict:
    scored = df.with_columns(
        (pl.col("auction_turnover_pct") ** beta
         * pl.col("auction_pct") ** alpha
         * pl.col("abs_unm") ** gamma).alias("score")
    )
    scored = scored.with_columns(
        pl.col("score").rank("ordinal", descending=True).alias("rank"))
    ranks = {}
    for c in QD:
        r = scored.filter(pl.col("code") == c).select("rank")
        ranks[c] = r.item() if r.height else None
    valid = [ranks[c] for c in QD if ranks[c] is not None]
    if not valid:
        return {"alpha": alpha, "beta": beta, "gamma": gamma, "worst": None,
                "correct": 0, "score": -1e9}
    ordered = [ranks.get(c) for c in TARGET]
    correct = sum(1 for i in range(len(ordered) - 1)
                  if ordered[i] is not None and ordered[i + 1] is not None
                  and ordered[i] < ordered[i + 1])
    worst = max(valid)
    return {
        "alpha": alpha, "beta": beta, "gamma": gamma,
        "worst": worst, "correct": correct,
        "score": correct * 100 - math.log10(worst + 1) * 10,
    }


def main() -> None:
    df = load()
    print(f"样本(正涨幅): {df.height}")

    alphas = [0.3, 0.5, 0.7, 0.9, 1.0, 1.2]
    betas = [0.8, 1.0, 1.2, 1.5, 2.0]
    gammas = [0.0, 0.05, 0.1, 0.15, 0.2, 0.3]

    results = []
    for a, b, g in itertools.product(alphas, betas, gammas):
        results.append(evaluate(df, a, b, g))

    results.sort(key=lambda r: -r["score"])
    print("\n" + "=" * 88)
    print("最终搜索 Top 12  (强度 = 换手^β × 涨幅^α × (|未匹配量变化|+1)^γ)")
    print("=" * 88)
    for r in results[:12]:
        print(f"α={r['alpha']:<4} β={r['beta']:<4} γ={r['gamma']:<5} "
              f"| 最差排名={r['worst']:<5} 内部顺序={r['correct']}/12  分={r['score']:.2f}")

    best = results[0]
    print(f"\n最佳: 强度 = 换手^{best['beta']} × 涨幅^{best['alpha']} "
          f"× (|未匹配量变化|+1)^{best['gamma']}")

    # 输出该公式下的全市场 top 20
    scored = df.with_columns(
        (pl.col("auction_turnover_pct") ** best["beta"]
         * pl.col("auction_pct") ** best["alpha"]
         * pl.col("abs_unm") ** best["gamma"]).alias("score")
    ).with_columns(pl.col("score").rank("ordinal", descending=True).alias("rank"))
    qdf = pl.DataFrame({"code": list(QD), "截图强度": [QD[c] for c in QD]})
    print("\n=== 13 只票排名 ===")
    print(scored.join(qdf, on="code", how="inner").sort("截图强度", descending=True)
          .select(["rank", "name", "截图强度", "auction_pct", "auction_turnover_pct"]))
    print("\n=== 全市场 TOP 20 ===")
    print(scored.sort("rank").head(20).select(
        ["rank", "name", "auction_pct", "auction_turnover_pct", "score"]))


if __name__ == "__main__":
    main()
