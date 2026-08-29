"""Log-Log 回归：让系数自由取负，检验「换手越高强度越低」的假设

log(强度) = a + b·log(竞价涨幅) + c·log(竞价换手率) + d·log(|未匹配量|) + e·log(竞价量比)
"""
from __future__ import annotations

import numpy as np
import polars as pl


def _ols(X: np.ndarray, y: np.ndarray):
    """最小二乘（含截距），项目 venv 无 sklearn，用 numpy 实现。"""
    A = np.column_stack([np.ones(len(X)), X])
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = A @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return coef[0], coef[1:], pred, r2

# 扶摇 0925 精确字段（已验证与截图 13/13 吻合）
ROWS = [
    # name, code, 强度, 竞价涨幅%, 竞价换手%, |未匹配量|, 竞价量比
    ("任子行", "300311", 6.32, 5.9567, 0.2759, 2481.0, 4.4302),
    ("矽电股份", "301629", 6.04, 7.4055, 0.1542, 1.0, 10.5714),
    ("肯特股份", "301591", 4.86, 13.9881, 2.3425, 4.38, 3.0727),
    ("赛微电子", "300456", 4.83, 7.3664, 0.6035, 2667.9, 3.2910),
    ("神农种业", "300189", 4.82, 2.1116, 0.5583, 5490.84, 1.9627),
    ("联特科技", "301205", 4.79, 1.0015, 0.2868, 101.25, 4.6837),
    ("鼎捷数智", "300378", 4.36, 13.9378, 0.2451, 662.0, 55.656),
    ("万得凯", "301309", 4.30, 13.4596, 0.1589, 1.0, 64.2),
    ("欣天科技", "300615", 4.21, 1.7391, 0.4085, 322.0, 0.9487),
    ("星网锐捷", "002396", 4.11, 1.3493, 0.3054, 1455.15, 3.2799),
    ("凌玮科技", "301373", 4.09, 1.0103, 0.3276, 0.0, 2.4225),
    ("山外山", "688410", 4.06, 6.5498, 0.1305, 471.32, 25.247),
    ("锦鸡股份", "300798", 3.93, 4.3147, 0.1307, 31.0, 1.1787),
]

df = pl.DataFrame(ROWS, schema=["name", "code", "qd", "pct", "turn", "unm", "vr"])

df = df.with_columns([
    pl.col("qd").log().alias("log_qd"),
    pl.col("pct").log().alias("log_pct"),
    pl.col("turn").log().alias("log_turn"),
    (pl.col("unm").abs() + 1).log().alias("log_unm"),
    (pl.col("vr") + 1).log().alias("log_vr"),
])

FEATURES = {
    "log_pct": "log(竞价涨幅)",
    "log_turn": "log(竞价换手)",
    "log_unm": "log(|未匹配量|+1)",
    "log_vr": "log(竞价量比+1)",
}


def fit(cols: list[str]) -> None:
    X = df.select(cols).to_numpy()
    y = df["log_qd"].to_numpy()
    intercept, coefs, pred, r2 = _ols(X, y)
    # 还原到原始尺度算 MAE
    pred_raw = np.exp(pred)
    mae = float(np.abs(pred_raw - df["qd"].to_numpy()).mean())
    spearman = df.with_columns(pl.Series("pred", pred)).select(
        pl.corr("qd", "pred", method="spearman")
    ).item()
    terms = " + ".join(f"{c:+.4f}·{FEATURES[col]}" for c, col in zip(coefs, cols))
    print(f"log(强度) = {intercept:+.4f} {terms}")
    print(f"  R²={r2:.4f}  Spearman={spearman:.4f}  MAE(原始尺度)={mae:.4f}")
    out = df.select(["name", "qd"]).with_columns(
        pl.Series("预测", pred_raw.round(3)),
        pl.Series("误差", (pred_raw - df["qd"]).round(3)),
    ).sort("qd", descending=True)
    print(out)
    print()


def main() -> None:
    print("=" * 92)
    print("Log-Log 回归（扶摇 0925 精确字段）")
    print("=" * 92)

    print("【单变量】")
    for c in FEATURES:
        fit([c])

    print("【涨幅 + 换手】← 检验换手系数符号")
    fit(["log_pct", "log_turn"])

    print("【涨幅 + 换手 + 未匹配量】")
    fit(["log_pct", "log_turn", "log_unm"])

    print("【全变量】")
    fit(["log_pct", "log_turn", "log_unm", "log_vr"])


if __name__ == "__main__":
    main()
