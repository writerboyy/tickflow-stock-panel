"""Account-level frozen training/out-of-sample performance summaries."""
from __future__ import annotations

from datetime import date
from typing import Any


TRAIN_END = date(2024, 12, 31)
OOS_START = date(2025, 1, 1)


def _value(row: dict[str, Any] | None, key: str, fallback: float) -> float:
    if row is None or row.get(key) is None:
        return fallback
    return float(row[key])


def _segment(
    rows: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    start: date,
    end: date,
) -> dict[str, Any] | None:
    selected = [row for row in rows if start <= date.fromisoformat(str(row["date"])) <= end]
    if not selected:
        return None
    prior = [row for row in rows if date.fromisoformat(str(row["date"])) < start]
    base = prior[-1] if prior else None
    strategy_base = _value(base, "strategy_nav", 1.0)
    benchmark_base = _value(base, "benchmark_nav", 1.0)
    strategy_nav = _value(selected[-1], "strategy_nav", strategy_base)
    benchmark_nav = _value(selected[-1], "benchmark_nav", benchmark_base)
    strategy_return = strategy_nav / strategy_base - 1 if strategy_base > 0 else 0.0
    benchmark_return = benchmark_nav / benchmark_base - 1 if benchmark_base > 0 else 0.0
    nav_values = [strategy_base, *(_value(row, "strategy_nav", strategy_base) for row in selected)]
    peak = nav_values[0]
    max_drawdown = 0.0
    for value in nav_values:
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = max(max_drawdown, 1 - value / peak)
    entries = sum(
        str(fill.get("side")) == "buy"
        and start <= date.fromisoformat(str(fill["timestamp"])[:10]) <= end
        for fill in fills
    )
    exits = sum(
        str(fill.get("side")) == "sell"
        and start <= date.fromisoformat(str(fill["timestamp"])[:10]) <= end
        for fill in fills
    )
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "return_pct": round(strategy_return * 100, 4),
        "benchmark_return_pct": round(benchmark_return * 100, 4),
        "excess_return_pct": round((strategy_return - benchmark_return) * 100, 4),
        "max_drawdown_pct": round(max_drawdown * 100, 4),
        "entry_count": entries,
        "exit_count": exits,
    }


def build_research_periods(
    result: dict[str, Any],
    start: date,
    end: date,
) -> dict[str, Any]:
    rows = sorted(result.get("daily_equity_curve") or [], key=lambda row: str(row["date"]))
    fills = list(result.get("fills") or [])
    training = _segment(rows, fills, start, min(end, TRAIN_END)) if start <= TRAIN_END else None
    out_of_sample = _segment(rows, fills, max(start, OOS_START), end) if end >= OOS_START else None
    annual = [
        value
        for year in range(start.year, end.year + 1)
        if (value := _segment(
            rows,
            fills,
            max(start, date(year, 1, 1)),
            min(end, date(year, 12, 31)),
        )) is not None
    ]
    return {
        "parameters_frozen_after": TRAIN_END.isoformat(),
        "training": training,
        "out_of_sample": out_of_sample,
        "annual": annual,
    }
