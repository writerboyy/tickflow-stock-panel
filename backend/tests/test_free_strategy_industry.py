from datetime import date, datetime

import polars as pl
import pytest

from app.free_strategy.engine import FreeStrategyEngine
from app.free_strategy.industry import IndustryHistoryUnavailable, load_industry_history


def write_history(tmp_path, rows):
    path = (
        tmp_path
        / "pit_reference"
        / "history"
        / "industry_membership_history"
        / "part.parquet"
    )
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path)


def row(symbol, code, start, end=None, standard="申银万国行业分类标准"):
    return {
        "member_symbol": symbol,
        "industry_standard": standard,
        "industry_code": code,
        "industry_name": code,
        "effective_from": start,
        "effective_to": end,
        "source": "tickflow-test",
        "provenance": "historical_event",
    }


def test_industry_history_uses_half_open_intervals(tmp_path):
    write_history(tmp_path, [
        row("X", "OLD", date(2024, 1, 1), date(2024, 2, 1)),
        row("X", "NEW", date(2024, 2, 1)),
    ])

    assert load_industry_history(
        tmp_path, ["X"], date(2024, 1, 31), "申银万国行业分类标准",
    )["X"]["industry_code"] == "OLD"
    assert load_industry_history(
        tmp_path, ["X"], date(2024, 2, 1), "申银万国行业分类标准",
    )["X"]["industry_code"] == "NEW"


def test_industry_history_fails_closed_on_gap_overlap_and_unknown_level(tmp_path):
    write_history(tmp_path, [
        row("X", "A", date(2024, 1, 1), date(2024, 3, 1)),
        row("X", "B", date(2024, 2, 1)),
    ])

    with pytest.raises(IndustryHistoryUnavailable, match="区间重叠"):
        load_industry_history(
            tmp_path, ["X"], date(2024, 2, 15), "申银万国行业分类标准",
        )
    with pytest.raises(IndustryHistoryUnavailable, match="区间缺口"):
        load_industry_history(
            tmp_path, ["Y"], date(2024, 2, 15), "申银万国行业分类标准",
        )
    with pytest.raises(IndustryHistoryUnavailable, match="industry_level"):
        load_industry_history(
            tmp_path,
            ["X"],
            date(2024, 1, 15),
            "申银万国行业分类标准",
            1,
        )


def test_context_industry_query_rejects_future_as_of(tmp_path):
    write_history(tmp_path, [row("X", "A", date(2024, 1, 1))])
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    pass\n",
        timeframe="1m",
    )
    engine.set_industry_history_loader(
        lambda symbols, cutoff, standard, level: load_industry_history(
            tmp_path, symbols, cutoff, standard, level,
        )
    )
    engine.context.now = datetime(2024, 1, 2, 9, 31)

    assert engine.context.get_industry(
        ["X"], date(2024, 1, 2), "申银万国行业分类标准",
    )["X"]["industry_code"] == "A"
    with pytest.raises(ValueError, match="不能晚于"):
        engine.context.get_industry(
            ["X"], date(2024, 1, 3), "申银万国行业分类标准",
        )
