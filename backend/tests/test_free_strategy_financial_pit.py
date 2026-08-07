from datetime import date

import polars as pl
import pytest

from app.free_strategy.financial_pit import (
    FinancialPitUnavailable,
    select_financial_periods,
)


def test_financial_pit_selects_latest_period_before_latest_revision():
    frame = pl.DataFrame({
        "symbol": ["X", "X", "X", "X"],
        "period_end": ["2024-03-31", "2024-06-30", "2024-06-30", "2024-09-30"],
        "announce_date": ["2024-10-15", "2024-08-20", "2024-08-30", "2024-11-01"],
        "revenue": [1.0, 2.0, 3.0, 4.0],
    })

    rows = select_financial_periods(
        frame,
        table="income",
        symbols=["X"],
        as_of=date(2024, 10, 31),
        period_count=2,
        required_fields=["revenue"],
    )["X"]

    assert [(row["period_end"], row["announce_date"], row["revenue"]) for row in rows] == [
        ("2024-06-30", "2024-08-30", 3.0),
        ("2024-03-31", "2024-10-15", 1.0),
    ]


def test_financial_pit_rejects_same_key_conflict():
    frame = pl.DataFrame({
        "symbol": ["X", "X"],
        "period_end": ["2024-06-30", "2024-06-30"],
        "announce_date": ["2024-08-30", "2024-08-30"],
        "revenue": [2.0, 3.0],
    })

    with pytest.raises(FinancialPitUnavailable, match="同键修订冲突"):
        select_financial_periods(
            frame,
            table="income",
            symbols=["X"],
            as_of=date(2024, 9, 1),
        )


def test_financial_pit_returns_empty_when_only_future_announcement_exists():
    frame = pl.DataFrame({
        "symbol": ["X"],
        "period_end": ["2024-06-30"],
        "announce_date": ["2024-08-30"],
        "revenue": [2.0],
    })

    assert select_financial_periods(
        frame,
        table="income",
        symbols=["X"],
        as_of=date(2024, 8, 29),
    ) == {}
