from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.services.index_sync import IndexDailyQualityError, _validate_index_daily


def _index_rows(**overrides) -> pl.DataFrame:
    row = {
        "symbol": "000001.SH",
        "date": date(2026, 7, 31),
        "open": 3500.0,
        "high": 3520.0,
        "low": 3490.0,
        "close": 3510.0,
        "volume": 100_000_000.0,
        "amount": 500_000_000_000.0,
        **overrides,
    }
    return pl.DataFrame([row])


def test_index_daily_quality_accepts_valid_provider_rows() -> None:
    frame = _index_rows()

    assert _validate_index_daily(frame).equals(frame)


@pytest.mark.parametrize("overrides", [
    {"volume": -2_147_000_000.0},
    {"amount": 2.4e18},
    {"high": 3400.0},
    {"close": None},
])
def test_index_daily_quality_rejects_overflow_and_invalid_prices(overrides) -> None:
    with pytest.raises(IndexDailyQualityError, match="拒绝发布批次"):
        _validate_index_daily(_index_rows(**overrides))
