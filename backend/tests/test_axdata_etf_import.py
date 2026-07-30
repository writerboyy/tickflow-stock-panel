from __future__ import annotations

from datetime import date, datetime

import pytest
import polars as pl

from app.indicators.pipeline import compute_enriched
from app.services.etf_data_repair import _normalize_pure_split_factors
from scripts.import_axdata_etf import _daily_frame, _dividend_factors, _minute_frame


def test_daily_frames_keep_raw_prices_for_unadjusted_axdata_rows():
    rows = [{
        "instrument_id": "161226.SZ",
        "trade_time": "2025-10-13T15:00:00+08:00",
        "open": 1.29,
        "high": 1.363,
        "low": 1.289,
        "close": 1.36,
        "volume": 2_452_661.92,
        "amount": 324_024_864.0,
    }]

    daily = _daily_frame(
        "161226.SZ", rows, date(2025, 10, 1), date(2025, 10, 31),
    )

    assert daily.to_dicts() == [{
        "symbol": "161226.SZ",
        "date": date(2025, 10, 13),
        "open": 1.29,
        "high": 1.363,
        "low": 1.289,
        "close": 1.36,
        "volume": 2_452_661.92,
        "amount": 324_024_864.0,
    }]


def test_dividend_factors_replace_vendor_cash_factor():
    daily = pl.DataFrame({
        "symbol": ["512400.SH", "512400.SH"],
        "date": [date(2025, 9, 12), date(2025, 9, 15)],
        "open": [1.538, 1.549],
        "high": [1.572, 1.557],
        "low": [1.533, 1.521],
        "close": [1.562, 1.535],
        "volume": [1_000_000.0, 1_000_000.0],
        "amount": [156_200_000.0, 153_500_000.0],
    })
    existing = pl.DataFrame({
        "symbol": ["512400.SH"],
        "trade_date": [date(2025, 9, 15)],
        "ex_factor": [1.010884],
    })
    dividends = [
        {"dividend_date": "20240918", "accumulated_dividend": 0.01},
        {"dividend_date": "20250915", "accumulated_dividend": 0.025},
    ]

    factors = _dividend_factors("512400.SH", daily, dividends, existing)

    factor = factors.row(0, named=True)
    assert factor["symbol"] == "512400.SH"
    assert factor["trade_date"] == date(2025, 9, 15)
    assert factor["ex_factor"] == pytest.approx(1.562 / 1.547)

    enriched = compute_enriched(daily, factors=factors)
    assert enriched.filter(pl.col("date") == date(2025, 9, 12))["close"][0] == pytest.approx(1.547)


def test_pure_split_factor_is_normalized_without_changing_cash_dividend_factor():
    factors = pl.DataFrame({
        "symbol": ["515000.SH", "512400.SH"],
        "trade_date": [date(2025, 9, 8), date(2025, 9, 15)],
        "ex_factor": [1.994565, 2.010884],
    })
    dividends = [{"dividend_date": "20250915", "accumulated_dividend": 0.015}]

    normalized = _normalize_pure_split_factors(factors, dividends)

    assert normalized.filter(pl.col("symbol") == "515000.SH")["ex_factor"][0] == 2.0
    assert normalized.filter(pl.col("symbol") == "512400.SH")["ex_factor"][0] == 2.010884


def test_minute_frame_uses_adjusted_price_and_raw_amount():
    frame = _minute_frame("161226.SZ", [{
        "trade_time": "2025-10-13T13:11:00+08:00",
        "price": 1.324,
        "volume": 12_031,
    }], adjustment_ratio=0.98)

    row = frame.row(0, named=True)
    assert row["datetime"] == datetime(2025, 10, 13, 13, 11)
    assert row["close"] == pytest.approx(1.324 * 0.98)
    assert row["open"] == row["high"] == row["low"] == row["close"]
    assert row["volume"] == 12_031
    assert row["amount"] == pytest.approx(1.324 * 12_031 * 100)
