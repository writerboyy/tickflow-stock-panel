from __future__ import annotations

from datetime import date, datetime

import pytest

from scripts.import_axdata_etf import _daily_frames, _minute_frame


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

    daily, enriched = _daily_frames(
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
    assert enriched.select("raw_close", "raw_high", "raw_low").row(0) == (1.36, 1.363, 1.289)


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
