from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.services import index_sync
from app.services.index_crosscheck import _normalize_easy_tdx_bars, crosscheck_index_daily
from app.services.index_sync import IndexDailyQualityError


def _row(**updates) -> dict:
    return {
        "symbol": "399379.SZ",
        "date": date(2026, 7, 30),
        "open": 9072.993,
        "high": 9102.392,
        "low": 8917.402,
        "close": 8983.985,
        "volume": -1_519_207_424.0,
        "amount": 512_785_300_000.0,
        **updates,
    }


def test_crosscheck_confirms_tickflow_overflow_when_valid_fields_match() -> None:
    source = pl.DataFrame([_row()])

    def fetcher(_symbols, _start, _end):
        return pl.DataFrame([_row(volume=2_775_759_872.0, close=8983.991)])

    result = crosscheck_index_daily(source, fetcher=fetcher)

    assert result["status"] == "complete"
    assert result["status_counts"] == {"tickflow_anomaly_confirmed": 1}
    assert result["rows"][0]["tickflow_anomalies"] == ["volume_negative"]
    assert result["rows"][0]["differing_valid_fields"] == []


def test_easy_tdx_timestamp_dates_are_normalized_to_trading_dates() -> None:
    frame = pl.DataFrame([{
        **_row(volume=2_775_759_872.0),
        "date": "2026-07-30 00:00:00",
        "vol": 2_775_759_872.0,
    }]).drop("volume")

    normalized = _normalize_easy_tdx_bars(frame, "399379.SZ")

    assert normalized["date"].to_list() == [date(2026, 7, 30)]
    assert normalized["volume"].to_list() == [2_775_759_872.0]


def test_crosscheck_reports_source_conflict_instead_of_replacing_tickflow() -> None:
    source = pl.DataFrame([_row()])

    def fetcher(_symbols, _start, _end):
        return pl.DataFrame([_row(volume=2_775_759_872.0, close=9000.0)])

    result = crosscheck_index_daily(source, fetcher=fetcher)

    assert result["status_counts"] == {"source_conflict": 1}
    assert result["rows"][0]["differing_valid_fields"] == ["close"]


def test_crosscheck_reports_missing_easy_tdx_rows() -> None:
    result = crosscheck_index_daily(
        pl.DataFrame([_row()]),
        fetcher=lambda _symbols, _start, _end: pl.DataFrame(),
    )

    assert result["status"] == "partial"
    assert result["status_counts"] == {"tdx_unavailable": 1}


def test_crosscheck_rejects_anomalous_easy_tdx_as_both_anomalous() -> None:
    result = crosscheck_index_daily(
        pl.DataFrame([_row()]),
        fetcher=lambda _symbols, _start, _end: pl.DataFrame([_row(volume=-1.0)]),
    )

    assert result["status_counts"] == {"both_anomalous": 1}


def test_index_quality_failure_includes_crosscheck_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.index_consensus.crosscheck_index_daily_consensus",
        lambda _rows: {
            "status": "complete",
            "requested_rows": 1,
            "confirmed_rows": 1,
            "sources": {},
            "status_counts": {"replacement_confirmed": 1},
        },
    )

    with pytest.raises(IndexDailyQualityError, match="replacement_confirmed=1"):
        index_sync._validate_index_daily_with_crosscheck(pl.DataFrame([_row()]))


def test_missing_tickflow_fields_do_not_trigger_crosscheck(monkeypatch) -> None:
    called = False

    def crosscheck(_rows):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr("app.services.index_consensus.crosscheck_index_daily_consensus", crosscheck)

    with pytest.raises(IndexDailyQualityError, match="缺少字段"):
        index_sync._validate_index_daily_with_crosscheck(pl.DataFrame({"symbol": ["000001.SH"]}))
    assert called is False
