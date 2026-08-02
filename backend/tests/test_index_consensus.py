from __future__ import annotations

from datetime import date

import polars as pl

from app.services.index_consensus import (
    IndexReferenceSource,
    crosscheck_index_daily_consensus,
)


FIELDS = ("open", "high", "low", "close", "volume", "amount")
OHLCV = ("open", "high", "low", "close", "volume")


def _row(**updates):
    return {
        "symbol": "000691.SH",
        "date": date(2021, 8, 3),
        "open": 1694.03,
        "high": 1694.03,
        "low": 1662.226,
        "close": 1665.02,
        "volume": 1_816_780_256.0,
        "amount": 2.3220621017e18,
        **updates,
    }


def _source(name, fields, rows=None, error=None):
    def fetcher(_symbols, _start, _end):
        if error:
            raise error
        return pl.DataFrame(rows or [])

    return IndexReferenceSource(name, fields, fetcher, name, name)


def test_consensus_repairs_anomaly_and_related_corrupt_volume():
    easy = _row(volume=2_638_927.0, amount=23_220_621_312.0)
    eastmoney = _row(volume=2_638_927.0, amount=23_220_621_017.0)
    tencent = {key: value for key, value in _row(volume=2_638_930.0).items() if key != "amount"}
    sources = (
        _source("easy_tdx", FIELDS, [easy]),
        _source("baostock", FIELDS),
        _source("astock_data_eastmoney", FIELDS, [eastmoney]),
        _source("astock_data_tencent", OHLCV, [tencent]),
    )

    result = crosscheck_index_daily_consensus(pl.DataFrame([_row()]), sources=sources)

    evidence = result["rows"][0]
    assert result["status_counts"] == {"replacement_confirmed": 1}
    assert evidence["changed_fields"] == ["amount", "volume"]
    assert evidence["related_corrupt_fields"] == ["volume"]
    assert evidence["replacement"] == {
        "amount": 23_220_621_312.0,
        "volume": 2_638_927.0,
    }
    assert evidence["field_consensus"]["amount"]["sources"] == [
        "easy_tdx",
        "astock_data_eastmoney",
    ]


def test_consensus_uses_two_astock_backends_when_other_sources_have_no_coverage():
    tickflow = _row(symbol="000902.SH", volume=-6.0, amount=1.4e12)
    eastmoney = _row(symbol="000902.SH", volume=889_252_209.0, amount=1.4e12)
    tencent = {
        key: value
        for key, value in _row(symbol="000902.SH", volume=889_252_202.0).items()
        if key != "amount"
    }
    sources = (
        _source("easy_tdx", FIELDS),
        _source("baostock", FIELDS),
        _source("astock_data_eastmoney", FIELDS, [eastmoney]),
        _source("astock_data_tencent", OHLCV, [tencent]),
    )

    result = crosscheck_index_daily_consensus(pl.DataFrame([tickflow]), sources=sources)

    evidence = result["rows"][0]
    assert evidence["status"] == "replacement_confirmed"
    assert evidence["changed_fields"] == ["volume"]
    assert evidence["replacement"] == {"volume": 889_252_209.0}


def test_consensus_accepts_external_exact_uint32_recovery():
    tickflow = _row(symbol="000902.SH", volume=-1_917_736_060.0, amount=1.4e12)
    tencent = {
        key: value
        for key, value in _row(symbol="000902.SH", volume=2_377_231_236.0).items()
        if key != "amount"
    }

    result = crosscheck_index_daily_consensus(
        pl.DataFrame([tickflow]),
        sources=(_source("astock_data_tencent", OHLCV, [tencent]),),
    )

    evidence = result["rows"][0]
    assert evidence["status"] == "replacement_confirmed"
    assert evidence["replacement"] == {"volume": 2_377_231_236.0}
    assert evidence["field_consensus"]["volume"] == {
        "value": 2_377_231_236.0,
        "sources": ["astock_data_tencent", "tickflow_uint32_recovery"],
        "evidence_kind": "external_source_plus_exact_uint32_recovery",
    }


def test_wrong_instrument_reference_is_rejected_by_valid_ohlc():
    wrong = _row(open=12.3, high=13.01, low=12.11, close=13.0, volume=100.0, amount=200.0)
    result = crosscheck_index_daily_consensus(
        pl.DataFrame([_row()]),
        sources=(_source("astock_data_baidu", FIELDS, [wrong]),),
    )

    evidence = result["rows"][0]
    assert evidence["status"] == "reference_unavailable"
    assert evidence["rejected_references"] == {
        "astock_data_baidu": ["close", "high", "low", "open"]
    }


def test_consensus_fails_closed_with_only_one_reference_source():
    source = _row(volume=2_638_927.0, amount=23_220_621_312.0)
    result = crosscheck_index_daily_consensus(
        pl.DataFrame([_row()]),
        sources=(
            _source("easy_tdx", FIELDS, [source]),
            _source("baostock", FIELDS, error=TimeoutError()),
        ),
    )

    assert result["status_counts"] == {"insufficient_consensus": 1}
    assert result["rows"][0]["missing_consensus_fields"] == ["amount"]
    assert result["sources"]["baostock"]["status"] == "unavailable"
