from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl

from app.services import instrument_sync
from app.tickflow.catalog import fetch_instrument_details, list_cn_exchanges


def test_list_cn_exchanges_uses_tickflow_exchange_catalog():
    exchanges = SimpleNamespace(list=MagicMock(return_value=[
        {"exchange": "SZ", "region": "CN"},
        {"exchange": "SH", "region": "CN"},
        {"exchange": "US", "region": "US"},
    ]))
    tf = SimpleNamespace(exchanges=exchanges)

    assert list_cn_exchanges(tf) == ["SH", "SZ"]
    exchanges.list.assert_called_once_with()


def test_fetch_instrument_details_batches_symbols_and_uses_single_get_fallback():
    requested: list[list[str]] = []
    instruments = SimpleNamespace(
        batch=lambda symbols: requested.append(symbols) or [
            {"symbol": symbol, "name": symbol} for symbol in symbols
        ],
    )
    tf = SimpleNamespace(instruments=instruments)
    symbols = [f"{index:06d}.SZ" for index in range(1001)]

    rows = fetch_instrument_details(tf, symbols)

    assert [len(chunk) for chunk in requested] == [1000, 1]
    assert len(rows) == 1001

    get = MagicMock(return_value={"symbol": "600000.SH", "name": "浦发银行"})
    single_tf = SimpleNamespace(instruments=SimpleNamespace(get=get))
    assert fetch_instrument_details(single_tf, ["600000.SH"]) == [
        {"symbol": "600000.SH", "name": "浦发银行"},
    ]
    get.assert_called_once_with("600000.SH")

    failed_batch_get = MagicMock(return_value={"symbol": "600519.SH", "name": "贵州茅台"})
    failed_batch_tf = SimpleNamespace(instruments=SimpleNamespace(
        batch=MagicMock(side_effect=RuntimeError("batch unavailable")),
        get=failed_batch_get,
    ))
    assert fetch_instrument_details(failed_batch_tf, ["600519.SH"]) == [
        {"symbol": "600519.SH", "name": "贵州茅台"},
    ]
    failed_batch_get.assert_called_once_with("600519.SH")


def test_stock_sync_prefers_universe_and_instrument_batch_catalog(monkeypatch, tmp_path):
    exchange_list = MagicMock(return_value=[
        {"exchange": "SH", "region": "CN"},
        {"exchange": "SZ", "region": "CN"},
    ])
    exchange_instruments = MagicMock(side_effect=AssertionError("exchange fallback should not run"))
    universe_get = MagicMock(return_value={"id": "CN_Equity_A", "symbols": ["600000.SH", "000001.SZ"]})
    instrument_batch = MagicMock(return_value=[
        {"symbol": "600000.SH", "name": "浦发银行", "type": "stock", "ext": {}},
        {"symbol": "000001.SZ", "name": "平安银行", "type": "stock", "ext": {}},
    ])
    tf = SimpleNamespace(
        exchanges=SimpleNamespace(list=exchange_list, get_instruments=exchange_instruments),
        universes=SimpleNamespace(get=universe_get),
        instruments=SimpleNamespace(batch=instrument_batch),
    )
    monkeypatch.setattr(instrument_sync, "get_client", lambda: tf)
    monkeypatch.setattr(instrument_sync, "_fetch_instruments_via_provider", lambda: None)
    monkeypatch.setattr(
        instrument_sync,
        "apply_lifecycle_supplement",
        lambda _data_dir: {"rows": 2, "matched_symbols": 0, "appended_symbols": 0},
    )

    count = instrument_sync.sync_instruments(tmp_path)

    assert count == 2
    exchange_list.assert_called_once_with()
    universe_get.assert_called_once_with("CN_Equity_A")
    instrument_batch.assert_called_once_with(["600000.SH", "000001.SZ"])
    frame = pl.read_parquet(tmp_path / "instruments" / "instruments.parquet")
    assert set(frame["symbol"].to_list()) == {"600000.SH", "000001.SZ"}
