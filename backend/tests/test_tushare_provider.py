from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import polars as pl
import pytest

from app.plugins.tushare import provider as tp
from app.services.tushare_history import TusharePermissionError


def _response(api_name: str, fields: tuple[str, ...], rows: list[tuple]) -> SimpleNamespace:
    return SimpleNamespace(
        api_name=api_name,
        fields=fields,
        items=tuple(rows),
        rows=[dict(zip(fields, row, strict=False)) for row in rows],
    )


def test_tushare_provider_reads_key_lazily_and_normalizes_daily_units(monkeypatch):
    keys: list[str] = []

    class Client:
        def request(self, api_name, params):
            assert api_name == "daily"
            return _response(
                api_name,
                ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"),
                [("000001.SZ", "20250102", 10, 11, 9, 10.5, 100, 200)],
            )

    def factory(key):
        keys.append(key)
        return Client()

    monkeypatch.setattr(tp, "load_tushare_key", lambda: "lazy-key")
    frame = tp.TushareProvider(client_factory=factory).get_daily(
        ["000001.SZ"], datetime(2025, 1, 2), datetime(2025, 1, 2)
    )

    assert keys == ["lazy-key"]
    assert frame.select("date", "volume", "amount").to_dicts() == [
        {"date": datetime(2025, 1, 2).date(), "volume": 10_000.0, "amount": 200_000.0}
    ]


def test_tushare_provider_routes_etf_adjustment_and_minute_qfq(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class Client:
        def request(self, api_name, params):
            calls.append((api_name, dict(params)))
            if api_name == "fund_adj":
                return _response(
                    api_name,
                    ("ts_code", "trade_date", "adj_factor"),
                    [("510300.SH", "20250101", 1.0), ("510300.SH", "20250102", 2.0), ("510300.SH", "20250103", 3.0)],
                )
            if api_name == "etf_mins":
                return _response(
                    api_name,
                    ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"),
                    [("510300.SH", "2025-01-02 09:31:00", 10, 10, 10, 10, 100, 1_000)],
                )
            raise AssertionError(api_name)

    monkeypatch.setattr(tp, "load_tushare_key", lambda: "lazy-key")
    provider = tp.TushareProvider(client_factory=lambda _key: Client())
    factors = provider.get_adj_factors(
        ["510300.SH"], datetime(2025, 1, 2), datetime(2025, 1, 3), asset_type="etf"
    )
    minute = provider.get_minute(
        ["510300.SH"], datetime(2025, 1, 2, 9, 30), datetime(2025, 1, 2, 15), asset_type="etf"
    )

    assert factors["ex_factor"].to_list() == [2.0, 1.5]
    assert minute["open"].to_list() == pytest.approx([20 / 3])
    assert minute["close"].to_list() == pytest.approx([20 / 3])
    assert minute.select("volume", "amount").to_dicts() == [
        {"volume": 100.0, "amount": 1_000.0}
    ]
    assert [name for name, _params in calls] == ["fund_adj", "etf_mins", "fund_adj"]


def test_tushare_provider_paginates_minute_cursor(monkeypatch):
    monkeypatch.setattr(tp, "MAX_MINUTE_ROWS", 2)
    monkeypatch.setattr(tp, "load_tushare_key", lambda: "lazy-key")

    class Client:
        def request(self, api_name, params):
            if api_name == "stk_mins":
                if params["end_date"] > "2025-01-02 09:30:00":
                    rows = [
                        ("000001.SZ", "2025-01-02 09:32:00", 12, 12, 12, 12, 1, 12),
                        ("000001.SZ", "2025-01-02 09:31:00", 11, 11, 11, 11, 1, 11),
                    ]
                else:
                    rows = [("000001.SZ", "2025-01-02 09:30:00", 10, 10, 10, 10, 1, 10)]
                return _response(
                    api_name,
                    ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"),
                    rows,
                )
            if api_name == "adj_factor":
                return _response(
                    api_name,
                    ("ts_code", "trade_date", "adj_factor"),
                    [("000001.SZ", "20250101", 1.0), ("000001.SZ", "20250102", 1.0)],
                )
            raise AssertionError(api_name)

    progress: list[tuple[int, int]] = []
    frame = tp.TushareProvider(client_factory=lambda _key: Client()).get_minute(
        ["000001.SZ"], datetime(2025, 1, 2, 9, 30), datetime(2025, 1, 2, 15),
        on_chunk_done=lambda current, total: progress.append((current, total)),
    )

    assert frame.height == 3
    assert frame["datetime"].to_list() == [
        datetime(2025, 1, 2, 9, 30),
        datetime(2025, 1, 2, 9, 31),
        datetime(2025, 1, 2, 9, 32),
    ]
    assert progress == [(1, 1)]


def test_tushare_provider_clips_minute_pages_to_requested_range(monkeypatch):
    monkeypatch.setattr(tp, "MAX_MINUTE_ROWS", 2)
    monkeypatch.setattr(tp, "load_tushare_key", lambda: "lazy-key")

    class Client:
        def request(self, api_name, params):
            if api_name == "stk_mins":
                if params["end_date"] > "2025-01-01 15:00:00":
                    rows = [
                        ("000001.SZ", "2025-01-02 09:31:00", 11, 11, 11, 11, 1, 11),
                        ("000001.SZ", "2025-01-02 09:30:00", 10, 10, 10, 10, 1, 10),
                    ]
                else:
                    rows = [
                        ("000001.SZ", "2025-01-01 15:00:00", 9, 9, 9, 9, 1, 9),
                    ]
                return _response(
                    api_name,
                    ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"),
                    rows,
                )
            if api_name == "adj_factor":
                return _response(
                    api_name,
                    ("ts_code", "trade_date", "adj_factor"),
                    [("000001.SZ", "20250102", 1.0)],
                )
            raise AssertionError(api_name)

    frame = tp.TushareProvider(client_factory=lambda _key: Client()).get_minute(
        ["000001.SZ"], datetime(2025, 1, 2, 9, 30), datetime(2025, 1, 2, 15)
    )

    assert frame["datetime"].to_list() == [
        datetime(2025, 1, 2, 9, 30),
        datetime(2025, 1, 2, 9, 31),
    ]


def test_daily_router_passes_etf_asset_type_to_tushare(monkeypatch):
    calls: list[dict] = []
    expected = pl.DataFrame({"symbol": ["510300.SH"]})

    class Provider:
        def get_daily(self, symbols, **kwargs):
            calls.append({"symbols": symbols, **kwargs})
            return expected

    from app.data_providers import custom as custom_sources
    from app.services import kline_sync

    monkeypatch.setattr(kline_sync.preferences, "get_daily_data_provider", lambda: "tushare")
    monkeypatch.setattr(custom_sources, "provider_has_dataset", lambda name, dataset: (name, dataset) == ("tushare", "daily"))
    monkeypatch.setattr(custom_sources, "get_provider", lambda _name: Provider())

    result = kline_sync.sync_daily_batch(
        ["510300.SH"],
        start_time=datetime(2025, 1, 2),
        end_time=datetime(2025, 1, 3),
        asset_type="etf",
    )

    assert result.equals(expected)
    assert calls[0]["asset_type"] == "etf"


@pytest.mark.parametrize(
    ("asset_type", "sync_name"),
    [("index", "sync_and_persist_index_daily"), ("etf", "sync_and_persist_etf_daily")],
)
def test_tushare_index_and_etf_daily_do_not_require_tickflow_capability(
    monkeypatch, tmp_path, asset_type, sync_name
):
    from app.services import index_sync, kline_sync
    from app.tickflow.capabilities import CapabilitySet

    calls: list[str] = []
    frame = pl.DataFrame(
        {
            "symbol": ["000001.SH" if asset_type == "index" else "510300.SH"],
            "date": [datetime(2025, 1, 2).date()],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1.0],
            "amount": [1.0],
        }
    )

    class Repo:
        store = SimpleNamespace(data_dir=tmp_path)

        def append_index_daily(self, _frame):
            calls.append("append_index_daily")

        def append_index_enriched(self, _frame):
            calls.append("append_index_enriched")

        def append_etf_daily(self, _frame):
            calls.append("append_etf_daily")

        def append_etf_enriched(self, _frame):
            calls.append("append_etf_enriched")

        def refresh_index_views(self):
            calls.append("refresh_index_views")

    def fetch(_symbols, **kwargs):
        calls.append(kwargs["asset_type"])
        return frame

    monkeypatch.setattr(index_sync.preferences, "get_daily_data_provider", lambda: "tushare")
    monkeypatch.setattr(index_sync.preferences, "get_index_daily_batch_size", lambda: 10)
    monkeypatch.setattr(kline_sync, "_provider_has_dataset", lambda _name, dataset: dataset == "daily")
    monkeypatch.setattr(kline_sync, "sync_daily_batch", fetch)
    monkeypatch.setattr(index_sync, "compute_enriched", lambda raw, **_kwargs: raw)

    written = getattr(index_sync, sync_name)(
        Repo(),
        CapabilitySet({}),
        symbols_override=frame["symbol"].to_list(),
        start_date=datetime(2025, 1, 2),
        end_date=datetime(2025, 1, 3),
    )

    assert written == 1
    assert asset_type in calls
    assert f"append_{asset_type}_daily" in calls


def test_tushare_provider_requires_key(monkeypatch):
    monkeypatch.setattr(tp, "load_tushare_key", lambda: "")
    with pytest.raises(TusharePermissionError, match="not configured"):
        tp.TushareProvider().get_daily(["000001.SZ"], None, None)
