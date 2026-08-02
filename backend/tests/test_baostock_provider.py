from __future__ import annotations

from datetime import datetime

import pytest

from app.data_providers.baostock_provider import BaoStockProvider
from app.services import kline_sync


class _LoginResult:
    error_code = "0"
    error_msg = ""


class _BaoStockResult:
    error_code = "0"
    error_msg = ""

    def __init__(self, rows: list[list[str]]) -> None:
        self.fields = [
            "date", "time", "code", "open", "high", "low", "close",
            "volume", "amount", "adjustflag",
        ]
        self._rows = rows
        self._idx = -1

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._idx]


class _FakeBaoStock:
    def __init__(self) -> None:
        self.queries: list[dict] = []
        self.logout_count = 0

    def login(self) -> _LoginResult:
        return _LoginResult()

    def logout(self) -> None:
        self.logout_count += 1

    def query_history_k_data_plus(self, code, fields, **kwargs):
        self.queries.append({"code": code, "fields": fields, **kwargs})
        return _BaoStockResult([
            [
                "2020-01-03", "20200103093500000", code,
                "10.1", "10.3", "10.0", "10.2", "1000", "10200", "2",
            ],
            [
                "2020-01-03", "09:40:00", code,
                "10.2", "10.5", "10.1", "10.4", "2000", "20800", "2",
            ],
        ])


def test_baostock_provider_normalizes_supported_coarse_minute_rows():
    fake = _FakeBaoStock()
    provider = BaoStockProvider(bs_module=fake)
    progress: list[tuple[int, int]] = []

    df = provider.get_minute(
        ["600000.SH", "000001.SZ"],
        start_time=datetime(2020, 1, 3),
        end_time=datetime(2020, 1, 4),
        asset_type="stock",
        freq="5m",
        on_chunk_done=lambda current, total: progress.append((current, total)),
    )

    assert fake.logout_count == 1
    assert [query["code"] for query in fake.queries] == ["sh.600000", "sz.000001"]
    assert {query["frequency"] for query in fake.queries} == {"5"}
    assert {query["adjustflag"] for query in fake.queries} == {"2"}
    assert {query["start_date"] for query in fake.queries} == {"2020-01-03"}
    assert {query["end_date"] for query in fake.queries} == {"2020-01-04"}
    assert progress == [(1, 2), (2, 2)]
    assert df.columns == [
        "symbol", "datetime", "open", "high", "low", "close", "volume", "amount",
    ]
    assert df["symbol"].to_list() == [
        "000001.SZ", "000001.SZ", "600000.SH", "600000.SH",
    ]
    assert df["datetime"].to_list() == [
        datetime(2020, 1, 3, 9, 35),
        datetime(2020, 1, 3, 9, 40),
        datetime(2020, 1, 3, 9, 35),
        datetime(2020, 1, 3, 9, 40),
    ]
    assert df["close"].to_list() == [10.2, 10.4, 10.2, 10.4]


def test_baostock_provider_rejects_unsupported_frequency_and_asset_type():
    provider = BaoStockProvider(bs_module=_FakeBaoStock())

    assert provider.supports_minute_freq("5m")
    assert not provider.supports_minute_freq("1m")
    with pytest.raises(ValueError, match="5m/15m/30m/60m"):
        provider.get_minute(["600000.SH"], None, None, freq="1m")
    with pytest.raises(ValueError, match="stock symbols only"):
        provider.get_minute(["510300.SH"], None, None, asset_type="etf", freq="5m")


def test_builtin_baostock_minute_resolver_respects_frequency_boundary():
    provider, fallback, error = kline_sync._resolve_minute_provider("baostock", freq="5m")
    assert error is None
    assert fallback is False
    assert provider.name == "baostock"

    provider, fallback, error = kline_sync._resolve_minute_provider("baostock", freq="1m")
    assert error is None
    assert fallback is True
    assert provider is None
