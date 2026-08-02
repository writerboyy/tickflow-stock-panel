from __future__ import annotations

from datetime import date
import sys
from types import SimpleNamespace

from app.services import backup_data_sources


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Client:
    def __init__(self, responses, captured, **_kwargs):
        self.responses = iter(responses)
        self.captured = captured

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def get(self, url, params):
        self.captured.append((url, params))
        return _Response(next(self.responses))


def test_baostock_index_volume_is_normalized_from_shares_to_lots(monkeypatch):
    class Query:
        error_code = "0"

        def __init__(self):
            self.done = False

        def next(self):
            self.done = not self.done
            return self.done

        def get_row_data(self):
            return ["2026-07-30", "10", "12", "9", "11", "1200", "3456"]

    fake = SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0"),
        logout=lambda: None,
        query_history_k_data_plus=lambda *_args, **_kwargs: Query(),
    )
    monkeypatch.setitem(sys.modules, "baostock", fake)

    result = backup_data_sources.fetch_baostock_index_daily(
        ["000001.SH"], date(2026, 7, 30), date(2026, 7, 30)
    )

    assert result["volume"].to_list() == [12.0]
    assert result["amount"].to_list() == [3456.0]


def test_astock_data_eastmoney_normalizes_full_index_bar(monkeypatch):
    captured = []
    payload = {
        "data": {
            "klines": ["2026-07-30,10.1,10.5,10.8,9.9,1234,567890.00"]
        }
    }
    monkeypatch.setattr(
        backup_data_sources.httpx,
        "Client",
        lambda **kwargs: _Client([payload], captured, **kwargs),
    )

    result = backup_data_sources.fetch_astock_data_eastmoney_index_daily(
        ["000691.SH"], date(2026, 7, 30), date(2026, 7, 30)
    )

    assert result.to_dicts() == [{
        "symbol": "000691.SH",
        "date": date(2026, 7, 30),
        "open": 10.1,
        "close": 10.5,
        "high": 10.8,
        "low": 9.9,
        "volume": 1234.0,
        "amount": 567890.0,
    }]
    assert captured[0][1]["secid"] == "1.000691"
    assert captured[0][1]["fqt"] == "0"


def test_astock_data_tencent_keeps_amount_absent(monkeypatch):
    captured = []
    payload = {
        "data": {
            "sh000902": {
                "day": [["2026-07-30", "10.1", "10.5", "10.8", "9.9", "1234"]]
            }
        }
    }
    monkeypatch.setattr(
        backup_data_sources.httpx,
        "Client",
        lambda **kwargs: _Client([payload], captured, **kwargs),
    )

    result = backup_data_sources.fetch_astock_data_tencent_index_daily(
        ["000902.SH"], date(2026, 7, 30), date(2026, 7, 30)
    )

    assert result.columns == ["symbol", "date", "open", "close", "high", "low", "volume"]
    assert result["volume"].to_list() == [1234.0]
    assert "amount" not in result.columns
    assert captured[0][1]["param"].startswith("sh000902,day,2026-07-30,2026-07-30")


def test_astock_data_baidu_uses_index_group_and_normalizes_volume(monkeypatch):
    captured = []
    payload = {
        "Result": {
            "newMarketData": {
                "keys": ["time", "open", "close", "volume", "high", "low", "amount"],
                "marketData": "2026-07-30,10.1,10.5,123400,10.8,9.9,567890.00",
            }
        }
    }
    monkeypatch.setattr(
        backup_data_sources.httpx,
        "Client",
        lambda **kwargs: _Client([payload], captured, **kwargs),
    )

    result = backup_data_sources.fetch_astock_data_baidu_index_daily(
        ["000985.SH"], date(2026, 7, 30), date(2026, 7, 30)
    )

    assert result["volume"].to_list() == [1234.0]
    assert result["amount"].to_list() == [567890.0]
    assert captured[0][1]["group"] == "quotation_index_kline"
    assert captured[0][1]["isIndex"] == "true"
