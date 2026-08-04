from __future__ import annotations

from datetime import date
import asyncio

from app.services.large_order_service import LargeOrderService
from app.services.large_order_store import LargeOrderStore


class FakeQuoteService:
    def __init__(self):
        self.listeners = []
        self.events = 0

    def add_fetch_listener(self, callback):
        self.listeners.append(callback)

    def remove_fetch_listener(self, callback):
        self.listeners.remove(callback)

    def get_latest_quotes(self):
        return []

    def notify_large_orders_updated(self):
        self.events += 1

    def status(self):
        return {"quote_age_ms": 100, "interval_s": 6, "symbol_count": 1, "market_phase": "continuous", "is_trading_hours": True}


def test_snapshot_delta_ignores_amount_reset_and_builds_proxy_candidate():
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    service._process_snapshot([{"symbol": "000001.SZ", "name": "平安银行", "last_price": 10, "amount": 1_000_000, "volume": 100}])
    service._process_snapshot([{"symbol": "000001.SZ", "name": "平安银行", "last_price": 10.1, "amount": 3_000_000, "volume": 300}])
    ranking = service.ranking(60)
    assert ranking["count"] == 1
    assert ranking["rows"][0]["source"] == "tick_proxy"
    assert ranking["rows"][0]["confidence"] == "medium"
    assert ranking["rows"][0]["last_seen_ts"] is not None
    service._process_snapshot([{"symbol": "000001.SZ", "name": "平安银行", "last_price": 10, "amount": 100, "volume": 1}])
    assert service.ranking(60)["count"] == 0
    assert quote.events >= 2
    service.stop()


def test_snapshot_storage_only_keeps_effective_deltas(tmp_path, monkeypatch):
    quote = FakeQuoteService()
    storage = LargeOrderStore(tmp_path, flush_interval=0.01)
    storage.start()
    service = LargeOrderService(quote)
    service._storage = storage
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    monkeypatch.setattr("app.services.large_order_service.cn_today", lambda: date(2026, 8, 4))

    base = {"symbol": "000001.SZ", "name": "平安银行", "last_price": 10, "amount": 1_000_000, "volume": 100}
    service._process_snapshot([base])
    service._process_snapshot([base])
    service._process_snapshot([{**base, "last_price": 10.1, "amount": 2_000_000, "volume": 200}])
    storage.flush_now()

    result = storage.query("proxy_flow", date(2026, 8, 4))
    assert result["count"] == 1
    assert result["rows"][0]["delta_amount"] == 1_000_000
    assert result["rows"][0]["buy_amount"] == 1_000_000
    service.stop()


def test_deep_dive_stores_parsed_events_and_raw_payloads(tmp_path, monkeypatch):
    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, endpoint, _params):
            if endpoint == 13:
                return {"List": [[2, 1_754_280_660, 100, 12_000, 12.0, "09:31:00"]]}
            return {"List": [["09:31:01", "order-1", 12.0, 100, 12_000, 1, "", 0, 1, 1_754_280_661]]}

    quote = FakeQuoteService()
    storage = LargeOrderStore(tmp_path, flush_interval=0.01)
    storage.start()
    service = LargeOrderService(quote)
    service._storage = storage
    service._trade_date = date(2026, 8, 4)
    monkeypatch.setattr("app.services.large_order_service.load_credentials", lambda: object())
    monkeypatch.setattr("app.services.large_order_service.KaipanlaClient", lambda **_kwargs: FakeClient())

    asyncio.run(service._deep_dive_async("000001.SZ"))
    storage.flush_now()

    trades = storage.query("kaipanla_trade", date(2026, 8, 4))
    intents = storage.query("kaipanla_intent", date(2026, 8, 4))
    assert trades["count"] == 1
    assert trades["rows"][0]["direction"] == "active_buy"
    assert intents["count"] == 1
    assert intents["rows"][0]["cancel_flag"] is True
    raw_files = list((tmp_path / "ext_data" / "_kaipanla_raw").glob("snapshot=*/large_order_*/*.json.gz"))
    assert len(raw_files) == 2
    storage.stop()
