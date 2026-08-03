from __future__ import annotations

from app.services.large_order_service import LargeOrderService


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
    service._process_snapshot([{"symbol": "000001.SZ", "name": "平安银行", "last_price": 10, "amount": 100, "volume": 1}])
    assert service.ranking(60)["count"] == 0
    assert quote.events >= 2
    service.stop()
