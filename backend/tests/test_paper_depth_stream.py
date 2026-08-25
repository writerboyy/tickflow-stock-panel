import queue
from types import SimpleNamespace

from app.free_strategy.paper import MarketDataHub
from app.services.depth_service import DepthService
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet


class _QuoteService:
    def __init__(self, app_state):
        self._app_state = app_state
        self.symbol_consumers = {}

    def set_symbol_consumer(self, consumer_id, symbols):
        self.symbol_consumers[consumer_id] = set(symbols)

    def remove_symbol_consumer(self, consumer_id):
        self.symbol_consumers.pop(consumer_id, None)

    def record_quotes(self, _records):
        pass

    def status(self):
        return {"interval_s": None, "fetch_ms": None}

    def get_min_interval(self):
        return 6.0


class _Stream:
    instance = None

    def __init__(self, _client):
        self.subscriptions = []
        self.depth_handler = None
        _Stream.instance = self

    def on_quotes(self, _handler):
        pass

    def on_depth(self, handler):
        self.depth_handler = handler

    def on_error(self, _handler):
        pass

    def subscribe(self, channel, symbols):
        self.subscriptions.append((channel, tuple(symbols)))

    def unsubscribe(self, channel, symbols):
        self.subscriptions.remove((channel, tuple(symbols)))

    def connect(self, *, block):
        assert block is False

    def close(self):
        pass


def test_depth_stream_updates_shared_orderbook_cache(monkeypatch):
    depth = DepthService()
    state = SimpleNamespace(
        capabilities=CapabilitySet({Cap.DEPTH5: CapabilityLimits()}),
        depth_service=depth,
    )
    service = _QuoteService(state)
    client = SimpleNamespace(_client=object())
    monkeypatch.setattr("app.tickflow.client.get_paid_realtime_client", lambda: client)
    monkeypatch.setattr("tickflow.resources.stream.MarketStream", _Stream)

    hub = MarketDataHub(service, repo=None)
    hub.register("paper", "websocket", {"600000.SH"}, "stock", queue.Queue())
    stream = _Stream.instance

    assert stream.subscriptions == [
        ("quotes", ("600000.SH",)),
        ("depth", ("600000.SH",)),
    ]
    stream.depth_handler([
        {
            "symbol": "600000.SH",
            "timestamp": 1_750_000_000_000,
            "bid_prices": [10.0],
            "bid_volumes": [100],
            "ask_prices": [10.1],
            "ask_volumes": [50],
        },
    ])

    cached = depth.get_cached_orderbooks({"600000.SH"})["600000.SH"]
    assert cached["bid_volumes"] == [100.0]
    assert cached["book_imbalance"] == 1 / 3
    assert hub.status()["websocket"]["depth_symbols"] == 1


def test_depth_stream_is_not_subscribed_without_depth_capability(monkeypatch):
    state = SimpleNamespace(capabilities=CapabilitySet())
    service = _QuoteService(state)
    client = SimpleNamespace(_client=object())
    monkeypatch.setattr("app.tickflow.client.get_paid_realtime_client", lambda: client)
    monkeypatch.setattr("tickflow.resources.stream.MarketStream", _Stream)

    hub = MarketDataHub(service, repo=None)
    hub.register("paper", "websocket", {"600000.SH"}, "stock", queue.Queue())

    assert _Stream.instance.subscriptions == [("quotes", ("600000.SH",))]
    assert hub.status()["websocket"]["depth_supported"] is True
    assert hub.status()["websocket"]["depth_symbols"] == 0
