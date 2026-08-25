import json

from app.services.limit_up_queue import (
    LimitUpQueueService,
    _D202Watcher,
    _D202WebSocketSource,
    _watcher_url,
    d202_code,
    watcher_snapshot,
)


def test_watcher_url_defaults_to_d202_and_allows_future_switches():
    assert _watcher_url(None) == "ws://127.0.0.1:8080/d202"
    assert _watcher_url("ws://localhost:9000/d203") == "ws://localhost:9000/d203"


def test_d202_code_uses_market_prefix():
    assert d202_code("600000.SH") == "SH600000"
    assert d202_code("000001.SZ") == "SZ000001"
    assert d202_code("430001.BJ") is None


def test_builtin_d202_watcher_tracks_front_and_back_queue():
    watcher = _D202Watcher("SH600000", 11000)
    captured = []
    watcher.on_tick(lambda current: captured.append(watcher_snapshot(current)))
    watcher.feed_queue({
        "totalCount": 2,
        "volumes": [200, 300],
    }, 1_000)
    watcher.queue(100)
    watcher.feed_queue({"totalCount": 4, "volumes": [200, 300, 100, 50]}, 2_000)

    snapshot = watcher_snapshot(watcher)
    assert snapshot["current"]["volume"] == 650
    assert snapshot["order_status"] == "queueing"
    assert snapshot["order"]["front"]["volume"] == 500
    assert snapshot["order"]["back"]["volume"] == 50
    assert snapshot["order"]["elapsed_ms"] == 1_000
    assert snapshot["cancelled"]["volume"] == 0

    watcher.feed_queue({"totalCount": 3, "volumes": [200, 100, 50]}, 3_000)
    snapshot = captured[-1]
    assert snapshot["order"]["front"]["volume"] == 200
    assert snapshot["order"]["back"]["volume"] == 50
    assert snapshot["cancelled"]["volume"] == 300


def test_d202_source_subscribes_queue_channel():
    class FakeWebSocket:
        def __init__(self):
            self.messages = []

        def send(self, message):
            self.messages.append(message)

    source = _D202WebSocketSource("ws://127.0.0.1:8080/d202")
    source.add_watcher(_D202Watcher("SH600000", 11000))
    ws = FakeWebSocket()
    source._on_open(ws)
    assert ws.messages == [
        '[{"type": "queue", "code": "SH600000", "enable": 1, "dir": "B", "level": 0}]'
    ]


def test_d202_source_routes_queue_response_to_watcher():
    source = _D202WebSocketSource("ws://127.0.0.1:8080/d202")
    watcher = _D202Watcher("SH600000", 11000)
    source.add_watcher(watcher)
    source._on_message(None, json.dumps({
        "ts": 1000,
        "list": [{"type": "queue", "data": {
            "code": "SH600000",
            "dir": "B",
            "totalCount": 2,
            "batchCount": 2,
            "volumes": [200, 300],
        }}],
    }))
    assert watcher.current.count == 2
    assert watcher.current.volume == 500


def test_service_uses_builtin_d202_protocol_adapter():
    service = LimitUpQueueService()
    service.start()
    assert service._watcher_factory is _D202Watcher
    assert service._source_factory is _D202WebSocketSource


def test_service_isolates_source_and_registers_queue_order():
    class FakeWatcher(_D202Watcher):
        pass

    class FakeSource:
        def __init__(self, url):
            self.url = url
            self.watchers = []
            self.connected = False

        def add_watcher(self, watcher):
            self.watchers.append(watcher)

        def connect(self, block=False):
            self.connected = True

        def disconnect(self):
            self.connected = False

    service = LimitUpQueueService(
        watcher_factory=FakeWatcher,
        source_factory=FakeSource,
    )
    service.start()
    assert service.status()["url"] == "ws://127.0.0.1:8080/d202"
    service.sync({
        "600000.SH": {
            "limit_up": 11.0,
            "queue_key": "limit-board-20260825-600000.SH",
            "queue_volume": 100,
        },
    })

    assert service.status()["state"] == "connecting"
    assert service.status()["symbols"] == 1
    assert service._watchers["600000.SH"].my_orders[0].hand_count == 1

    service.stop()
    assert service.status()["symbols"] == 0
