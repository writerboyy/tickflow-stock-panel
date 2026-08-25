from app.services.limit_up_queue import (
    LimitUpQueueService,
    _D202Watcher,
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
    watcher.feed({
        "isFirst": 1,
        "totalCount": 2,
        "totalVolume": 150,
        "seq": 0,
        "records": [{"id": 10, "volume": 100}, {"id": 20, "volume": 50}],
    }, 1_000)
    watcher.queue(25)
    watcher.feed({
        "isFirst": 0,
        "records": [
            {"id": 30, "volume": 25, "status": 64},
            {"id": 40, "volume": 10, "status": 64},
        ],
    }, 2_000)

    snapshot = watcher_snapshot(watcher)
    assert snapshot["current"]["volume"] == 185
    assert snapshot["order_status"] == "queueing"
    assert snapshot["order"]["front"]["volume"] == 150
    assert snapshot["order"]["back"]["volume"] == 10
    assert snapshot["order"]["elapsed_ms"] == 1_000


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
