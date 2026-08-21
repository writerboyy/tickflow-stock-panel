from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

from app.services.tick_latency_probe import (
    QmtWholeQuoteSource,
    _quote_address,
    run_latency_probe,
    summarize_observations,
)


def test_quote_address_uses_rpc_host_and_push_port():
    assert _quote_address(
        "tcp://qmt.example:15648", advertised="tcp://0.0.0.0:15649",
    ) == "tcp://qmt.example:15649"
    assert _quote_address(
        "tcp://qmt.example:15648", explicit="tcp://quotes.example:17000",
    ) == "tcp://quotes.example:17000"


def test_qmt_whole_quote_source_only_uses_read_only_subscription_methods(monkeypatch):
    stop = threading.Event()
    received = []

    class Client:
        connect_address = "tcp://qmt.example:15648"

        def __init__(self):
            self.calls = []
            self.closed = False

        def call(self, method, params=None):
            self.calls.append((method, params))
            if method == "subscribe_whole_quote":
                return {
                    "topic": "SH",
                    "push_endpoint": "tcp://0.0.0.0:15649",
                }
            return {}

        def close(self):
            self.closed = True

    class Socket:
        def setsockopt(self, *_args):
            pass

        def connect(self, address):
            assert address == "tcp://qmt.example:15649"

        def recv_multipart(self):
            return [b"SH", json.dumps({
                "data": {
                    "600000.SH": {
                        "time": "20240801093000",
                        "lastPrice": 10,
                        "volume": 100,
                        "amount": 1000,
                    },
                },
            }).encode()]

        def close(self, linger=0):
            assert linger == 0

    socket = Socket()

    class Poller:
        def register(self, *_args):
            pass

        def poll(self, _timeout):
            return [(socket, 1)]

    fake_zmq = SimpleNamespace(
        SUB=1,
        SUBSCRIBE=2,
        LINGER=3,
        POLLIN=1,
        Context=SimpleNamespace(instance=lambda: SimpleNamespace(socket=lambda _kind: socket)),
        Poller=Poller,
    )
    monkeypatch.setitem(sys.modules, "zmq", fake_zmq)
    client = Client()
    source = QmtWholeQuoteSource(client, ["600000.SH"])

    def callback(row):
        received.append(row)
        stop.set()

    source.collect(callback, stop)

    assert received[0]["symbol"] == "600000.SH"
    assert [method for method, _params in client.calls] == [
        "subscribe_whole_quote", "unsubscribe_whole_quote",
    ]
    assert client.closed is True


def test_summary_compares_matched_observer_arrivals_without_clock_claims():
    base = {
        "symbol": "600000.SH",
        "source_timestamp": "2024-08-01T09:30:00+08:00",
        "observer_wall_timestamp": "2024-08-01T09:30:01+08:00",
        "price": 10.0,
        "volume": 100.0,
        "amount": 1000.0,
        "event_id": "600000.SH|1",
        "queue_delay_ms": 0.1,
        "strategy_processing_delay_ms": 0.2,
    }
    report = summarize_observations([
        {**base, "source": "qmt", "observer_monotonic_timestamp": 10.002},
        {**base, "source": "tickflow", "observer_monotonic_timestamp": 10.001},
    ], clocks_synchronized=False)

    assert report["comparison"]["clock_basis"] == "observer_match_only"
    assert report["comparison"]["matched_events"] == 1
    assert report["comparison"]["effective_coverage"] == 1
    assert report["sources"]["qmt"]["source_to_observer_delay_ms"] is None


def test_probe_reports_source_that_exits_before_observation_window(tmp_path: Path):
    class Source:
        @staticmethod
        def collect(_callback, _stop):
            return None

    report = run_latency_probe(
        {"qmt": Source()},
        duration_seconds=60,
        output_dir=tmp_path,
    )

    assert report["errors"] == {"qmt": "行情源在观测结束前提前退出"}
    assert (tmp_path / "observations.jsonl").exists()
    assert (tmp_path / "report.json").exists()
