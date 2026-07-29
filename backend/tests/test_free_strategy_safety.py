from __future__ import annotations

import multiprocessing as mp
import queue
import time
from datetime import datetime
from types import SimpleNamespace

import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.paper import PaperTradingSupervisor, _equity_snapshot, _persist_engine_state
from app.free_strategy.process import start_process
from app.free_strategy.store import PaperAccountStore
from app.market_time import CN_TZ, as_cn_naive, cn_naive_from_timestamp


def _append_same_event(data_dir: str) -> None:
    PaperAccountStore(data_dir).append_event_once(
        "paper",
        {"id": "same-event", "type": "signal"},
    )


def _timeout_payload(tmp_path, source: str) -> dict:
    return {
        "data_dir": str(tmp_path),
        "source": source,
        "start": "2024-01-02",
        "end": "2024-01-02",
        "asset_type": "stock",
        "timeframe": "1d",
        "symbols": [],
        "config": {
            "callback_timeout_seconds": 0.2,
        },
    }


def _wait_for_error(process, output) -> str:
    deadline = time.monotonic() + 5
    error = ""
    while time.monotonic() < deadline:
        try:
            event = output.get(timeout=0.2)
        except queue.Empty:
            continue
        if event.get("type") == "error":
            error = str(event.get("error") or "")
            break
    process.join(timeout=2)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
    return error


@pytest.mark.parametrize(
    "source",
    [
        "while True:\n    pass\n",
        "def initialize(context):\n    while True:\n        pass\n\ndef on_bar(context, bars):\n    pass\n",
    ],
)
def test_backtest_watchdog_terminates_strategy_initialization_timeout(tmp_path, source):
    process, output = start_process(_timeout_payload(tmp_path, source))

    error = _wait_for_error(process, output)

    assert "超过 0.2 秒" in error
    assert not process.is_alive()


def test_capacity_analysis_is_diagnostic_only_and_does_not_cap_fill():
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    context.buy('X', quantity=500)\n",
        config=FreeStrategyConfig(
            initial_capital=10_000,
            fees_pct=0,
            slippage_bps=0,
            lot_size=1,
            fill_policy="close",
        ),
    )

    result = engine.run([
        Bar(
            "X",
            datetime(2024, 1, 2, 15),
            10,
            10,
            10,
            10,
            volume=1_000,
            amount=10_000,
        ),
    ])

    assert result["fills"][0]["quantity"] == 500
    assert result["fills"][0]["price"] == 10
    assert result["fills"][0]["participation_pct"] == 50
    assert result["capacity_analysis"] == {
        "model": "bar_volume_participation",
        "diagnostic_only": True,
        "total_fills": 1,
        "covered_fills": 1,
        "max_participation_pct": 50,
        "p95_participation_pct": 50,
        "fills_over_1_pct": 1,
        "fills_over_5_pct": 1,
        "fills_over_10_pct": 1,
    }


def test_market_timestamp_conversion_is_explicitly_shanghai_wall_time():
    assert cn_naive_from_timestamp(0) == datetime(1970, 1, 1, 8)
    assert as_cn_naive(datetime(2026, 7, 29, 1, 30, tzinfo=CN_TZ)) == datetime(2026, 7, 29, 1, 30)
    assert as_cn_naive(datetime.fromisoformat("2026-07-28T17:30:00+00:00")) == datetime(2026, 7, 29, 1, 30)


def test_event_ledger_migrates_jsonl_once_and_is_multiprocess_idempotent(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped"})
    ledger = store._path("paper") / "ledger.jsonl"
    ledger.write_text(
        '{"id":"legacy","timestamp":"2026-07-28T10:00:00","type":"created","sequence":7}\n',
        encoding="utf-8",
    )

    context = mp.get_context("spawn")
    workers = [context.Process(target=_append_same_event, args=(str(tmp_path),)) for _ in range(6)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
        assert worker.exitcode == 0

    events = store.events("paper")
    assert [(event["id"], event["sequence"]) for event in events] == [
        ("legacy", 7),
        ("same-event", 8),
    ]
    assert store.events("paper") == events
    assert ledger.read_text(encoding="utf-8").count("legacy") == 1


def test_state_update_and_engine_persist_preserve_concurrent_account_fields(tmp_path):
    store = PaperAccountStore(tmp_path)
    stale = store.save({
        "id": "paper",
        "name": "old",
        "status": "running",
        "config": {"initial_capital": 100},
        "equity_peak": 100,
    })
    store.update_fields("paper", {"name": "new", "notification_channels": ["feishu"]})
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    pass\n",
        config=FreeStrategyConfig(initial_capital=100),
    )

    saved = _persist_engine_state(store, "paper", stale, engine, [])

    assert saved["name"] == "new"
    assert saved["notification_channels"] == ["feishu"]


def test_max_drawdown_is_monotonic_when_curve_rows_expire(tmp_path):
    store = PaperAccountStore(tmp_path)
    state = store.save({
        "id": "paper",
        "status": "running",
        "config": {"initial_capital": 100},
        "equity_peak": 120,
    })
    store.replace_equity_curve("paper", [{
        "timestamp": "2025-01-01T15:00:00",
        "equity": 90,
        "cash": 90,
        "nav": 0.9,
        "drawdown_pct": 25,
        "positions": {},
        "source": "backtest",
    }])
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    pass\n",
        config=FreeStrategyConfig(initial_capital=100),
    )
    engine.account.cash = 108

    row = _equity_snapshot(engine, state, datetime(2026, 7, 29, 15))
    saved = _persist_engine_state(store, "paper", state, engine, [row])

    assert row["drawdown_pct"] == 10
    assert saved["max_drawdown_pct"] == 25


def test_paper_supervisor_pauses_worker_when_strategy_deadline_expires(tmp_path):
    class FakeProcess:
        alive = True
        terminated = False

        def is_alive(self):
            return self.alive

        def terminate(self):
            self.alive = False
            self.terminated = True

        def join(self, timeout):
            return None

    class FakeHub:
        def unregister(self, _account_id):
            return None

    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running", "config": {}})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.store = store
    supervisor.hub = FakeHub()
    supervisor._lock = __import__("threading").RLock()  # noqa: SLF001
    process = FakeProcess()
    supervisor._processes = {"paper": process}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue()}  # noqa: SLF001
    supervisor._deadlines = {  # noqa: SLF001
        "paper": SimpleNamespace(value=time.monotonic() - 1),
    }

    supervisor._monitor_once()  # noqa: SLF001

    saved = store.get("paper")
    assert process.terminated is True
    assert saved["status"] == "paused"
    assert "超过" in saved["last_error"]
