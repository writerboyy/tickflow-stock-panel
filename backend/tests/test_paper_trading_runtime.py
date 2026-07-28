import queue
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import polars as pl
import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine, Quote, RiskConfig
from app.free_strategy.paper import MarketDataHub, PaperTradingSupervisor, _equity_snapshot
from app.free_strategy.store import PaperAccountStore
from app.services.quote_service import QuoteService


def quote(second: int, price: float) -> Quote:
    return Quote("X", datetime(2024, 1, 2, 9, 30, second), price, prev_close=10, open=10, high=max(10, price), low=min(10, price))


def test_on_quote_current_and_next_quote_fill_rules():
    source = """
def on_quote(context, quotes):
    if not context.state.get('ordered'):
        context.state['ordered'] = True
        context.buy('X', quantity=10)
"""
    current = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(initial_capital=1_000, lot_size=1, fees_pct=0, slippage_bps=0, fill_policy="close", settlement="t0"),
    )
    current.process_quotes([quote(0, 10)])
    assert current.account.fills[0].price == 10

    following = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(initial_capital=1_000, lot_size=1, fees_pct=0, slippage_bps=0, fill_policy="next_open"),
    )
    following.process_quotes([quote(0, 10)])
    assert following.account.fills == []
    following.process_quotes([quote(3, 11)])
    assert following.account.fills[0].price == 11


def test_quote_mapping_and_quote_values_are_read_only():
    source = """
def on_quote(context, quotes):
    try:
        quotes['X'] = None
    except TypeError:
        context.state['mapping_readonly'] = True
    try:
        quotes['X'].last_price = 99
    except Exception:
        context.state['value_readonly'] = True
"""
    engine = FreeStrategyEngine(source)
    engine.process_quotes([quote(0, 10)])
    assert engine.state == {"mapping_readonly": True, "value_readonly": True}


def test_recovery_snapshot_updates_valuation_without_callback():
    source = """
def on_quote(context, quotes):
    context.state['called'] = True
"""
    engine = FreeStrategyEngine(source)
    engine.account.cash = 0
    engine.account.positions = {"X": 10}
    engine.preload_quote_snapshot([quote(0, 12)])

    assert engine.state == {}
    assert engine.callbacks_executed == 0
    assert engine.context.portfolio.total_value == 120


def test_daily_loss_risk_cancels_buys_but_allows_reducing_orders():
    source = """
def on_quote(context, quotes):
    calls = context.state.get('calls', 0)
    context.state['calls'] = calls + 1
    if calls == 0:
        context.buy('X', quantity=50)
    else:
        context.buy('X', quantity=1)
        context.sell('X', quantity=1)
"""
    engine = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_000, lot_size=1, fees_pct=0, slippage_bps=0,
            fill_policy="close", settlement="t0",
        ),
        risk_config=RiskConfig(daily_loss_pct=0.1, max_drawdown_pct=0.3),
    )
    engine.process_quotes([quote(0, 10)])
    engine.process_quotes([quote(3, 5)])

    assert engine.risk_status["daily_loss_locked"] is True
    assert engine.account.orders[-2].status == "rejected"
    assert engine.account.orders[-1].status == "filled"


def test_order_rate_limit_still_allows_reducing_orders():
    source = """
def on_quote(context, quotes):
    calls = context.state.get('calls', 0)
    context.state['calls'] = calls + 1
    if calls == 0:
        context.buy('X', quantity=10)
    else:
        context.sell('X', quantity=1)
"""
    engine = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_000, lot_size=1, fees_pct=0, slippage_bps=0,
            fill_policy="close", settlement="t0",
        ),
        risk_config=RiskConfig(max_orders_per_minute=1),
    )
    engine.process_quotes([quote(0, 10)])
    engine.process_quotes([quote(3, 10)])

    assert engine.account.orders[-1].status == "filled"
    assert engine.account.positions["X"] == 9


class FakeQuoteService:
    def __init__(self):
        self.listener = None
        self.acquired = 0
        self.released = 0
        self.requested_assets = []
        self.frame = pl.DataFrame([
            {"symbol": "A", "last_price": 10.0, "prev_close": 9.0},
            {"symbol": "B", "last_price": 20.0, "prev_close": 19.0},
        ])

    def add_fetch_listener(self, callback):
        self.listener = callback

    def remove_fetch_listener(self, callback):
        assert callback == self.listener
        self.listener = None

    def acquire_temporary_polling(self, interval):
        assert interval == 3
        self.acquired += 1

    def release_temporary_polling(self):
        self.released += 1

    def get_quotes_compat(self, _asset_type="stock"):
        self.requested_assets.append(_asset_type)
        return self.frame

    def status(self):
        return {"interval_s": 3, "fetch_ms": 120}


def test_poll_accounts_share_one_feed_and_receive_filtered_quotes():
    service = FakeQuoteService()
    hub = MarketDataHub(service, repo=None)
    first = queue.Queue(maxsize=2)
    second = queue.Queue(maxsize=2)
    hub.register("first", "poll_3s", {"A"}, "stock", first)
    hub.register("second", "poll_3s", {"B"}, "stock", second)

    assert service.acquired == 1
    service.listener()
    assert set(service.requested_assets) == {"stock", "etf"}
    assert [row["symbol"] for row in first.get_nowait()["quotes"]] == ["A"]
    assert [row["symbol"] for row in second.get_nowait()["quotes"]] == ["B"]

    hub.unregister("first")
    assert service.released == 0
    hub.unregister("second")
    assert service.released == 1


def test_websocket_account_is_rejected_above_deduplicated_limit():
    hub = MarketDataHub(FakeQuoteService(), repo=None)
    with pytest.raises(ValueError, match="最多 100"):
        hub.register("too-many", "websocket", {f"S{index}" for index in range(101)}, "stock", queue.Queue())


def test_websocket_registration_failure_rolls_back_subscription(monkeypatch):
    monkeypatch.setattr("app.tickflow.client.get_paid_realtime_client", lambda: None)
    hub = MarketDataHub(FakeQuoteService(), repo=None)

    with pytest.raises(ValueError, match="TickFlow Key"):
        hub.register("paper", "websocket", {"A"}, "stock", queue.Queue())

    assert "paper" not in hub._subscriptions  # noqa: SLF001
    assert hub._stream is None  # noqa: SLF001


def test_supervisor_detaches_runtime_after_worker_pauses(tmp_path):
    class FakeHub:
        def __init__(self):
            self.unregistered = []

        def unregister(self, account_id):
            self.unregistered.append(account_id)

    class FakeProcess:
        def __init__(self):
            self.alive = True
            self.terminated = False

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            pass

        def terminate(self):
            self.alive = False
            self.terminated = True

    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "paused"})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.store = store
    supervisor.hub = FakeHub()
    supervisor._lock = threading.RLock()  # noqa: SLF001
    process = FakeProcess()
    supervisor._processes = {"paper": process}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue(maxsize=2)}  # noqa: SLF001

    supervisor._monitor_once()  # noqa: SLF001

    assert supervisor.hub.unregistered == ["paper"]
    assert process.terminated is True
    assert supervisor._processes == {}  # noqa: SLF001


def test_websocket_reconnect_resubscribes_and_recovers_before_quotes(monkeypatch):
    class FakeQuotes:
        def get(self, symbols):
            return [{"symbol": symbol, "last_price": 10, "timestamp": "2024-01-02T09:30:00"} for symbol in symbols]

    client = type("Client", (), {"_client": object(), "quotes": FakeQuotes()})()
    monkeypatch.setattr("app.tickflow.client.get_paid_realtime_client", lambda: client)

    class FakeStream:
        instance = None

        def __init__(self, _client):
            self.subscribed = set()
            self.quote_handler = None
            self.error_handler = None
            FakeStream.instance = self

        def on_quotes(self, handler):
            self.quote_handler = handler

        def on_error(self, handler):
            self.error_handler = handler

        def subscribe(self, _channel, symbols):
            self.subscribed.update(symbols)

        def unsubscribe(self, _channel, symbols):
            self.subscribed.difference_update(symbols)

        def connect(self, *, block):
            assert block is False

        def close(self):
            pass

    monkeypatch.setattr("tickflow.resources.stream.MarketStream", FakeStream)

    def no_minute_data(*_args, **_kwargs):
        raise ValueError("no data")

    monkeypatch.setattr("app.free_strategy.process._read_rows", no_minute_data)
    hub = MarketDataHub(FakeQuoteService(), repo=None)
    first = queue.Queue(maxsize=2)
    second = queue.Queue(maxsize=2)
    hub.register("first", "websocket", {"A"}, "stock", first)
    hub.register("second", "websocket", {"B"}, "stock", second)
    stream = FakeStream.instance
    assert stream.subscribed == {"A", "B"}

    stream.error_handler("connection lost")
    assert first.get_nowait()["type"] == "gap"
    assert second.get_nowait()["type"] == "gap"
    stream.quote_handler([
        {"symbol": "A", "last_price": 10, "timestamp": "2024-01-02T09:30:03"},
        {"symbol": "B", "last_price": 20, "timestamp": "2024-01-02T09:30:03"},
    ])
    assert [first.get_nowait()["type"], first.get_nowait()["type"]] == ["recovery", "quotes"]
    assert [second.get_nowait()["type"], second.get_nowait()["type"]] == ["recovery", "quotes"]


def test_quote_poll_cycle_is_start_to_start_and_never_overlaps(monkeypatch):
    service = QuoteService()
    service._running = True
    service._enabled = True
    service._interval = 0.02
    starts = []
    active = 0
    max_active = 0

    monkeypatch.setattr(service, "_market_phase", lambda: "continuous")
    monkeypatch.setattr(service, "_should_fetch_for_phase", lambda _phase: True)

    def slow_fetch(*, final=False):
        nonlocal active, max_active
        starts.append(time.monotonic())
        active += 1
        max_active = max(max_active, active)
        time.sleep(0.04)
        active -= 1
        if len(starts) == 3:
            service._running = False
        return True

    monkeypatch.setattr(service, "_fetch_quotes", slow_fetch)
    thread = threading.Thread(target=service._poll_loop)
    thread.start()
    thread.join(timeout=1)

    assert max_active == 1
    assert len(starts) == 3
    assert all((right - left) < 0.065 for left, right in zip(starts, starts[1:]))


def test_checkpoint_is_versioned_and_events_are_cursor_paginated(tmp_path):
    store = PaperAccountStore(tmp_path)
    saved = store.save({"id": "paper", "status": "stopped"})
    assert saved["schema_version"] == 2
    assert not list((tmp_path / "paper_accounts" / "paper").glob("*.tmp"))
    for index in range(5):
        store.append_event("paper", {"type": "log", "message": str(index)})

    first = store.events_page("paper", limit=2)
    second = store.events_page("paper", cursor=first["next_cursor"], limit=2)
    assert [row["message"] for row in first["events"]] == ["4", "3"]
    assert [row["message"] for row in second["events"]] == ["2", "1"]
    assert len({row["id"] for row in store.events("paper")}) == 5


def test_paper_equity_curve_is_upserted_and_limited_to_recent_year(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped"})
    now = datetime.now()
    recent = (now - timedelta(days=2)).replace(microsecond=0).isoformat()
    stale = (now - timedelta(days=370)).replace(microsecond=0).isoformat()
    store.replace_equity_curve("paper", [
        {"timestamp": stale, "equity": 90, "cash": 90, "nav": 0.9,
         "drawdown_pct": 10, "positions": {}, "source": "backtest"},
        {"timestamp": recent, "equity": 100, "cash": 100, "nav": 1,
         "drawdown_pct": 0, "positions": {}, "source": "backtest"},
    ])
    store.upsert_equity_curve("paper", [
        {"timestamp": recent, "equity": 110, "cash": 10, "nav": 1.1,
         "drawdown_pct": 0, "positions": {"X": 10}, "source": "paper"},
    ])

    assert store.equity_curve("paper") == [{
        "timestamp": recent,
        "equity": 110.0,
        "cash": 10.0,
        "nav": 1.1,
        "drawdown_pct": 0.0,
        "positions": {"X": 10},
    }]
    with sqlite3.connect(tmp_path / "paper_accounts" / "paper" / "equity.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM equity_curve").fetchone()[0] == 1


def test_equity_snapshot_uses_continuous_peak_and_nav():
    engine = FreeStrategyEngine(
        "def initialize(context):\n    context.set_universe(['X'])\n"
        "def on_bar(context, bars):\n    pass\n",
        config=FreeStrategyConfig(initial_capital=100),
    )
    engine.account.cash = 20
    engine.account.positions = {"X": 10}
    engine.account.avg_cost = {"X": 8}
    engine._current_close_prices = {"X": 8}  # noqa: SLF001
    state = {"equity_peak": 120}

    row = _equity_snapshot(engine, state, datetime(2026, 7, 28, 9, 30))

    assert row["equity"] == 100
    assert row["nav"] == 1
    assert row["drawdown_pct"] == pytest.approx(100 / 6)
    assert row["positions"] == {"X": 10}
    assert state["equity_peak"] == 120


def test_checkpoint_restores_dynamic_universe():
    engine = FreeStrategyEngine(
        "def initialize(context):\n    context.set_universe(['A'])\n"
        "def on_bar(context, bars):\n    pass\n",
    )
    checkpoint = engine.checkpoint()
    checkpoint["universe"] = ["A", "B"]
    restored = FreeStrategyEngine(engine.source)

    restored.restore_checkpoint(checkpoint)

    assert restored.universe == ["A", "B"]


def test_market_history_loader_refreshes_before_read_and_skips_older_overlap():
    engine = FreeStrategyEngine("def on_bar(context, bars):\n    pass\n")
    calls = []

    def load(cutoff):
        calls.append(cutoff)
        engine.preload_market_history([
            Bar("X", datetime(2026, 7, 27, 15), 10, 10, 10, 10),
            Bar("X", datetime(2026, 7, 24, 15), 9, 9, 9, 9),
        ])

    engine.preload_market_history([
        Bar("X", datetime(2026, 7, 27, 15), 10, 10, 10, 10),
    ])
    engine.set_market_history_loader(load)
    engine.context.now = datetime(2026, 7, 28, 9, 29)

    rows = engine.context.market_history_bars("X", count=2)

    assert len(calls) == 1
    assert [(row.date.isoformat(), row.close) for row in rows] == [("2026-07-27", 10)]
