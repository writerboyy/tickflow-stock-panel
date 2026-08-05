import queue
import sqlite3
import threading
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.engine import Fill, FreeStrategyConfig, FreeStrategyEngine, Order, Quote, RiskConfig
from app.free_strategy.paper import (
    MarketDataHub,
    PaperTradingSupervisor,
    _Subscription,
    _append_engine_events,
    _append_strategy_logs,
    _catch_up_bars,
    _compatible_checkpoint,
    _dispatch_paper_notification,
    _engine_from_state,
    _equity_snapshot,
    _queue_delay_seconds,
    _queued_payload,
    _process_bar_rows,
)
from app.free_strategy.process import MarketData
from app.free_strategy.store import PaperAccountStore
from app.free_strategy.templates import TEMPLATES
from app.market_time import cn_now
from app.services.quote_service import QuoteService


def quote(second: int, price: float) -> Quote:
    return Quote("X", datetime(2024, 1, 2, 9, 30, second), price, prev_close=10, open=10, high=max(10, price), low=min(10, price))


def test_enabled_paper_notification_uses_current_wecom_hook(monkeypatch):
    submitted = []

    class ImmediateExecutor:
        def submit(self, fn, *args):
            submitted.append((fn, args))
            return fn(*args)

    sent = []
    monkeypatch.setattr("app.free_strategy.paper._PAPER_WEBHOOK_EXECUTOR", ImmediateExecutor())
    monkeypatch.setattr("app.services.notify_adapter.notify", lambda *_args: True)
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_url", lambda: "")
    monkeypatch.setattr("app.services.preferences.get_wecom_webhook_url", lambda: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test")
    monkeypatch.setattr("app.services.webhook_adapter.send_wecom", lambda *args: sent.append(args) or True)

    _dispatch_paper_notification(
        {"id": "paper", "name": "报价策略", "system_notify_enabled": True, "notification_channels": []},
        {"type": "fill", "symbol": "510300.SH", "status": "filled"},
    )

    assert len(submitted) == 1
    assert sent == [("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test", "模拟", "报价策略 510300.SH filled")]


def test_disabled_paper_notification_sends_nothing(monkeypatch):
    monkeypatch.setattr(
        "app.services.notify_adapter.notify",
        lambda *_args: pytest.fail("disabled account must not send a system notification"),
    )
    monkeypatch.setattr(
        "app.services.preferences.get_wecom_webhook_url",
        lambda: pytest.fail("disabled account must not resolve webhook channels"),
    )

    _dispatch_paper_notification(
        {"id": "paper", "system_notify_enabled": False, "notification_channels": ["wecom"]},
        {"type": "fill", "symbol": "510300.SH"},
    )


class FakePaperProcess:
    def __init__(self, alive=False, exitcode=None):
        self.alive = alive
        self.exitcode = exitcode
        self.terminated = False
        self.on_is_alive = None

    def start(self):
        self.alive = True

    def is_alive(self):
        if self.on_is_alive is not None:
            callback, self.on_is_alive = self.on_is_alive, None
            callback()
        return self.alive

    def join(self, timeout):
        pass

    def terminate(self):
        self.alive = False
        self.terminated = True


class FakeSharedValue:
    def __init__(self, value):
        self.value = value
        self._lock = threading.RLock()

    def get_lock(self):
        return self._lock

    def get_obj(self):
        return self


class FakePaperHub:
    def __init__(self):
        self.unregistered = []

    def unregister(self, account_id):
        self.unregistered.append(account_id)


class FakePaperContext:
    def __init__(self, replacement):
        self.replacement = replacement

    @staticmethod
    def Queue(maxsize):
        return queue.Queue(maxsize=maxsize)

    @staticmethod
    def Value(_typecode, value):
        return FakeSharedValue(value)

    @staticmethod
    def Array(_typecode, _size):
        return FakeSharedValue("")

    @staticmethod
    def BoundedSemaphore(size):
        return threading.BoundedSemaphore(size)

    def Process(self, *_args, **_kwargs):
        return self.replacement


def initialize_supervisor_runtime(supervisor):
    supervisor._deadlines = {}  # noqa: SLF001
    supervisor._callback_labels = {}  # noqa: SLF001
    supervisor._queue_delays = {}  # noqa: SLF001
    supervisor._catch_up_slots = threading.BoundedSemaphore(2)  # noqa: SLF001


def test_small_cap_paper_engine_loads_daily_instrument_universe(monkeypatch, tmp_path):
    requested_timeframes = []

    def instrument_records(_repo, _asset_type, timeframe):
        requested_timeframes.append(timeframe)
        return [{
            "symbol": "X",
            "asset_type": "stock",
            "has_minute": timeframe == "1d",
        }]

    monkeypatch.setattr("app.free_strategy.process._instrument_records", instrument_records)
    account_root = tmp_path / "free_strategy_paper" / "paper"
    account_root.mkdir(parents=True)
    (account_root / "strategy.py").write_text(
        TEMPLATES["small_cap_limitup"]["source"],
        encoding="utf-8",
    )

    engine = _engine_from_state(
        {
            "config": {
                "market_mode": "bar_1m",
                "asset_type": "stock",
                "benchmark_symbol": "X",
            },
        },
        account_root,
        tmp_path,
    )

    assert engine.execution_mode == "scheduled"
    assert engine.universe == ["X"]
    assert engine.scheduled_times == [
        "09:05", "10:00", "10:15", "10:30", "14:20", "14:50", "14:55",
    ]
    assert requested_timeframes == ["1d"]


def test_paper_engine_defaults_missing_callback_timeout_to_two_minutes(tmp_path):
    account_root = tmp_path / "paper_accounts" / "paper"
    account_root.mkdir(parents=True)
    (account_root / "strategy.py").write_text(
        "def on_bar(context, bars):\n"
        "    pass\n",
        encoding="utf-8",
    )

    engine = _engine_from_state(
        {"config": {"market_mode": "bar_1m", "asset_type": "etf"}},
        account_root,
        tmp_path,
    )
    custom = _engine_from_state(
        {
            "config": {
                "market_mode": "bar_1m",
                "asset_type": "etf",
                "callback_timeout_seconds": 15,
            },
        },
        account_root,
        tmp_path,
    )

    assert engine.config.callback_timeout_seconds == 120
    assert custom.config.callback_timeout_seconds == 15


def test_legacy_five_fortunes_v2_checkpoint_uses_current_state_key():
    checkpoint = {
        "state": {
            "five_fortunes": {
                "version": "2.0",
                "target": ["510300.SH"],
            },
        },
    }

    migrated = _compatible_checkpoint(
        'def _state(context):\n    return context.state["five_fortunes_v2"]\n',
        checkpoint,
    )

    assert "five_fortunes" not in migrated["state"]
    assert migrated["state"]["five_fortunes_v2"]["target"] == ["510300.SH"]
    assert checkpoint["state"]["five_fortunes"]["version"] == "2.0"


def test_paper_engine_preloads_history_for_the_whole_universe(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "app.free_strategy.process._instrument_records",
        lambda _repo, _asset_type, _timeframe: [
            {"symbol": "X", "asset_type": "etf"},
            {"symbol": "Y", "asset_type": "etf"},
        ],
    )
    monkeypatch.setattr(
        "app.free_strategy.process._load_market_data",
        lambda _repo, symbols, start, end, asset_type: (
            calls.append((symbols, start, end, asset_type)) or MarketData()
        ),
    )
    account_root = tmp_path / "paper_accounts" / "paper"
    account_root.mkdir(parents=True)
    (account_root / "strategy.py").write_text(
        "def initialize(context):\n"
        "    context.set_universe(['X', 'Y'])\n"
        "    context.require_history('1d', bars=45)\n"
        "def on_bar(context, bars):\n"
        "    pass\n",
        encoding="utf-8",
    )

    _engine_from_state(
        {
            "config": {
                "market_mode": "bar_1m",
                "asset_type": "etf",
                "benchmark_symbol": "X",
            },
            "last_bar": "2026-08-03T15:00:00",
        },
        account_root,
        tmp_path,
    )

    assert calls == [
        (["X", "Y"], datetime(2026, 8, 3).date() - timedelta(days=104), cn_now().date(), "etf"),
    ]


def test_paper_engine_short_circuits_missing_market_history_without_symbol_reads(monkeypatch, tmp_path):
    reads = []
    prepared = []

    monkeypatch.setattr(
        "app.free_strategy.process._instrument_records",
        lambda _repo, _asset_type, _timeframe: [
            {"symbol": "PRESENT", "asset_type": "etf"},
            {"symbol": "MISSING", "asset_type": "etf"},
        ],
    )

    def prepare(_repo, engine, _start, _end, _asset_type, _market):
        prepared.append(True)
        engine.preload_market_history([
            Bar("PRESENT", datetime(2026, 8, 4, 15), 10, 10, 10, 10),
        ])
        return {"enabled": True, "asset_type": "etf", "timeframe": "1d", "requested_bars": 5,
                "rows": 1, "symbols": 1, "start": "2026-08-04", "end": "2026-08-04"}

    monkeypatch.setattr("app.free_strategy.process._prepare_market_reference", prepare)
    monkeypatch.setattr(
        "app.free_strategy.process._read_rows",
        lambda *_args, **_kwargs: reads.append(True) or [],
    )
    account_root = tmp_path / "paper_accounts" / "paper"
    account_root.mkdir(parents=True)
    (account_root / "strategy.py").write_text(
        "def initialize(context):\n"
        "    context.set_universe(['PRESENT', 'MISSING'])\n"
        "    context.require_market_history('etf', bars=5)\n"
        "def on_bar(context, bars):\n"
        "    pass\n",
        encoding="utf-8",
    )
    engine = _engine_from_state(
        {"config": {"market_mode": "bar_1m", "asset_type": "etf"}},
        account_root,
        tmp_path,
    )
    engine.context.now = datetime(2026, 8, 5, 10)

    assert engine.context.history_bars("MISSING", count=5, timeframe="1d") == []
    assert engine.context.history_bars("MISSING", count=5, timeframe="1d") == []
    assert engine.context.history_bars("UNKNOWN", count=5, timeframe="1d") == []
    assert len(prepared) == 1
    assert reads == [True]


def test_queued_payload_preserves_dict_shape_and_reports_wait():
    payload = _queued_payload({"type": "bars"})
    payload.enqueued_at = time.monotonic() - 0.5

    assert payload == {"type": "bars"}
    assert _queue_delay_seconds(payload) >= 0.49


def test_paper_persists_only_strategy_owned_logs(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running"})

    _append_strategy_logs(store, "paper", [
        {
            "timestamp": "2026-08-05T09:30:00",
            "level": "INFO",
            "message": "自由输出\n第二行",
            "source": "strategy",
        },
        {
            "timestamp": "2026-08-05T09:30:00",
            "level": "INFO",
            "message": "框架固定日志",
            "source": "engine",
        },
    ])

    events = store.events("paper")
    assert len(events) == 1
    assert events[0]["message"] == "自由输出\n第二行"
    assert events[0]["source"] == "strategy"


def test_supervisor_start_passes_shared_catch_up_slot(tmp_path):
    class FakeContext:
        def __init__(self):
            self.slot = object()
            self.args = None

        def BoundedSemaphore(self, _size):
            return self.slot

        @staticmethod
        def Queue(maxsize):
            return queue.Queue(maxsize=maxsize)

        @staticmethod
        def Value(_typecode, value):
            return FakeSharedValue(value)

        @staticmethod
        def Array(_typecode, _size):
            return FakeSharedValue("")

        def Process(self, *args, **kwargs):
            self.args = kwargs.get("args", args)
            return FakePaperProcess()

    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped", "config": {}})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.data_dir = tmp_path
    supervisor.store = store
    supervisor.hub = SimpleNamespace(quote_service=SimpleNamespace(get_min_interval=lambda: 1))
    supervisor._ctx = FakeContext()  # noqa: SLF001
    supervisor._catch_up_slots = supervisor._ctx.slot  # noqa: SLF001
    supervisor._lock = threading.RLock()  # noqa: SLF001
    supervisor._processes = {}  # noqa: SLF001
    supervisor._queues = {}  # noqa: SLF001
    initialize_supervisor_runtime(supervisor)

    supervisor.start("paper")

    assert supervisor._ctx.args[4] is supervisor._catch_up_slots  # noqa: SLF001


def test_supervisor_timeout_reports_callback_and_queue_delay(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running", "config": {"callback_timeout_seconds": 1}})
    process = FakePaperProcess(alive=True)
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.store = store
    supervisor.hub = FakePaperHub()
    supervisor._lock = threading.RLock()  # noqa: SLF001
    supervisor._processes = {"paper": process}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue(maxsize=2)}  # noqa: SLF001
    supervisor._deadlines = {"paper": FakeSharedValue(time.monotonic() - 1)}  # noqa: SLF001
    supervisor._callback_labels = {"paper": FakeSharedValue("定时回调 13:10")}  # noqa: SLF001
    supervisor._queue_delays = {"paper": FakeSharedValue(2.5)}  # noqa: SLF001

    supervisor._monitor_once()  # noqa: SLF001

    message = store.get("paper")["last_error"]
    assert "定时回调 13:10" in message
    assert "队列等待 2.5 秒" in message


def test_performance_small_cap_paper_engine_uses_backtest_selection_loaders(monkeypatch, tmp_path):
    calls = []

    monkeypatch.setattr(
        "app.free_strategy.process._instrument_records",
        lambda _repo, _asset_type, _timeframe: [{"symbol": "X", "asset_type": "stock"}],
    )
    monkeypatch.setattr(
        "app.free_strategy.process._load_financial_snapshot",
        lambda data_dir, symbols, cutoff: calls.append(("financial", data_dir, symbols, cutoff)) or {"X": {}},
    )
    monkeypatch.setattr(
        "app.free_strategy.process._load_dividend_ratio_ranked",
        lambda repo, data_dir, symbols, cutoff: calls.append(("dividend", repo, data_dir, symbols, cutoff)) or ["X"],
    )
    monkeypatch.setattr(
        "app.free_strategy.process._load_valuation_market_caps",
        lambda data_dir, symbols, cutoff: calls.append(("valuation", data_dir, symbols, cutoff)) or {"X": 1.0},
    )
    monkeypatch.setattr(
        "app.free_strategy.process._load_smallcap_index_value",
        lambda repo, data_dir, symbols, cutoff: calls.append(("smallcap", repo, data_dir, symbols, cutoff)) or 12.34,
    )
    account_root = tmp_path / "free_strategy_paper" / "performance"
    account_root.mkdir(parents=True)
    (account_root / "strategy.py").write_text(
        TEMPLATES["performance_small_cap"]["source"],
        encoding="utf-8",
    )

    engine = _engine_from_state(
        {"config": {"market_mode": "bar_1m", "asset_type": "stock", "benchmark_symbol": "X"}},
        account_root,
        tmp_path,
    )
    cutoff = datetime(2025, 7, 24).date()
    engine.context.now = datetime(2025, 7, 25, 9, 30)

    assert engine.context.financial_snapshot(["X"], cutoff) == {"X": {}}
    assert engine.context.dividend_ratio_ranked(["X"], cutoff) == ["X"]
    assert engine.context.valuation_market_caps(["X"], cutoff) == {"X": 1.0}
    assert engine.context.smallcap_index_value(["X"], cutoff) == 12.34
    assert [item[0] for item in calls] == ["financial", "dividend", "valuation", "smallcap"]


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


def test_quote_driven_fill_dispatches_current_wecom_hook(monkeypatch, tmp_path):
    source = """
def on_quote(context, quotes):
    if not context.state.get('ordered'):
        context.state['ordered'] = True
        context.buy('X', quantity=10)
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=1_000,
            lot_size=1,
            fees_pct=0,
            slippage_bps=0,
            fill_policy="close",
            settlement="t0",
        ),
    )
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper",
        "name": "报价策略",
        "status": "running",
        "system_notify_enabled": True,
        "notification_channels": [],
    })
    sent = []

    class ImmediateExecutor:
        def submit(self, fn, *args):
            return fn(*args)

    monkeypatch.setattr("app.free_strategy.paper._PAPER_WEBHOOK_EXECUTOR", ImmediateExecutor())
    monkeypatch.setattr("app.services.notify_adapter.notify", lambda *_args: True)
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_url", lambda: "")
    monkeypatch.setattr("app.services.preferences.get_wecom_webhook_url", lambda: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test")
    monkeypatch.setattr("app.services.webhook_adapter.send_wecom", lambda *args: sent.append(args) or True)

    before_risk = dict(engine.risk_status)
    engine.process_quotes([quote(0, 10)])
    _append_engine_events(
        store,
        "paper",
        engine,
        before_orders=0,
        before_fills=0,
        before_logs=0,
        before_risk=before_risk,
        notify=lambda event: _dispatch_paper_notification(store.get("paper"), event),
    )

    assert [event["type"] for event in store.events("paper")] == ["order", "fill"]
    assert len(sent) == 2
    assert all(call[0].endswith("key=test") and call[1] == "模拟" for call in sent)


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
        self.symbol_consumers = {}
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

    def set_symbol_consumer(self, consumer_id, symbols):
        self.symbol_consumers[consumer_id] = set(symbols)

    def remove_symbol_consumer(self, consumer_id):
        self.symbol_consumers.pop(consumer_id, None)

    def record_quotes(self, _records):
        pass

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


def test_bar_account_registers_only_held_symbols_for_live_valuation():
    service = FakeQuoteService()
    hub = MarketDataHub(service, repo=None)

    hub.register("paper", "bar_1m", {"A", "513690.SH"}, "etf", queue.Queue(), valuation_symbols={"513690.SH"})

    assert service.symbol_consumers == {"paper:paper": {"513690.SH"}}
    hub.unregister("paper")
    assert service.symbol_consumers == {}


def test_scheduled_bar_account_receives_clock_without_reading_minute_ranges():
    target = queue.Queue(maxsize=2)
    subscription = _Subscription(
        "paper",
        "bar_1m",
        {"A", "B"},
        "stock",
        target,
        "2024-01-01T15:00:00",
        execution_mode="scheduled",
        scheduled_times=("10:15",),
    )
    hub = MarketDataHub(FakeQuoteService(), repo=None)

    hub._dispatch_scheduled_clocks([subscription], datetime(2024, 1, 2, 10, 16))  # noqa: SLF001
    hub._dispatch_scheduled_clocks([subscription], datetime(2024, 1, 2, 10, 16))  # noqa: SLF001

    assert target.get_nowait() == {
        "type": "scheduled_clock",
        "account_id": "paper",
        "cutoff": "2024-01-02T10:15:00",
    }
    assert target.empty()


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
    initialize_supervisor_runtime(supervisor)

    supervisor._monitor_once()  # noqa: SLF001

    assert supervisor.hub.unregistered == ["paper"]
    assert process.terminated is True
    assert supervisor._processes == {}  # noqa: SLF001


def test_supervisor_does_not_detach_concurrently_restarted_worker(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running", "config": {}})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.store = store
    supervisor.hub = FakePaperHub()
    supervisor._lock = threading.RLock()  # noqa: SLF001
    old_process = FakePaperProcess(False)
    new_process = FakePaperProcess(True)
    new_queue = queue.Queue(maxsize=2)
    supervisor._processes = {"paper": old_process}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue(maxsize=2)}  # noqa: SLF001
    initialize_supervisor_runtime(supervisor)

    def replace_runtime():
        supervisor._processes["paper"] = new_process  # noqa: SLF001
        supervisor._queues["paper"] = new_queue  # noqa: SLF001

    old_process.on_is_alive = replace_runtime

    supervisor._monitor_once()  # noqa: SLF001

    saved = store.get("paper")
    assert supervisor._processes["paper"] is new_process  # noqa: SLF001
    assert supervisor._queues["paper"] is new_queue  # noqa: SLF001
    assert new_process.terminated is False
    assert saved["status"] == "running"
    assert saved.get("last_error") is None
    assert store.events("paper") == []


def test_supervisor_restarts_worker_after_unreported_exit(tmp_path):
    replacement = FakePaperProcess()

    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running", "config": {}})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.data_dir = tmp_path
    supervisor.store = store
    supervisor.hub = FakePaperHub()
    supervisor._ctx = FakePaperContext(replacement)  # noqa: SLF001
    supervisor._lock = threading.RLock()  # noqa: SLF001
    supervisor._processes = {"paper": FakePaperProcess(exitcode=-15)}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue(maxsize=2)}  # noqa: SLF001
    initialize_supervisor_runtime(supervisor)
    supervisor._restart_attempts = {}  # noqa: SLF001

    supervisor._monitor_once()  # noqa: SLF001

    saved = store.get("paper")
    assert supervisor._processes["paper"] is replacement  # noqa: SLF001
    assert replacement.is_alive()
    assert saved["status"] == "running"
    assert saved.get("last_error") is None
    assert [event["type"] for event in store.events("paper")] == ["worker_restart", "start"]
    assert store.events("paper")[0]["exit_code"] == -15


def test_supervisor_pauses_after_repeated_unreported_exits(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running", "config": {}})
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.store = store
    supervisor.hub = FakePaperHub()
    supervisor._lock = threading.RLock()  # noqa: SLF001
    supervisor._processes = {"paper": FakePaperProcess(exitcode=1)}  # noqa: SLF001
    supervisor._queues = {"paper": queue.Queue(maxsize=2)}  # noqa: SLF001
    initialize_supervisor_runtime(supervisor)
    now = time.monotonic()
    supervisor._restart_attempts = {  # noqa: SLF001
        "paper": deque([now - 3, now - 2, now - 1]),
    }

    supervisor._monitor_once()  # noqa: SLF001

    saved = store.get("paper")
    assert saved["status"] == "paused"
    assert "5 分钟内连续异常退出" in saved["last_error"]
    assert "退出码 1" in saved["last_error"]
    assert [event["type"] for event in store.events("paper")] == ["error"]


def test_supervisor_start_defers_strategy_initialization_to_worker(tmp_path):
    class FakeProcess:
        def __init__(self):
            self.alive = False

        def start(self):
            self.alive = True

        def is_alive(self):
            return self.alive

    class FakeContext:
        @staticmethod
        def Queue(maxsize):
            return queue.Queue(maxsize=maxsize)

        @staticmethod
        def Value(_typecode, value):
            return FakeSharedValue(value)

        @staticmethod
        def Array(_typecode, _size):
            return FakeSharedValue("")

        @staticmethod
        def Process(*_args, **_kwargs):
            return FakeProcess()

    sync = {
        "phase": "live",
        "from": "2026-07-27T15:00:00",
        "target": "2026-07-28T15:00:00",
        "through": "2026-07-28T15:00:00",
        "processed_days": 1,
        "total_days": 1,
        "missing_symbols": ["MISSING"],
        "updated_at": "2026-07-28T16:00:00+08:00",
    }
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper",
        "status": "running",
        "last_bar": "2026-07-28T15:00:00",
        "config": {"market_mode": "bar_1m", "asset_type": "stock"},
        "sync": sync,
    })
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.data_dir = tmp_path
    supervisor.store = store
    supervisor.hub = object()
    supervisor._ctx = FakeContext()  # noqa: SLF001
    supervisor._lock = threading.RLock()  # noqa: SLF001
    supervisor._processes = {}  # noqa: SLF001
    supervisor._queues = {}  # noqa: SLF001
    initialize_supervisor_runtime(supervisor)
    result = supervisor.start("paper")

    assert result["sync"] == sync
    assert [row["type"] for row in store.events("paper")] == ["start"]


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


def test_quote_service_fetches_registered_etf_when_etf_universe_is_disabled(monkeypatch):
    class Repo:
        @staticmethod
        def get_index_symbol_set():
            return set()

        @staticmethod
        def get_etf_instruments():
            return pl.DataFrame({"symbol": ["513690.SH"]})

    class Quotes:
        requested = []

        @staticmethod
        def get_by_universes(*, universes):
            assert universes == ["CN_Equity_A"]
            return [{"symbol": "600000.SH", "last_price": 10, "timestamp": cn_now().isoformat()}]

        @classmethod
        def get(cls, *, symbols):
            cls.requested.append(symbols)
            return [{"symbol": symbol, "last_price": 1.2, "timestamp": cn_now().isoformat()} for symbol in symbols]

    service = QuoteService()
    service.set_repo(Repo())
    service.set_symbol_consumer("paper:one", {"513690.SH"})
    captured = {}
    monkeypatch.setattr("app.tickflow.client.get_paid_realtime_client", lambda: type("Client", (), {"quotes": Quotes()})())
    monkeypatch.setattr("app.services.preferences.get_realtime_data_provider", lambda: "tickflow")
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_stock", lambda: True)
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_etf", lambda: False)
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_index", lambda: False)
    monkeypatch.setattr("app.services.preferences.get_realtime_index_symbols", lambda: [])
    monkeypatch.setattr("app.services.preferences.get_realtime_index_mode", lambda: "core")
    monkeypatch.setattr(service, "_process_full_market_records", lambda records, **kwargs: captured.update(records=records, **kwargs))

    service._fetch_full_market_quotes()  # noqa: SLF001

    assert Quotes.requested == [["513690.SH"]]
    assert {row["symbol"] for row in captured["records"]} == {"600000.SH", "513690.SH"}
    assert captured["merge_assets"] == {"etf"}


def test_new_symbol_consumer_reopens_completed_final_sync(monkeypatch):
    service = QuoteService()
    key = (cn_now().date(), "close")
    service._final_sync_done.add(key)  # noqa: SLF001
    monkeypatch.setattr(service, "_market_phase", lambda: "close_final")

    service.set_symbol_consumer("paper:one", {"513690.SH"})

    assert key not in service._final_sync_done  # noqa: SLF001


def test_final_sync_retries_when_symbol_is_registered_during_fetch(monkeypatch):
    service = QuoteService()
    service._fetched_at = 1  # noqa: SLF001

    def fetch():
        service.set_symbol_consumer("paper:one", {"159920.SZ"})
        service._fetched_at = 2  # noqa: SLF001

    monkeypatch.setattr(service, "_fetch_full_market_quotes", fetch)
    monkeypatch.setattr(service, "realtime_mode", lambda: "full_market")

    assert service._fetch_quotes(final=True) is False  # noqa: SLF001


def test_live_valuation_uses_current_quote_without_mutating_state():
    class QuoteSnapshot:
        @staticmethod
        def get_fresh_quotes(symbols):
            assert symbols == {"513690.SH"}
            return {
                "live": True,
                "quotes": {"513690.SH": {"last_price": 2.0}},
                "missing_symbols": [],
                "as_of": "2026-07-29T10:00:00+08:00",
                "date": "2026-07-29",
            }

    state = {
        "status": "running",
        "cash": 100,
        "positions": {"513690.SH": 500},
        "equity": 1_000,
        "equity_peak": 1_200,
        "config": {"initial_capital": 1_000},
    }
    original = deepcopy(state)
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.hub = SimpleNamespace(quote_service=QuoteSnapshot())

    valuation = supervisor.live_valuation(state)

    assert valuation["live"] is True
    assert valuation["equity"] == 1_100
    assert valuation["return_pct"] == pytest.approx(10)
    assert valuation["drawdown_pct"] == pytest.approx(100 / 1_200 * 100)
    assert state == original


def test_live_valuation_reports_missing_quote_without_false_zero():
    supervisor = PaperTradingSupervisor.__new__(PaperTradingSupervisor)
    supervisor.hub = SimpleNamespace(quote_service=SimpleNamespace(
        get_fresh_quotes=lambda _symbols: {
            "live": False,
            "quotes": {},
            "missing_symbols": ["513690.SH"],
            "as_of": None,
            "date": "2026-07-29",
        },
    ))

    valuation = supervisor.live_valuation({
        "status": "running",
        "cash": 100,
        "positions": {"513690.SH": 500},
        "config": {"initial_capital": 1_000},
    })

    assert valuation == {
        "live": False,
        "as_of": None,
        "date": "2026-07-29",
        "missing_symbols": ["513690.SH"],
    }


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


def test_paper_event_id_is_appended_once(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped"})

    assert store.append_event_once("paper", {"id": "decision:2026-07-28", "type": "signal"}) is True
    assert store.append_event_once("paper", {"id": "decision:2026-07-28", "type": "signal"}) is False
    assert [row["id"] for row in store.events("paper")] == ["decision:2026-07-28"]


def test_order_event_uses_strategy_timestamp_and_executed_side(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped"})
    order = Order(
        id="o1",
        symbol="159920.SZ",
        side="buy",
        cash_weight=1.0,
        submitted_at="2026-07-28T13:11:00",
        status="filled",
    )
    fill = Fill(
        order_id="o1",
        symbol="159920.SZ",
        side="buy",
        quantity=100,
        price=1.49,
        value=149,
        fee=0.01,
        timestamp="2026-07-28T13:11:00",
    )
    engine = SimpleNamespace(
        account=SimpleNamespace(orders=[order], fills=[fill]),
        logs=[],
        risk_status={},
        drain_signals=lambda: [],
    )

    _append_engine_events(
        store,
        "paper",
        engine,
        before_orders=0,
        before_fills=0,
        before_logs=0,
        before_risk={},
    )

    event = next(row for row in store.events("paper") if row["type"] == "order")
    assert event["timestamp"] == "2026-07-28T13:11:00"
    assert event["executed_side"] == "buy"
    assert event["cash_weight"] == 1.0


def test_up_to_date_restart_preserves_live_sync_state(monkeypatch, tmp_path):
    sync = {
        "phase": "live",
        "from": "2026-07-27T15:00:00",
        "target": "2026-07-28T15:00:00",
        "through": "2026-07-28T15:00:00",
        "processed_days": 1,
        "total_days": 1,
        "missing_symbols": ["MISSING"],
        "updated_at": "2026-07-28T16:00:00+08:00",
    }
    store = PaperAccountStore(tmp_path)
    current = store.save({
        "id": "paper",
        "status": "running",
        "last_bar": "2026-07-28T15:00:00",
        "config": {"market_mode": "bar_1m", "asset_type": "stock"},
        "sync": sync,
    })
    engine = FreeStrategyEngine(
        "def initialize(context):\n    context.set_universe(['X'])\n"
        "def on_bar(context, bars):\n    pass\n",
        timeframe="1m",
    )
    monkeypatch.setattr("app.free_strategy.process._read_rows", lambda *_args, **_kwargs: [])

    result = _catch_up_bars(store, "paper", current, engine, tmp_path)

    assert result["sync"] == sync
    assert store.get("paper")["sync"] == sync
    assert store.events("paper") == []


def test_scheduled_catch_up_uses_scoped_snapshots_not_minute_ranges(monkeypatch, tmp_path):
    calls = {"range": 0, "snapshot": []}
    trading_day = datetime(2024, 1, 2).date()

    class Repository:
        @staticmethod
        def _daily_rows(symbols, start, end):
            return pl.DataFrame([
                {
                    "symbol": symbol,
                    "date": day,
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.5,
                    "close": 10.0,
                    "volume": 1_000.0,
                    "amount": 10_000.0,
                    "raw_close": 10.0,
                    "raw_high": 10.5,
                    "raw_low": 9.5,
                    "turnover_rate": 1.0,
                    "total_shares": 1_000_000.0,
                    "float_shares": 800_000.0,
                }
                for symbol in symbols
                for day in (datetime(2024, 1, 1).date(), trading_day)
                if start <= day <= end
            ])

        def get_daily_asset_batch(self, _asset_type, symbols, start, end, _columns):
            return self._daily_rows(symbols, start, end)

        def get_daily_asset(self, _asset_type, symbol, start, end, _columns):
            return self._daily_rows([symbol], start, end).drop("symbol")

        @staticmethod
        def get_instruments_asset(_asset_type):
            return pl.DataFrame([
                {"symbol": "X", "name": "X"},
                {"symbol": "Y", "name": "Y"},
            ])

        def get_minute_snapshot(self, symbols, at, _asset_type):
            calls["snapshot"].append((list(symbols), at))
            return pl.DataFrame([
                {
                    "symbol": symbol,
                    "datetime": at,
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 100.0,
                    "amount": 1_000.0,
                }
                for symbol in symbols
            ])

        @staticmethod
        def get_minute_next(_symbols, _after, _until, _asset_type):
            return pl.DataFrame()

        @staticmethod
        def get_minute_range(*_args, **_kwargs):
            calls["range"] += 1
            raise AssertionError("scheduled catch-up must not scan minute ranges")

    engine = FreeStrategyEngine(
        "def initialize(context):\n"
        "    context.set_universe(['X', 'Y'])\n"
        "    context.schedule(run, '10:15', symbols=['X'])\n"
        "def run(context):\n"
        "    context.state['ran'] = context.now.isoformat()\n",
        timeframe="1m",
        config=FreeStrategyConfig(asset_type="stock", benchmark_symbol="X"),
    )
    store = PaperAccountStore(tmp_path)
    current = store.save({
        "id": "paper",
        "status": "running",
        "config": {"market_mode": "bar_1m", "asset_type": "stock"},
    })
    monkeypatch.setattr("app.free_strategy.paper.cn_naive_now", lambda: datetime(2024, 1, 2, 15, 5))

    result = _catch_up_bars(
        store,
        "paper",
        current,
        engine,
        tmp_path,
        repo=Repository(),
        scheduled_market=MarketData(),
    )

    assert calls == {
        "range": 0,
        "snapshot": [
            (["X"], datetime(2024, 1, 2, 10, 15)),
            (["X"], datetime(2024, 1, 2, 15, 0)),
        ],
    }
    assert engine.context.state["ran"] == "2024-01-02T10:15:00"
    assert engine.execution_mode == "scheduled"
    assert result["last_bar"] == "2024-01-02T15:00:00"
    assert result["sync"]["phase"] == "live"


def test_paper_bar_batch_preloads_delayed_open_as_tradable(tmp_path):
    source = """
def initialize(context):
    context.schedule(sell_x, '09:45')

def sell_x(context):
    if context.now.day == 2:
        context.sell('X', quantity=100)

def on_bar(context, bars):
    if context.now.day == 1 and 'X' in bars:
        context.buy('X', quantity=100)
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=1_000,
            settlement="t0",
            allow_stale_fills=True,
            fill_policy="close",
            fees_pct=0,
            slippage_bps=0,
        ),
    )
    store = PaperAccountStore(tmp_path)
    current = store.save({"id": "paper", "status": "running", "equity_peak": 1_000})
    current = _process_bar_rows(
        store,
        "paper",
        current,
        engine,
        [Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10)],
    )

    _process_bar_rows(
        store,
        "paper",
        current,
        engine,
        [
            Bar("Y", datetime(2024, 1, 2, 9, 45), 1, 1, 1, 1),
            Bar("X", datetime(2024, 1, 2, 10), 11, 11, 11, 11),
        ],
    )

    assert [
        (fill.symbol, fill.side, fill.timestamp)
        for fill in engine.account.fills
    ] == [
        ("X", "buy", "2024-01-01T15:00:00"),
        ("X", "sell", "2024-01-02T09:45:00"),
    ]


def test_paper_bar_rows_persist_in_timestamp_chunks(monkeypatch, tmp_path):
    from app.free_strategy import paper as paper_module

    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    pass\n",
        timeframe="1m",
        config=FreeStrategyConfig(settlement="t0", fees_pct=0, slippage_bps=0),
    )
    store = PaperAccountStore(tmp_path)
    current = store.save({"id": "paper", "status": "running"})
    persist_calls = []
    original = paper_module._persist_engine_state

    def persist(*args, **kwargs):
        persist_calls.append(args[4])
        return original(*args, **kwargs)

    monkeypatch.setattr(paper_module, "_persist_engine_state", persist)
    bars = [
        Bar("X", datetime(2024, 1, 2, 9, 30) + timedelta(minutes=index), 10, 10, 10, 10)
        for index in range(61)
    ]

    result = _process_bar_rows(store, "paper", current, engine, bars)

    assert len(persist_calls) == 3
    assert result["last_bar"] == "2024-01-02T10:30:00"


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
         "drawdown_pct": 0, "positions": {"X": 10}, "avg_cost": {"X": 8.5},
         "source": "paper"},
    ])

    assert store.equity_curve("paper") == [{
        "timestamp": recent,
        "equity": 110.0,
        "cash": 10.0,
        "nav": 1.1,
        "drawdown_pct": 0.0,
        "positions": {"X": 10},
        "avg_cost": {"X": 8.5},
    }]
    with sqlite3.connect(tmp_path / "paper_accounts" / "paper" / "equity.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM equity_curve").fetchone()[0] == 1


def test_paper_equity_curve_migrates_legacy_database(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "stopped"})
    database = tmp_path / "paper_accounts" / "paper" / "equity.sqlite3"
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE equity_curve (
                timestamp TEXT PRIMARY KEY,
                equity REAL NOT NULL,
                cash REAL NOT NULL,
                nav REAL NOT NULL,
                drawdown_pct REAL NOT NULL,
                positions TEXT NOT NULL,
                source TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO equity_curve VALUES (?, ?, ?, ?, ?, ?, ?)",
            (timestamp, 100, 20, 1, 0, '{"X": 10}', "paper"),
        )

    assert store.equity_curve("paper") == [{
        "timestamp": timestamp,
        "equity": 100.0,
        "cash": 20.0,
        "nav": 1.0,
        "drawdown_pct": 0.0,
        "positions": {"X": 10},
        "avg_cost": {},
    }]


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
    assert row["avg_cost"] == {"X": 8}
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
