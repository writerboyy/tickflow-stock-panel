from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from types import SimpleNamespace
import asyncio
import time

import pytest

from app.services.large_order_service import LargeOrderService
from app.services import large_order_service, webhook_adapter
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


class InlineExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, function, *args):
        self.calls.append((function, args))
        return function(*args)


class MutableClock:
    def __init__(self):
        self.value = datetime(2026, 8, 5, 9, 30, tzinfo=large_order_service.CN_TZ)

    def __call__(self):
        return self.value

    def advance(self, seconds: int):
        self.value += timedelta(seconds=seconds)


def _quote(*, price=10.0, amount=1_000_000.0, volume=100.0, symbol="000001.SZ", name="平安银行"):
    return {
        "symbol": symbol,
        "name": name,
        "last_price": price,
        "prev_close": 10.0,
        "change_pct": price / 10.0 - 1.0,
        "amount": amount,
        "volume": volume,
    }


def test_candidate_market_segments_default_to_main_star_and_chinext():
    service = LargeOrderService(FakeQuoteService())
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    initial = [
        _quote(symbol="000001.SZ", name="平安银行"),
        _quote(symbol="688001.SH", name="科创测试"),
        _quote(symbol="300001.SZ", name="创业测试"),
        _quote(symbol="430001.BJ", name="北交测试"),
        _quote(symbol="000002.SZ", name="*ST测试"),
    ]
    updated = [
        _quote(symbol=row["symbol"], name=row["name"], amount=2_000_000, volume=200)
        for row in initial
    ]
    service._process_snapshot(initial)
    service._process_snapshot(updated)

    assert {row["symbol"] for row in service.ranking(60)["rows"]} == {
        "000001.SZ",
        "688001.SH",
        "300001.SZ",
    }

    service._config["market_segments"] = ["main", "star", "chinext", "bse", "st"]
    rankings, _, _ = service._build_rankings_locked(time.time())
    assert {row["symbol"] for row in rankings[60]} == {
        "000001.SZ",
        "688001.SH",
        "300001.SZ",
        "430001.BJ",
        "000002.SZ",
    }
    service.stop()


def test_position_mode_only_tracks_holdings_without_market_segment_filter():
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    portfolio_store = SimpleNamespace(load=lambda: {
        "positions": [{"symbol": "430001.BJ", "name": "北交持仓"}],
    })
    service._app_state = SimpleNamespace(
        position_risk_service=SimpleNamespace(store=portfolio_store),
    )
    initial = [
        _quote(symbol="430001.BJ", name="北交持仓"),
        _quote(symbol="000001.SZ", name="非持仓"),
    ]
    updated = [
        _quote(symbol="430001.BJ", name="北交持仓", price=10.1, amount=3_000_000, volume=300),
        _quote(symbol="000001.SZ", name="非持仓", price=10.1, amount=3_000_000, volume=300),
    ]

    service._process_snapshot(initial)
    service._process_snapshot(updated)

    assert set(service._states) == {"430001.BJ"}
    assert {row["symbol"] for row in service.ranking(60)["rows"]} == {"430001.BJ"}
    service.stop()


def test_limit_board_score_symbols_share_realtime_flow_scope_with_positions():
    service = LargeOrderService(FakeQuoteService())
    service._app_state = SimpleNamespace(
        position_risk_service=SimpleNamespace(
            store=SimpleNamespace(load=lambda: {"positions": [{"symbol": "000001.SZ"}]}),
        ),
    )

    service.set_score_symbols({"600000.SH", "600001.SZ"})

    assert service._scope_symbols() == {"000001.SZ", "600000.SH", "600001.SZ"}


@pytest.mark.parametrize(
    ("symbol", "name", "expected"),
    [
        ("600000.SH", "浦发银行", "main"),
        ("688001.SH", "科创测试", "star"),
        ("689001.SH", "科创存托", "star"),
        ("301001.SZ", "创业测试", "chinext"),
        ("430001.BJ", "北交测试", "bse"),
        ("688002.SH", "*ST科创", "st"),
    ],
)
def test_market_segment_classification(symbol, name, expected):
    assert LargeOrderService._market_segment(symbol, name) == expected


def test_large_order_alert_uses_selected_wecom_channel(monkeypatch):
    executor = InlineExecutor()
    deliveries = []
    monkeypatch.setattr(large_order_service, "_LARGE_ORDER_WEBHOOK_EXECUTOR", executor)
    monkeypatch.setattr("app.services.preferences.get_webhook_default_channels", lambda: ["wecom"])
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_url", lambda: "feishu-url")
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_secret", lambda: "secret")
    monkeypatch.setattr("app.services.preferences.get_wecom_webhook_url", lambda: "wecom-url")
    monkeypatch.setattr(
        webhook_adapter,
        "send_wecom",
        lambda *args: deliveries.append(args) or True,
    )

    LargeOrderService()._dispatch_alert_notifications([
        {
            "rule_name": "实时大单",
            "message": "平安银行 主力买入候选：60秒主动净买额 2,000,000 元，评分 88",
        },
    ])

    assert deliveries == [
        (
            "wecom-url",
            "实时大单",
            "平安银行 主力买入候选：60秒主动净买额 2,000,000 元，评分 88",
        ),
    ]
    assert len(executor.calls) == 1


def test_snapshot_delta_ignores_amount_reset_and_builds_proxy_candidate():
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    service._process_snapshot([_quote()])
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])
    ranking = service.ranking(60)
    assert ranking["count"] == 1
    assert ranking["rows"][0]["source"] == "tick_proxy"
    assert ranking["rows"][0]["confidence"] == "medium"
    assert ranking["rows"][0]["last_seen_ts"] is not None
    service._process_snapshot([_quote(amount=100, volume=1)])
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

    base = _quote()
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
    requests = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, endpoint, params):
            requests.append((endpoint, params))
            if endpoint == 13:
                return {
                    "code": "000001",
                    "day": "20260804",
                    "dadanjinge": [
                        [f"09:{minute:02d}", minute * 1_000_000]
                        for minute in range(30, 36)
                    ],
                }
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

    intents = storage.query("kaipanla_intent", date(2026, 8, 4))
    assert intents["count"] == 1
    assert intents["rows"][0]["cancel_flag"] is True
    ranking = service.ranking(60)["rows"][0]
    assert ranking["source"] == "kaipanla_net_flow"
    assert ranking["data_quality"] == "net_flow"
    assert ranking["net_flow_amount"] == 35_000_000
    assert ranking["net_flow_delta"] == 5_000_000
    assert ranking["net_flow_speed"] == 1_000_000
    assert service.status()["net_flow_count"] == 1
    assert service.tape("000001.SZ")["net_flow"][-1]["time"] == "09:35"
    assert requests == [
        (13, {"StockID": "000001"}),
        (14, {"StockID": "000001"}),
    ]
    raw_root = tmp_path / "ext_data" / "_kaipanla_raw"
    assert len(list(raw_root.glob("snapshot=*/large_order_net_flow/*.json.gz"))) == 1
    assert len(list(raw_root.glob("snapshot=*/large_order_intents/*.json.gz"))) == 1
    assert list(raw_root.glob("snapshot=*/large_order_trades/*.json.gz")) == []
    storage.stop()


def test_intent_parse_failure_does_not_discard_main_net_flow(monkeypatch):
    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, endpoint, _params):
            if endpoint == 13:
                return {
                    "day": "20260804",
                    "dadanjinge": [
                        [f"09:{minute:02d}", minute * 1_000_000]
                        for minute in range(30, 36)
                    ],
                }
            return {
                "List": [[
                    "09:31:01", "order-1", 12.0, 100, 12_000, 1, "", 0, 2,
                    1_754_280_661,
                ]],
            }

    service = LargeOrderService(FakeQuoteService())
    service._trade_date = date(2026, 8, 4)
    monkeypatch.setattr("app.services.large_order_service.load_credentials", lambda: object())
    monkeypatch.setattr(
        "app.services.large_order_service.KaipanlaClient", lambda **_kwargs: FakeClient(),
    )

    asyncio.run(service._deep_dive_async("000001.SZ"))

    assert service.ranking(60)["rows"][0]["data_quality"] == "net_flow"
    tape = service.tape("000001.SZ")
    assert len(tape["net_flow"]) == 6
    assert tape["intents"] == []
    assert tape["source"] == "kaipanla_net_flow"
    assert tape["error"] == "List[0] 撤单标记无效"
    assert service.status()["last_error"] == "开盘啦委托响应解析失败"
    service.stop()


def test_each_window_uses_its_own_cached_score_and_expires(monkeypatch):
    clock = MutableClock()
    monkeypatch.setattr(large_order_service, "cn_now", clock)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: clock.value.date())
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._trade_date = clock.value.date()
    service._running = True
    service._config["max_deep_dive_symbols"] = 0

    service._process_snapshot([_quote()])
    clock.advance(1)
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])
    score_15 = service.ranking(15)["rows"][0]["score"]
    assert service.ranking(60)["rows"][0]["score"] == score_15

    clock.advance(19)
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])

    assert service.ranking(15)["rows"] == []
    assert service.ranking(60)["rows"][0]["net_buy_amount"] == 2_000_000
    assert service.ranking(300)["rows"][0]["net_buy_amount"] == 2_000_000
    service.stop()


def test_deep_dive_scheduler_has_no_local_daily_quota(monkeypatch):
    service = LargeOrderService(FakeQuoteService())
    service._config["max_deep_dive_symbols"] = 3
    service._rankings[60] = tuple(
        {"symbol": f"00000{index}.SZ"}
        for index in range(1, 5)
    )
    service._deep_calls_used = 60
    submitted = []

    class CaptureExecutor:
        def submit(self, function, *args):
            submitted.append(args[0])
            return None

        def shutdown(self, **_kwargs):
            return None

    service._deep_executor = CaptureExecutor()
    monkeypatch.setattr(large_order_service, "load_credentials", lambda: object())

    service._schedule_deep_dive()

    assert len(submitted) == 3
    assert service._deep_calls_used == 66
    status = service.status()
    assert status["deep_dive_request_count"] == 66
    assert status["deep_dive_symbol_limit"] == 3
    assert "deep_dive_calls_remaining" not in status
    service._deep_pending.clear()
    service.stop()


def test_precise_trade_expires_and_window_falls_back_to_proxy(monkeypatch):
    clock = MutableClock()
    monkeypatch.setattr(large_order_service, "cn_now", clock)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: clock.value.date())
    service = LargeOrderService(FakeQuoteService())
    service._trade_date = clock.value.date()
    service._running = True
    service._config["max_deep_dive_symbols"] = 0

    service._process_snapshot([_quote()])
    state = service._states["000001.SZ"]
    state["trade_events"].append({
        "event_id": "precise-1",
        "timestamp": clock.value.timestamp(),
        "direction": "active_buy",
        "amount": 3_000_000,
    })
    rankings, filtered, unassessable = service._build_rankings_locked(clock.value.timestamp())
    service._rankings = rankings
    service._filtered_near_limit_count = filtered
    service._unassessable_count = unassessable
    assert service.ranking(15)["rows"][0]["data_quality"] == "precise"
    assert service.ranking(15)["rows"][0]["active_buy_amount"] == 3_000_000

    clock.advance(20)
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])

    row = service.ranking(15)["rows"][0]
    assert row["data_quality"] == "proxy_only"
    assert row["active_buy_amount"] == 2_000_000
    assert row["max_order_amount"] == 0
    service.stop()


def test_new_trading_day_clears_window_state_and_published_rankings(monkeypatch):
    clock = MutableClock()
    monkeypatch.setattr(large_order_service, "cn_now", clock)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: clock.value.date())
    service = LargeOrderService(FakeQuoteService())
    service._trade_date = clock.value.date()
    service._running = True
    service._config["max_deep_dive_symbols"] = 0

    service._process_snapshot([_quote()])
    clock.advance(1)
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])
    assert service.ranking(60)["count"] == 1
    service._deep_calls_used = 4

    clock.advance(24 * 60 * 60)
    service._process_snapshot([_quote(price=10.1, amount=3_000_000, volume=300)])

    assert service.ranking(15)["rows"] == []
    assert service.ranking(60)["rows"] == []
    assert service.ranking(300)["rows"] == []
    assert service._deep_calls_used == 0
    assert list(service._states["000001.SZ"]["windows"][60]["events"]) == []
    service.stop()


def test_drain_marks_worker_idle_before_releasing_empty_queue_lock():
    service = LargeOrderService(FakeQuoteService())
    service._running = True
    service._snapshot_running = True
    service._pending_snapshot = None

    class InjectPendingSnapshotOnUnlock:
        triggered = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            if not self.triggered and service._pending_snapshot is None:
                self.triggered = True
                if service._snapshot_running:
                    service._pending_snapshot = [_quote()]

    service._lock = InjectPendingSnapshotOnUnlock()
    service._drain_snapshots()

    assert service._snapshot_running is False
    assert service._pending_snapshot is None
    service.stop()


def test_zscore_compares_complete_buckets_from_the_same_window(monkeypatch):
    clock = MutableClock()
    monkeypatch.setattr(large_order_service, "cn_now", clock)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: clock.value.date())
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._trade_date = clock.value.date()
    service._running = True
    service._config["max_deep_dive_symbols"] = 0

    amount = 1_000_000.0
    volume = 100.0
    service._process_snapshot([_quote(amount=amount, volume=volume)])
    for delta in (100_000, 110_000, 90_000, 105_000, 95_000, 100_000):
        clock.advance(15)
        amount += delta
        volume += 10
        service._process_snapshot([_quote(price=10.01, amount=amount, volume=volume)])
    clock.advance(15)
    amount += 1_000_000
    service._process_snapshot([_quote(price=10.02, amount=amount, volume=volume + 10)])

    row = service.ranking(15)["rows"][0]
    assert row["zscore"] > 20
    assert row["large_threshold"] == pytest.approx(1_000_000)
    service.stop()


@pytest.mark.parametrize(
    "limit_up",
    [10.20, 10.21],
)
def test_near_limit_symbols_remain_eligible(limit_up, monkeypatch):
    today = date(2026, 8, 5)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: today)
    quote = FakeQuoteService()
    service = LargeOrderService(quote)
    service._trade_date = today
    service._instrument_limits_date = today
    service._instrument_limits = {
        "000001.SZ": {"symbol": "000001.SZ", "as_of": today, "limit_up": limit_up},
    }
    service._running = True
    service._config["max_deep_dive_symbols"] = 0

    service._process_snapshot([_quote()])
    service._process_snapshot([_quote(amount=2_000_000, volume=200)])

    assert service.ranking(60)["count"] == 1
    assert service.status()["filtered_near_limit_count"] == 0
    assert service._deep_calls_used == 0
    assert service._build_alerts_locked("000001.SZ") == []
    service.stop()


def test_price_limit_context_prefers_current_authority_then_rule_fallback():
    today = date(2026, 8, 5)
    service = LargeOrderService()
    state = service._new_state("000001.SZ", "平安银行", time.time())
    raw = _quote(price=10.5)

    service._instrument_limits = {
        "000001.SZ": {"symbol": "000001.SZ", "as_of": today, "limit_up": 12.34},
    }
    service._update_price_context(state, raw, symbol="000001.SZ", price=10.5, trade_date=today)
    assert state["limit_up_price"] == 12.34
    assert state["change_pct"] == pytest.approx(0.05)

    service._instrument_limits["000001.SZ"] = {
        "symbol": "000001.SZ",
        "as_of": today - timedelta(days=1),
        "limit_up": 12.34,
    }
    service._update_price_context(state, raw, symbol="000001.SZ", price=10.5, trade_date=today)
    assert state["limit_up_price"] == 11.0


def test_no_limit_and_missing_reference_are_distinguished(monkeypatch):
    today = date(2026, 8, 5)
    monkeypatch.setattr(large_order_service, "cn_today", lambda: today)
    service = LargeOrderService(FakeQuoteService())
    service._trade_date = today
    service._instrument_limits_date = today
    service._instrument_limits = {
        "000001.SZ": {"symbol": "000001.SZ", "as_of": today, "limit_up": 10_000},
    }
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    service._process_snapshot([_quote()])
    service._process_snapshot([_quote(amount=2_000_000, volume=200)])
    assert service.ranking(60)["count"] == 1
    assert service.ranking(60)["rows"][0]["limit_up_gap_pct"] is None

    unknown = _quote(symbol="000002.SZ")
    unknown.pop("prev_close")
    unknown.pop("change_pct")
    service._process_snapshot([unknown])
    unknown["amount"] = 2_000_000
    service._process_snapshot([unknown])
    assert service.status()["unassessable_count"] == 0
    assert any(row["symbol"] == "000002.SZ" for row in service.ranking(60)["rows"])
    service.stop()


def test_ranking_and_status_do_not_wait_for_processing_lock(monkeypatch):
    service = LargeOrderService(FakeQuoteService())
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    service._process_snapshot([_quote()])
    service._process_snapshot([_quote(amount=2_000_000, volume=200)])
    monkeypatch.setattr(
        service,
        "_build_rankings_locked",
        lambda _now: pytest.fail("API must not rebuild rankings"),
    )

    with service._lock, ThreadPoolExecutor(max_workers=2) as executor:
        ranking = executor.submit(service.ranking, 60).result(timeout=0.2)
        status = executor.submit(service.status).result(timeout=0.2)
    assert ranking["count"] == 1
    assert status["candidate_count"] == 1
    service.stop()


def test_full_market_snapshot_and_cached_ranking_performance():
    service = LargeOrderService(FakeQuoteService())
    service._running = True
    service._config["max_deep_dive_symbols"] = 0
    initial = []
    updated = []
    for index in range(5_600):
        symbol = f"{index:06d}.SZ"
        initial.append(_quote(symbol=symbol))
        updated.append(_quote(symbol=symbol, price=10.01, amount=1_100_000 + index, volume=200))

    service._process_snapshot(initial)
    started = time.perf_counter()
    service._process_snapshot(updated)
    processing_seconds = time.perf_counter() - started
    latencies = []
    for _ in range(20):
        started = time.perf_counter()
        assert service.ranking(60)["count"] == 50
        latencies.append(time.perf_counter() - started)

    assert processing_seconds < 2.0
    assert sorted(latencies)[18] < 0.5
    service.stop()


def test_ranking_evidence_mode_filters_published_candidates():
    service = LargeOrderService()
    service._rankings[60] = (
        {
            "symbol": "000001.SZ",
            "score": 88,
            "net_buy_amount": 2_000_000,
            "data_quality": "precise",
            "intent_count": 0,
        },
        {
            "symbol": "600000.SH",
            "score": 70,
            "net_buy_amount": 1_000_000,
            "data_quality": "proxy_only",
            "intent_count": 3,
        },
    )

    assert service.ranking(60, mode="combined")["count"] == 2
    assert [row["symbol"] for row in service.ranking(60, mode="execution")["rows"]] == [
        "000001.SZ"
    ]
    assert [row["symbol"] for row in service.ranking(60, mode="intent")["rows"]] == [
        "600000.SH"
    ]
