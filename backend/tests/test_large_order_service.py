from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
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


def _quote(*, price=10.0, amount=1_000_000.0, volume=100.0, symbol="000001.SZ"):
    return {
        "symbol": symbol,
        "name": "平安银行",
        "last_price": price,
        "prev_close": 10.0,
        "change_pct": price / 10.0 - 1.0,
        "amount": amount,
        "volume": volume,
    }


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
    ("limit_up", "expected_count", "filtered"),
    [(10.20, 0, 1), (10.21, 1, 0)],
)
def test_near_limit_threshold_is_hard_filter(limit_up, expected_count, filtered, monkeypatch):
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

    assert service.ranking(60)["count"] == expected_count
    assert service.status()["filtered_near_limit_count"] == filtered
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
    assert service.status()["unassessable_count"] == 1
    assert all(row["symbol"] != "000002.SZ" for row in service.ranking(60)["rows"])
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
