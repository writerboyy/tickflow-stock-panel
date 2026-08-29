from datetime import date, datetime, timedelta
import json
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.limit_board import router
from app.market_time import CN_TZ
from app.services.limit_board_service import (
    LimitBoardService,
    _qualified_premium_stats,
    _sweep_ready,
)
from app.services.limit_board_store import LimitBoardStore, default_config
from app.services.quote_service import QuoteService
from app.services.qmt_trading import QmtOrderPreflightError
from app.services.screener import ScreenerService


class FakeRepo:
    def get_enriched_latest(self):
        return pl.DataFrame({"symbol": ["600000.SH"], "date": [datetime(2026, 8, 12).date()]}), datetime(2026, 8, 12).date()

    def get_enriched_range(self, *_args, **_kwargs):
        return pl.DataFrame({
            "symbol": ["600000.SH", "600001.SH"],
            "date": [datetime(2026, 8, 12).date(), datetime(2026, 8, 12).date()],
            "signal_limit_up": [False, True],
        })

    def get_instruments(self):
        return pl.DataFrame({
            "symbol": ["600000.SH", "600001.SH", "600002.SH"],
            "name": ["浦发银行", "邯郸钢铁", "*ST风险"],
        })

    def get_name_map(self, symbols=None):
        names = {"600000.SH": "浦发银行", "600001.SH": "邯郸钢铁", "600002.SH": "*ST风险"}
        return names.copy() if symbols is None else {symbol: names[symbol] for symbol in symbols if symbol in names}

    def resolve_asset_type(self, _symbol):
        return "stock"


class FakeQuotes:
    def __init__(self):
        self.events = []
        self.consumers = {}
        self.enriched = pl.DataFrame()
        self.enriched_date = None
        self.latest_quotes = []
        self.final_sync_done = False

    @staticmethod
    def realtime_provider():
        return "tickflow"

    def get_fresh_quotes(self, symbols):
        requested = {str(symbol).strip().upper() for symbol in symbols}
        rows = {
            str(row.get("symbol") or "").strip().upper(): dict(row)
            for row in self.latest_quotes
            if str(row.get("symbol") or "").strip().upper() in requested
        }
        return {
            "live": rows.keys() == requested,
            "quotes": rows,
            "missing_symbols": sorted(requested - rows.keys()),
            "as_of": max((row.get("timestamp") for row in rows.values()), default=None),
        }

    @staticmethod
    def get_min_interval():
        return 5.0

    def publish_external_alerts(self, events):
        self.events.extend(events)

    @staticmethod
    def enrich_external_alerts(events):
        for event in events:
            event["ext_gn_ths__所属概念"] = "银行;金融科技"
            event["concept"] = "银行;金融科技"

    def get_latest_quotes(self, symbols=None):
        if not symbols:
            return [dict(row) for row in self.latest_quotes]
        return [
            dict(row) for row in self.latest_quotes
            if str(row.get("symbol") or "").strip().upper() in symbols
        ]

    def status(self):
        return {"final_sync_done": self.final_sync_done}

    def get_enriched_today(self):
        return self.enriched, self.enriched_date

    @staticmethod
    def get_index_quotes():
        return pl.DataFrame()

    def notify_limit_board_updated(self):
        pass

    def set_symbol_consumer(self, consumer_id, symbols):
        self.consumers[consumer_id] = set(symbols)

    def remove_symbol_consumer(self, consumer_id):
        self.consumers.pop(consumer_id, None)


class FakeQmt:
    def __init__(self):
        self.orders = []

    @staticmethod
    def status():
        return {
            "configured": True,
            "state": "ready",
            "trade_enabled": True,
            "reason": "QMT RPC 在线",
        }

    def submit_order(self, request):
        order = dict(request)
        if order.get("volume") is None:
            price = float(order["price"])
            mode = order.get("allocation_mode")
            if mode == "lot":
                volume = 100
            elif mode == "fixed":
                volume = int(float(order.get("allocation_value") or 0) / price / 100) * 100
            elif mode == "quarter":
                volume = int(120_000 * 0.25 / price / 100) * 100
            elif mode == "third":
                volume = int(120_000 / 3 / price / 100) * 100
            elif mode == "half":
                volume = int(120_000 * 0.5 / price / 100) * 100
            else:
                volume = 0
            if volume < 100:
                raise ValueError("金额不足一手")
            order["volume"] = volume
            order["estimated_amount"] = round(volume * price, 2)
        self.orders.append(order)
        return {**order, "status": "accepted_pending", "order_sys_id": "qmt-1"}

    @staticmethod
    def preview_order(request):
        price = float(request["price"])
        mode = request.get("allocation_mode")
        if mode == "lot":
            volume = 100
        elif mode == "fixed":
            volume = int(float(request.get("allocation_value") or 0) / price / 100) * 100
        else:
            volume = 100
        return {
            "volume": volume,
            "actual_amount": round(price * volume, 2),
            "target_amount": round(price * volume, 2),
            "capped": False,
            "reason": "金额不足一手" if volume < 100 else None,
        }


def test_sector_candidate_score_prefers_institutional_score():
    assert LimitBoardService._sector_candidate_score({
        "score": 12.0,
        "institutional_score": 36.0,
        "institutional_max_score": 45.0,
    }) == pytest.approx(40.0)
    assert LimitBoardService._sector_candidate_score({"score": 12.0}) == pytest.approx(12.0)


def test_institutional_sector_fields_expose_history_and_realtime_confirmation():
    dates = [f"2026-08-{day:02d}" for day in range(10, 15)]
    rotation = {
        "dates": dates,
        "columns": {
            day: [["通信", 0.01], ["板块一", 0.0], ["板块二", -0.01]]
            for day in dates
        },
    }
    fields = LimitBoardService._institutional_sector_fields(
        {"plate_name": "通信", "amount": 1_000_000.0, "main_net": 100_000.0, "volume_ratio": 1.4},
        [rotation],
        date(2026, 8, 17),
    )

    assert fields["institutional_score"] > 0
    assert fields["institutional_max_score"] > 50
    assert fields["one_day_change_pct"] == pytest.approx(0.01)
    assert fields["five_day_change_pct"] is not None
    assert fields["twenty_day_change_pct"] is None
    assert "money_flow" in fields["institutional_components"]
    assert "liquidity" in fields["institutional_components"]


class ImmediateExecutor:
    @staticmethod
    def submit(callback, *args):
        callback(*args)

    @staticmethod
    def shutdown(**_kwargs):
        pass


def make_service(tmp_path, qmt=None):
    quotes = FakeQuotes()
    service = LimitBoardService(
        tmp_path,
        FakeRepo(),
        quotes,
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=qmt),
    )
    config = default_config()
    service._history_ready = True
    service._first_board_eligible = {"600000.SH"}
    return service, quotes, config


def test_jijiang_realtime_view_adds_cached_yesterday_boards(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 27)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    calls = []

    def load_prior(_as_of):
        calls.append(1)
        return pl.DataFrame({
            "symbol": ["600000.SH", "600001.SH"],
            "boards": [1, 3],
        })

    monkeypatch.setattr(service._screener, "load_prior_ladder_boards", load_prior)
    service.app_state.kaipanla_collector = SimpleNamespace(
        jijiang_realtime_snapshot=lambda: {
            "provider": "kaipanla_socket",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-27T10:00:00+08:00",
            "rows": [
                {"thscode": "600000.SH", "name": "浦发银行"},
                {"thscode": "600001.SH", "name": "邯郸钢铁"},
                {"thscode": "600002.SH", "name": "无连板"},
            ],
        },
    )

    first = service.jijiang_realtime_view()
    second = service.jijiang_realtime_view()

    assert [row["yesterday_boards"] for row in first["rows"]] == [1, 3, 0]
    assert second["rows"] == first["rows"]
    assert len(calls) == 1


def test_jijiang_realtime_view_uses_system_ladder_boards(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 27)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    monkeypatch.setattr(
        service._screener,
        "load_prior_ladder_boards",
        lambda _as_of: pl.DataFrame({
            "symbol": ["600000.SH", "600001.SH", "600002.SH"],
            "boards": [1, 2, 3],
        }),
    )
    service.app_state.kaipanla_collector = SimpleNamespace(
        jijiang_realtime_snapshot=lambda: {
            "provider": "kaipanla_socket",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-27T10:00:00+08:00",
            "rows": [
                {"thscode": "600000.SH", "name": "昨日首板"},
                {"thscode": "600001.SH", "name": "昨日二板"},
                {"thscode": "600002.SH", "name": "昨日三板"},
                {"thscode": "600003.SH", "name": "不在天梯"},
            ],
        },
    )

    result = service.jijiang_realtime_view()

    assert [row["yesterday_boards"] for row in result["rows"]] == [1, 2, 3, 0]


def test_load_prior_ladder_boards_matches_limit_board_statuses(monkeypatch):
    screener = ScreenerService(FakeRepo())
    prior_date = date(2026, 8, 26)
    monkeypatch.setattr(screener, "_find_prior_enriched_date", lambda _as_of: prior_date)
    monkeypatch.setattr(
        screener,
        "_load_enriched_for_date",
        lambda _date: pl.DataFrame({
            "symbol": ["600000.SH", "600001.SH", "600002.SH", "600003.SH"],
            "signal_limit_up": [True, False, False, False],
            "signal_broken_limit_up": [False, True, False, False],
            "consecutive_limit_ups": [2, 0, 0, 0],
        }),
    )
    monkeypatch.setattr(
        screener,
        "load_prior_consecutive",
        lambda _as_of, _column: pl.DataFrame({
            "symbol": ["600001.SH", "600002.SH"],
            "prev_consec": [1, 2],
        }),
    )

    result = screener.load_prior_ladder_boards(date(2026, 8, 27))

    assert result.sort("symbol").to_dicts() == [
        {"symbol": "600000.SH", "boards": 2},
        {"symbol": "600001.SH", "boards": 2},
        {"symbol": "600002.SH", "boards": 3},
    ]


def test_top_sector_rows_returns_top_fifteen():
    rows = [
        {"plate_id": f"P{rank:02d}", "plate_name": f"板块{rank:02d}", "rank": rank, "strength": 100 - rank}
        for rank in range(1, 21)
    ]

    selected = LimitBoardService._top_sector_rows(rows)

    assert len(selected) == 15
    assert [row["rank"] for row in selected] == list(range(1, 16))


def premium_snapshot(*symbols: str) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": list(symbols),
        "limit_up_count": [4] * len(symbols),
        "next_day_red_rate": [0.80] * len(symbols),
        "first_board_broken_rate": [0.75] * len(symbols),
    })


def quote(price=11.0, limit=11.0):
    return {
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": price,
        "limit_up": limit,
        "limit_gap_pct": max(0.0, limit / price - 1.0),
        "timestamp": "2026-08-13T10:00:00+08:00",
        "source_modes": ["first_board"],
    }


def test_start_registers_limit_board_consumer(tmp_path):
    service, quotes, _config = make_service(tmp_path)

    service.start()
    try:
        assert service._thread is not None
        assert quotes.consumers["limit_board"] == set()
    finally:
        service.stop()


def test_start_does_not_acquire_polling_when_realtime_is_disabled(tmp_path, monkeypatch):
    class LeaseQuotes(FakeQuotes):
        def __init__(self):
            super().__init__()
            self.acquired = False

        def acquire_temporary_polling(self, _interval):
            self.acquired = True

    quotes = LeaseQuotes()
    service = LimitBoardService(
        tmp_path,
        FakeRepo(),
        quotes,
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=None),
    )
    monkeypatch.setattr("app.services.preferences.get_realtime_quotes_enabled", lambda: False)
    monkeypatch.setattr("app.services.preferences.get_limit_ladder_monitor_enabled", lambda: True)

    service.start()
    try:
        assert quotes.acquired is False
        assert service._polling_lease is False
    finally:
        service.stop()


def test_start_keeps_service_available_when_initial_refresh_fails(tmp_path, monkeypatch):
    service, quotes, _config = make_service(tmp_path)

    def fail_consumer():
        raise RuntimeError("consumer unavailable")

    def fail_initial_refresh():
        raise RuntimeError("initial refresh unavailable")

    monkeypatch.setattr(service, "_refresh_symbol_consumer", fail_consumer)
    monkeypatch.setattr(service, "_on_market_fetch", fail_initial_refresh)

    service.start()
    try:
        assert service._thread is not None
        assert service._started is True
        assert "首次行情刷新失败" in (service._last_error or "")
        assert quotes.events == []
    finally:
        service.stop()


def test_first_touch_records_only_private_board_event(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {}
    service._evaluate_quotes({"600000.SH": quote()}, runtime, config)
    service._evaluate_quotes({"600000.SH": quote()}, runtime, config)

    assert runtime["symbols"]["600000.SH"]["status"] == "touched"
    assert quotes.events == []
    events = service.store.events("2026-08-13")
    assert len(events) == 1
    assert events[0]["type"] == "touched"


def test_first_touch_is_deduplicated_after_runtime_state_is_trimmed(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 3, 20, 150000, tzinfo=CN_TZ),
    )
    runtime = service._runtime_for_today()
    current_quote = {
        **quote(),
        "timestamp": "2026-08-13T10:03:20.150+08:00",
    }

    service._evaluate_quotes({"600000.SH": current_quote}, runtime, config)
    runtime["symbols"].pop("600000.SH")
    service._evaluate_quotes({"600000.SH": current_quote}, runtime, config)

    events = service.store.events("2026-08-13")
    assert len(events) == 1
    assert events[0]["trigger_at"] == "2026-08-13T10:03:20.150+08:00"
    assert quotes.events == []


def test_event_store_keeps_real_break_cycles_and_collapses_legacy_duplicates(tmp_path):
    store = LimitBoardStore(tmp_path)
    base = {
        "trading_date": "2026-08-13",
        "symbol": "600000.SH",
        "name": "浦发银行",
    }
    store.append_event({**base, "ts": 200, "type": "touched", "break_count": 0})
    store.append_event({**base, "ts": 100, "type": "touched", "break_count": 0})
    assert store.append_event_once({
        **base, "ts": 300, "type": "touched", "break_count": 0,
    }) is False
    assert store.append_event_once({
        **base, "ts": 400, "type": "broken", "break_count": 1,
    }) is True
    assert store.append_event_once({
        **base, "ts": 500, "type": "resealed", "break_count": 1,
    }) is True
    assert store.append_event_once({
        **base, "ts": 600, "type": "broken", "break_count": 2,
    }) is True

    events = store.events("2026-08-13")

    assert [(item["type"], item["break_count"]) for item in events] == [
        ("broken", 2),
        ("resealed", 1),
        ("broken", 1),
        ("touched", 0),
    ]
    assert events[-1]["ts"] == 100


def test_full_market_quote_prefers_authoritative_instrument_name(tmp_path, monkeypatch):
    service, quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service._history_date = datetime(2026, 8, 13).date()
    monkeypatch.setattr(
        service, "_automatic_candidate_symbols", lambda _day: {"600000.SH"},
    )

    service._process_quotes([{
        "symbol": "600000.SH",
        "name": "600000.SH",
        "last_price": 11.0,
        "limit_up": 11.0,
        "timestamp": "2026-08-13T10:00:00+08:00",
    }])

    assert service._quotes["600000.SH"]["name"] == "浦发银行"
    events = service.store.events("2026-08-13")
    assert events[0]["name"] == "浦发银行"
    assert events[0]["message"] == "浦发银行：涨停"
    assert quotes.events == []


@pytest.mark.skip(reason="短线猎手已改为开盘啦 socket 行情，不再订阅 TickFlow")
def test_heat_quote_snapshot_registers_batch_consumer_and_returns_quotes(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    service._sector_candidates_by_symbol = {
        "600000.SH": [{"plate_id": "801001", "plate_name": "人工智能"}],
    }
    quotes.latest_quotes = [{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": 10.25,
        "change_pct": 0.025,
        "limit_up": 11.0,
        "timestamp": "2026-08-18T10:00:00+08:00",
    }]

    snapshot = service.quote_snapshot(["600000.sh", "600001.SH"])

    assert quotes.consumers["limit_board"] == {"600000.SH", "600001.SH"}
    assert snapshot["state"] == "partial"
    assert snapshot["as_of"] == "2026-08-18T10:00:00+08:00"
    assert snapshot["quotes"]["600000.SH"]["last_price"] == 10.25
    assert snapshot["quotes"]["600000.SH"]["change_pct"] == 0.025
    assert snapshot["sector_links"] == {
        "600000.SH": [{"plate_id": "801001", "plate_name": "人工智能"}],
    }
    assert snapshot["missing_symbols"] == ["600001.SH"]


@pytest.mark.skip(reason="短线猎手已改为开盘啦 socket 行情，不再回退日线快照")
def test_heat_quote_snapshot_uses_daily_snapshot_when_realtime_is_empty(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    quotes.enriched = pl.DataFrame({
        "symbol": ["600000.SH"],
        "name": ["浦发银行"],
        "close": [10.25],
        "prev_close": [10.0],
        "change_pct": [0.025],
    })
    quotes.enriched_date = date(2026, 8, 18)

    snapshot = service.quote_snapshot(["600000.SH"])

    assert snapshot["state"] == "snapshot"
    assert snapshot["quotes"]["600000.SH"]["last_price"] == 10.25
    assert snapshot["quotes"]["600000.SH"]["limit_up"] == 11.0
    assert snapshot["quotes"]["600000.SH"]["source"] == "daily_snapshot"


def test_automatic_candidate_is_scored_without_near_limit_filter(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    monkeypatch.setattr(
        service, "_automatic_candidate_symbols", lambda _day: {"600000.SH"},
    )
    service._history_date = now.date()

    def score(runtime, candidates, _now):
        runtime["candidate_scores"] = {
            str(row["symbol"]): {
                "candidate_score": 80.0,
                "candidate_score_detail": {},
            }
            for row in candidates
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)

    service._process_quotes([{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": 8.0,
        "limit_up": 11.0,
        "timestamp": now.isoformat(),
    }])

    state = service._runtime_for_today()["symbols"]["600000.SH"]
    assert state["limit_gap_pct"] == pytest.approx(0.375)
    assert state["status"] == "watching"
    assert state["source_modes"] == ["first_board"]


def test_automatic_candidate_is_retained_before_open_without_limit_price(
    tmp_path, monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 13, 9, 25, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    monkeypatch.setattr(
        service, "_automatic_candidate_symbols", lambda _day: {"600000.SH"},
    )
    monkeypatch.setattr(service, "_refresh_candidate_scores", lambda *_args: False)
    service._history_date = now.date()

    service._process_quotes([{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": 10.2,
        "change_pct": 0.02,
        "timestamp": now.isoformat(),
        "source": "tickflow",
    }])

    state = service._runtime_for_today()["symbols"]["600000.SH"]
    assert state["source_modes"] == ["first_board"]
    assert state["status"] == "watching"
    assert state["limit_up"] is None
    assert state["limit_gap_pct"] is None
    assert [row["symbol"] for row in service.view()["first_board"]] == ["600000.SH"]


def test_new_stock_sentinel_does_not_infer_limit_price(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    service.repo.get_instruments = lambda: pl.DataFrame({
        "symbol": ["600000.SH"],
        "limit_up": [100000.0],
    })
    quotes.latest_quotes = [{
        "symbol": "600000.SH",
        "name": "新股",
        "last_price": 20.0,
        "prev_close": 10.0,
        "change_pct": 1.0,
        "timestamp": "2026-08-18T10:00:00+08:00",
    }]

    snapshot = service.quote_snapshot(["600000.SH"])

    assert snapshot["quotes"]["600000.SH"]["limit_up"] is None


def test_authoritative_limit_price_is_preserved_in_quote_snapshot(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    service.repo.get_instruments = lambda: pl.DataFrame({
        "symbol": ["600000.SH"],
        "limit_up": [11.0],
    })
    quotes.latest_quotes = [{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": 11.0,
        "prev_close": 10.0,
        "change_pct": 0.1,
        "timestamp": "2026-08-18T10:00:00+08:00",
    }]

    snapshot = service.quote_snapshot(["600000.SH"])

    assert snapshot["quotes"]["600000.SH"]["limit_up"] == 11.0


def test_instrument_limit_up_cache_reloads_after_repository_snapshot_changes(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    repo_instruments = pl.DataFrame({
        "symbol": ["600000.SH"],
        "limit_up": [20.55],
    })
    service.repo.get_instruments = lambda: repo_instruments
    quotes.latest_quotes = [{
        "symbol": "600000.SH",
        "last_price": 21.53,
        "timestamp": "2026-08-24T09:30:00+08:00",
    }]

    first = service._fresh_tickflow_quotes({"600000.SH"})
    assert first["quotes"]["600000.SH"]["limit_up"] == 20.55

    repo_instruments = pl.DataFrame({
        "symbol": ["600000.SH"],
        "limit_up": [22.61],
    })
    second = service._fresh_tickflow_quotes({"600000.SH"})

    assert second["quotes"]["600000.SH"]["limit_up"] == 22.61


def test_auto_trade_blocks_quote_above_limit_price(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {}

    service._maybe_auto_trade(
        "600000.SH",
        quote(price=21.61, limit=20.55),
        state,
        config,
    )

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "最新价" in state["auto_order_error"]
    assert "涨停价" in state["auto_order_error"]


def test_evaluate_quotes_ignores_quote_above_limit_price(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ),
    )
    runtime = service._runtime_for_today()

    service._evaluate_quotes(
        {"600000.SH": quote(price=21.61, limit=20.55)},
        runtime,
        config,
    )

    state = runtime["symbols"]["600000.SH"]
    assert state["status"] == "watching"
    assert state.get("touched") is not True
    assert service.store.events("2026-08-13") == []
    assert quotes.events == []


def test_automatic_preselection_keeps_top_ten_per_sector_and_manual_rows():
    updates = {
        f"600{index:03d}.SH": {
            "symbol": f"600{index:03d}.SH",
            "change_pct": index / 100,
            "limit_gap_pct": (20 - index) / 100,
            "source_modes": ["first_board"],
            "top_sector_ids": ["P01"],
        }
        for index in range(12)
    }
    updates["000001.SZ"] = {
        "symbol": "000001.SZ",
        "change_pct": -0.05,
        "source_modes": ["selected"],
        "top_sector_ids": [],
    }

    selected = LimitBoardService._preselect_automatic_updates(updates)

    assert set(selected) == {
        *(f"600{index:03d}.SH" for index in range(2, 12)),
        "000001.SZ",
    }


def test_automatic_candidates_keep_only_scored_top_thirty(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    symbols = [f"{600000 + index}.SH" for index in range(35)]
    runtime = {
        "symbols": {
            symbol: {"source_modes": ["first_board"]}
            for symbol in symbols
        },
        "candidate_scores": {
            symbol: {
                "candidate_score": float(100 - index),
                "candidate_score_state": "live",
                "candidate_score_detail": {},
            }
            for index, symbol in enumerate(symbols)
        },
    }
    runtime["symbols"]["300001.SZ"] = {"source_modes": ["selected"]}
    runtime["candidate_scores"]["300001.SZ"] = {
        "candidate_score": None,
        "candidate_score_detail": {},
    }

    retained = service._trim_automatic_candidates(runtime)

    assert retained == set(symbols[:30])
    assert set(runtime["symbols"]) == {*symbols[:30], "300001.SZ"}
    assert set(runtime["candidate_scores"]) == {*symbols[:30], "300001.SZ"}


def test_automatic_candidate_trim_does_not_prioritize_cached_scores(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    symbols = [f"{600000 + index}.SH" for index in range(35)]
    runtime = {
        "symbols": {
            symbol: {"source_modes": ["first_board"]}
            for symbol in symbols
        },
        "candidate_scores": {
            symbol: {
                "candidate_score": float(index),
                "candidate_score_state": "cached",
                "candidate_score_detail": {},
            }
            for index, symbol in enumerate(symbols)
        },
    }

    retained = service._trim_automatic_candidates(runtime)

    assert retained == set(symbols[:30])


def test_automatic_candidates_keep_unscored_rows_when_score_context_unavailable(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    symbols = [f"600{index:03d}.SH" for index in range(3)]
    runtime = {
        "symbols": {
            symbol: {"source_modes": ["first_board"], "change_pct": 0.03 - index / 100}
            for index, symbol in enumerate(symbols)
        },
        "candidate_scores": {
            symbol: {
                "candidate_score": None,
                "candidate_score_state": "unavailable",
                "candidate_score_detail": {},
            }
            for symbol in symbols
        },
    }

    retained = service._trim_automatic_candidates(runtime)

    assert retained == set(symbols)
    assert set(runtime["symbols"]) == set(symbols)


def test_main_board_only_filters_automatic_candidate_universe(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    symbols = {
        "600000.SH",
        "000001.SZ",
        "300001.SZ",
        "688001.SH",
        "920001.BJ",
    }
    service._first_board_eligible = set(symbols)
    monkeypatch.setattr(
        service, "_refresh_sector_candidate_universe", lambda _day: set(symbols),
    )
    service.store.update(
        0,
        lambda value: value["settings"].update({"main_board_only": True}),
    )

    automatic = service._automatic_candidate_symbols(date(2026, 8, 17))

    assert automatic == {"600000.SH", "000001.SZ"}


def test_first_board_filters_st_from_history_and_realtime_quotes(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    config["settings"]["first_board_lookback_days"] = 1
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene.refresh",
        lambda _repo: premium_snapshot("600000.SH", "600001.SH", "600002.SH"),
    )

    service._history_date = None
    service._refresh_history(config)
    assert "600002.SH" not in service._first_board_eligible
    service._first_board_eligible.add("600002.SH")

    service._process_quotes([{
        "symbol": "600002.SH",
        "name": "*ST风险",
        "last_price": 11.0,
        "limit_up": 11.0,
        "timestamp": "2026-08-13T10:00:00+08:00",
    }])

    assert "600002.SH" not in service._quotes
    assert service._runtime_for_today()["symbols"] == {}
    assert quotes.events == []


def test_history_separates_clean_first_board_from_rebound_setup(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    config["settings"]["first_board_lookback_days"] = 3
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene.refresh",
        lambda _repo: premium_snapshot("600000.SH", "600001.SH"),
    )
    dates = [datetime(2026, 8, day).date() for day in (10, 11, 12)]
    history = pl.DataFrame({
        "symbol": ["600000.SH"] * 3 + ["600001.SH"] * 3,
        "date": dates + dates,
        "signal_limit_up": [True, False, False, False, False, False],
        "signal_broken_limit_up": [False, True, False, False, False, False],
    })
    service.repo.get_enriched_latest = lambda: (history, dates[-1])
    service.repo.get_enriched_range = lambda *_args, **_kwargs: history
    service._history_date = None

    service._refresh_history(config)

    assert "600001.SH" in service._first_board_eligible
    assert "600000.SH" not in service._first_board_eligible
    assert service._rebound_board_eligible == {"600000.SH"}


def test_automatic_candidates_require_all_premium_gene_thresholds():
    rows = pl.DataFrame({
        "symbol": ["PASS", "COUNT", "RED", "BROKEN", "MISSING"],
        "limit_up_count": [4, 3, 4, 4, 4],
        "next_day_red_rate": [0.80, 0.90, 0.7999, 0.90, None],
        "first_board_broken_rate": [0.75, 0.10, 0.10, 0.7501, 0.10],
    })

    qualified = _qualified_premium_stats(rows)

    assert qualified == {
        "PASS": {
            "limit_up_count": 4,
            "next_day_red_rate": 0.80,
            "first_board_broken_rate": 0.75,
        },
    }


def test_missing_premium_gene_snapshot_blocks_automatic_scan(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    config["settings"]["first_board_lookback_days"] = 1
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene.refresh",
        lambda _repo: pl.DataFrame(),
    )
    service._history_date = None

    service._refresh_history(config)

    assert service._history_ready is False
    assert service._first_board_eligible == set()
    assert service._rebound_board_eligible == set()
    assert "溢价基因数据不足" in service._history_reason


def test_missing_history_cache_remains_on_fast_warmup_retry(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    config["settings"]["first_board_lookback_days"] = 1
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service.repo.get_enriched_range = lambda *_args, **_kwargs: None
    service._history_date = None

    service._refresh_history(config)

    assert service._history_ready is False
    assert "历史指标缓存尚未就绪" in service._history_reason


def test_valid_gene_snapshot_no_longer_blocks_sector_candidate(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    config["settings"]["first_board_lookback_days"] = 1
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene.refresh",
        lambda _repo: pl.DataFrame({
            "symbol": ["600000.SH"],
            "limit_up_count": [3],
            "next_day_red_rate": [0.90],
            "first_board_broken_rate": [0.10],
        }),
    )
    service._history_date = None

    service._refresh_history(config)

    assert service._history_ready is True
    assert service._first_board_eligible == {"600000.SH"}
    assert "涨停基因用于 10 分个股排序" in service._history_reason


def test_history_retries_after_cache_warmup_without_market_event(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    today = datetime(2026, 8, 13).date()
    current_mono = [104.0]
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: today,
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.time.monotonic",
        lambda: current_mono[0],
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene.refresh",
        lambda _repo: premium_snapshot("600000.SH"),
    )
    service.store.update(
        0,
        lambda value: value["settings"].update({"first_board_lookback_days": 1}),
    )
    service._history_ready = False
    service._history_date = today
    service._history_attempt_at = 100.0
    service._history_reason = "历史指标缓存尚未就绪，首板/反包扫描已暂停"

    service._retry_history()
    assert service._history_ready is False

    current_mono[0] = 105.0
    service._retry_history()

    assert service._history_ready is True
    assert "已核对前 1 个交易日" in service._history_reason
    assert service._queue.get_nowait() == {"type": "market", "quotes": []}


def test_rebound_quote_enters_candidate_pool_with_rebound_source(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service._history_date = datetime(2026, 8, 13).date()
    service._first_board_eligible = set()
    service._rebound_board_eligible = {"600000.SH"}
    monkeypatch.setattr(
        service, "_automatic_candidate_symbols", lambda _day: {"600000.SH"},
    )

    def score(runtime, candidates, _now):
        runtime["candidate_scores"] = {
            str(row["symbol"]): {
                "candidate_score": 80.0,
                "candidate_score_detail": {},
                "candidate_reasons": ["反包候选"],
            }
            for row in candidates
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)

    service._process_quotes([{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "last_price": 10.8,
        "limit_up": 11.0,
        "timestamp": "2026-08-13T10:00:00+08:00",
    }])

    view = service.view()
    assert view["first_board"][0]["source_modes"] == ["rebound_board"]
    assert view["candidate_pool"][0]["source"] == "rebound_board"
    assert "反包候选" in view["candidate_pool"][0]["candidate_reasons"]


def test_manual_candidate_rejects_st_stock(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    with pytest.raises(ValueError, match="已过滤 ST"):
        service.add_candidate("600002.SH", 0)


def test_view_enriches_themes_and_hides_legacy_st_rows(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {"name": "浦发银行", "source_modes": ["first_board"]},
        "600002.SH": {"name": "*ST风险", "source_modes": ["first_board"]},
    }
    service.store.save_runtime(runtime)

    view = service.view()

    assert [row["symbol"] for row in view["first_board"]] == ["600000.SH"]
    assert view["first_board"][0]["concept"] == "银行;金融科技"


def test_view_repairs_code_names_in_existing_runtime_and_events(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {
        "name": "600000.SH",
        "source_modes": ["first_board"],
        "status": "resealed",
    }
    service.store.save_runtime(runtime)
    service.store.append_event({
        "ts": 1,
        "trading_date": "2026-08-13",
        "source": "limit_board",
        "type": "resealed",
        "rule_name": "回封",
        "symbol": "600000.SH",
        "name": "600000.SH",
        "message": "600000.SH：回封",
        "reasons": ["连续 3 个五档快照确认回封"],
    })

    view = service.view()

    assert view["first_board"][0]["name"] == "浦发银行"
    assert view["events"][0]["name"] == "浦发银行"
    assert view["events"][0]["message"] == "浦发银行：回封"


def test_view_attaches_batched_qmt_timeline_to_touch_event(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service.store.append_event({
        "ts": 1_786_594_600_150,
        "trigger_at": "2026-08-13T10:03:20.150+08:00",
        "trading_date": "2026-08-13",
        "source": "limit_board",
        "type": "touched",
        "rule_name": "涨停",
        "symbol": "600000.SH",
        "name": "浦发银行",
        "message": "浦发银行：涨停",
        "break_count": 0,
        "reasons": ["首次触及涨停价"],
    })
    qmt.get_orders = lambda keys: {
        "limit-board-20260813-600000.SH": {
            "idempotency_key": "limit-board-20260813-600000.SH",
            "status": "accepted_pending",
            "trigger_at": "2026-08-13T10:03:20.150+08:00",
            "system_order_at": "2026-08-13T10:03:20.200+08:00",
            "qmt_submit_at": "2026-08-13T10:03:20.250+08:00",
            "qmt_response_at": "2026-08-13T10:03:20.500+08:00",
            "broker_order_at": "2026-08-13T10:05:00.000+08:00",
        }
    } if "limit-board-20260813-600000.SH" in keys else {}

    event = service.view()["events"][0]

    assert event["order_timeline"]["qmt_submit_at"] == "2026-08-13T10:03:20.250+08:00"
    assert event["order_timeline"]["broker_order_at"] == "2026-08-13T10:05:00.000+08:00"
    assert event["order_timeline"]["system_to_broker_delay_ms"] == 99_800


def test_view_keeps_unscored_candidate_when_context_is_unavailable(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {
            "name": "浦发银行",
            "source_modes": ["first_board"],
            "status": "near_limit",
            "limit_gap_pct": 0.004,
            "bid1_volume": 10000,
        },
        "600001.SH": {
            "name": "邯郸钢铁",
            "source_modes": ["first_board"],
            "status": "near_limit",
            "limit_gap_pct": 0.015,
            "bid1_volume": 100,
        },
    }
    service.store.save_runtime(runtime)
    service.store.update(0, lambda value: value["board_pool"].append({"symbol": "600001.SH", "name": "邯郸钢铁", "auto_trade": False}))

    view = service.view()

    assert [row["symbol"] for row in view["candidate_pool"]] == ["600000.SH"]
    assert view["candidate_pool"][0]["candidate_rank"] is None
    assert view["candidate_pool"][0]["candidate_score"] is None
    assert view["candidate_pool"][0]["candidate_score_state"] == "unavailable"


def test_view_scores_candidate_with_sector_gene_and_technical_context(tmp_path, monkeypatch):
    class SectorService:
        @staticmethod
        def targets_for_symbol(_symbol, *, kind=None, industry_level=None):
            assert industry_level in (None, 2)
            return [{"key": "concept-ai", "kind": "concept", "name": "人工智能"}] if kind == "concept" else []

        @staticmethod
        def build_snapshots(_stock_df, _index_df, targets, _windows, *, now):
            assert now > 0
            return {
                target["key"]: {
                    **target,
                    "valid": True,
                    "change_pct": 0.02,
                    "coverage_ratio": 1.0,
                    "up_count": 5,
                    "down_count": 0,
                    "valid_count": 5,
                    "total_count": 5,
                }
                for target in targets
            }

        @staticmethod
        def member_symbols(_key):
            return {"600000.SH", "600001.SH", "A", "B", "C"}

    qmt = FakeQmt()
    service, quotes, _config = make_service(tmp_path, qmt)
    now = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    service.app_state.sector_monitor_service = SectorService()
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "state": "live",
            "as_of": now.date().isoformat(),
            "rows": [{
                "plate_id": "P-AI",
                "plate_name": "人工智能",
                "change_pct_pct": 2.0,
                "speed_pct_pct": 0.2,
                "strength": 88.0,
                "rank": 1,
                "rank_count": 10,
            }],
        },
    )
    service._sector_memberships = pl.DataFrame({
        "plate_id": ["P-AI"] * 5,
        "symbol": ["600000.SH", "600001.SH", "A", "B", "C"],
    })
    service._sector_live_quotes = {
        symbol: {
            "symbol": symbol,
            "change_pct": change,
            "amount": amount,
            "source": "kaipanla_socket",
        }
        for symbol, change, amount in zip(
            ["600000.SH", "600001.SH", "A", "B", "C"],
            [0.02, 0.10, 0.03, 0.01, -0.01],
            [200.0, 100.0, 100.0, 100.0, 100.0],
            strict=True,
        )
    }
    service.app_state.large_order_service = SimpleNamespace(
        ranking=lambda **_kwargs: {
            "rows": [{
                "symbol": "600000.SH",
                "source": "kaipanla_net_flow",
                "data_quality": "net_flow",
                "net_flow_amount": 8_000_000,
                "net_flow_delta": 1_000_000,
                "net_flow_speed": 200_000,
                "net_flow_window_minutes": 5,
                "net_flow_as_of": now.isoformat(),
            }]
        },
    )
    service._premium_stats["600000.SH"] = {
        "as_of": "2026-08-14",
        "window_days": 200,
        "limit_up_count": 12,
        "premium_5_count": 5,
        "next_day_observation_count": 10,
        "next_day_red_rate": 0.8,
        "first_board_attempt_count": 10,
        "first_board_sealed_count": 8,
        "first_board_seal_rate": 0.8,
        "first_board_broken_rate": 0.2,
        "consecutive_rate": 0.5,
    }
    symbols = ["600000.SH", "600001.SH", "A", "B", "C"]
    quotes.enriched_date = now.date()
    quotes.enriched = pl.DataFrame({
        "symbol": symbols,
        "name": ["浦发银行", "邯郸钢铁", "A", "B", "C"],
        "close": [11.0, 10.9, 10.5, 10.3, 10.1],
        "change_pct": [0.10, 0.09, 0.05, 0.03, 0.01],
        "amount": [200.0, 100.0, 100.0, 100.0, 100.0],
        "ma5": [10.5] * 5,
        "ma10": [10.0] * 5,
        "ma20": [9.5] * 5,
        "ma60": [9.0] * 5,
        "momentum_5d": [0.10] * 5,
        "momentum_20d": [0.30] * 5,
        "vol_ratio_5d": [2.5] * 5,
        "macd_dif": [0.3] * 5,
        "macd_dea": [0.2] * 5,
        "macd_hist": [0.1] * 5,
        "rsi_14": [70.0] * 5,
    })
    quotes.get_intraday_features = lambda symbols, **_kwargs: {
        symbol: {
            "available": True,
            "session_vwap": 10.7,
            "closed_bars": [
                {"open": 10.0, "close": close, "amount": 1_000_000}
                for close in (10.6, 10.7, 10.8, 10.9, 11.0)
            ],
        }
        for symbol in symbols
    }
    rotation_dates = ["2026-08-14", "2026-08-13", "2026-08-12", "2026-08-11", "2026-08-10"]
    rotation = {
        "dates": rotation_dates,
        "columns": {
            day: [["人工智能", value], ["其他", 0.0]]
            for day, value in zip(rotation_dates, [0.03, 0.02, 0.01, 0.0, -0.01], strict=True)
        },
    }
    monkeypatch.setattr(
        "app.services.limit_board_service.rps_rotation.build_rps_rotation",
        lambda *_args, **_kwargs: rotation,
    )
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {
            "name": "浦发银行",
            "source_modes": ["first_board"],
            "status": "touched",
            "limit_gap_pct": 0.0,
        },
    }
    service.store.save_runtime(runtime)

    view = service.view()
    row = view["candidate_pool"][0]
    strong_row = view["first_board"][0]

    assert row["candidate_rank"] == 1
    assert row["candidate_score"] is not None
    assert strong_row["symbol"] == row["symbol"]
    assert strong_row["candidate_score"] == row["candidate_score"]
    assert strong_row["candidate_score_as_of"] == row["candidate_score_as_of"]
    assert strong_row["candidate_score_detail"] == row["candidate_score_detail"]
    assert row["candidate_score_state"] == "live"
    assert row["candidate_score_detail"]["intraday_flow"]["max_score"] == 15.0
    assert row["candidate_score_detail"]["intraday_flow"]["flow_metric"] == "main_net_speed"
    assert row["candidate_score_detail"]["intraday_flow"]["capital_available"] is True
    assert row["candidate_score_detail"]["sector"]["max_score"] == 50.0
    assert row["candidate_score_detail"]["premium_gene"]["max_score"] == 10.0
    assert row["candidate_score_detail"]["technical"]["score"] == 5.0
    assert row["candidate_score_detail"]["sector"]["is_sector_leader"] is False
    assert row["candidate_score_detail"]["sector"]["stock_rank"] == 3
    assert row["candidate_score_detail"]["sector"]["leader"]["symbol"] == "600001.SH"
    assert row["candidate_score_detail"]["sector"]["data_source"] == "kaipanla_socket"
    assert row["change_pct"] == pytest.approx(0.10)
    assert "proximity" not in row["candidate_score_detail"]
    assert view["board_pool"] == []
    assert qmt.orders == []

    service._score_refresh_at = 0.0
    service._refresh_candidate_scores(
        runtime,
        [row],
        now.replace(hour=15, minute=30),
    )
    assert runtime["candidate_scores"]["600000.SH"]["candidate_score_state"] == "cached"


def test_candidate_score_refresh_uses_five_second_window_and_bypasses_for_new_symbol(
    tmp_path, monkeypatch,
):
    service, quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    current_mono = [100.0]
    calls = [0]

    def get_enriched_today():
        calls[0] += 1
        return pl.DataFrame(), None

    quotes.get_enriched_today = get_enriched_today
    monkeypatch.setattr(
        "app.services.limit_board_service.time.monotonic", lambda: current_mono[0],
    )
    runtime = {"candidate_scores": {}}
    first = [{"symbol": "600000.SH", "source_modes": ["first_board"]}]

    assert service._refresh_candidate_scores(runtime, first, now) is True
    assert calls[0] == 1

    current_mono[0] = 105.0
    second = [*first, {"symbol": "600001.SH", "source_modes": ["first_board"]}]
    assert service._refresh_candidate_scores(runtime, second, now) is True
    assert calls[0] == 2

    current_mono[0] = 109.0
    assert service._refresh_candidate_scores(runtime, second, now) is False
    assert calls[0] == 2

    current_mono[0] = 110.0
    service._refresh_candidate_scores(runtime, second, now)
    assert calls[0] == 3


def test_candidate_intraday_features_allow_same_day_final_bars_after_close(tmp_path):
    service, quotes, _config = make_service(tmp_path)
    captured = {}

    def get_intraday_features(symbols, **kwargs):
        captured.update(kwargs)
        return {symbol: {"available": True} for symbol in symbols}

    quotes.get_intraday_features = get_intraday_features
    result = service._candidate_intraday_features(
        {"600000.SH"},
        datetime(2026, 8, 18, 15, 30, tzinfo=CN_TZ),
    )

    assert result["600000.SH"]["available"] is True
    assert captured["freshness_seconds"] == 24 * 60 * 60


def test_candidate_score_refresh_uses_recent_valid_score_when_capital_is_temporarily_missing(
    tmp_path, monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.time.monotonic", lambda: 100.0)
    previous_detail = {
        "intraday_flow": {"score": 35.0, "capital_available": True, "as_of": now.isoformat()},
        "sector": {
            "score": 20.0,
            "name": "人工智能",
            "as_of": now.isoformat(),
            "realtime_available": True,
        },
        "premium_gene": {"score": 11.0, "as_of": "2026-08-14"},
        "technical": {"score": 4.0, "as_of": now.isoformat()},
    }
    runtime = {
        "candidate_scores": {
            "600000.SH": {
                "candidate_score": 70.0,
                "candidate_rank": 1,
                "candidate_score_state": "live",
                "candidate_score_as_of": now.isoformat(),
                "candidate_score_detail": previous_detail,
                "candidate_reasons": [],
            },
        },
    }

    changed = service._refresh_candidate_scores(
        runtime,
        [{"symbol": "600000.SH", "source_modes": ["selected"]}],
        now,
    )

    cached = runtime["candidate_scores"]["600000.SH"]
    assert changed is True
    assert cached["candidate_score"] == 70.0
    assert cached["candidate_score_state"] == "cached"
    assert cached["candidate_score_as_of"] == now.isoformat()
    assert cached["candidate_score_detail"] == previous_detail
    assert runtime["candidate_score_snapshots"]["600000.SH"]["candidate_score"] == 70.0


def test_candidate_score_refresh_expires_display_cache_during_trading(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    captured_at = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    now = captured_at + timedelta(seconds=61)
    monkeypatch.setattr("app.services.limit_board_service.time.monotonic", lambda: 100.0)
    runtime = {
        "candidate_scores": {
            "600000.SH": {
                "candidate_score": 70.0,
                "candidate_score_state": "live",
                "candidate_score_as_of": captured_at.isoformat(),
                "candidate_score_detail": {},
                "candidate_reasons": [],
            },
        },
    }

    service._refresh_candidate_scores(
        runtime,
        [{"symbol": "600000.SH", "source_modes": ["first_board"]}],
        now,
    )

    current = runtime["candidate_scores"]["600000.SH"]
    assert current["candidate_score"] is None
    assert current["candidate_score_state"] == "unavailable"
    assert runtime["candidate_score_snapshots"]["600000.SH"]["candidate_score"] == 70.0


def test_persist_runtime_keeps_newer_candidate_score_snapshots(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    captured_at = datetime(2026, 8, 19, 10, 0, tzinfo=CN_TZ)
    runtime["candidate_score_snapshots"] = {
        "600000.SH": {
            "candidate_score": 70.0,
            "candidate_score_as_of": captured_at.isoformat(),
        },
    }
    service.store.save_runtime(runtime)

    stale_runtime = {
        **runtime,
        "candidate_scores": {"600000.SH": {"candidate_score": None}},
        "candidate_score_snapshots": {},
    }
    service._persist_runtime(stale_runtime)

    persisted = service.store.load_runtime()
    assert persisted["candidate_score_snapshots"]["600000.SH"]["candidate_score"] == 70.0


def test_candidate_score_refresh_restores_persisted_snapshot_after_restart(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    captured_at = datetime(2026, 8, 19, 10, 0, tzinfo=CN_TZ)
    runtime = service._runtime_for_today()
    runtime["candidate_score_snapshots"] = {
        "600000.SH": {
            "candidate_score": 70.0,
            "candidate_score_state": "live",
            "candidate_score_as_of": captured_at.isoformat(),
            "candidate_score_detail": {},
            "candidate_reasons": [],
        },
    }
    service.store.save_runtime(runtime)
    restored = service._runtime_for_today()
    monkeypatch.setattr("app.services.limit_board_service.time.monotonic", lambda: 100.0)

    service._refresh_candidate_scores(
        restored,
        [{"symbol": "600000.SH", "source_modes": ["first_board"]}],
        captured_at + timedelta(seconds=30),
    )

    current = restored["candidate_scores"]["600000.SH"]
    assert current["candidate_score"] == 70.0
    assert current["candidate_score_state"] == "cached"


@pytest.mark.parametrize("hour,minute", [(11, 45), (15, 30)])
def test_candidate_score_refresh_keeps_final_valid_score_outside_trading_hours(
    tmp_path, monkeypatch, hour, minute,
):
    service, _quotes, _config = make_service(tmp_path)
    captured_at = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    now = captured_at.replace(hour=hour, minute=minute)
    monkeypatch.setattr("app.services.limit_board_service.time.monotonic", lambda: 100.0)
    runtime = {
        "candidate_scores": {
            "600000.SH": {
                "candidate_score": 70.0,
                "candidate_score_state": "live",
                "candidate_score_as_of": captured_at.isoformat(),
                "candidate_score_detail": {},
                "candidate_reasons": [],
            },
        },
    }

    service._refresh_candidate_scores(
        runtime,
        [{"symbol": "600000.SH", "source_modes": ["first_board"]}],
        now,
    )

    current = runtime["candidate_scores"]["600000.SH"]
    assert current["candidate_score"] == 70.0
    assert current["candidate_score_state"] == "cached"
    assert current["candidate_score_as_of"] == captured_at.isoformat()


def test_candidate_score_refresh_drops_legacy_local_sector_cache(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.time.monotonic", lambda: 100.0)
    runtime = {
        "candidate_scores": {
            "600000.SH": {
                "candidate_score": 70.0,
                "candidate_score_detail": {
                    "intraday_flow": {"score": 35.0, "capital_available": True},
                    "sector": {
                        "score": 20.0,
                        "name": "本地聚合板块",
                        "realtime_available": False,
                    },
                    "premium_gene": {"score": 11.0},
                    "technical": {"score": 4.0},
                },
            },
        },
    }

    service._refresh_candidate_scores(
        runtime,
        [{"symbol": "600000.SH", "source_modes": ["selected"]}],
        now,
    )

    cached = runtime["candidate_scores"]["600000.SH"]
    assert "sector" not in cached["candidate_score_detail"]
    assert cached["candidate_score"] is None
    assert cached["candidate_score_state"] == "unavailable"


def test_candidate_score_cache_is_cleared_on_next_trading_day(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    current_day = [datetime(2026, 8, 17).date()]
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today", lambda: current_day[0],
    )
    runtime = service._runtime_for_today()
    runtime["candidate_scores"] = {"600000.SH": {"candidate_score": 70.0}}
    service.store.save_runtime(runtime)

    current_day[0] = datetime(2026, 8, 18).date()

    next_day = service._runtime_for_today()
    assert next_day["trading_date"] == "2026-08-18"
    assert "candidate_scores" not in next_day


def test_candidate_ranking_ignores_gap_status_and_break_count():
    scores = {
        symbol: {
            "candidate_score": 60.0,
            "candidate_score_detail": {
                "sector": {"score": 30.0},
                "premium_gene": {"score": 18.0},
                "technical": {"score": 12.0},
            },
        }
        for symbol in ("600000.SH", "600001.SH")
    }
    candidates = [
        {
            "symbol": "600001.SH", "limit_gap_pct": 0.0,
            "status": "sealed", "break_count": 0,
        },
        {
            "symbol": "600000.SH", "limit_gap_pct": 0.03,
            "status": "broken", "break_count": 9,
        },
    ]

    ranked = LimitBoardService._rank_candidates(candidates, scores)

    assert [row["symbol"] for row in ranked] == ["600000.SH", "600001.SH"]
    assert [row["candidate_rank"] for row in ranked] == [1, 2]


def test_candidate_ranking_prioritizes_live_sector_before_total_score():
    scores = {
        "600000.SH": {
            "candidate_score": 70.0,
            "candidate_score_detail": {
                "sector": {
                    "score": 30.0,
                    "realtime_available": True,
                    "realtime_rank": 2,
                    "realtime_strength": 70.0,
                    "leadership": "leader",
                    "stock_rank": 1,
                },
            },
        },
        "600001.SH": {
            "candidate_score": 90.0,
            "candidate_score_detail": {
                "sector": {
                    "score": 30.0,
                    "realtime_available": True,
                    "realtime_rank": 8,
                    "realtime_strength": 95.0,
                    "leadership": "leader",
                    "stock_rank": 1,
                },
            },
        },
    }
    ranked = LimitBoardService._rank_candidates(
        [{"symbol": "600000.SH"}, {"symbol": "600001.SH"}], scores,
    )
    assert [row["symbol"] for row in ranked] == ["600000.SH", "600001.SH"]


def test_rank_candidates_backfills_premium_gene_for_manual_entry_only_when_provider_given():
    """Manual-entry 标的（score_cache 没快照）应当通过 provider 注入历史涨停基因；自动源已存在的 premium_gene 不被覆盖；不传 provider 不注入。"""
    raw_gene: dict[str, Any] = {
        "limit_up_count": 6,
        "next_day_red_rate": 0.85,
        "first_board_broken_rate": 0.10,
        "first_board_attempt_count": 5,
        "first_board_sealed_count": 4,
        "first_board_seal_rate": 0.8,
        "consecutive_rate": 0.4,
        "premium_5_count": 3,
        "next_day_observation_count": 6,
        "window_days": 200,
        "as_of": "2026-08-29",
    }

    def provider(symbol: str) -> dict[str, Any]:
        return raw_gene if symbol.upper() == "600127.SH" else {}

    # case A: manual entry（detail={}）传入 provider → 注入 premium_gene
    scores_empty: dict[str, dict[str, Any]] = {}
    ranked = LimitBoardService._rank_candidates(
        [{"symbol": "600127.SH", "source_modes": ["selected"]}],
        scores_empty,
        premium_stats_provider=provider,
    )
    gene = ranked[0]["candidate_score_detail"]["premium_gene"]
    assert gene["max_score"] == 10.0
    assert gene["score"] > 0
    assert gene["limit_up_count"] == 6
    assert gene["passed"] is True
    assert any("涨停基因" in reason for reason in ranked[0]["candidate_reasons"])

    # case B: 自动源已有 premium_gene → provider 注入逻辑跳过，保留原值（幂等）
    existing_gene = {"score": 7.5, "max_score": 10.0, "marker": "auto"}
    ranked2 = LimitBoardService._rank_candidates(
        [{"symbol": "600127.SH", "source_modes": ["first_board"]}],
        {"600127.SH": {"candidate_score": 80.0, "candidate_score_state": "live",
                       "candidate_score_detail": {"premium_gene": existing_gene},
                       "candidate_reasons": []}},
        premium_stats_provider=provider,
    )
    assert ranked2[0]["candidate_score_detail"]["premium_gene"] is existing_gene

    # case C: 不传 provider → 历史涨停基因不会被注入，detail 维持空 dict
    ranked3 = LimitBoardService._rank_candidates(
        [{"symbol": "600127.SH", "source_modes": ["selected"]}],
        scores_empty,
    )
    assert ranked3[0]["candidate_score_detail"] == {}


def test_rank_candidates_skips_premium_gene_backfill_when_raw_stats_incomplete():
    """Provider 返回的 raw stats 缺必需字段（limit_up_count / next_day_red_rate / first_board_broken_rate）时，不注入 premium_gene。"""
    incomplete: dict[str, Any] = {"limit_up_count": 3, "next_day_red_rate": 0.9}
    ranked = LimitBoardService._rank_candidates(
        [{"symbol": "600127.SH", "source_modes": ["selected"]}],
        {},
        premium_stats_provider=lambda _symbol: incomplete,
    )
    assert ranked[0]["candidate_score_detail"] == {}


def test_entry_metrics_requires_open_space_fresh_quote_and_rising_score(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 20, 10, 0, tzinfo=CN_TZ)
    candidate = {
        "symbol": "600000.SH",
        "status": "near_limit",
        "limit_gap_pct": 0.015,
        "last_quote_at": now.isoformat(),
    }
    detail = {"intraday_flow": {"score": 12.0}}

    first = service._entry_metrics(candidate, 80.0, {}, detail, now)
    assert first["tradability_state"] == "warming"
    assert first["entry_score"] is not None

    second = service._entry_metrics(
        candidate,
        81.0,
        {"candidate_score": 80.0, "candidate_score_rising_rounds": 1},
        detail,
        now,
    )
    assert second["tradability_state"] == "tradable"
    assert second["candidate_score_velocity"] == pytest.approx(1.0)
    assert second["candidate_score_rising_rounds"] == 2

    sealed = service._entry_metrics(
        {**candidate, "status": "sealed"},
        90.0,
        {"candidate_score": 80.0, "candidate_score_rising_rounds": 1},
        detail,
        now,
    )
    assert sealed["tradability_state"] == "limit_reached"


def test_opportunity_ranking_excludes_untradable_rows_and_assigns_entry_rank(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 20, 10, 0, tzinfo=CN_TZ)
    candidates = [
        {"symbol": "600000.SH", "status": "near_limit", "limit_gap_pct": 0.015, "last_quote_at": now.isoformat()},
        {"symbol": "600001.SH", "status": "touched", "limit_gap_pct": 0.0, "last_quote_at": now.isoformat()},
        {"symbol": "600002.SH", "status": "near_limit", "limit_gap_pct": 0.04, "last_quote_at": now.isoformat()},
    ]
    scores = {
        "600000.SH": {
            "candidate_score": 80.0,
            "candidate_score_state": "live",
            "entry_score": 75.0,
            "candidate_score_velocity": 1.0,
            "tradability_state": "tradable",
        },
        "600001.SH": {
            "candidate_score": 99.0,
            "entry_score": 99.0,
            "candidate_score_velocity": 5.0,
            "tradability_state": "limit_reached",
        },
        "600002.SH": {
            "candidate_score": 95.0,
            "entry_score": 95.0,
            "candidate_score_velocity": 5.0,
            "tradability_state": "too_far",
        },
    }

    ranked = service._rank_opportunities(candidates, scores, now)

    assert [row["symbol"] for row in ranked] == ["600000.SH"]
    assert ranked[0]["entry_rank"] == 1


def test_candidate_sector_selection_prefers_best_concept_then_falls_back_to_industry(
    tmp_path, monkeypatch,
):
    class SectorService:
        @staticmethod
        def targets_for_symbol(_symbol, *, kind=None, industry_level=None):
            if kind == "concept":
                return [
                    {"key": "c1", "kind": "concept", "name": "概念一"},
                    {"key": "c2", "kind": "concept", "name": "概念二"},
                ]
            assert industry_level == 2
            return [{"key": "i1", "kind": "industry", "name": "二级行业"}]

        @staticmethod
        def build_snapshots(_stock_df, _index_df, targets, _windows, *, now):
            assert now > 0
            return {target["key"]: {"valid": True} for target in targets}

        @staticmethod
        def member_symbols(_key):
            return {"600000.SH"}

    service, quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ)
    current_mono = [100.0]
    concept_available = [True]
    service.app_state.sector_monitor_service = SectorService()
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "state": "live",
            "as_of": now.date().isoformat(),
            "rows": [
                {
                    "plate_id": f"P-{index}",
                    "plate_name": name,
                    "change_pct_pct": 1.0,
                    "strength": 80.0,
                    "rank": index + 1,
                    "rank_count": 3,
                }
                for index, name in enumerate(("概念一", "概念二", "二级行业"))
            ],
        },
    )
    service._sector_memberships = pl.DataFrame({
        "plate_id": ["P-0", "P-1", "P-2"],
        "symbol": ["600000.SH"] * 3,
    })
    service._sector_live_quotes = {
        "600000.SH": {
            "symbol": "600000.SH",
            "change_pct": 0.01,
            "amount": 100.0,
            "source": "kaipanla_socket",
        },
    }
    quotes.enriched_date = now.date()
    quotes.enriched = pl.DataFrame({"symbol": ["600000.SH"]})
    monkeypatch.setattr(
        "app.services.limit_board_service.time.monotonic", lambda: current_mono[0],
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.premium_gene_detail",
        lambda _values: {"score": 10.0, "max_score": 10.0},
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.technical_detail",
        lambda _values, **_kwargs: {"score": 20.0},
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.intraday_flow_detail",
        lambda *_args, **_kwargs: {"score": 40.0, "max_score": 50.0, "capital_available": True},
    )

    def fake_sector_detail(**kwargs):
        target = kwargs["target"]
        if target["kind"] == "concept":
            if not concept_available[0]:
                return None
            score = 35.0 if target["key"] == "c1" else 42.0
        else:
            score = 49.0
        return {"score": score, "name": target["name"], "kind": target["kind"]}

    monkeypatch.setattr(
        "app.services.limit_board_service.sector_detail", fake_sector_detail,
    )
    candidate = [{
        "symbol": "600000.SH",
        "source_modes": ["selected"],
        "limit_gap_pct": 0.10,
        "change_pct": 0.0,
    }]
    runtime = {"candidate_scores": {}}

    service._refresh_candidate_scores(runtime, candidate, now)
    sector = runtime["candidate_scores"]["600000.SH"]["candidate_score_detail"]["sector"]
    assert sector["name"] == "概念二"
    assert sector["score"] == pytest.approx(42.0)
    assert runtime["candidate_scores"]["600000.SH"]["candidate_score"] == pytest.approx(69.0)
    assert "proximity" not in runtime["candidate_scores"]["600000.SH"]["candidate_score_detail"]

    concept_available[0] = False
    current_mono[0] = 116.0
    service._refresh_candidate_scores(runtime, candidate, now)
    sector = runtime["candidate_scores"]["600000.SH"]["candidate_score_detail"]["sector"]
    assert sector["name"] == "二级行业"
    assert sector["score"] == pytest.approx(49.0)


def test_weekend_scoring_anchors_to_last_completed_trading_day(tmp_path, monkeypatch):
    service, quotes, _config = make_service(tmp_path)
    friday = date(2026, 8, 28)
    now = datetime(2026, 8, 29, 18, 0, tzinfo=CN_TZ)  # 周六傍晚

    # 日线快照是周五的（非当日）——周末应被接受为「最近已完成交易日」
    quotes.enriched_date = friday
    quotes.enriched = pl.DataFrame({
        "symbol": ["600000.SH"], "close": [11.0], "last_price": [11.0],
        "prev_close": [10.0], "change_pct": [0.10], "amount": [100.0],
        "ma5": [10.5], "ma10": [10.2], "ma20": [10.0], "ma60": [9.5],
        "momentum_5d": [0.08], "momentum_20d": [0.2], "vol_ratio_5d": [2.0],
        "macd_dif": [0.3], "macd_dea": [0.2], "macd_hist": [0.1], "rsi_14": [70.0],
    })

    stock_df, stock_rows = service._candidate_stock_snapshot(now)
    assert not stock_df.is_empty()
    assert stock_rows["600000.SH"]["ma5"] == pytest.approx(10.5)

    # 交易时段内仍只接受当日实时快照（周五盘中拿到周四快照 → 拒绝）
    quotes.enriched_date = date(2026, 8, 27)
    intraday = datetime(2026, 8, 28, 10, 0, tzinfo=CN_TZ)
    stale_df, stale_rows = service._candidate_stock_snapshot(intraday)
    assert stale_df.is_empty()
    assert stale_rows == {}

    # 周末分钟数据 freshness 覆盖整个周末（回看周五分钟K）
    quotes.enriched_date = friday
    captured: dict = {}
    def fake_features(symbols, **kwargs):
        captured.update(kwargs)
        return {}
    monkeypatch.setattr(quotes, "get_intraday_features", fake_features, raising=False)
    service._candidate_intraday_features({"600000.SH"}, now)
    assert captured["freshness_seconds"] == 7 * 24 * 60 * 60


def test_rotation_only_fallback_when_realtime_sector_unavailable(tmp_path, monkeypatch):
    class SectorService:
        @staticmethod
        def targets_for_symbol(_symbol, *, kind=None, industry_level=None):
            if kind == "concept":
                return [{"key": "c1", "kind": "concept", "name": "人工智能"}]
            return []

    service, quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 29, 18, 0, tzinfo=CN_TZ)  # 周六
    service.app_state.sector_monitor_service = SectorService()
    # 无 kaipanla_collector / 实时板块行情 → 走日频轮动降级路径
    dates = ["2026-08-28", "2026-08-27", "2026-08-26", "2026-08-25", "2026-08-24"]
    changes = [0.03, 0.02, 0.01, 0.0, -0.01]
    rotation_payload = {
        "dates": dates,
        "columns": {
            day: [["人工智能", change]] + [[f"板块{index}", 0.0] for index in range(1, 10)]
            for day, change in zip(dates, changes, strict=True)
        },
        "concept_count": 10,
    }
    monkeypatch.setattr(
        "app.services.limit_board_service.rps_rotation.build_rps_rotation",
        lambda _repo, _days, kind, level=None: (
            rotation_payload if kind == "concept" else {"dates": [], "columns": {}}
        ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.intraday_flow_detail",
        lambda *_args, **_kwargs: None,
    )
    quotes.enriched_date = date(2026, 8, 28)
    quotes.enriched = pl.DataFrame({"symbol": ["600000.SH"]})
    runtime = {"candidate_scores": {}}
    candidates = [{"symbol": "600000.SH", "source_modes": ["selected"], "change_pct": 0.10}]

    service._refresh_candidate_scores(runtime, candidates, now)

    detail = runtime["candidate_scores"]["600000.SH"]["candidate_score_detail"]
    sector = detail["sector"]
    assert sector["data_source"] == "rps_rotation"
    assert sector["realtime_available"] is False
    assert sector["rotation_available"] is True
    assert sector["rotation_label"] == "主线"
    comprehensive = detail["comprehensive"]
    sentiment = comprehensive["dimensions"]["sentiment"]
    assert sentiment["components"]["sector_pattern"] > 0
    assert "sector_current" in sentiment["unavailable_components"]
    assert "sector_position" in comprehensive["dimensions"]["health"]["unavailable_components"]
    assert "板块相对强度高" in comprehensive["strengths"]


def test_close_frozen_sector_inputs_score_realtime_components_after_hours(
    tmp_path, monkeypatch,
):
    """盘后/周末：实时成员行情缺失时，用收盘快照冻结值给实时类组件出分。"""
    class SectorService:
        @staticmethod
        def targets_for_symbol(_symbol, *, kind=None, industry_level=None):
            if kind == "concept":
                return [{"key": "c1", "kind": "concept", "name": "人工智能"}]
            return []

    friday = date(2026, 8, 28)
    now = datetime(2026, 8, 29, 18, 0, tzinfo=CN_TZ)  # 周六
    members = {
        "600000.SH": (0.10, 300.0),
        "600001.SH": (0.06, 200.0),
        "600002.SH": (0.04, 100.0),
        "600003.SH": (0.01, 100.0),
        "600004.SH": (-0.01, 100.0),
        "600005.SH": (-0.02, 100.0),
    }
    service, quotes, _config = make_service(tmp_path)
    service.app_state.sector_monitor_service = SectorService()
    # 模拟周末重启：内存成分表/实时行情为空， collector 只有持久化数据
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: None,
        latest_completed_trading_date=lambda _today: friday,
        sector_strength_snapshot_at=lambda trade_date, _captured: {
            "state": "live",
            "as_of": trade_date.isoformat(),
            "rows": [{
                "plate_id": "P-0",
                "plate_name": "人工智能",
                "change_pct_pct": 3.0,
                "strength": 88.0,
                "rank": 1,
                "rank_count": 10,
            }],
        },
        sector_constituent_memberships=lambda _date: pl.DataFrame({
            "plate_id": ["P-0"] * len(members),
            "symbol": list(members),
        }),
    )
    dates = ["2026-08-28", "2026-08-27", "2026-08-26", "2026-08-25", "2026-08-24"]
    rotation_payload = {
        "dates": dates,
        "columns": {
            day: [["人工智能", change]] + [[f"板块{index}", 0.0] for index in range(1, 10)]
            for day, change in zip(dates, [0.03, 0.02, 0.01, 0.0, -0.01], strict=True)
        },
        "concept_count": 10,
    }
    monkeypatch.setattr(
        "app.services.limit_board_service.rps_rotation.build_rps_rotation",
        lambda _repo, _days, kind, level=None: (
            rotation_payload if kind == "concept" else {"dates": [], "columns": {}}
        ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.intraday_flow_detail",
        lambda *_args, **_kwargs: None,
    )
    quotes.enriched_date = friday
    quotes.enriched = pl.DataFrame({
        "symbol": list(members),
        "change_pct": [value[0] for value in members.values()],
        "amount": [value[1] for value in members.values()],
    })
    runtime = {"candidate_scores": {}}
    candidates = [{"symbol": "600000.SH", "source_modes": ["selected"], "change_pct": 0.10}]

    service._refresh_candidate_scores(runtime, candidates, now)

    detail = runtime["candidate_scores"]["600000.SH"]["candidate_score_detail"]
    sector = detail["sector"]
    assert sector["data_source"] == "daily_close"
    assert sector["close_frozen"] is True
    assert sector["realtime_available"] is True
    assert sector["change_pct"] == pytest.approx(0.03)
    assert sector["up_ratio"] == pytest.approx(4 / 6)
    assert sector["leadership"] == "leader"
    assert sector["stock_rank"] == 1
    assert sector["realtime_rank"] == 1
    comprehensive = detail["comprehensive"]
    sentiment = comprehensive["dimensions"]["sentiment"]
    # 当日表现（板块 +3%、上涨占比 4/6）与过热排名（1/10 顶部）按收盘值出分
    assert sentiment["components"]["sector_current"] == pytest.approx(3.8, abs=0.05)
    assert sentiment["components"]["overheat_risk"] < 10.0
    health = comprehensive["dimensions"]["health"]
    assert health["components"]["sector_position"] >= 9.0


def test_candidate_pool_marks_legacy_selected_rows_as_manual(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.store.update(0, lambda value: value["selected"].append({"symbol": "600000.SH", "name": "浦发银行"}))

    view = service.view()

    assert view["candidate_pool"][0]["source"] == "manual"
    assert "手工加入" in view["candidate_pool"][0]["candidate_reasons"]


def test_candidate_pool_reuses_strong_stock_scores_without_scoring_manual_rows(
    tmp_path, monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {"source_modes": ["first_board"]},
        "600001.SH": {"source_modes": ["selected"]},
    }
    service.store.update(
        0,
        lambda value: value["selected"].append({"symbol": "600001.SH", "name": "邯郸钢铁"}),
    )
    service.store.save_runtime(runtime)
    calls: list[list[str]] = []

    def score(runtime, rows, _now):
        calls.append([str(row["symbol"]) for row in rows])
        runtime["candidate_scores"] = {
            "600000.SH": {
                "candidate_score": 80.0,
                "candidate_score_state": "live",
                "candidate_score_as_of": "2026-08-19T10:00:00+08:00",
                "candidate_score_detail": {"sector": {"score": 50.0}},
                "candidate_reasons": [],
            },
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)

    view = service.view()
    strong = view["first_board"][0]
    automatic = next(row for row in view["candidate_pool"] if row["symbol"] == "600000.SH")
    manual = next(row for row in view["candidate_pool"] if row["symbol"] == "600001.SH")

    assert calls == [["600000.SH"]]
    assert automatic["candidate_score"] == strong["candidate_score"] == 80.0
    assert automatic["candidate_score_detail"] == strong["candidate_score_detail"]
    assert manual["candidate_score"] is None


def test_view_scores_board_pool_members_and_merges_detail(tmp_path, monkeypatch):
    """打板池成员（板块强度手动入口）进入评分扫描集，view() 的 board_pool 行直接携带评分快照。"""
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {"source_modes": ["first_board"]},
    }
    service.store.update(
        0,
        lambda value: value["board_pool"].append({
            "symbol": "605159.SH",
            "name": "上海亚虹",
            "source": "manual",
            "auto_trade": True,
            "order_mode": "sweep",
        }),
    )
    service.store.save_runtime(runtime)
    calls: list[list[str]] = []

    def score(runtime, rows, _now):
        calls.append(sorted(str(row["symbol"]) for row in rows))
        runtime["candidate_scores"] = {
            "605159.SH": {
                "candidate_score": 82.5,
                "candidate_rank": None,
                "candidate_score_state": "live",
                "candidate_score_as_of": "2026-08-29T10:00:00+08:00",
                "candidate_score_detail": {
                    "comprehensive": {"comprehensive_score": 82.5, "max_score": 100.0},
                },
                "candidate_reasons": [],
                "change_pct": 0.05,
            },
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)

    view = service.view()
    assert calls == [["600000.SH", "605159.SH"]]
    pool_row = next(row for row in view["board_pool"] if row["symbol"] == "605159.SH")
    assert pool_row["candidate_score"] == 82.5
    assert pool_row["candidate_score_state"] == "live"
    assert pool_row["candidate_score_detail"]["comprehensive"]["comprehensive_score"] == 82.5


def test_view_board_pool_score_merge_keeps_row_change_pct(tmp_path):
    """评分快照里的 change_pct 不得覆盖行上更新的行情值。"""
    merged = LimitBoardService._merge_candidate_score(
        {"symbol": "605159.SH", "name": "上海亚虹", "change_pct": 0.099},
        {"605159.SH": {
            "candidate_score": 70.0,
            "candidate_score_detail": {},
            "change_pct": 0.01,
        }},
    )
    assert merged["candidate_score"] == 70.0
    assert merged["change_pct"] == 0.099
    # 行上没有 change_pct 时保留评分快照里的值
    merged_none = LimitBoardService._merge_candidate_score(
        {"symbol": "605159.SH", "name": "上海亚虹"},
        {"605159.SH": {"candidate_score": 70.0, "change_pct": 0.01}},
    )
    assert merged_none["change_pct"] == 0.01
    # 评分缓存没有该标的时行原样返回
    merged_missing = LimitBoardService._merge_candidate_score(
        {"symbol": "605159.SH", "name": "上海亚虹"},
        {},
    )
    assert "candidate_score" not in merged_missing


def test_trim_automatic_candidates_keeps_board_pool_scores(tmp_path):
    """_trim_automatic_candidates 默认剪掉非 runtime 标的的分数；传入 keep_symbols（打板池成员）后保留。"""
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    runtime["symbols"] = {"600000.SH": {"source_modes": ["first_board"]}}
    runtime["candidate_scores"] = {
        "600000.SH": {"candidate_score": 80.0, "candidate_score_detail": {}},
        "605159.SH": {"candidate_score": 82.5, "candidate_score_detail": {}},
    }

    service._trim_automatic_candidates(runtime, keep_symbols={"605159.SH"})
    assert "600000.SH" in runtime["candidate_scores"]
    assert "605159.SH" in runtime["candidate_scores"]

    runtime["candidate_scores"] = {
        "600000.SH": {"candidate_score": 80.0, "candidate_score_detail": {}},
        "605159.SH": {"candidate_score": 82.5, "candidate_score_detail": {}},
    }
    service._trim_automatic_candidates(runtime)
    assert "605159.SH" not in runtime["candidate_scores"]
    assert "600000.SH" in runtime["candidate_scores"]


def test_remove_automatic_candidate_excludes_it_for_current_trading_day(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    current_day = [datetime(2026, 8, 13).date()]
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: current_day[0],
    )
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        "600000.SH": {
            "name": "浦发银行",
            "source_modes": ["first_board"],
            "status": "near_limit",
            "limit_gap_pct": 0.01,
        },
    }
    service.store.save_runtime(runtime)
    assert [row["symbol"] for row in service.view()["candidate_pool"]] == ["600000.SH"]

    service.remove_candidate("600000.SH", 0)

    assert service.view()["candidate_pool"] == []
    assert [row["symbol"] for row in service.view()["first_board"]] == ["600000.SH"]
    assert service._runtime_for_today()["candidate_excluded"] == ["600000.SH"]

    current_day[0] = datetime(2026, 8, 14).date()
    assert service._runtime_for_today()["candidate_excluded"] == []


def test_manual_candidate_add_clears_same_day_exclusion(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    runtime = service._runtime_for_today()
    runtime["candidate_excluded"] = ["600000.SH"]
    service.store.save_runtime(runtime)

    service.add_candidate("600000.SH", 0)

    assert service._runtime_for_today()["candidate_excluded"] == []
    assert [row["symbol"] for row in service.view()["candidate_pool"]] == ["600000.SH"]


def test_board_and_buy_pool_symbols_share_websocket_capacity(tmp_path, monkeypatch):
    class FakeHub:
        def __init__(self):
            self.registered = set()

        @staticmethod
        def websocket_available(*, exclude):
            assert exclude == "limit_board"
            return 10

        def register(self, account_id, mode, symbols, asset_type, _queue):
            assert (account_id, mode, asset_type) == ("limit_board", "websocket", "stock")
            self.registered = set(symbols)

        def update_symbols(self, _account_id, symbols):
            self.registered = set(symbols)

        def unregister(self, _account_id):
            self.registered = set()

    service, _quotes, config = make_service(tmp_path)
    hub = FakeHub()
    service.app_state.paper_supervisor = SimpleNamespace(hub=hub)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    service._quotes = {
        "600000.SH": {
            "symbol": "600000.SH",
            "limit_gap_pct": 0.001,
            "source_modes": ["first_board"],
        },
        "600001.SH": {
            "symbol": "600001.SH",
            "limit_gap_pct": 0.001,
            "source_modes": ["board_pool"],
        },
    }
    config["board_pool"] = [{"symbol": "600001.SH", "auto_trade": True}]
    config["buy_pool"] = [{"symbol": "600000.SH", "allocation_mode": "lot"}]

    service._sync_websocket(service._runtime_for_today(), config)

    assert service._ws_symbols == {"600000.SH", "600001.SH"}
    assert hub.registered == {"600000.SH", "600001.SH"}


def test_pool_add_fails_closed_when_websocket_capacity_is_exhausted(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.app_state.paper_supervisor = SimpleNamespace(
        hub=SimpleNamespace(websocket_available=lambda *, exclude: 0),
    )

    with pytest.raises(RuntimeError, match="超过可用 WebSocket 容量"):
        service.add_pool("600000.SH", "first_board", 0)

    assert service.store.load_config()["board_pool"] == []


def test_started_service_blocks_auto_trade_without_pool_websocket(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service.app_state.paper_supervisor = SimpleNamespace(hub=object())
    service._started = True
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "WebSocket" in state["auto_order_error"]


def test_buy_pool_quote_survives_automatic_preselection():
    updates = {
        "600000.SH": {
            "symbol": "600000.SH",
            "source_modes": ["buy_pool"],
        },
    }

    retained = LimitBoardService._preselect_automatic_updates(updates)

    assert set(retained) == {"600000.SH"}


def test_add_pool_enables_auto_trade_by_default(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    saved = service.add_pool("600000.SH", "first_board", 0)

    assert saved["board_pool"][0]["auto_trade"] is True
    assert saved["board_pool"][0]["order_mode"] == "sweep"


def test_board_pool_persists_per_stock_fixed_amount(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    saved = service.add_pool(
        "600000.SH", "first_board", 0, "fixed", 10_000,
    )

    assert saved["board_pool"][0]["allocation_mode"] == "fixed"
    assert saved["board_pool"][0]["allocation_value"] == 10_000


def test_board_pool_persists_fraction_allocation(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    saved = service.add_pool(
        "600000.SH", "first_board", 0, "sixth",
    )

    assert saved["board_pool"][0]["allocation_mode"] == "sixth"
    assert "allocation_value" not in saved["board_pool"][0]


def test_board_pool_updates_per_stock_fixed_volume(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.add_pool("600000.SH", "first_board", 0)

    saved = service.update_pool(
        "600000.SH", True, "queue", 1, "volume", 300,
    )

    member = saved["board_pool"][0]
    assert member["order_mode"] == "queue"
    assert member["allocation_mode"] == "volume"
    assert member["allocation_value"] == 300


def test_board_pool_defaults_to_fixed_amount(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    saved = service.add_pool("600000.SH", "first_board", 0)

    member = saved["board_pool"][0]
    assert member["allocation_mode"] == "fixed"
    assert member["allocation_value"] == 20_000


def test_board_pool_rejects_legacy_allocation_mode(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.add_pool("600000.SH", "first_board", 0)

    with pytest.raises(ValueError, match="跟随全局"):
        service.add_pool("600001.SH", "first_board", 1, "global")

    with pytest.raises(ValueError, match="一手"):
        service.update_pool("600000.SH", True, "queue", 1, "lot")

    saved = service.store.load_config()["board_pool"]
    assert [item["symbol"] for item in saved] == ["600000.SH"]
    assert saved[0]["allocation_mode"] == "fixed"


def test_buy_pool_submits_current_price_with_default_fixed_amount(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, quotes, _config = make_service(tmp_path, qmt)
    now = datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    quotes.latest_quotes = [{
        **quote(price=10.5, limit=11.0),
        "timestamp": now.isoformat(),
    }]

    result = service.add_buy_pool("600000.SH", "first_board", 0)

    assert result["order"]["status"] == "accepted_pending"
    assert qmt.orders[0]["allocation_mode"] == "fixed"
    assert qmt.orders[0]["allocation_value"] == 20_000
    assert qmt.orders[0]["price"] == 10.5
    # 20000 / 10.5 = 1904 股, 向下取整到百股 = 1900 股
    assert qmt.orders[0]["volume"] == 1_900
    saved = service.store.load_config()
    assert saved["buy_pool"][0]["order_price"] == 10.5
    assert saved["buy_pool"][0]["order_volume"] == 1_900
    assert service._runtime_for_today()["buy_orders"]["600000.SH"]["order_status"] == "accepted_pending"


def test_buy_pool_supports_fixed_amount_and_fixed_volume(tmp_path, monkeypatch):
    now = datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())

    fixed_qmt = FakeQmt()
    fixed_service, fixed_quotes, _config = make_service(tmp_path / "fixed", fixed_qmt)
    fixed_quotes.latest_quotes = [{**quote(price=10.5, limit=11.0), "timestamp": now.isoformat()}]
    fixed_service.add_buy_pool("600000.SH", "manual", 0, "fixed", 1_200)
    assert fixed_qmt.orders[0]["volume"] == 100

    volume_qmt = FakeQmt()
    volume_service, volume_quotes, _config = make_service(tmp_path / "volume", volume_qmt)
    volume_quotes.latest_quotes = [{**quote(price=10.5, limit=11.0), "timestamp": now.isoformat()}]
    volume_service.add_buy_pool("600000.SH", "manual", 0, "volume", 300)
    assert volume_qmt.orders[0]["volume"] == 300
    assert volume_qmt.orders[0]["price"] == 10.5


def test_buy_pool_persists_and_submits_credit_buy_mode(tmp_path, monkeypatch):
    now = datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    qmt = FakeQmt()
    service, quotes, _config = make_service(tmp_path, qmt)
    quotes.latest_quotes = [{**quote(price=10.5, limit=11.0), "timestamp": now.isoformat()}]

    service.add_buy_pool("600000.SH", "manual", 0, "fixed", 1_050, "financing")

    assert qmt.orders[0]["credit_buy_mode"] == "financing"
    assert service.store.load_config()["buy_pool"][0]["credit_buy_mode"] == "financing"


def test_buy_pool_after_hours_uses_custom_order_price_without_market_quote(tmp_path, monkeypatch):
    now = datetime(2026, 8, 13, 20, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    service._pool_quote = lambda _symbol: (_ for _ in ()).throw(AssertionError("盘后不应读取行情"))

    result = service.add_buy_pool(
        "600000.SH", "manual", 0, "fixed", 1_050, "collateral", 9.876,
    )

    assert result["order"]["status"] == "accepted_pending"
    assert qmt.orders[0]["price"] == 9.876
    assert service.store.load_config()["buy_pool"][0]["order_price"] == 9.876


def test_buy_pool_after_hours_requires_custom_order_price(tmp_path, monkeypatch):
    now = datetime(2026, 8, 13, 20, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    service, _quotes, _config = make_service(tmp_path, FakeQmt())

    with pytest.raises(ValueError, match="盘后委托需要填写"):
        service.add_buy_pool("600000.SH", "manual", 0)


def test_default_config_preserves_current_sweep_and_queue_triggers():
    settings = default_config()["settings"]

    assert settings["sweep_price_levels"] == 5
    assert settings["queue_wait_seconds"] == 0
    assert settings["queue_confirm_snapshots"] == 0
    assert "order_allocation_mode" not in settings
    assert "order_amount_per_board" not in settings
    assert settings["max_auto_board_count"] == 0
    assert settings["main_board_only"] is False


def test_sweep_uses_configured_price_levels():
    five_levels = {
        "ask_prices": [10.95, 10.96, 10.97, 10.98, 10.99],
        "ask_volumes": [100, 200, 300, 400, 500],
    }
    six_levels = {
        "ask_prices": [10.94, 10.95, 10.96, 10.97, 10.98],
        "ask_volumes": [100, 200, 300, 400, 500],
    }

    assert _sweep_ready(five_levels, 11.0) is True
    assert _sweep_ready(six_levels, 11.0) is False
    assert _sweep_ready(six_levels, 11.0, 10) is True
    assert _sweep_ready({**five_levels, "ask_volumes": [0, 0, 0, 0, 0]}, 11.0) is False


def test_five_level_depth_triggers_default_sweep_order(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service.add_pool("600000.SH", "first_board", 0)
    service._quotes["600000.SH"] = {
        **quote(price=10.94),
        "source_modes": ["board_pool"],
    }
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "near_limit"}
    service.store.save_runtime(runtime)

    service._process_depth([{
        "symbol": "600000.SH",
        "timestamp": "2026-08-13T10:00:00+08:00",
        "bid_prices": [10.94, 10.93, 10.92, 10.91, 10.90],
        "bid_volumes": [100] * 5,
        "ask_prices": [10.95, 10.96, 10.97, 10.98, 10.99],
        "ask_volumes": [100] * 5,
    }])

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["price"] == 11.0
    assert service._runtime_for_today()["symbols"]["600000.SH"]["auto_order_mode"] == "sweep"


def test_limit_touch_triggers_sweep_order_without_depth(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "sweep",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    runtime = service._runtime_for_today()

    service._evaluate_quotes({"600000.SH": quote()}, runtime, config)

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["price"] == 11.0
    assert runtime["symbols"]["600000.SH"]["auto_order_mode"] == "sweep"


def test_depth_processing_uses_configured_sweep_price_levels(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )
    service.store.update(
        0,
        lambda value: value["settings"].update({"sweep_price_levels": 10}),
    )
    service.add_pool("600000.SH", "first_board", 1)
    service._quotes["600000.SH"] = {
        **quote(price=10.89),
        "source_modes": ["board_pool"],
    }
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "near_limit"}
    service.store.save_runtime(runtime)

    service._process_depth([{
        "symbol": "600000.SH",
        "timestamp": "2026-08-13T10:00:00+08:00",
        "bid_prices": [10.89 - index / 100 for index in range(10)],
        "bid_volumes": [100] * 10,
        "ask_prices": [10.90 + index / 100 for index in range(10)],
        "ask_volumes": [100] * 10,
    }])

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["price"] == 11.0
    assert service._runtime_for_today()["symbols"]["600000.SH"]["auto_order_mode"] == "sweep"


def test_close_auction_depth_does_not_mark_board_broken(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    now = [datetime(2026, 8, 13, 14, 57, tzinfo=CN_TZ)]
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now[0])
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now[0].date())
    service._quotes["600000.SH"] = {**quote(), "timestamp": now[0].isoformat()}
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "sealed", "sealed": True}
    service.store.save_runtime(runtime)
    open_depth = {
        "symbol": "600000.SH",
        "timestamp": now[0].isoformat(),
        "bid_prices": [10.99],
        "bid_volumes": [100],
        "ask_prices": [11.0],
        "ask_volumes": [100],
    }

    service._process_depth([open_depth])

    state = service._runtime_for_today()["symbols"]["600000.SH"]
    assert state["sealed"] is True
    assert state["status"] == "sealed"
    assert quotes.events == []
    assert service.store.events(now[0].date().isoformat()) == []

    now[0] = datetime(2026, 8, 13, 15, 0, tzinfo=CN_TZ)
    service._quotes["600000.SH"]["timestamp"] = now[0].isoformat()
    final_sealed = {
        **open_depth,
        "timestamp": now[0].isoformat(),
        "bid_prices": [11.0],
        "bid_volumes": [100],
        "ask_prices": [0],
        "ask_volumes": [0],
    }
    service._process_depth([final_sealed])

    assert service._runtime_for_today()["symbols"]["600000.SH"]["sealed"] is True
    assert service.store.events(now[0].date().isoformat()) == []

def test_close_auction_final_depth_marks_break_only_after_15h(tmp_path, monkeypatch):
    service, _quotes, config = make_service(tmp_path)
    now = [datetime(2026, 8, 13, 14, 57, tzinfo=CN_TZ)]
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now[0])
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now[0].date())
    service._quotes["600000.SH"] = {**quote(), "timestamp": now[0].isoformat()}
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "sealed", "sealed": True}
    service.store.save_runtime(runtime)
    open_depth = {
        "symbol": "600000.SH",
        "timestamp": now[0].isoformat(),
        "bid_prices": [10.99],
        "bid_volumes": [100],
        "ask_prices": [11.0],
        "ask_volumes": [100],
    }

    service._process_depth([open_depth])
    assert service._runtime_for_today()["symbols"]["600000.SH"]["sealed"] is True

    now[0] = datetime(2026, 8, 13, 15, 0, tzinfo=CN_TZ)
    service._quotes["600000.SH"]["timestamp"] = now[0].isoformat()
    open_depth["timestamp"] = now[0].isoformat()
    service._process_depth([open_depth])

    state = service._runtime_for_today()["symbols"]["600000.SH"]
    assert state["sealed"] is False
    assert state["status"] == "broken"
    event = service.store.events(now[0].date().isoformat())[0]
    assert event["type"] == "broken"
    assert "收盘集合竞价结束时卖一恢复" in event["reasons"][0]


def test_close_auction_final_without_ask_does_not_mark_break(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 13, 15, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    service._quotes["600000.SH"] = {**quote(), "timestamp": now.isoformat()}
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "sealed", "sealed": True}
    service.store.save_runtime(runtime)

    service._process_depth([{
        "symbol": "600000.SH",
        "timestamp": now.isoformat(),
        "bid_prices": [10.99],
        "bid_volumes": [0],
        "ask_prices": [0],
        "ask_volumes": [0],
    }])

    assert service._runtime_for_today()["symbols"]["600000.SH"]["sealed"] is True
    assert service.store.events(now.date().isoformat()) == []


def test_close_auction_depth_is_not_reused_across_trading_dates(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    service._close_auction_date = date(2026, 8, 12)
    service._close_auction_depth = {
        "600000.SH": {
            "timestamp": datetime(2026, 8, 12, 14, 59, tzinfo=CN_TZ),
            "bid_prices": [10.99],
            "bid_volumes": [100],
            "ask_prices": [11.0],
            "ask_volumes": [100],
        },
    }
    now = datetime(2026, 8, 13, 15, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    service._quotes["600000.SH"] = {**quote(), "timestamp": now.isoformat()}
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {"status": "sealed", "sealed": True}
    service.store.save_runtime(runtime)

    service._process_depth([])

    assert service._close_auction_depth == {}
    assert service._runtime_for_today()["symbols"]["600000.SH"]["sealed"] is True
    assert service.store.events(now.date().isoformat()) == []


def test_close_auction_finalizes_symbols_independently_across_batches(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    now = datetime(2026, 8, 13, 15, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    service._quotes.update({
        symbol: {**quote(), "symbol": symbol, "timestamp": now.isoformat()}
        for symbol in ("600000.SH", "600001.SH")
    })
    runtime = service._runtime_for_today()
    runtime["symbols"] = {
        symbol: {"status": "sealed", "sealed": True}
        for symbol in ("600000.SH", "600001.SH")
    }
    service.store.save_runtime(runtime)

    def broken_depth(symbol: str) -> dict:
        return {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "bid_prices": [10.99],
            "bid_volumes": [100],
            "ask_prices": [11.0],
            "ask_volumes": [100],
        }

    service._process_depth([broken_depth("600000.SH")])
    service._process_depth([broken_depth("600001.SH")])

    assert {event["symbol"] for event in service.store.events(now.date().isoformat())} == {
        "600000.SH", "600001.SH",
    }


def test_queue_waits_from_first_touch_before_submitting(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["settings"]["queue_wait_seconds"] = 5
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    current = [datetime(2026, 8, 13, 10, 0, 4, tzinfo=CN_TZ)]
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: current[0],
    )
    state = {
        "touched": True,
        "touched_at": "2026-08-13T10:00:00+08:00",
    }

    service._maybe_auto_trade("600000.SH", quote(), state, config)
    assert qmt.orders == []

    current[0] = datetime(2026, 8, 13, 10, 0, 5, tzinfo=CN_TZ)
    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert len(qmt.orders) == 1
    assert state["auto_order_mode"] == "queue"


def test_queue_submits_after_configured_sealed_snapshots(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_today",
        lambda: datetime(2026, 8, 13).date(),
    )

    def configure(value):
        value["settings"]["queue_confirm_snapshots"] = 3
        value["board_pool"].append({
            "symbol": "600000.SH",
            "auto_trade": True,
            "order_mode": "queue",
            "allocation_mode": "fixed",
            "allocation_value": 20_000,
        })

    service.store.update(0, configure)
    service._quotes["600000.SH"] = {
        **quote(),
        "source_modes": ["board_pool"],
    }
    runtime = service._runtime_for_today()
    runtime["symbols"]["600000.SH"] = {
        "status": "touched",
        "touched": True,
        "touched_at": "2026-08-13T10:00:00+08:00",
    }
    service.store.save_runtime(runtime)
    sealed_depth = {
        "symbol": "600000.SH",
        "timestamp": "2026-08-13T10:00:00+08:00",
        "bid_prices": [11.0] + [10.99 - index / 100 for index in range(9)],
        "bid_volumes": [100] * 10,
        "ask_prices": [0] * 10,
        "ask_volumes": [0] * 10,
    }

    service._process_depth([sealed_depth])
    service._process_depth([sealed_depth])
    assert qmt.orders == []

    service._process_depth([sealed_depth])

    assert len(qmt.orders) == 1
    assert service._runtime_for_today()["symbols"]["600000.SH"]["auto_order_mode"] == "queue"


def test_board_pool_auto_trade_uses_member_fixed_amount(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 10_000,
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["volume"] == 900
    assert qmt.orders[0]["allocation_mode"] == "fixed"
    assert qmt.orders[0]["allocation_value"] == 10_000
    result = service._order_results.get_nowait()
    assert result["volume"] == 900
    assert result["estimated_amount"] == 9900.0


def test_board_pool_auto_trade_ignores_legacy_global_settings(tmp_path):
    """全局资金方式已废弃, 即使旧配置仍留有该字段也不参与计算。"""
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["settings"]["order_allocation_mode"] = "fixed"
    config["settings"]["order_amount_per_board"] = 10_000
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 5_000,
    }]

    service._maybe_auto_trade("600000.SH", quote(), {}, config)

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["allocation_value"] == 5_000


def test_board_pool_auto_trade_blocks_legacy_one_lot_mode(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "lot",
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "一手" in state["auto_order_error"]
    assert "已废弃" in state["auto_order_error"]


def test_board_pool_auto_trade_blocks_legacy_global_mode(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "global",
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "跟随全局" in state["auto_order_error"]
    assert "已废弃" in state["auto_order_error"]


def test_board_pool_auto_trade_blocks_missing_allocation_mode(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert state["auto_order_error"] == "单板资金分配方式无效"


def test_board_pool_auto_trade_marks_qmt_preflight_failure_blocked(tmp_path):
    class PreflightQmt(FakeQmt):
        def submit_order(self, _request):
            raise QmtOrderPreflightError("QMT 未返回信用账户可买额度")

    qmt = PreflightQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]

    assert service._order_slots.acquire(blocking=False)
    service._submit_auto_order(
        "600000.SH", 11.0, "limit-board-test", "lot", None,
        "2026-08-13T10:00:00+08:00", "2026-08-13T10:00:00+08:00",
    )

    result = service._order_results.get_nowait()
    assert result["status"] == "blocked"
    assert result["order_sys_id"] is None
    assert "未返回信用账户可买额度" in result["error"]


def test_board_pool_auto_trade_blocks_amount_below_one_lot(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 500,
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "一手" in state["auto_order_error"]


def test_board_pool_auto_trade_passes_ratio_allocation_to_qmt(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "quarter",
    }]

    service._maybe_auto_trade("600000.SH", quote(), {}, config)

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["allocation_mode"] == "quarter"
    assert qmt.orders[0]["allocation_value"] is None
    assert qmt.orders[0]["volume"] == 2_700
    result = service._order_results.get_nowait()
    assert result["allocation_mode"] == "quarter"
    assert result["volume"] == 2_700


def test_board_pool_auto_trade_respects_daily_board_limit(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["settings"]["max_auto_board_count"] = 1
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    runtime = service._runtime_for_today()
    runtime["symbols"]["600001.SH"] = {"auto_order_key": "limit-board-existing"}
    state = runtime["symbols"].setdefault("600000.SH", {})

    service._maybe_auto_trade(
        "600000.SH", quote(), state, config, runtime=runtime,
    )

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "上限" in state["auto_order_error"]


def test_limit_board_notification_body_contains_name_and_concept(monkeypatch):
    service = QuoteService.__new__(QuoteService)
    service._app_state = object()
    service._repo = object()
    monkeypatch.setattr(
        "app.services.preferences.get_monitor_ext_fields",
        lambda: {"concept": {"field": "ext_gn_ths.所属概念"}, "industry": None},
    )
    monkeypatch.setattr(
        "app.api.screener._load_ext_value_maps",
        lambda _repo, _columns: {"ext_gn_ths__所属概念": {"600000.SH": "银行;金融科技"}},
    )
    event = {
        "source": "limit_board",
        "symbol": "600000.SH",
        "name": "浦发银行",
        "message": "浦发银行：涨停",
    }

    service.enrich_external_alerts([event])
    body = QuoteService._format_alert_notification_body(event)

    assert event["ext_gn_ths__所属概念"] == "银行;金融科技"
    assert event["concept"] == "银行;金融科技"
    assert body == "600000.SH 浦发银行：涨停\n概念：银行、金融科技"


def test_limit_board_notification_body_limits_and_deduplicates_concepts():
    event = {
        "source": "limit_board",
        "symbol": "300985.SZ",
        "name": "致远新能",
        "message": "致远新能：炸板",
        "concept": "石墨烯;天然气;石墨烯;氢能源;锂电池概念;燃料电池",
    }

    body = QuoteService._format_alert_notification_body(event)

    assert body == "300985.SZ 致远新能：炸板\n概念：石墨烯、天然气、氢能源"


def test_stale_quote_does_not_trigger_touch(tmp_path, monkeypatch):
    service, quotes, config = make_service(tmp_path)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 13, 10, 0),
    )
    runtime = service._runtime_for_today()
    service._evaluate_quotes(
        {"600000.SH": {**quote(), "timestamp": "2026-08-13T09:58:00+08:00"}},
        runtime,
        config,
    )

    assert runtime["symbols"] == {}
    assert quotes.events == []


def test_fourth_break_adds_symbol_to_daily_blacklist(tmp_path):
    service, quotes, config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    state = runtime["symbols"].setdefault("600000.SH", {})
    for count in range(1, 5):
        state["sealed"] = True
        service._mark_broken(quote(), state, runtime, config, "卖一恢复")

    assert state["break_count"] == 4
    assert state["status"] == "blacklisted"
    assert runtime["blacklist"] == ["600000.SH"]
    fourth_break = next(
        event for event in service.store.events(runtime["trading_date"])
        if event["break_count"] == 4
    )
    assert "第 4 次炸板" in fourth_break["reasons"][0]
    assert quotes.events == []


def test_board_pool_auto_trade_submits_once_per_day(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "source": "first_board",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    runtime = service._runtime_for_today()
    state = runtime["symbols"].setdefault("600000.SH", {})

    service._maybe_auto_trade("600000.SH", quote(), state, config)
    service._persist_runtime(runtime)
    result = service._order_results.get_nowait()
    service._apply_auto_order_result(result)
    state = service._runtime_for_today()["symbols"]["600000.SH"]
    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["strategy_name"] == "limit_board"
    # 20000 / 11.0 = 1818 股, 向下取整到百股 = 1800 股
    assert qmt.orders[0]["volume"] == 1_800
    assert qmt.orders[0]["price"] == 11.0
    assert qmt.orders[0]["price_type"] == "LIMIT"
    assert state["auto_order_status"] == "accepted_pending"
    assert state["auto_order_sys_id"] == "qmt-1"


def test_board_pool_auto_trade_blocks_when_qmt_is_not_ready(tmp_path):
    service, _quotes, config = make_service(tmp_path)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert state["auto_order_status"] == "blocked"
    assert "QMT" in state["auto_order_error"]
    assert "auto_order_key" not in state


def test_board_pool_auto_trade_stops_when_live_market_broken_rate_is_high(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service.app_state.kaipanla_collector = SimpleNamespace(
        market_sentiment_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": "2026-08-13",
            "market_broken_rate_pct": 42.3,
        },
    )
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]

    state = {}
    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "自动打板已停止" in state["auto_order_error"]


def test_board_pool_auto_trade_allows_stale_market_snapshot(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    service.app_state.kaipanla_collector = SimpleNamespace(
        market_sentiment_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "stale",
            "as_of": "2026-08-12",
            "market_broken_rate_pct": 80.0,
        },
    )
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]

    state = {}
    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert len(qmt.orders) == 1


def test_board_pool_auto_trade_allows_live_market_snapshot_below_threshold(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    service.app_state.kaipanla_collector = SimpleNamespace(
        market_sentiment_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": "2026-08-13",
            "market_broken_rate_pct": 39.9,
        },
    )
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]

    service._maybe_auto_trade("600000.SH", quote(), {}, config)

    assert len(qmt.orders) == 1


def test_limit_board_view_exposes_market_sentiment_guard(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.app_state.kaipanla_collector = SimpleNamespace(
        market_sentiment_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": "2026-08-13",
            "refreshed_at": "2026-08-13T10:00:00+08:00",
            "market_broken_rate_pct": 42.3,
            "market_evaluation": "分化",
            "max_consecutive": 5,
        },
    )

    result = service.view()

    assert result["market_sentiment"]["market_evaluation"] == "分化"
    assert result["market_sentiment"]["max_consecutive"] == 5
    assert result["runtime"]["sentiment_guard"]["blocked"] is True
    assert result["runtime"]["trading_enabled"] is False
    assert "自动打板已停止" in result["runtime"]["trading_reason"]


def test_limit_board_view_exposes_live_sector_strength_tree(tmp_path, monkeypatch):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    current_at = "2026-08-17T10:00:00+08:00"
    previous_at = "2026-08-17T09:30:05+08:00"
    current_rows = [
        {"plate_id": "P1", "plate_name": "通信", "strength": 100, "main_net": 20},
        {"plate_id": "P2", "plate_name": "算力", "strength": 90, "main_net": 10},
        {
            "plate_id": "C1",
            "plate_name": "光模块",
            "parent_plate_id": "P1",
            "is_child": True,
            "strength": 80,
            "main_net": 5,
        },
    ]
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": current_at,
            "institution_label": "第二季度机构增仓",
            "history_state": "live",
            "rows": current_rows,
        },
        sector_strength_timeline=lambda _day: [previous_at, current_at],
        sector_strength_snapshot_at=lambda _day, captured_at: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": captured_at,
            "institution_label": "第二季度机构增仓",
            "history_state": "live",
            "rows": [{"plate_id": "P1", "plate_name": "通信", "strength": 70}],
        },
    )

    result = service.view()

    snapshot = result["sector_strength"]
    assert snapshot["state"] == "live"
    assert snapshot["institution_label"] == "第二季度机构增仓"
    assert snapshot["history_state"] == "live"
    assert snapshot["timeline"] == [previous_at, current_at]
    assert [row["plate_name"] for row in snapshot["rows"]] == ["通信", "光模块", "算力"]
    assert snapshot["rows"][1]["parent_plate_id"] == "P1"
    historical = service.sector_strength_view(previous_at)
    assert historical["refreshed_at"] == previous_at
    assert historical["rows"][0]["strength"] == 70
    with pytest.raises(ValueError, match="时间点格式无效"):
        service.sector_strength_view("not-a-time")


def test_sector_strength_view_uses_previous_day_before_nine_and_today_at_nine(
    tmp_path,
    monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 25)
    previous = date(2026, 8, 24)
    previous_snapshot = {
        "provider": "kaipanla",
        "state": "live",
        "as_of": previous.isoformat(),
        "refreshed_at": "2026-08-24T15:00:00+08:00",
        "history_state": "closed",
        "rows": [{"plate_id": "P1", "plate_name": "芯片", "strength": 88}],
    }
    collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "unavailable",
            "as_of": today.isoformat(),
            "rows": [],
        },
        latest_completed_trading_date=lambda _day: previous,
        sector_strength_snapshot_at=lambda trade_date, _captured_at: (
            previous_snapshot if trade_date == previous else None
        ),
        sector_strength_timeline=lambda trade_date: (
            ["2026-08-24T15:00:00+08:00"] if trade_date == previous else []
        ),
    )
    service.app_state.kaipanla_collector = collector
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)

    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 25, 8, 59, tzinfo=CN_TZ),
    )
    before_open = service.sector_strength_view()
    assert before_open["as_of"] == previous.isoformat()
    assert before_open["rows"][0]["plate_name"] == "芯片"

    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 25, 9, 0, tzinfo=CN_TZ),
    )
    at_switch = service.sector_strength_view()
    assert at_switch["as_of"] == today.isoformat()
    assert at_switch["rows"] == []


@pytest.mark.asyncio
async def test_sector_constituents_view_uses_previous_day_history_before_nine(
    tmp_path,
    monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 25)
    previous = date(2026, 8, 24)
    captured_at = "2026-08-24T15:00:00+08:00"
    snapshot = {
        "provider": "kaipanla",
        "state": "live",
        "as_of": previous.isoformat(),
        "refreshed_at": captured_at,
        "history_state": "closed",
        "rows": [{"plate_id": "P1", "plate_name": "芯片"}],
    }

    async def constituents_at(trade_date, plate_id):
        assert trade_date == previous
        assert plate_id == "P1"
        return [{
            "code": "600000",
            "name": "浦发银行",
            "change_pct": 10.01,
            "turnover_rate": 9.63,
            "amount": 123456789,
            "main_net": 456789,
        }]

    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: snapshot,
        sector_strength_snapshot_at=lambda _day, _captured_at: snapshot,
        sector_strength_timeline=lambda _day: [captured_at],
        latest_completed_trading_date=lambda _day: previous,
        sector_constituents_at=constituents_at,
    )
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 25, 8, 59, tzinfo=CN_TZ),
    )

    result = await service.sector_constituents_view("P1", captured_at)

    assert result["state"] == "closed"
    assert result["as_of"] == previous.isoformat()
    assert result["membership_as_of"] == previous.isoformat()
    assert result["rows"][0]["change_pct"] == pytest.approx(0.1001)
    assert result["rows"][0]["turnover_rate"] == pytest.approx(0.0963)
    assert result["rows"][0]["main_net"] == 456789


@pytest.mark.skip(reason="短线猎手改用开盘啦当日 socket 成分，不再读取上一交易日分区")
def test_sector_candidate_universe_uses_top_ten_and_one_membership_batch(
    tmp_path,
    monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    membership_date = date(2026, 8, 14)
    calls = []
    rows = [
        {
            "plate_id": f"P{rank:02d}",
            "plate_name": f"板块{rank:02d}",
            "strength": 110 - rank,
            "rank": rank,
            "rank_count": 11,
        }
        for rank in range(1, 12)
    ]

    def memberships(trade_date):
        calls.append(trade_date)
        return pl.DataFrame({
            "plate_id": [row["plate_id"] for row in rows],
            "symbol": [f"600{rank:03d}.SH" for rank in range(1, 12)],
        })

    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-17T10:00:05+08:00",
            "rows": rows,
        },
        latest_completed_trading_date=lambda _day: membership_date,
        sector_constituent_memberships=memberships,
    )

    first = service._refresh_sector_candidate_universe(today)
    second = service._refresh_sector_candidate_universe(today)

    assert first == {f"600{rank:03d}.SH" for rank in range(1, 11)}
    assert second == first
    assert "600011.SH" not in first
    assert service._sector_candidate_plate_ids == {f"P{rank:02d}" for rank in range(1, 11)}
    assert calls == [membership_date]


@pytest.mark.skip(reason="短线猎手改用开盘啦当日 socket 成分，不再读取上一交易日分区")
def test_sector_candidate_universe_uses_available_memberships_when_one_is_missing(
    tmp_path,
):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    rows = [
        {
            "plate_id": f"P{rank:02d}",
            "plate_name": f"板块{rank:02d}",
            "strength": 110 - rank,
            "rank": rank,
        }
        for rank in range(1, 11)
    ]
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-17T10:00:05+08:00",
            "rows": rows,
        },
        latest_completed_trading_date=lambda _day: date(2026, 8, 14),
        sector_constituent_memberships=lambda _day: pl.DataFrame({
            "plate_id": [f"P{rank:02d}" for rank in range(1, 10)],
            "symbol": [f"600{rank:03d}.SH" for rank in range(1, 10)],
        }),
    )

    assert service._refresh_sector_candidate_universe(today) == {
        *(f"600{rank:03d}.SH" for rank in range(1, 10)),
    }
    assert service._sector_candidate_scope["state"] == "partial"
    assert service._sector_candidate_scope["plate_count"] == 9
    assert "1 个板块缺口已跳过" in service._sector_candidate_scope["reason"]


@pytest.mark.skip(reason="短线猎手候选行情已由开盘啦 socket 快照提供")
def test_market_fetch_requests_only_top_ten_sector_candidates(tmp_path, monkeypatch):
    service, quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    requested = []
    service._history_date = today
    service._history_ready = True
    service._first_board_eligible = {f"600{rank:03d}.SH" for rank in range(1, 12)}
    rows = [
        {
            "plate_id": f"P{rank:02d}",
            "plate_name": f"板块{rank:02d}",
            "strength": 110 - rank,
            "rank": rank,
        }
        for rank in range(1, 12)
    ]
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-17T10:00:05+08:00",
            "rows": rows,
        },
        latest_completed_trading_date=lambda _day: date(2026, 8, 14),
        sector_constituent_memberships=lambda _day: pl.DataFrame({
            "plate_id": [row["plate_id"] for row in rows],
            "symbol": [f"600{rank:03d}.SH" for rank in range(1, 12)],
        }),
    )

    def latest(symbols=None):
        requested.append(set(symbols or []))
        return []

    quotes.get_latest_quotes = latest
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)

    service._on_market_fetch()

    assert requested == [{f"600{rank:03d}.SH" for rank in range(1, 11)}]
    assert "600011.SH" not in requested[0]


def test_sector_strength_exposes_stable_five_minute_horizontal_trend(
    tmp_path,
    monkeypatch,
):
    service, _quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    points = [
        "2026-08-17T09:35:00+08:00",
        "2026-08-17T09:59:55+08:00",
        "2026-08-17T10:05:00+08:00",
        "2026-08-17T10:05:05+08:00",
    ]
    values = {
        points[0]: (80.0, -5.0),
        points[1]: (100.0, 10.0),
        points[2]: (110.0, 20.0),
        points[3]: (999.0, -999.0),
    }

    def snapshot_at(_day, captured_at):
        strength, main_net = values[captured_at]
        return {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": captured_at,
            "rows": [{
                "plate_id": "P1",
                "plate_name": "芯片",
                "rank": 1,
                "strength": strength,
                "main_net": main_net,
            }],
        }

    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-17T10:07:30+08:00",
            "history_state": "live",
            "rows": [{
                "plate_id": "P1",
                "plate_name": "芯片",
                "rank": 1,
                "strength": 120.0,
                "main_net": 25.0,
            }],
        },
        sector_strength_timeline=lambda _day: points,
        sector_strength_snapshot_at=snapshot_at,
    )

    result = service.sector_strength_view()

    trend = result["trend_5m"]
    assert trend["state"] == "accelerating"
    assert trend["captured_at"] == points[2]
    assert trend["base_at"] == points[1]
    assert trend["elapsed_minutes"] == pytest.approx(305 / 60)
    assert trend["strength_delta"] == pytest.approx(10.0)
    assert trend["main_net_delta"] == pytest.approx(10.0)
    assert trend["comparable_count"] == 1
    assert result["rows"][0]["strength_delta_5m"] == pytest.approx(10.0)
    assert result["rows"][0]["main_net_delta_5m"] == pytest.approx(10.0)
    assert result["rows"][0]["strength_speed_per_min_5m"] == pytest.approx(120 / 61)
    assert result["rows"][0]["main_net_speed_per_min_5m"] == pytest.approx(120 / 61)
    assert result["trend_30m"]["captured_at"] == points[2]
    assert result["trend_30m"]["base_at"] == points[0]
    assert result["trend_30m"]["elapsed_minutes"] == pytest.approx(30.0)
    assert result["rows"][0]["strength_delta_30m"] == pytest.approx(30.0)
    assert result["rows"][0]["main_net_delta_30m"] == pytest.approx(25.0)
    assert result["rows"][0]["strength_speed_per_min_30m"] == pytest.approx(1.0)
    assert result["rows"][0]["main_net_speed_per_min_30m"] == pytest.approx(5 / 6)


@pytest.mark.asyncio
@pytest.mark.skip(reason="短线猎手成分详情已改为开盘啦 socket 当日快照")
async def test_sector_constituents_use_previous_membership_and_tickflow_close(
    tmp_path,
    monkeypatch,
):
    service, quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    membership_date = date(2026, 8, 14)
    captured_at = "2026-08-17T15:00:00+08:00"
    calls = []

    async def constituents_at(trade_date, plate_id):
        calls.append((trade_date, plate_id))
        return [
            {
                "plate_id": plate_id,
                "symbol": "600000",
                "code": "600000",
                "name": "浦发银行",
                "last_price": 10.1,
                "change_pct": 2.18,
                "turnover_rate": 3.5,
                "amount": 100,
                "main_net": 20,
                "limit_count": 1,
            },
            {
                "plate_id": plate_id,
                "symbol": "600001",
                "code": "600001",
                "name": "邯郸钢铁",
                "last_price": 11.0,
                "change_pct": 10.0,
                "turnover_rate": 5.5,
                "amount": 200,
                "main_net": 40,
                "limit_count": 2,
            },
        ]

    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 17, 21, 4, 53, tzinfo=CN_TZ),
    )
    quotes.final_sync_done = True
    quotes.latest_quotes = [
        {
            "symbol": "600000.SH",
            "name": "浦发银行",
            "last_price": 10.1,
            "prev_close": 10.0,
            "change_pct": 0.01,
            "turnover_rate": 0.035,
            "amount": 300,
            "timestamp": "2026-08-17T15:31:00+08:00",
        },
        {
            "symbol": "600001.SH",
            "name": "邯郸钢铁",
            "last_price": 11.0,
            "prev_close": 10.0,
            "change_pct": 0.10,
            "turnover_rate": 0.055,
            "amount": 400,
            "timestamp": "2026-08-17T15:31:00+08:00",
        },
    ]
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": "2026-08-17T21:04:53+08:00",
            "history_state": "closed",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 16807}],
        },
        sector_strength_snapshot_at=lambda _day, selected_at: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": selected_at,
            "history_state": "live",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 16807}],
        },
        sector_strength_timeline=lambda _day: [captured_at],
        latest_completed_trading_date=lambda _day: membership_date,
        sector_constituents_at=constituents_at,
    )

    result = await service.sector_constituents_view("801001", captured_at)

    assert calls == [(membership_date, "801001")]
    assert result["plate_name"] == "芯片"
    assert result["membership_as_of"] == "2026-08-14"
    assert result["quote_provider"] == "tickflow"
    assert result["quote_state"] == "closed"
    assert result["quote_as_of"] == "2026-08-17T15:00:00+08:00"
    assert [row["symbol"] for row in result["rows"]] == ["600001.SH", "600000.SH"]
    assert result["rows"][0]["change_pct"] == 0.10
    assert result["rows"][0]["turnover_rate"] == 0.055
    assert result["rows"][0]["amount"] == 400
    assert result["rows"][0]["main_net"] is None
    assert result["rows"][0]["limit_count"] is None
    assert result["rows"][0]["limit_tag"] == "涨停"
    assert result["rows"][0]["rank"] == 1
    after_close = await service.sector_constituents_view(
        "801001",
        "2026-08-17T21:04:53+08:00",
    )
    assert calls[-1] == (membership_date, "801001")
    assert after_close["captured_at"] == "2026-08-17T15:00:00+08:00"
    with pytest.raises(ValueError, match="选定时间点不可用"):
        await service.sector_constituents_view("999999", captured_at)


@pytest.mark.asyncio
@pytest.mark.skip(reason="短线猎手成分详情已改为开盘啦 socket 当日快照")
async def test_sector_constituents_do_not_reuse_historical_quote_fields(
    tmp_path,
    monkeypatch,
):
    service, quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    captured_at = "2026-08-17T10:00:00+08:00"
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ),
    )

    async def constituents_at(_trade_date, plate_id):
        return [{
            "plate_id": plate_id,
            "symbol": "600000",
            "code": "600000",
            "name": "浦发银行",
            "last_price": 99.0,
            "change_pct": 9.9,
            "amount": 999,
            "main_net": 888,
            "limit_count": 3,
        }]

    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": captured_at,
            "history_state": "live",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 100}],
        },
        sector_strength_snapshot_at=lambda _day, selected_at: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": selected_at,
            "history_state": "live",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 100}],
        },
        sector_strength_timeline=lambda _day: [captured_at],
        latest_completed_trading_date=lambda _day: date(2026, 8, 14),
        sector_constituents_at=constituents_at,
    )

    result = await service.sector_constituents_view("801001", captured_at)

    assert result["quote_state"] == "unavailable"
    assert result["quote_available"] is False
    assert result["rows"][0]["last_price"] is None
    assert result["rows"][0]["change_pct"] is None
    assert result["rows"][0]["amount"] is None
    assert result["rows"][0]["main_net"] is None
    assert result["rows"][0]["limit_count"] is None
    assert quotes.consumers["limit_board"] == {"600000.SH"}


@pytest.mark.asyncio
@pytest.mark.skip(reason="开盘啦 socket 不提供历史时点快照")
async def test_historical_sector_point_does_not_show_current_stock_quotes(
    tmp_path,
    monkeypatch,
):
    service, quotes, _config = make_service(tmp_path)
    today = date(2026, 8, 17)
    current_at = "2026-08-17T10:00:00+08:00"
    selected_at = "2026-08-17T09:35:00+08:00"
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    monkeypatch.setattr(
        "app.services.limit_board_service.cn_now",
        lambda: datetime(2026, 8, 17, 10, 0, tzinfo=CN_TZ),
    )
    quotes.latest_quotes = [{
        "symbol": "600000.SH",
        "last_price": 10.1,
        "prev_close": 10.0,
        "change_pct": 0.01,
        "timestamp": current_at,
    }]

    async def constituents_at(_trade_date, plate_id):
        return [{"plate_id": plate_id, "code": "600000", "name": "浦发银行"}]

    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": current_at,
            "history_state": "live",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 100}],
        },
        sector_strength_snapshot_at=lambda _day, captured_at: {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "refreshed_at": captured_at,
            "history_state": "live",
            "rows": [{"plate_id": "801001", "plate_name": "芯片", "strength": 80}],
        },
        sector_strength_timeline=lambda _day: [selected_at, current_at],
        latest_completed_trading_date=lambda _day: date(2026, 8, 14),
        sector_constituents_at=constituents_at,
    )

    result = await service.sector_constituents_view("801001", selected_at)

    assert result["quote_state"] == "historical_unavailable"
    assert result["quote_available"] is False
    assert result["quote_as_of"] is None
    assert result["rows"][0]["last_price"] is None


def test_board_pool_auto_trade_blocks_legacy_st_member(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600002.SH",
        "auto_trade": True,
        "order_mode": "queue",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {}

    service._maybe_auto_trade(
        "600002.SH",
        {**quote(), "symbol": "600002.SH", "name": "*ST风险"},
        state,
        config,
    )

    assert qmt.orders == []
    assert state["auto_order_status"] == "blocked"
    assert "ST 风险警示" in state["auto_order_error"]
    assert "auto_order_key" not in state


def test_sweep_submits_on_limit_touch_without_queue_delay(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["settings"].update({
        "queue_wait_seconds": 60,
        "queue_confirm_snapshots": 3,
    })
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "sweep",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {
        "touched": True,
        "touched_at": "2026-08-13T10:00:00+08:00",
    }

    service._maybe_auto_trade(
        "600000.SH", quote(), state, config, trigger_mode="limit_touch",
    )

    assert len(qmt.orders) == 1
    assert qmt.orders[0]["price"] == 11.0
    assert qmt.orders[0]["price_type"] == "LIMIT"
    assert state["auto_order_mode"] == "sweep"


def test_sweep_ignores_queue_only_depth_trigger(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "sweep",
        "allocation_mode": "fixed",
        "allocation_value": 20_000,
    }]
    state = {}

    service._maybe_auto_trade(
        "600000.SH", quote(), state, config, trigger_mode="queue",
    )

    assert qmt.orders == []
    assert "auto_order_key" not in state


def test_store_revision_conflict_is_fail_closed(tmp_path):
    store = LimitBoardStore(tmp_path)
    saved = store.update(0, lambda value: value["selected"].append({"symbol": "600000.SH"}))
    assert saved["revision"] == 1
    try:
        store.update(0, lambda value: value["selected"].clear())
    except ValueError as exc:
        assert "revision=1" in str(exc)
    else:
        raise AssertionError("expected revision conflict")


def test_store_loads_legacy_config_with_empty_board_pool(tmp_path):
    store = LimitBoardStore(tmp_path)
    store.config_path.write_text(
        '{"schema_version":1,"revision":3,"settings":{},"selected":[]}',
        encoding="utf-8",
    )

    config = store.load_config()

    assert config["revision"] == 3
    assert config["board_pool"] == []


def test_limit_board_api_exposes_view_and_revision_conflict(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    view = client.get("/api/limit-board")
    assert view.status_code == 200
    assert view.json()["revision"] == 0
    assert "opportunity_pool" in view.json()
    assert "four_mode" not in view.json()
    assert not any(route.path == "/api/limit-board/four-mode" for route in router.routes)

    added = client.post(
        "/api/limit-board/candidate",
        json={"symbol": "600000.SH", "revision": 0},
    )
    assert added.status_code == 200
    assert added.json()["config"]["revision"] == 1

    pooled = client.post(
        "/api/limit-board/pool",
        json={"symbol": "600000.SH", "source": "manual", "revision": 1},
    )
    assert pooled.status_code == 200
    assert pooled.json()["config"]["board_pool"][0]["auto_trade"] is True

    enabled = client.put(
        "/api/limit-board/pool/600000.SH",
        json={"auto_trade": True, "order_mode": "queue", "revision": 2},
    )
    assert enabled.status_code == 200
    assert enabled.json()["config"]["board_pool"][0]["auto_trade"] is True
    assert enabled.json()["config"]["board_pool"][0]["order_mode"] == "queue"

    removed = client.delete("/api/limit-board/pool/600000.SH?revision=3")
    assert removed.status_code == 200
    assert removed.json()["config"]["board_pool"] == []

    stale = client.post(
        "/api/limit-board/candidate",
        json={"symbol": "600001.SH", "revision": 0},
    )
    assert stale.status_code == 409


def test_limit_board_api_exposes_selected_sector_strength_snapshot(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    captured_at = "2026-08-17T09:30:05+08:00"
    service.sector_strength_view = lambda value=None: {
        "state": "live",
        "refreshed_at": value,
        "rows": [{"plate_id": "P1", "strength": 16807}],
    }
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    response = client.get(
        "/api/limit-board/sector-strength",
        params={"captured_at": captured_at},
    )

    assert response.status_code == 200
    assert response.json()["refreshed_at"] == captured_at
    assert response.json()["rows"][0]["strength"] == 16807


def test_limit_board_api_exposes_sector_constituents_at_selected_time(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    captured_at = "2026-08-17T09:35:05+08:00"

    async def constituents_view(plate_id, value=None):
        return {
            "state": "live",
            "plate_id": plate_id,
            "captured_at": value,
            "rows": [{"symbol": "600000.SH", "name": "浦发银行"}],
        }

    service.sector_constituents_view = constituents_view
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    response = client.get(
        "/api/limit-board/sector-strength/801001/constituents",
        params={"captured_at": captured_at},
    )

    assert response.status_code == 200
    assert response.json()["plate_id"] == "801001"
    assert response.json()["captured_at"] == captured_at
    assert response.json()["rows"][0]["symbol"] == "600000.SH"


def test_legacy_selected_api_remains_compatible(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    added = client.post(
        "/api/limit-board/selected",
        json={"symbol": "600000.SH", "revision": 0},
    )

    assert added.status_code == 200
    view = client.get("/api/limit-board").json()
    assert view["candidate_pool"][0]["symbol"] == "600000.SH"
    assert view["candidate_pool"][0]["source"] == "manual"


def test_limit_board_buy_pool_api_submits_and_removes_order(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, quotes, _config = make_service(tmp_path, qmt)
    now = datetime(2026, 8, 13, 10, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    quotes.latest_quotes = [{**quote(price=10.5, limit=11.0), "timestamp": now.isoformat()}]
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    added = client.post(
        "/api/limit-board/buy-pool",
        json={
            "symbol": "600000.SH",
            "source": "first_board",
            "revision": 0,
            "allocation_mode": "fixed",
            "allocation_value": 1_050,
        },
    )

    assert added.status_code == 200
    assert added.json()["order"]["status"] == "accepted_pending"
    assert added.json()["config"]["revision"] == 1
    assert client.get("/api/limit-board").json()["buy_pool"][0]["order_volume"] == 100

    removed = client.delete("/api/limit-board/buy-pool/600000.SH?revision=1")

    assert removed.status_code == 200
    assert removed.json()["config"]["buy_pool"] == []
    assert service._runtime_for_today()["buy_orders"] == {}


def test_limit_board_pool_batch_remove_api_is_atomic(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.store.update(0, lambda value: value.update({
        "board_pool": [
            {"symbol": "600000.SH"},
            {"symbol": "600001.SH"},
            {"symbol": "600002.SH"},
        ],
        "buy_pool": [{"symbol": "600003.SH"}],
    }))
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    removed = client.request(
        "DELETE",
        "/api/limit-board/pool",
        json={"symbols": ["600000.SH", "600002.SH", "600000.SH"], "revision": 1},
    )

    assert removed.status_code == 200
    assert removed.json()["config"]["revision"] == 2
    assert [item["symbol"] for item in removed.json()["config"]["board_pool"]] == ["600001.SH"]
    assert removed.json()["config"]["buy_pool"] == [{"symbol": "600003.SH"}]

    stale = client.request(
        "DELETE",
        "/api/limit-board/pool",
        json={"symbols": ["600001.SH"], "revision": 1},
    )
    assert stale.status_code == 409

    empty = client.request(
        "DELETE",
        "/api/limit-board/pool",
        json={"symbols": [], "revision": 2},
    )
    assert empty.status_code == 422


def test_limit_board_buy_pool_batch_remove_api_clears_runtime_orders(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.store.update(0, lambda value: value.update({
        "buy_pool": [{"symbol": "600000.SH"}, {"symbol": "600001.SH"}],
        "board_pool": [{"symbol": "600002.SH"}],
    }))
    runtime = service._runtime_for_today()
    runtime["buy_orders"] = {"600000.SH": {"order_status": "accepted_pending"}, "600001.SH": {"order_status": "blocked"}}
    service._persist_runtime(runtime)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    removed = client.request(
        "DELETE",
        "/api/limit-board/buy-pool",
        json={"symbols": ["600000.SH", "600001.SH"], "revision": 1},
    )

    assert removed.status_code == 200
    assert removed.json()["config"]["buy_pool"] == []
    assert removed.json()["config"]["board_pool"] == [{"symbol": "600002.SH"}]
    assert service._runtime_for_today()["buy_orders"] == {}


def test_limit_board_buy_pool_api_accepts_after_hours_order_price(tmp_path, monkeypatch):
    qmt = FakeQmt()
    service, _quotes, _config = make_service(tmp_path, qmt)
    now = datetime(2026, 8, 13, 20, 0, tzinfo=CN_TZ)
    monkeypatch.setattr("app.services.limit_board_service.cn_now", lambda: now)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: now.date())
    service._pool_quote = lambda _symbol: (_ for _ in ()).throw(AssertionError("盘后不应读取行情"))
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    added = client.post(
        "/api/limit-board/buy-pool",
        json={
            "symbol": "600000.SH",
            "source": "manual",
            "revision": 0,
            "allocation_mode": "fixed",
            "allocation_value": 1_050,
            "order_price": 9.876,
        },
    )

    assert added.status_code == 200
    assert qmt.orders[0]["price"] == 9.876
    assert added.json()["config"]["buy_pool"][0]["order_price"] == 9.876


def test_limit_board_api_rejects_legacy_notification_settings(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    rejected = client.put(
        "/api/limit-board/settings/notifications",
        json={
            "revision": 0,
            "notifications": {
                "touched": False,
                "broken": True,
                "resealed": False,
            },
        },
    )

    assert rejected.status_code == 404
    assert "notifications" not in service.store.load_config()["settings"]


def test_limit_board_config_drops_legacy_global_allocation_settings(tmp_path):
    """旧配置里的全局资金方式字段在读取时清理，避免继续落盘。"""
    service, _quotes, _config = make_service(tmp_path)
    raw = default_config()
    raw["settings"]["order_allocation_mode"] = "quarter"
    raw["settings"]["order_amount_per_board"] = 10_000
    service.store.config_path.write_text(json.dumps(raw), encoding="utf-8")

    settings = service.store.load_config()["settings"]

    assert "order_allocation_mode" not in settings
    assert "order_amount_per_board" not in settings


def test_limit_board_quote_api_limits_batch_size(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    response = client.post(
        "/api/limit-board/quotes",
        json={"symbols": [f"{index:06d}.SH" for index in range(31)]},
    )

    assert response.status_code == 422


def test_limit_board_api_updates_advanced_settings(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)
    settings = {
        "sweep_price_levels": 10,
        "queue_wait_seconds": 8,
        "queue_confirm_snapshots": 4,
        "max_auto_board_count": 3,
        "max_market_broken_rate_pct": 35.5,
        "main_board_only": True,
        "near_limit_pct": 0.015,
        "exit_limit_pct": 0.04,
        "exit_sustain_seconds": 45,
        "first_board_lookback_days": 20,
        "blacklist_after_breaks": 5,
    }

    updated = client.put(
        "/api/limit-board/settings/advanced",
        json={"revision": 0, "settings": settings},
    )

    assert updated.status_code == 200
    assert updated.json()["config"]["revision"] == 1
    assert {
        key: updated.json()["config"]["settings"][key]
        for key in settings
    } == settings
    assert service.store.load_config()["settings"]["sweep_price_levels"] == 10


def test_limit_board_api_rejects_invalid_advanced_settings(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)
    settings = {
        "sweep_price_levels": 5,
        "queue_wait_seconds": 0,
        "queue_confirm_snapshots": 0,
        "max_auto_board_count": 0,
        "max_market_broken_rate_pct": 40,
        "main_board_only": False,
        "near_limit_pct": 0.02,
        "exit_limit_pct": 0.03,
        "exit_sustain_seconds": 30,
        "first_board_lookback_days": 10,
        "blacklist_after_breaks": 3,
    }

    too_many_levels = client.put(
        "/api/limit-board/settings/advanced",
        json={"revision": 0, "settings": {**settings, "sweep_price_levels": 11}},
    )
    exit_below_entry = client.put(
        "/api/limit-board/settings/advanced",
        json={
            "revision": 0,
            "settings": {
                **settings,
                "near_limit_pct": 0.03,
                "exit_limit_pct": 0.02,
            },
        },
    )
    too_many_queue_snapshots = client.put(
        "/api/limit-board/settings/advanced",
        json={
            "revision": 0,
            "settings": {**settings, "queue_confirm_snapshots": 11},
        },
    )
    too_many_boards = client.put(
        "/api/limit-board/settings/advanced",
        json={
            "revision": 0,
            "settings": {**settings, "max_auto_board_count": 101},
        },
    )

    assert too_many_levels.status_code == 422
    assert exit_below_entry.status_code == 400
    assert too_many_queue_snapshots.status_code == 422
    assert too_many_boards.status_code == 422
    assert service.store.load_config()["revision"] == 0


def test_candidate_score_snapshot_appends_symbol_outside_scan_set(tmp_path, monkeypatch):
    """按需评分：板块强度/雷达入口标的（不在评分扫描集）也能拿到完整 v5 快照。"""
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    runtime["symbols"] = {"600000.SH": {"source_modes": ["first_board"]}}
    service.store.save_runtime(runtime)
    calls: list[list[str]] = []

    def score(runtime, rows, _now):
        calls.append(sorted(str(row["symbol"]) for row in rows))
        runtime["candidate_scores"] = {
            "605159.SH": {
                "candidate_score": 88.0,
                "candidate_score_state": "live",
                "candidate_score_as_of": "2026-08-29T10:00:00+08:00",
                "candidate_score_detail": {
                    "comprehensive": {"comprehensive_score": 88.0, "max_score": 100.0},
                },
                "candidate_reasons": ["板块强度 27.5/30"],
            },
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)

    snapshot = service.candidate_score_snapshot("605159.SH")
    # 全量扫描集（strong rows）+ 追加的按需标的，一次传入，防止缓存被整体覆盖。
    assert calls == [["600000.SH", "605159.SH"]]
    assert snapshot["symbol"] == "605159.SH"
    assert snapshot["candidate_score"] == 88.0
    assert snapshot["candidate_score_state"] == "live"
    assert snapshot["candidate_score_detail"]["comprehensive"]["comprehensive_score"] == 88.0

    with pytest.raises(ValueError):
        service.candidate_score_snapshot("")


def test_candidate_score_snapshot_no_duplicates_for_board_pool_member(tmp_path, monkeypatch):
    """按需评分：打板池成员已在扫描集内，不重复追加。"""
    service, _quotes, _config = make_service(tmp_path)
    runtime = service._runtime_for_today()
    service.store.update(
        0,
        lambda value: value["board_pool"].append({
            "symbol": "605159.SH",
            "name": "上海亚虹",
            "source": "manual",
            "auto_trade": True,
            "order_mode": "sweep",
        }),
    )
    service.store.save_runtime(runtime)
    calls: list[list[str]] = []

    def score(runtime, rows, _now):
        calls.append([str(row["symbol"]) for row in rows])
        runtime["candidate_scores"] = {
            "605159.SH": {
                "candidate_score": 70.0,
                "candidate_score_state": "live",
                "candidate_score_detail": {},
                "candidate_reasons": [],
            },
        }
        return True

    monkeypatch.setattr(service, "_refresh_candidate_scores", score)
    snapshot = service.candidate_score_snapshot("605159.SH")
    assert calls == [["605159.SH"]]
    assert snapshot["candidate_score"] == 70.0


def test_limit_board_api_exposes_candidate_score_snapshot(tmp_path, monkeypatch):
    """GET /api/limit-board/candidate-score/{symbol} 返回按需 v5 评分快照。"""
    service, _quotes, _config = make_service(tmp_path)
    monkeypatch.setattr(
        service,
        "candidate_score_snapshot",
        lambda symbol: {
            "symbol": symbol,
            "candidate_score": 88.0,
            "candidate_score_state": "live",
            "candidate_score_as_of": "2026-08-29T10:00:00+08:00",
            "candidate_score_detail": {
                "comprehensive": {"comprehensive_score": 88.0, "max_score": 100.0},
            },
            "candidate_reasons": [],
        },
    )
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    response = client.get("/api/limit-board/candidate-score/605159.SH")
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "605159.SH"
    assert payload["candidate_score"] == 88.0
    assert payload["candidate_score_detail"]["comprehensive"]["max_score"] == 100.0
