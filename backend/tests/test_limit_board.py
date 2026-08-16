from datetime import datetime
from types import SimpleNamespace

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

    def publish_external_alerts(self, events):
        self.events.extend(events)

    @staticmethod
    def enrich_external_alerts(events):
        for event in events:
            event["ext_gn_ths__所属概念"] = "银行;金融科技"
            event["concept"] = "银行;金融科技"

    def get_latest_quotes(self, _symbols=None):
        return []

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
        self.orders.append(request)
        return {**request, "status": "accepted_pending", "order_sys_id": "qmt-1"}


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


def test_first_touch_emits_once_and_records_source(tmp_path, monkeypatch):
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
    assert len(quotes.events) == 1
    assert quotes.events[0]["type"] == "touched"
    assert quotes.events[0]["concept"] == "银行;金融科技"


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

    service._process_quotes([{
        "symbol": "600000.SH",
        "name": "600000.SH",
        "last_price": 11.0,
        "limit_up": 11.0,
        "timestamp": "2026-08-13T10:00:00+08:00",
    }])

    assert service._quotes["600000.SH"]["name"] == "浦发银行"
    assert quotes.events[0]["name"] == "浦发银行"
    assert quotes.events[0]["message"] == "浦发银行：触板"


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


def test_valid_snapshot_with_no_qualified_symbols_keeps_scan_ready(tmp_path, monkeypatch):
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
    assert service._first_board_eligible == set()
    assert "0 只通过" in service._history_reason


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


def test_view_builds_ranked_candidate_pool_without_board_members(tmp_path, monkeypatch):
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
    assert view["candidate_pool"][0]["candidate_rank"] == 1
    assert view["candidate_pool"][0]["candidate_score"] > 0
    assert "首板候选" in view["candidate_pool"][0]["candidate_reasons"]


def test_candidate_pool_marks_legacy_selected_rows_as_manual(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    service.store.update(0, lambda value: value["selected"].append({"symbol": "600000.SH", "name": "浦发银行"}))

    view = service.view()

    assert view["candidate_pool"][0]["source"] == "manual"
    assert "手工加入" in view["candidate_pool"][0]["candidate_reasons"]


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


def test_candidate_pool_does_not_consume_websocket_capacity(tmp_path, monkeypatch):
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

    service._sync_websocket(service._runtime_for_today(), config)

    assert service._ws_symbols == {"600001.SH"}
    assert hub.registered == {"600001.SH"}


def test_add_pool_enables_auto_trade_by_default(tmp_path):
    service, _quotes, _config = make_service(tmp_path)

    saved = service.add_pool("600000.SH", "first_board", 0)

    assert saved["board_pool"][0]["auto_trade"] is True
    assert saved["board_pool"][0]["order_mode"] == "sweep"


def test_default_config_preserves_current_sweep_and_queue_triggers():
    settings = default_config()["settings"]

    assert settings["sweep_price_levels"] == 5
    assert settings["queue_wait_seconds"] == 0
    assert settings["queue_confirm_snapshots"] == 0


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
        "message": "浦发银行：触板",
    }

    service.enrich_external_alerts([event])
    body = QuoteService._format_alert_notification_body(event)

    assert event["ext_gn_ths__所属概念"] == "银行;金融科技"
    assert event["concept"] == "银行;金融科技"
    assert body == "600000.SH 浦发银行：触板\n概念：银行、金融科技"


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
    assert "第 4 次炸板" in quotes.events[-1]["reasons"][0]


def test_board_pool_auto_trade_submits_one_lot_once_per_day(tmp_path):
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
    assert qmt.orders[0]["volume"] == 100
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
    }]
    state = {}

    service._maybe_auto_trade("600000.SH", quote(), state, config)

    assert state["auto_order_status"] == "blocked"
    assert "QMT" in state["auto_order_error"]
    assert "auto_order_key" not in state


def test_board_pool_auto_trade_blocks_legacy_st_member(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    config["board_pool"] = [{
        "symbol": "600002.SH",
        "auto_trade": True,
        "order_mode": "queue",
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


def test_default_sweep_ignores_queue_trigger_and_submits_on_sweep_trigger(tmp_path):
    qmt = FakeQmt()
    service, _quotes, config = make_service(tmp_path, qmt)
    service._order_executor.shutdown(wait=False, cancel_futures=True)
    service._order_executor = ImmediateExecutor()
    config["board_pool"] = [{
        "symbol": "600000.SH",
        "auto_trade": True,
        "order_mode": "sweep",
    }]
    state = {}

    service._maybe_auto_trade(
        "600000.SH", quote(), state, config, trigger_mode="queue",
    )
    assert qmt.orders == []

    service._maybe_auto_trade(
        "600000.SH", quote(), state, config, trigger_mode="sweep",
    )

    assert len(qmt.orders) == 1
    assert state["auto_order_mode"] == "sweep"


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


def test_limit_board_api_updates_notification_settings(tmp_path):
    service, _quotes, _config = make_service(tmp_path)
    app = FastAPI()
    app.state.limit_board_service = service
    app.include_router(router)
    client = TestClient(app)

    updated = client.put(
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

    assert updated.status_code == 200
    assert updated.json()["config"]["revision"] == 1
    assert updated.json()["config"]["settings"]["notifications"] == {
        "touched": False,
        "broken": True,
        "resealed": False,
    }
    assert service.store.load_config()["settings"]["notifications"] == {
        "touched": False,
        "broken": True,
        "resealed": False,
    }


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

    assert too_many_levels.status_code == 422
    assert exit_below_entry.status_code == 400
    assert too_many_queue_snapshots.status_code == 422
    assert service.store.load_config()["revision"] == 0
