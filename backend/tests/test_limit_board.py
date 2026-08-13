from datetime import datetime
from types import SimpleNamespace

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.limit_board import router
from app.services.limit_board_service import LimitBoardService
from app.services.limit_board_store import LimitBoardStore, default_config


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
        return pl.DataFrame({"symbol": ["600000.SH", "600001.SH"], "name": ["浦发银行", "邯郸钢铁"]})

    def get_name_map(self, symbols):
        return {"600000.SH": "浦发银行", "600001.SH": "邯郸钢铁"}.copy()

    def resolve_asset_type(self, _symbol):
        return "stock"


class FakeQuotes:
    def __init__(self):
        self.events = []
        self.consumers = {}

    def publish_external_alerts(self, events):
        self.events.extend(events)

    def get_latest_quotes(self, _symbols=None):
        return []

    def notify_limit_board_updated(self):
        pass

    def set_symbol_consumer(self, consumer_id, symbols):
        self.consumers[consumer_id] = set(symbols)

    def remove_symbol_consumer(self, consumer_id):
        self.consumers.pop(consumer_id, None)


def make_service(tmp_path):
    quotes = FakeQuotes()
    service = LimitBoardService(tmp_path, FakeRepo(), quotes, SimpleNamespace(paper_supervisor=None))
    config = default_config()
    service._history_ready = True
    service._first_board_eligible = {"600000.SH"}
    return service, quotes, config


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
        "/api/limit-board/selected",
        json={"symbol": "600000.SH", "revision": 0},
    )
    assert added.status_code == 200
    assert added.json()["config"]["revision"] == 1

    stale = client.post(
        "/api/limit-board/selected",
        json={"symbol": "600001.SH", "revision": 0},
    )
    assert stale.status_code == 409
