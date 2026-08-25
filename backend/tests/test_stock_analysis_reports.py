import json
from datetime import date
from types import SimpleNamespace

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.stock_analysis import router
from app.jobs import daily_pipeline
from app.services import stock_reports


def test_latest_reports_returns_one_report_per_requested_symbol(monkeypatch):
    reports = [
        {"id": "new", "symbol": "000001.SZ", "created_at": "2026-08-23T18:00:00"},
        {"id": "old", "symbol": "000001.SZ", "created_at": "2026-08-22T18:00:00"},
        {"id": "other", "symbol": "600000.SH", "created_at": "2026-08-23T18:00:00"},
    ]
    monkeypatch.setattr(stock_reports._store, "list_reports", lambda: reports)

    assert stock_reports.latest_reports(["000001.sz", "missing"]) == {"000001.SZ": reports[0]}

    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).get(
        "/api/stock-analysis/reports/latest?symbols=000001.SZ,600000.SH"
    )
    assert response.status_code == 200
    assert response.json() == {"reports": {"000001.SZ": reports[0], "600000.SH": reports[2]}}


def test_scheduled_stock_reports_continue_after_single_symbol_failure(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 8, 23)

    class FakeStore:
        def get_runtime(self, key, default=None):
            return {"qmt_sync": {"synced_at": "2026-08-23T15:05:00"}}.get(key, default)

        def load(self):
            return {"positions": [
                {"symbol": "000001.SZ", "name": "平安银行"},
                {"symbol": "000002.SZ", "name": "万科A"},
                {"symbol": "600000.SH", "name": "浦发银行"},
            ]}

    class FakeRepo:
        store = SimpleNamespace(data_dir="/tmp")

        def get_enriched_latest(self):
            return pl.DataFrame(), FixedDate.today()

    saved = []

    async def fake_stream(_repo, _data_dir, symbol, _focus=""):
        if symbol == "000002.SZ":
            yield json.dumps({"type": "error", "message": "行情不足"})
            return
        yield json.dumps({"type": "meta", "summary": "摘要", "close": 10.5, "levels": {}})
        yield json.dumps({"type": "delta", "content": f"报告 {symbol}"})
        yield json.dumps({"type": "done"})

    monkeypatch.setattr(daily_pipeline, "date", FixedDate)
    monkeypatch.setattr("app.secrets_store.get_ai_key", lambda: "configured")
    monkeypatch.setattr(stock_reports, "latest_reports", lambda _symbols: {})
    monkeypatch.setattr(stock_reports, "save_report", lambda report: saved.append(report) or report)
    monkeypatch.setattr("app.services.stock_analyzer.analyze_stock_stream", fake_stream)

    app_state = SimpleNamespace(position_risk_service=SimpleNamespace(store=FakeStore()))
    daily_pipeline._run_scheduled_stock_reports(FakeRepo(), app_state)

    assert [item["symbol"] for item in saved] == ["000001.SZ", "600000.SH"]
    assert [item["content"] for item in saved] == ["报告 000001.SZ", "报告 600000.SH"]


def test_scheduled_stock_reports_skips_without_qmt_sync_or_ai(monkeypatch):
    class FakeRepo:
        def get_enriched_latest(self):
            return pl.DataFrame(), date.today()

    app_state = SimpleNamespace(
        position_risk_service=SimpleNamespace(
            store=SimpleNamespace(get_runtime=lambda *_args: None, load=lambda: {"positions": []}),
        ),
    )
    monkeypatch.setattr("app.secrets_store.get_ai_key", lambda: "")
    daily_pipeline._run_scheduled_stock_reports(FakeRepo(), app_state)

    monkeypatch.setattr("app.secrets_store.get_ai_key", lambda: "configured")
    daily_pipeline._run_scheduled_stock_reports(FakeRepo(), app_state)
