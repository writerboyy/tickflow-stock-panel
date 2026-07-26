import json
from datetime import date
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import free_strategy
from app.api.free_strategy import BacktestWrite, _job_payload, router
from app.free_strategy.store import PaperAccountStore


def test_etf_asset_type_is_preserved_in_engine_config(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {"id": "five", "name": "五福", "source": "def on_bar(context, bars): pass", "revision": 3}
    payload = _job_payload(
        BacktestWrite(
            strategy_id="five",
            symbols=["510300.SH"],
            timeframe="1m",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            asset_type="etf",
        ),
        strategy,
        request,
    )

    assert payload["config"]["asset_type"] == "etf"
    assert payload["strategy_name"] == "五福"


def test_saved_backtest_routes_are_not_captured_by_strategy_id(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "saved-run"
    run_dir.mkdir(parents=True)
    result = {
        "final_equity": 1_234_567.89,
        "return_pct": 23.45,
        "max_drawdown_pct": 6.78,
        "fills": [{"order_id": "o1"}],
        "metadata": {"strategy_name": "五福"},
    }
    (run_dir / "result.json").write_text(json.dumps(result), encoding="utf-8")

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    listing = client.get("/api/free-strategies/backtest")
    assert listing.status_code == 200
    assert listing.json()["runs"] == [{
        "job_id": "saved-run",
        "final_equity": 1_234_567.89,
        "return_pct": 23.45,
        "max_drawdown_pct": 6.78,
        "fills": 1,
        "metadata": {"strategy_name": "五福"},
    }]

    saved = client.get("/api/free-strategies/backtest/saved-run")
    assert saved.status_code == 200
    assert saved.json() == result


def test_resume_restarts_missing_paper_process(monkeypatch, tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-1", "status": "paused"})
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))

    class FakeProcess:
        def __init__(self):
            self.started = False

        def start(self):
            self.started = True

        def is_alive(self):
            return self.started

    process = FakeProcess()
    context = SimpleNamespace(Process=lambda **_kwargs: process)
    monkeypatch.setattr(free_strategy.mp, "get_context", lambda _method: context)
    free_strategy._paper.pop("paper-1", None)

    result = free_strategy.paper_action("paper-1", "resume", request)

    assert process.started is True
    assert result["status"] == "running"
    free_strategy._paper.pop("paper-1", None)


def test_paper_orders_and_fills_are_separate(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-1",
        "status": "paused",
        "account": {"orders": [{"id": "o1", "status": "filled"}]},
    })
    store.append_event("paper-1", {"type": "fill", "order_id": "o1", "symbol": "510300.SH"})

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    assert client.get("/api/free-strategies/paper/accounts/paper-1/orders").json() == {
        "orders": [{"id": "o1", "status": "filled"}],
    }
    fills = client.get("/api/free-strategies/paper/accounts/paper-1/fills").json()["fills"]
    assert fills[0]["type"] == "fill"
    assert fills[0]["order_id"] == "o1"
