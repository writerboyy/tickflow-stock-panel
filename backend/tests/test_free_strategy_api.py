import json
from datetime import date
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.free_strategy import BacktestWrite, _job_payload, router


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
