import json
from datetime import datetime
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.free_strategy import router
from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.paper import _append_five_fortunes_v2_decision
from app.free_strategy.store import PaperAccountStore


def _report(day: str, held: str | None, target: list[str]) -> dict:
    return {
        "date": day,
        "regime": "normal",
        "raw_regime": "normal",
        "target": target,
        "holdings": target,
        "candidates": [{"symbol": target[0], "score": 1.5}] if target else [],
        "decision": {
            "date": day,
            "reason": "pending",
            "held": held,
            "target": target,
            "candidate_count": len(target),
            "filter_fail_symbols": [],
        },
    }


def test_paper_signals_merge_backtest_checkpoint_and_live_events(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "source-run"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text(json.dumps({
        "state": {"five_fortunes": {"daily_reports": [
            _report("2026-07-25", "A", ["A"]),
            _report("2026-07-28", "A", ["B"]),
        ]}},
        "strategy_signals": [{
            "id": "threshold:1",
            "timestamp": "2026-07-24T10:00:00",
            "signal_type": "threshold",
            "payload": {"reason": "crossed"},
        }],
    }), encoding="utf-8")

    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-1",
        "status": "running",
        "continuation": {"job_id": "source-run"},
        "checkpoint": {"state": {"five_fortunes": {"daily_reports": [
            _report("2026-07-28", "A", ["C"]),
        ]}}},
    })
    store.append_event_once("paper-1", {
        "id": "signal:five_fortunes:2026-07-28:decision",
        "type": "signal",
        "timestamp": "2026-07-28T13:10:00",
        "signal_type": "daily_decision",
        "trading_date": "2026-07-28",
        "target_symbols": ["LIVE"],
    })

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    response = TestClient(app).get("/api/free-strategies/paper/accounts/paper-1/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert [row["timestamp"] for row in payload["signals"]] == [
        "2026-07-28T13:10:00",
        "2026-07-25T13:10:00",
        "2026-07-24T10:00:00",
    ]
    assert payload["signals"][0]["target_symbols"] == ["LIVE"]
    assert payload["signals"][1]["decision"] == "hold"


def test_paper_signals_label_five_fortunes_v2_from_historical_state(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "source-run"
    run_dir.mkdir(parents=True)
    report = _report("2026-07-28", "A", ["B"])
    report["decision"]["reason"] = "trend_pending"
    (run_dir / "result.json").write_text(json.dumps({
        "state": {"five_fortunes_v2": {
            "daily_reports": [report],
        }},
    }), encoding="utf-8")

    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-1",
        "continuation": {"job_id": "source-run"},
    })

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    response = TestClient(app).get("/api/free-strategies/paper/accounts/paper-1/signals")

    assert response.status_code == 200
    signal = response.json()["signals"][0]
    assert signal["id"] == "signal:five_fortunes_v2:2026-07-28:decision"
    assert signal["strategy"] == "five_fortunes_v2"
    assert signal["reason"] == "目标标的盘中趋势未确认，等待复检"


def test_paper_runtime_emits_five_fortunes_v2_decision_signal(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-1"})
    engine = SimpleNamespace(
        context=SimpleNamespace(state={"five_fortunes_v2": {
            "decision": {
                "date": "2026-07-28",
                "reason": "trend_pending",
                "target": ["159509.SZ"],
            },
            "target": ["159509.SZ"],
            "candidate_rows": [{"symbol": "159509.SZ", "score": 1.5}],
        }}),
        account=SimpleNamespace(positions={}),
    )

    _append_five_fortunes_v2_decision(store, "paper-1", engine, datetime(2026, 7, 28, 13, 10))

    signal = store.events("paper-1")[0]
    assert signal["id"] == "signal:five_fortunes_v2:2026-07-28:decision"
    assert signal["strategy"] == "five_fortunes_v2"
    assert signal["reason"] == "目标标的盘中趋势未确认，等待复检"


def test_backtest_result_preserves_structured_strategy_signals():
    source = """
def on_bar(context, bars):
    context.emit_signal('threshold', {'symbol': 'X'}, event_id='threshold:2026-07-28')
"""
    engine = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(initial_capital=1_000, fees_pct=0, slippage_bps=0),
    )

    result = engine.run([Bar("X", datetime(2026, 7, 28, 15), 10, 10, 10, 10)])

    assert result["strategy_signals"] == [{
        "id": "threshold:2026-07-28",
        "timestamp": "2026-07-28T15:00:00",
        "signal_type": "threshold",
        "payload": {"symbol": "X"},
    }]
