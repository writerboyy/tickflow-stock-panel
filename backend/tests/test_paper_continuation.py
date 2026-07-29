import json
from datetime import date, datetime
from pathlib import Path

import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.continuation import continue_account_from_backtest
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.store import PaperAccountStore


CONFIG = {
    "asset_type": "etf",
    "initial_capital": 100_000,
    "fees_pct": 0.0001,
    "commission_pct": 0.0001,
    "min_commission": 5,
    "stamp_tax_pct": 0,
    "slippage_bps": 0.5,
    "price_tick": 0.001,
    "lot_size": 100,
    "max_exposure_pct": 1,
    "settlement": "t1",
    "fill_policy": "close",
    "benchmark_symbol": "510300.SH",
}


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_backtest_daily_curve_captures_average_cost():
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    context.buy('X', quantity=10)\n",
        config=FreeStrategyConfig(
            initial_capital=1_000,
            lot_size=1,
            fees_pct=0,
            slippage_bps=0,
            fill_policy="close",
            settlement="t0",
        ),
    )

    result = engine.run([
        Bar("X", datetime(2026, 7, 24, 15), 10, 10, 10, 10),
    ])

    assert result["daily_equity_curve"][0]["avg_cost"] == {"X": 10}


def test_continue_account_inherits_state_but_not_historical_orders(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper",
        "strategy_id": "five",
        "source_hash": "source-hash",
        "status": "paused",
        "config": CONFIG,
    })
    run = tmp_path / "free_strategy_runs" / "run"
    write_json(run / "manifest.json", {
        "strategy_id": "five",
        "strategy_source_sha256": "source-hash",
        "payload": {"config": CONFIG},
    })
    checkpoint = {
        "account": {
            "cash": 10,
            "positions": {"X": 10},
            "available": {"X": 10},
            "avg_cost": {"X": 9},
            "orders": [{"id": "o1"}],
            "fills": [{"order_id": "o1"}],
            "corporate_actions": [{"symbol": "X"}],
            "equity_curve": [{"timestamp": "2026-07-24T15:00:00"}],
        },
        "state": {"five_fortunes": {
            "subscription_pool": ["X", "Y"],
            "daily": {"X": [{"date": "2026-07-24", "close": 10}]},
        }},
        "runtime": {"last_timestamp": "2026-07-24T15:00:00"},
        "pending_orders": [],
        "order_counter": 1,
        "risk": {},
    }
    write_json(run / "result.json", {
        "checkpoint": checkpoint,
        "initial_capital": 100_000,
        "final_equity": 110_000,
        "max_drawdown_pct": 17.5,
        "daily_equity_curve": [
            {"date": "2025-07-20", "timestamp": "2025-07-20T15:00:00",
             "equity": 90_000, "cash": 90_000, "strategy_nav": 0.9,
             "drawdown_pct": 10, "positions": {}},
            {"date": "2026-07-24", "timestamp": "2026-07-24T15:00:00",
             "equity": 110_000, "cash": 10, "strategy_nav": 1.1,
             "drawdown_pct": 0, "positions": {"X": 10},
             "avg_cost": {"X": 9}},
        ],
    })

    saved = continue_account_from_backtest(
        tmp_path, "paper", "run", today=date(2026, 7, 28),
    )

    assert saved["equity"] == 110_000
    assert saved["positions"] == {"X": 10}
    assert saved["checkpoint"]["account"]["orders"] == []
    assert saved["checkpoint"]["account"]["fills"] == []
    assert saved["checkpoint"]["order_counter"] == 0
    assert saved["checkpoint"]["universe"] == ["510300.SH", "X", "Y"]
    assert saved["checkpoint"]["state"]["five_fortunes"]["daily"] == {
        "__paper_history_loader__": [],
    }
    assert "account" not in saved
    assert "state" not in saved
    assert "runtime" not in saved
    assert saved["last_bar"] == "2026-07-24T15:00:00"
    assert saved["max_drawdown_pct"] == 17.5
    assert store.equity_curve("paper") == [{
        "timestamp": "2026-07-24T15:00:00",
        "equity": 110_000.0,
        "cash": 10.0,
        "nav": 1.1,
        "drawdown_pct": 0.0,
        "positions": {"X": 10.0},
        "avg_cost": {"X": 9.0},
    }]
    assert list((store._path("paper") / "backups").glob("state-*.json"))


def test_continue_account_rejects_mismatched_source(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper", "strategy_id": "five", "source_hash": "paper-hash",
        "status": "paused", "config": CONFIG,
    })
    run = tmp_path / "free_strategy_runs" / "run"
    write_json(run / "manifest.json", {
        "strategy_id": "five", "strategy_source_sha256": "other-hash",
        "payload": {"config": CONFIG},
    })
    write_json(run / "result.json", {})

    with pytest.raises(ValueError, match="源码"):
        continue_account_from_backtest(tmp_path, "paper", "run")
