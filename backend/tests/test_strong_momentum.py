from datetime import date, datetime

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.templates import TEMPLATES


def _snapshot(day):
    return {
        "date": day.isoformat(),
        "as_of": "2025-12-31",
        "coverage": 1.0,
        "industries": [],
        "subindustries": [],
        "candidates": [{
            "symbol": "000001.SZ",
            "name": "平安银行",
            "l1_key": "bank",
            "l1_name": "银行",
            "l2_key": "bank-2",
            "l2_name": "股份制银行",
            "l1_score": 72.0,
            "l2_score": 75.0,
            "stock_score": 80.0,
            "previous_raw_close": 10.0,
        }],
    }


def test_strong_momentum_template_uses_native_daily_contract():
    template = TEMPLATES["strong_momentum"]

    assert template["name"] == "强者恒强·项目适配"
    assert template["config"]["timeframe"] == "1d"
    assert template["config"]["asset_type"] == "stock"
    assert template["config"]["settlement"] == "t1"
    assert template["config"]["fill_policy"] == "next_open"
    assert "jqdata" not in template["source"]
    assert "context.require_mainline_snapshot" in template["source"]


def test_strong_momentum_submits_next_open_entry_from_pit_candidate():
    engine = FreeStrategyEngine(
        TEMPLATES["strong_momentum"]["source"],
        timeframe="1d",
        config=FreeStrategyConfig(
            initial_capital=100_000,
            asset_type="stock",
            fill_policy="next_open",
        ),
        instruments=[{
            "symbol": "000001.SZ",
            "name": "平安银行",
            "asset_type": "stock",
        }],
    )
    engine.set_mainline_snapshot_loader(_snapshot)
    engine.set_trading_calendar([date(2026, 1, 5), date(2026, 1, 6)])

    result = engine.run([
        Bar(
            "000001.SZ",
            datetime(2026, 1, 5, 15),
            10,
            10.4,
            9.9,
            10.3,
            volume=1_000,
            amount=1_030_000,
            raw_close=10.3,
            limit_up=11.0,
        ),
    ])

    assert result["orders"][0]["status"] == "pending"
    assert result["orders"][0]["target_percent"] == 0.30
    assert result["strategy_signals"][0]["signal_type"] == "strong_momentum_entry"
