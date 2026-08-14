from datetime import date

from app.free_strategy.research_periods import build_research_periods


def test_research_periods_use_prior_close_as_oos_base():
    result = {
        "daily_equity_curve": [
            {"date": "2024-12-30", "strategy_nav": 1.0, "benchmark_nav": 1.0},
            {"date": "2024-12-31", "strategy_nav": 1.1, "benchmark_nav": 1.05},
            {"date": "2025-01-02", "strategy_nav": 1.21, "benchmark_nav": 1.1025},
            {"date": "2025-01-03", "strategy_nav": 1.155, "benchmark_nav": 1.08},
        ],
        "fills": [
            {"side": "buy", "timestamp": "2024-12-31T10:00:00"},
            {"side": "buy", "timestamp": "2025-01-02T10:00:00"},
            {"side": "sell", "timestamp": "2025-01-03T14:55:00"},
        ],
    }

    analysis = build_research_periods(
        result, date(2024, 12, 30), date(2025, 1, 3),
    )

    assert analysis["training"]["return_pct"] == 10
    assert analysis["training"]["benchmark_return_pct"] == 5
    assert analysis["out_of_sample"]["return_pct"] == 5
    assert analysis["out_of_sample"]["benchmark_return_pct"] == 2.8571
    assert analysis["out_of_sample"]["max_drawdown_pct"] == 4.5455
    assert analysis["out_of_sample"]["entry_count"] == 1
    assert analysis["out_of_sample"]["exit_count"] == 1
