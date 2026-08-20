from datetime import datetime

from app.services.native_four_mode import build_native_four_mode_report


def _row(symbol, *modes, state="tradable"):
    return {
        "symbol": symbol,
        "name": symbol,
        "source_modes": list(modes),
        "candidate_score": 80.0,
        "entry_score": 75.0,
        "candidate_score_velocity": 1.0,
        "change_pct": 0.06,
        "limit_gap_pct": 0.015,
        "tradability_state": state,
        "tradability_reason": "系统原生机会",
        "candidate_score_detail": {
            "sector": {"name": "人工智能", "score": 40.0},
            "technical": {"score": 16.0, "momentum_20d": 0.08, "above_ma20": True},
            "intraday_flow": {"score": 20.0, "trend_state": "strong"},
        },
    }


def test_native_four_mode_uses_only_existing_system_rows():
    first = _row("600000.SH", "first_board")
    rebound = _row("600001.SH", "rebound_board")
    report = build_native_four_mode_report(
        first_board=[first],
        rebound_board=[rebound],
        candidate_pool=[first, rebound],
        opportunity_pool=[first],
        runtime={
            "trading_date": "2026-08-20",
            "history_ready": True,
            "history_reason": "ready",
            "candidate_scope": {"state": "live"},
        },
        market_sentiment={"state": "live"},
        sector_strength={"rows": [{}]},
        as_of=datetime(2026, 8, 20, 10, 0),
    )

    assert report["state"] == "live"
    assert report["source"]["provider"] == "tickflow_native"
    assert [mode["name"] for mode in report["modes"]] == ["一进二", "弱转强", "趋势股", "首板"]
    assert report["modes"][0]["candidates"][0]["symbol"] == "600000.SH"
    assert report["modes"][1]["candidates"][0]["symbol"] == "600001.SH"
    assert report["execution_state"] == "read_only"


def test_native_four_mode_is_partial_when_history_is_not_ready():
    report = build_native_four_mode_report(
        first_board=[], rebound_board=[], candidate_pool=[], opportunity_pool=[],
        runtime={"history_ready": False, "candidate_scope": {"state": "unavailable"}},
        market_sentiment=None, sector_strength=None, as_of=datetime(2026, 8, 20),
    )

    assert report["state"] == "partial"
    assert all(mode["candidate_count"] == 0 for mode in report["modes"])
