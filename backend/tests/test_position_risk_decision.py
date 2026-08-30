from __future__ import annotations

from app.services.position_risk_decision import build_position_decision


def _feature(**overrides):
    feature = {
        "available": True,
        "fresh": True,
        "reason": "分时数据可用",
        "as_of": "2026-08-21T10:00:00",
        "last_price": 10.2,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "session_vwap": 10.0,
        "ema20_1m": 10.0,
        "momentum_5m": 0.01,
        "buy_ratio": 0.58,
        "sell_ratio": 0.42,
        "flow_samples": 20,
        "orderbook_imbalance": 0.1,
        "hard_stop_price": 9.0,
        "hard_stop_enabled": True,
        "daily": {"available": True, "reason": "日线指标已获取", "as_of": "2026-08-20"},
        "context": {
            "state": "supportive",
            "market_state": "偏暖",
            "emotion_phase": "发酵",
            "missing": [],
        },
    }
    feature.update(overrides)
    return feature


def test_full_data_returns_hold_with_quality_blocks():
    decision = build_position_decision(
        _feature(),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "hold"
    assert decision["suggested_pct"] == 0
    assert decision["data_quality"]["news"]["status"] == "not_supported"
    assert decision["data_quality"]["technical"]["status"] == "available"
    assert decision["manual_confirmation"] is True


def test_auxiliary_data_missing_does_not_hide_core_evidence():
    feature = _feature(
        context={
            "state": "weakening",
            "market_state": "退潮",
            "emotion_phase": "退潮",
            "missing": ["auction", "fund_flow"],
        },
        buy_ratio=None,
        sell_ratio=None,
        flow_samples=0,
    )

    decision = build_position_decision(feature, position={"cost_price": 10.0})

    assert decision["data_quality"]["market_context"]["status"] == "partial"
    assert decision["data_quality"]["fund_flow"]["status"] == "missing"
    assert decision["action"] in {"hold", "observe", "reduce_25"}
    assert decision["action"] != "exit"


def test_core_data_missing_cannot_generate_reduce_or_exit():
    decision = build_position_decision(
        _feature(
            available=False,
            fresh=False,
            reason="需要分钟 K 数据权限",
            daily={"available": False, "reason": "日线指标不可用"},
            last_price=None,
        ),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "observe"
    assert decision["suggested_pct"] == 0
    assert decision["risk_level"] == "unknown"


def test_technical_and_fund_flow_conflict_stays_at_observe_without_two_sided_sell_evidence():
    decision = build_position_decision(
        _feature(
            context={"state": "neutral", "market_state": "分化", "missing": []},
            session_vwap=10.5,
            ema20_1m=10.4,
            momentum_5m=-0.01,
            buy_ratio=0.60,
            sell_ratio=0.40,
        ),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "observe"
    assert decision["suggested_pct"] == 0


def test_limit_up_is_labeled_realization_instead_of_stop_loss():
    decision = build_position_decision(
        _feature(last_price=11.0, session_vwap=10.8),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "hold"
    assert decision["suggested_pct"] == 25
    assert decision["event"] == {
        "kind": "limit_up_realization",
        "label": "涨停兑现",
        "optional_action_pct": 25,
    }
    assert decision["reason"].startswith("涨停兑现")


def test_hard_stop_can_exit_even_when_context_is_missing():
    decision = build_position_decision(
        _feature(
            last_price=8.9,
            context={"state": "unavailable", "missing": ["market", "sector"]},
        ),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "exit"
    assert decision["suggested_pct"] == 100
    assert decision["risk_level"] == "high"


def test_hard_stop_price_without_explicit_enable_does_not_force_exit():
    decision = build_position_decision(
        _feature(
            last_price=8.9,
            hard_stop_enabled=False,
            limit_down=8.0,
            context={"state": "unavailable", "missing": ["market", "sector"]},
        ),
        position={"cost_price": 10.0},
    )

    assert decision["action"] != "exit"


def test_dynamic_exit_rules_have_priority_over_ordinary_reduction():
    decision = build_position_decision(
        _feature(dynamic_exit_rules=["volume_price_divergence"]),
        position={"cost_price": 10.0},
    )

    assert decision["action"] == "exit"
    assert decision["suggested_pct"] == 100
    assert decision["event"]["kind"] == "dynamic_exit"
    assert decision["manual_confirmation"] is True
