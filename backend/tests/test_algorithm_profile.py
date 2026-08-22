import pytest

from app.free_strategy.algorithm_profile import build_algorithm_profile


def test_strong_momentum_profile_uses_strategy_specific_steps_and_runtime():
    profile = build_algorithm_profile(
        {
            "id": "strong",
            "name": "强者恒强",
            "source": '"""实时强势股策略。"""\nSTRATEGY_KIND = "strong_momentum"\n',
        },
        {
            "market_mode": "websocket",
            "execution_mode": "quote",
            "scheduled_times": ["09:30:16", "09:31:00"],
            "config": {
                "fill_policy": "close",
                "settlement": "t1",
                "slippage_bps": 10,
            },
            "risk_config": {
                "max_symbol_exposure_pct": 1,
                "daily_loss_pct": 0.1,
                "max_drawdown_pct": 0.3,
            },
        },
    )

    assert "D-1" in profile["steps"][0]["detail"]
    assert any("09:30:16" in step["detail"] for step in profile["steps"])
    assert "D-1" in profile["inputs"][0]
    assert any("-4%" in item for item in profile["parameters"])
    assert any("rank by intraday_lift" in item for item in profile["pseudocode"])
    assert profile["runtime"][0] == "按 WebSocket 实时报价事件推进策略"
    assert profile["runtime"][2] == "定时触发点：09:30:16, 09:31:00"


def test_custom_profile_falls_back_to_source_docstring_and_execution_mode():
    profile = build_algorithm_profile(
        {
            "id": "custom",
            "name": "自定义策略",
            "source": '"""按成交量突破选择股票。\n第二行细节。"""\n',
        },
        {
            "market_mode": "bar_1m",
            "execution_mode": "full_bar",
            "config": {},
        },
    )

    assert profile["summary"] == "按成交量突破选择股票。"
    assert profile["steps"][1]["detail"] == "每根闭合 K 线执行 on_bar 回调并更新订单"
    assert profile["inputs"] == ["按闭合 1 分钟 K 线推进策略"]
    assert profile["parameters"] == []
    assert len(profile["pseudocode"]) == 3
    assert profile["runtime"][0] == "按闭合 1 分钟 K 线推进策略"


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("seven_stars", "weighted_log_slope"),
        ("small_cap_limitup", "consecutive_limit_up"),
        ("five_fortunes", "regime_liquidity_threshold"),
        ("five_fortunes_v2", "30m regression"),
        ("strong_momentum", "intraday_lift"),
        ("performance_small_cap", "cash_dividend_1y"),
        ("four_mode", "09:25:45"),
    ],
)
def test_builtin_profiles_include_reproducible_inputs_parameters_and_pseudocode(kind, expected):
    profile = build_algorithm_profile(
        {
            "id": kind,
            "name": kind,
            "source": f'STRATEGY_KIND = "{kind}"\n',
        },
        {
            "market_mode": "bar_1m",
            "execution_mode": "full_bar",
            "config": {},
        },
    )

    assert len(profile["inputs"]) >= 3
    assert len(profile["steps"]) >= 6
    assert len(profile["parameters"]) >= 4
    assert expected in "\n".join(profile["pseudocode"])
