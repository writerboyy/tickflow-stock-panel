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
    assert "09:30:16" in profile["steps"][1]["detail"]
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
    assert profile["runtime"][0] == "按闭合 1 分钟 K 线推进策略"
