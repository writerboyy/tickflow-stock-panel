"""开箱即用的自由策略模板。"""
from __future__ import annotations

from .five_fortunes import DEFENSIVE_ETF, WUFU_MINUTE_POOL

TEMPLATES = {
    "dual_ma": {
        "name": "双均线模板",
    "source": '''def initialize(context):
    context.state.setdefault("closes", {})
    context.log("双均线策略初始化")

def on_bar(context, bars):
    for symbol, bar in bars.items():
        closes = context.state["closes"].setdefault(symbol, [])
        closes.append(bar.close)
        if len(closes) < 20:
            continue
        fast = sum(closes[-5:]) / 5
        slow = sum(closes[-20:]) / 20
        context.order_target_percent(symbol, 0.95 if fast > slow else 0.0)

def after_trading_end(context):
    context.log("双均线日终完成")
''',
    },
    "etf_rotation": {
        "name": "状态化 ETF 轮动模板",
        "source": '''def initialize(context):
    context.state.setdefault("history", {})
    context.state.setdefault("cooldown", 0)
    context.state.setdefault("regime", "neutral")
    context.log("ETF 轮动初始化：默认 T+1，可在账户设置切换 T+0")

def on_bar(context, bars):
    ranked = []
    for symbol, bar in bars.items():
        values = context.state["history"].setdefault(symbol, [])
        values.append(bar.close)
        if len(values) >= 21:
            momentum = values[-1] / values[-21] - 1
            ranked.append((momentum, symbol))
    if not ranked:
        return
    ranked.sort(reverse=True)
    winner = ranked[0][1]
    for _, symbol in ranked:
        context.order_target_percent(symbol, 0.95 if symbol == winner else 0.0)
    context.log(f"候选 {winner}，相关性/NAV 数据缺失时跳过对应过滤")
''',
    },
    "five_fortunes": {
        "name": "五福策略（TickFlow 完整适配）",
        "config": {
            "symbols": [*WUFU_MINUTE_POOL, DEFENSIVE_ETF],
            "timeframe": "1m",
            "asset_type": "etf",
            "benchmark_symbol": "510300.SH",
            "settlement": "t1",
            "fill_policy": "next_open",
        },
        "source": '''from app.free_strategy.five_fortunes import (
    after_trading_end,
    initialize,
    on_bar,
    on_session_start,
)
''',
    },
}
