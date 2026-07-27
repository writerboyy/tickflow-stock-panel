"""开箱即用的自由策略模板。"""
from __future__ import annotations

from pathlib import Path


LEGACY_FIVE_FORTUNES_SOURCE = '''from app.free_strategy.five_fortunes import (
    DEFENSIVE_ETF,
    WUFU_MINUTE_POOL,
    after_trading_end,
    before_trading_start,
    initialize as initialize_five_fortunes,
    on_bar,
)

ETF_POOL = [*WUFU_MINUTE_POOL, DEFENSIVE_ETF]

def initialize(context):
    context.set_universe(ETF_POOL)
    initialize_five_fortunes(context)
'''

FIVE_FORTUNES_SOURCE = Path(__file__).with_name("five_fortunes.py").read_text(encoding="utf-8")


TEMPLATES = {
    "dual_ma": {
        "name": "双均线模板",
        "source": '''ETF_POOL = ["510300.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
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
        "source": '''ETF_POOL = ["510300.SH", "510500.SH", "159915.SZ", "518880.SH", "511880.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
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
        "name": "五福策略（TickFlow 默认规则适配）",
        "config": {
            "timeframe": "1m",
            "asset_type": "etf",
            "benchmark_symbol": "510300.SH",
            "settlement": "t1",
            "fill_policy": "next_open",
        },
        "source": FIVE_FORTUNES_SOURCE,
    },
}
