"""开箱即用的自由策略模板。"""
from __future__ import annotations

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
        "name": "五福策略（TickFlow 适配示例）",
        "source": '''# 五福策略的自由运行时示例：保留状态识别、动量、相关性、冷却、调仓和风控。
# TickFlow 当前没有 ETF NAV，因此溢价过滤显式跳过并写入日志。
ETF_POOL = []  # 在配置的标的池中填入 ETF 代码

def initialize(context):
    context.state.setdefault("prices", {})
    context.state.setdefault("cooldown", 0)
    context.state.setdefault("regime", "neutral")
    context.state.setdefault("last_symbol", None)
    context.state.setdefault("peak_equity", context.portfolio.total_value)
    context.log("五福初始化：NAV/溢价过滤 skipped_no_data")

def _regime(context):
    # 没有指数池时采用候选 ETF 的 breadth 作为保守状态识别。
    histories = context.state["prices"]
    positive = 0
    total = 0
    for values in histories.values():
        if len(values) >= 21:
            total += 1
            positive += values[-1] > sum(values[-20:]) / 20
    return "risk_on" if total and positive / total >= 0.5 else "risk_off"

def _correlation_ok(values, selected):
    # 简单的收益相关性近似：最近收益方向一致过高时不重复持仓。
    if len(values) < 3 or len(selected) < 3:
        return True
    return not (values[-1] > values[-2] and selected[-1] > selected[-2])

def _rebalance(context, selected, scale):
    for symbol in context.state["prices"]:
        context.order_target_percent(symbol, scale if symbol == selected else 0.0)

def on_bar(context, bars):
    candidates = []
    for symbol, bar in bars.items():
        history = context.state["prices"].setdefault(symbol, [])
        history.append(bar.close)
        if len(history) < 22:
            continue
        momentum = history[-1] / history[-21] - 1
        ma = sum(history[-20:]) / 20
        if bar.close > ma and momentum > 0 and _correlation_ok(history, context.state["prices"].get(context.state["last_symbol"], [])):
            candidates.append((momentum, symbol))
    context.state["regime"] = _regime(context)
    if not candidates or context.state["cooldown"] > 0 or context.state["regime"] == "risk_off":
        context.state["cooldown"] = max(0, context.state["cooldown"] - 1)
        return
    candidates.sort(reverse=True)
    selected = candidates[0][1]
    equity = context.portfolio.total_value
    context.state["peak_equity"] = max(context.state["peak_equity"], equity)
    drawdown = 1 - equity / context.state["peak_equity"] if context.state["peak_equity"] else 0
    _rebalance(context, selected, 0.9 if drawdown < 0.1 else 0.5)
    context.state["last_symbol"] = selected
    context.state["cooldown"] = 2
    context.log(f"五福调仓: {selected}; regime={context.state['regime']}; NAV/溢价过滤因数据源缺失跳过")
''',
    },
}
