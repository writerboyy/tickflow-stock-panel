"""项目原生的“强者恒强”日线动量策略。

全市场筛选复用 PIT 主线快照，成交、T+1、涨跌停、费用和滑点全部交给
自由策略引擎处理。策略在 D-1 生成候选，D 日收盘确认强度，D+1 开盘成交。
"""
from __future__ import annotations

from datetime import date


STRATEGY_KIND = "strong_momentum"
MAX_POSITIONS = 3
TARGET_POSITION_PCT = 0.30
MIN_STOCK_SCORE = 60.0
MAX_HOLDING_DAYS = 5


def _state(context):
    return context.state.setdefault("strong_momentum", {
        "snapshot": {},
        "candidate_meta": {},
        "selected": [],
        "held_since": {},
        "peak": {},
        "pending_entries": {},
    })


def initialize(context):
    instruments = [
        item for item in context.instruments("stock")
        if item.get("symbol") and not str(item["symbol"]).startswith(("4", "8"))
    ]
    if not instruments:
        raise ValueError("强者恒强策略没有可用的股票标的")
    context.set_universe([str(instruments[0]["symbol"])])
    context.require_mainline_snapshot(
        lookback_days=60,
        industry_standard="申银万国行业分类标准",
        industry_levels=(1, 2),
        min_coverage=0.95,
    )
    _state(context)


def before_trading_start(context):
    state = _state(context)
    snapshot = context.mainline_snapshot(context.now.date())
    candidates = [
        row for row in snapshot.get("candidates") or []
        if float(row.get("stock_score") or 0) >= MIN_STOCK_SCORE
    ][:30]
    held = [
        symbol for symbol, quantity in context.portfolio.positions.items()
        if float(quantity) > 0
    ]
    state["snapshot"] = snapshot
    state["candidate_meta"] = {str(row["symbol"]): dict(row) for row in candidates}
    state["selected"] = []
    symbols = list(dict.fromkeys([*state["candidate_meta"], *held]))
    if symbols:
        context.set_universe(symbols)


def _days_held(context, state, symbol):
    raw = state["held_since"].get(symbol)
    if not raw:
        return 0
    return (context.now.date() - date.fromisoformat(str(raw))).days


def _emit(context, signal_type, symbol, meta, reason):
    context.emit_signal(signal_type, {
        "symbol": symbol,
        "name": str(meta.get("name") or symbol),
        "stock_score": round(float(meta.get("stock_score") or 0), 2),
        "industry_score": round(float(meta.get("l1_score") or 0), 2),
        "subindustry_score": round(float(meta.get("l2_score") or 0), 2),
        "industry": str(meta.get("l1_name") or ""),
        "subindustry": str(meta.get("l2_name") or ""),
        "reason": reason,
    }, event_id=f"strong-momentum:{signal_type}:{symbol}:{context.now.date().isoformat()}")


def _selected_rows(state, bars):
    rows = []
    used_industries = set()
    ordered = sorted(
        state["candidate_meta"].values(),
        key=lambda row: (
            float(row.get("stock_score") or 0),
            float(row.get("l2_score") or 0),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )
    for meta in ordered:
        symbol = str(meta["symbol"])
        bar = bars.get(symbol)
        if bar is None or not bar.tradable or bar.suspended or float(bar.volume or 0) <= 0:
            continue
        previous = float(meta.get("previous_raw_close") or 0)
        current = float(bar.raw_close if bar.raw_close is not None else bar.close)
        if previous <= 0 or current <= 0:
            continue
        daily_return = current / previous - 1
        if daily_return < -0.03:
            continue
        if bar.limit_up is not None and current >= float(bar.limit_up) * 0.999:
            continue
        industry = str(meta.get("l1_key") or meta.get("l1_name") or "")
        if industry and industry in used_industries:
            continue
        rows.append((symbol, meta, daily_return))
        if industry:
            used_industries.add(industry)
        if len(rows) >= MAX_POSITIONS:
            break
    return rows


def on_bar(context, bars):
    state = _state(context)
    selected_rows = _selected_rows(state, bars)
    selected = [symbol for symbol, _meta, _return in selected_rows]
    state["selected"] = selected
    positions = context.portfolio.positions
    available = context.portfolio.available_positions

    for symbol, quantity in list(positions.items()):
        if float(quantity) <= 0:
            state["held_since"].pop(symbol, None)
            state["peak"].pop(symbol, None)
            continue
        state["held_since"].setdefault(symbol, context.now.date().isoformat())
        bar = bars.get(symbol)
        current = float(bar.close) if bar is not None else float(context.portfolio.avg_cost.get(symbol) or 0)
        state["peak"][symbol] = max(float(state["peak"].get(symbol) or 0), current)
        if not float(available.get(symbol, 0)) or bar is None:
            continue
        avg_cost = float(context.portfolio.avg_cost.get(symbol) or 0)
        peak = float(state["peak"].get(symbol) or avg_cost)
        drawdown = current / peak - 1 if peak > 0 else 0
        cost_return = current / avg_cost - 1 if avg_cost > 0 else 0
        dropped = symbol not in state["candidate_meta"]
        if dropped or drawdown <= -0.08 or cost_return <= -0.06 or _days_held(context, state, symbol) >= MAX_HOLDING_DAYS:
            context.order_target_percent(symbol, 0.0)
            _emit(
                context,
                "strong_momentum_exit",
                symbol,
                state["candidate_meta"].get(symbol, {"symbol": symbol}),
                "掉出强势候选、回撤、止损或持有期达到退出条件",
            )

    held_count = sum(1 for quantity in positions.values() if float(quantity) > 0)
    pending = state["pending_entries"]
    for symbol in list(pending):
        if float(positions.get(symbol, 0)) > 0 or symbol not in state["candidate_meta"]:
            pending.pop(symbol, None)
    for symbol, meta, daily_return in selected_rows:
        if held_count >= MAX_POSITIONS or float(positions.get(symbol, 0)) > 0 or symbol in pending:
            continue
        context.order_target_percent(symbol, TARGET_POSITION_PCT)
        pending[symbol] = context.now.date().isoformat()
        _emit(
            context,
            "strong_momentum_entry",
            symbol,
            meta,
            f"D-1主线强度领先，D日收盘继续确认（当日 {daily_return:.2%}）",
        )
        held_count += 1


def after_trading_end(context):
    state = _state(context)
    held = sum(1 for quantity in context.portfolio.positions.values() if float(quantity) > 0)
    context.log(
        f"强者恒强：主线候选 {len(state['candidate_meta'])} 只，"
        f"入选 {len(state['selected'])} 只，持仓 {held} 只"
    )
