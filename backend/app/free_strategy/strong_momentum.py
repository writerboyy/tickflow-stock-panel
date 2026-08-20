"""“强者恒强”分钟策略的项目原生适配。

候选股只使用 D-1 及以前的日线和历史名称生成；D 日仅在早盘四个
分钟时点判断买入。成交、T+1、涨跌停、费用和滑点继续由公共引擎处理。
"""
from __future__ import annotations

from datetime import time


STRATEGY_KIND = "strong_momentum"
ENTRY_TIMES = {time(9, 31), time(9, 32), time(9, 37), time(10, 29)}
STOP_LOSS = -0.04
TAKE_PROFIT = 0.19
BREAK_LIMIT_DRAWDOWN = 0.015
MAX_HOLDING_SESSIONS = 3
MAX_EXPOSURE = 0.90


def _state(context):
    return context.state.setdefault("strong_momentum", {
        "snapshot": {},
        "candidate_meta": {},
        "session_index": 0,
        "session_open": {},
        "session_high": {},
        "previous_close": {},
        "hit_limit": [],
        "opening_exit_checked": [],
        "sold_today": {},
        "pending_entries": {},
        "pending_exits": {},
        "position_meta": {},
        "entry_events": [],
        "exit_events": [],
    })


def initialize(context):
    instruments = [
        item for item in context.instruments("stock")
        if item.get("symbol") and bool(item.get("has_minute", True))
    ]
    if not instruments:
        raise ValueError("强者恒强策略没有可用的股票分钟标的")
    context.set_universe([str(instruments[0]["symbol"])])
    context.require_strong_momentum_snapshot(lookback_days=30, require_auction=True)
    _state(context)


def before_trading_start(context):
    state = _state(context)
    snapshot = context.strong_momentum_snapshot(context.now.date())
    candidates = list(snapshot.get("candidates") or [])
    held = [
        symbol for symbol, quantity in context.portfolio.positions.items()
        if float(quantity) > 0
    ]
    state["snapshot"] = snapshot
    state["candidate_meta"] = {str(row["symbol"]): dict(row) for row in candidates}
    state["session_index"] = int(state.get("session_index") or 0) + 1
    state["session_open"] = {}
    state["session_high"] = {}
    state["hit_limit"] = []
    state["opening_exit_checked"] = []
    state["sold_today"] = {}
    state["pending_entries"] = {}
    state["pending_exits"] = {}
    symbols = list(dict.fromkeys([*state["candidate_meta"], *held]))
    if symbols:
        context.set_universe(symbols)
    context.log(
        f"强者恒强 {snapshot.get('as_of') or '无'} 盘前候选 {len(candidates)} 只"
    )


def _raw(bar, field):
    value = getattr(bar, f"raw_{field}", None)
    return float(value if value is not None else getattr(bar, field))


def _max_positions(equity):
    if equity < 100_000:
        return 2
    if equity < 300_000:
        return 3
    if equity < 1_000_000:
        return 4
    if equity < 5_000_000:
        return 5
    return 8


def _emit(context, signal_type, symbol, meta, reason, **extra):
    payload = {
        "symbol": symbol,
        "name": str(meta.get("name") or symbol),
        "reason": reason,
        "as_of": _state(context).get("snapshot", {}).get("as_of"),
        "previous_change": meta.get("previous_change"),
        "previous_turnover_rate": meta.get("previous_turnover_rate"),
        "previous_volume_growth": meta.get("previous_volume_growth"),
        **extra,
    }
    context.emit_signal(
        signal_type,
        payload,
        event_id=f"strong-momentum:{signal_type}:{symbol}:{context.now.isoformat()}",
    )


def _sync_positions(context, state):
    positions = context.portfolio.positions
    for symbol, quantity in positions.items():
        if float(quantity) <= 0 or symbol in state["position_meta"]:
            continue
        pending = state["pending_entries"].pop(symbol, {})
        state["position_meta"][symbol] = {
            "entry_session": int(state["session_index"]),
            "entry_time": pending.get("timestamp") or context.now.isoformat(),
            **dict(pending.get("meta") or {}),
        }
    for symbol in list(state["position_meta"]):
        if float(positions.get(symbol, 0)) <= 0 and symbol not in state["pending_entries"]:
            state["position_meta"].pop(symbol, None)
            state["pending_exits"].pop(symbol, None)


def _update_session_prices(state, bars):
    hit_limit = set(state["hit_limit"])
    for symbol, bar in bars.items():
        raw_open = _raw(bar, "open")
        raw_high = _raw(bar, "high")
        state["session_open"].setdefault(symbol, raw_open)
        state["session_high"][symbol] = max(
            float(state["session_high"].get(symbol) or 0), raw_high,
        )
        if bar.limit_up is not None and raw_high >= float(bar.limit_up) * 0.995:
            hit_limit.add(symbol)
    state["hit_limit"] = sorted(hit_limit)


def _submit_exit(context, state, symbol, quantity, meta, reason, current):
    if symbol in state["pending_exits"] or quantity <= 0:
        return
    context.sell(symbol, quantity=quantity, reason=reason)
    state["pending_exits"][symbol] = context.now.isoformat()
    state["sold_today"][symbol] = current
    event = {
        "timestamp": context.now.isoformat(),
        "symbol": symbol,
        "reason": reason,
        "price": current,
    }
    state["exit_events"].append(event)
    _emit(context, "strong_momentum_exit", symbol, meta, reason, price=current)


def _exit_positions(context, state, bars):
    hit_limit = set(state["hit_limit"])
    opening_checked = set(state["opening_exit_checked"])
    for symbol, quantity in list(context.portfolio.positions.items()):
        available = float(context.portfolio.available_positions.get(symbol, 0))
        bar = bars.get(symbol)
        if float(quantity) <= 0 or available <= 0 or bar is None:
            continue
        meta = state["position_meta"].get(symbol) or state["candidate_meta"].get(symbol) or {"symbol": symbol}
        current = _raw(bar, "close")
        open_price = float(state["session_open"].get(symbol) or _raw(bar, "open"))
        previous = float(
            state["previous_close"].get(symbol)
            or meta.get("previous_raw_close")
            or 0
        )

        if context.now.time() <= time(9, 31) and symbol not in opening_checked:
            opening_checked.add(symbol)
            open_gain = open_price / previous - 1 if previous > 0 else 0
            at_limit = bar.limit_up is not None and open_price >= float(bar.limit_up) * 0.995
            if not at_limit and open_gain < 0.05:
                _submit_exit(context, state, symbol, available, meta, "开盘未涨停且高开不足5%", current)
                continue

        avg_cost = float(context.portfolio.avg_cost.get(symbol) or 0)
        profit = current / avg_cost - 1 if avg_cost > 0 else 0
        holding_sessions = int(state["session_index"]) - int(meta.get("entry_session") or state["session_index"])
        session_high = float(state["session_high"].get(symbol) or current)
        limit_drawdown = session_high > 0 and current / session_high - 1 <= -BREAK_LIMIT_DRAWDOWN
        if profit <= STOP_LOSS:
            _submit_exit(context, state, symbol, available, meta, "成本止损-4%", current)
        elif profit >= TAKE_PROFIT and symbol not in hit_limit:
            _submit_exit(context, state, symbol, available, meta, "止盈19%", current)
        elif context.now.time() >= time(10, 20) and symbol in hit_limit and limit_drawdown:
            _submit_exit(context, state, symbol, available, meta, "涨停后回落1.5%", current)
        elif holding_sessions >= MAX_HOLDING_SESSIONS and symbol not in hit_limit:
            _submit_exit(context, state, symbol, available, meta, "持有满3个交易日", current)
    state["opening_exit_checked"] = sorted(opening_checked)


def _passes_intraday_gate(state, symbol, meta, bar):
    if bool(meta.get("auction_required")):
        auction_change = meta.get("auction_change_pct_0925")
        try:
            auction_change = float(auction_change)
        except (TypeError, ValueError):
            return None
        if not 0 <= auction_change <= 8:
            return None
    previous = float(meta.get("previous_raw_close") or 0)
    open_price = float(state["session_open"].get(symbol) or _raw(bar, "open"))
    current = _raw(bar, "close")
    if previous <= 0 or open_price <= 0 or current <= 0:
        return None
    open_gain = open_price / previous - 1
    if not 0 <= open_gain <= 0.08:
        return None
    if bar.limit_up is not None and open_price >= float(bar.limit_up) * 0.999:
        return None
    if bar.limit_down is not None and open_price <= float(bar.limit_down) * 1.001:
        return None
    if bool(meta.get("previous_high_volume_limit")) and open_gain >= 0.05:
        return None
    open_drop = current / open_price - 1
    if open_gain > 0 and open_drop < -0.003:
        return None
    current_gain = current / previous - 1
    if current_gain > 0.10:
        return None
    return current / open_price - 1, current, open_gain, current_gain


def _entry_candidates(context, state):
    if context.now.time() not in ENTRY_TIMES:
        return
    positions = context.portfolio.positions
    held = {symbol for symbol, quantity in positions.items() if float(quantity) > 0}
    max_positions = _max_positions(float(context.portfolio.total_value))
    slots = max_positions - len(held) - len(state["pending_entries"])
    if slots <= 0:
        return
    current_bars = context.current_bars()
    ranked = []
    for symbol, meta in state["candidate_meta"].items():
        if symbol in held or symbol in state["pending_entries"]:
            continue
        bar = current_bars.get(symbol)
        if bar is None or bar.timestamp != context.now or not bar.tradable or float(bar.volume or 0) <= 0:
            continue
        gate = _passes_intraday_gate(state, symbol, meta, bar)
        if gate is None:
            continue
        lift, current, open_gain, current_gain = gate
        sold_price = float(state["sold_today"].get(symbol) or 0)
        if sold_price > 0 and current >= sold_price * 0.997:
            continue
        ranked.append((lift, current_gain, symbol, meta, current, open_gain))
    ranked.sort(reverse=True, key=lambda item: (item[0], item[1], item[2]))
    target_percent = MAX_EXPOSURE / max_positions
    for lift, current_gain, symbol, meta, current, open_gain in ranked[:slots]:
        context.order_target_percent(symbol, target_percent)
        state["pending_entries"][symbol] = {
            "timestamp": context.now.isoformat(),
            "meta": meta,
        }
        event = {
            "timestamp": context.now.isoformat(),
            "symbol": symbol,
            "price": current,
            "open_gain": open_gain,
            "current_gain": current_gain,
            "intraday_lift": lift,
        }
        state["entry_events"].append(event)
        _emit(
            context,
            "strong_momentum_entry",
            symbol,
            meta,
            "D-1强势股通过竞价与早盘强度确认",
            price=current,
            open_gain=open_gain,
            current_gain=current_gain,
            intraday_lift=lift,
        )


def on_bar(context, bars):
    state = _state(context)
    _sync_positions(context, state)
    _update_session_prices(state, bars)
    _exit_positions(context, state, bars)
    _entry_candidates(context, state)


def after_trading_end(context):
    state = _state(context)
    _sync_positions(context, state)
    state["previous_close"] = {
        symbol: _raw(bar, "close")
        for symbol, bar in context.current_bars().items()
    }
    held = sum(1 for quantity in context.portfolio.positions.values() if float(quantity) > 0)
    context.log(
        f"强者恒强：候选 {len(state['candidate_meta'])} 只，当前持仓 {held} 只"
    )
