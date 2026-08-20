"""Native TickFlow port of the archived four-mode strategy.

The source keeps the original mode boundaries and schedule, while all market
inputs come from the native snapshot, historical bars and current bar/quote
contract.  It deliberately does not import an external hosted runtime or
fabricate depth data.
"""
from __future__ import annotations

import math
from datetime import time


STRATEGY_KIND = "four_mode"
MODES = ("yje", "rzq", "qs", "sb")
MODE_LABELS = {"yje": "一进二", "rzq": "弱转强", "qs": "趋势股", "sb": "首板"}
MAX_POSITIONS = 10
YJE_MAX_POSITIONS = 4
QS_MAX_COUNT = 3
QS_MAX_RATIO = 0.35
QS_MAX_HOLD_DAYS = 10
QS_ATR_PERIOD = 14
QS_ATR_MULTIPLIER = 1.5
SB_MAX_POSITIONS = 4
SB_MAX_RATIO = 0.30


def _state(context):
    return context.state.setdefault("four_mode", {
        "snapshot": {},
        "candidates": {mode: [] for mode in MODES},
        "candidate_meta": {},
        "priority": ["yje", "sb", "rzq"],
        "position_modes": {},
        "position_meta": {},
        "intraday": {"date": None, "bars": {}, "vwap": {}, "volume": {}},
        "pending_sell": {},
        "pending_buy": False,
        "opening_conditions": {},
        "selection_ready": False,
        "risk_ratio": 1.0,
        "tail_check": False,
        "audit": [],
        "events": [],
    })


def initialize(context):
    instruments = [item for item in context.instruments("stock") if item.get("symbol")]
    if not instruments:
        raise ValueError("四合一策略没有可用股票标的")
    # The dynamic loader owns the full stock catalog; this seed only satisfies
    # the native engine's non-empty-universe contract before the first session.
    context.set_universe([str(instruments[0]["symbol"])])
    context.require_four_mode_snapshot(
        lookback_days=80,
        trend_history_days=65,
        index_symbol="000852.SH",
        require_auction=True,
    )
    context.require_history("1d", 120)
    context.require_market_history("index", "1d", 120)
    _state(context)
    for callback, at in (
        (_preselect, "09:05"),
        (_market_risk, "09:24"),
        # The native scheduler is minute-resolution; the 09:25 callback
        # consumes the persisted final 09:25 auction snapshot.
        (_confirm_auction, "09:25"),
        (_open_sell, "09:27"),
        (_buy, "09:28"),
        (_sell_30m, "10:00"),
        (_midday_update, "11:30"),
        (_afternoon_sell, "14:30"),
        (_trend_status, "15:01"),
        (_daily_audit, "15:02"),
        (_daily_reset, "15:05"),
    ):
        context.schedule(callback, at)


def _log(context, message, level="INFO"):
    context.log(message if str(message).startswith("【") else f"四模式 | {message}", level)


def _held(context):
    return {str(symbol): float(quantity) for symbol, quantity in context.portfolio.positions.items() if float(quantity) > 0}


def _bar_price(bar):
    return float(bar.raw_close if bar.raw_close is not None else bar.close)


def _bar_open(bar):
    return float(bar.raw_open if bar.raw_open is not None else bar.open)


def _prev_close(bar, context, symbol):
    if bar.previous_close is not None and float(bar.previous_close) > 0:
        return float(bar.previous_close)
    history = context.history_bars(symbol, 1, "1d")
    return _bar_price(history[-1]) if history else None


def _append_intraday(context, bars):
    state = _state(context)
    day = context.now.date().isoformat()
    intraday = state["intraday"]
    if intraday.get("date") != day:
        intraday.clear()
        intraday.update({"date": day, "bars": {}, "vwap": {}, "volume": {}})
    for symbol, bar in bars.items():
        if symbol not in context.universe:
            continue
        rows = intraday["bars"].setdefault(symbol, [])
        rows.append({
            "timestamp": bar.timestamp.isoformat(), "open": _bar_open(bar),
            "high": float(bar.raw_high if bar.raw_high is not None else bar.high),
            "low": float(bar.raw_low if bar.raw_low is not None else bar.low),
            "close": _bar_price(bar), "volume": float(bar.volume or 0),
            "amount": float(bar.amount or 0), "limit_up": bar.limit_up,
        })
        if len(rows) > 300:
            del rows[:-300]
        volume = sum(float(row["volume"]) for row in rows)
        amount = sum(float(row["amount"]) for row in rows)
        intraday["volume"][symbol] = volume
        intraday["vwap"][symbol] = amount / volume if volume > 0 else _bar_price(bar)


def _is_limit(bar):
    return bar.limit_up is not None and _bar_price(bar) >= float(bar.limit_up) - 0.005


def _is_one_word(bar):
    return _is_limit(bar) and abs(_bar_open(bar) - _bar_price(bar)) <= 0.005 and abs(float(bar.high) - float(bar.low)) <= 0.005


def _record_event(context, kind, symbol=None, **payload):
    event = {"date": context.now.isoformat(), "type": kind, **payload}
    if symbol:
        event["symbol"] = str(symbol)
    _state(context)["events"].append(event)
    if len(_state(context)["events"]) > 2000:
        del _state(context)["events"][:-2000]


def _preselect(context, *_args):
    state = _state(context)
    snapshot = context.four_mode_snapshot(context.now.date())
    state["snapshot"] = snapshot
    state["selection_ready"] = False
    static_modes = snapshot.get("static_modes") or snapshot.get("modes") or {}
    state["candidates"] = {
        mode: list(static_modes.get(mode, {}).get("candidates") or [])
        for mode in MODES
    }
    state["candidate_meta"] = {
        str(row.get("symbol")): {**row, "mode": mode}
        for mode, rows in state["candidates"].items()
        for row in rows
        if row.get("symbol")
    }
    candidate_symbols = [
        str(row["symbol"])
        for rows in state["candidates"].values()
        for row in rows
        if row.get("symbol")
    ]
    held = list(_held(context))
    if candidate_symbols or held:
        context.set_universe(list(dict.fromkeys([*held, *candidate_symbols])))
    _record_event(context, "preselection", count=sum(len(rows) for rows in state["candidates"].values()), state=snapshot.get("static_state", snapshot.get("state")))
    _log(context, "【盘前预选】四模式静态计算完成 | "
         f"一进二静态={len(state['candidates']['yje'])}只 | "
         f"弱转强静态={len(state['candidates']['rzq'])}只 | "
         f"趋势股静态={len(state['candidates']['qs'])}只 | "
         f"首板静态={len(state['candidates']['sb'])}只")


def _benchmark_history(context):
    symbol = "000001.SH"
    values = context.history_bars(symbol, 120, "1d")
    return values


def _market_risk(context, *_args):
    state = _state(context)
    rows = _benchmark_history(context)
    if len(rows) < 20:
        state["risk_ratio"] = 1.0
        _log(context, "【微观结构风控错误】指数历史不足，回退到默认仓位", "WARNING")
        return
    closes = [_bar_price(row) for row in rows]
    volumes = [float(row.volume or 0) for row in rows]
    if len(closes) < 60:
        trend = 20
    else:
        ma3, ma10, ma20, ma60 = (sum(closes[-n:]) / n for n in (3, 10, 20, 60))
        bull = 30 if ma3 > ma10 > ma20 > ma60 else 20 if ma3 > ma10 > ma20 else 10 if ma10 > ma20 else 0
        ma20_prev = sum(closes[-25:-5]) / 20
        slope = (ma20 - ma20_prev) / ma20_prev * 100 if ma20_prev else 0
        slope_score = max(0, min(20, (slope + 2) * 5))
        bias = (closes[-1] - ma20) / ma20 * 100 if ma20 else 0
        bias_score = max(0, 15 - (bias - 5) * 3) if bias > 5 else min(20, abs(bias) * 2) if bias < -5 else 15
        trend = bull + slope_score + bias_score

    penalty = 0
    if len(rows) >= 20:
        price_new_high = closes[-1] >= max(closes[-10:]) * .998
        volume_decline = sum(volumes[-5:]) / 5 < sum(volumes[-10:-5]) / 5 * .85
        if price_new_high and volume_decline:
            penalty += 20
        deltas = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
        gains = [max(value, 0.0) for value in deltas[-14:]]
        losses = [max(-value, 0.0) for value in deltas[-14:]]
        rsi_now = 100 if not sum(losses) else 100 - 100 / (1 + sum(gains) / (sum(losses) + 1e-9))
        prior_deltas = deltas[-18:-4]
        prior_gains = sum(max(value, 0.0) for value in prior_deltas[-14:])
        prior_losses = sum(max(-value, 0.0) for value in prior_deltas[-14:])
        rsi_prev = 100 if not prior_losses else 100 - 100 / (1 + prior_gains / (prior_losses + 1e-9))
        if len(closes) >= 5 and closes[-1] > closes[-5] and rsi_now < rsi_prev * .98:
            penalty += 15
        def ema(values, period):
            value = values[0]
            alpha = 2 / (period + 1)
            for item in values[1:]:
                value = alpha * item + (1 - alpha) * value
            return value
        macd_hist = [ema(closes[:index], 12) - ema(closes[:index], 26) for index in range(26, len(closes) + 1)]
        if len(macd_hist) >= 4 and macd_hist[-1] > 0 and macd_hist[-1] < macd_hist[-2] < macd_hist[-3]:
            penalty += 10
    momentum = max(0, 30 - penalty)

    tr = [max(float(row.high) - float(row.low), abs(float(row.high) - _bar_price(rows[index - 1])), abs(float(row.low) - _bar_price(rows[index - 1]))) for index, row in enumerate(rows) if index]
    if len(rows) < 60:
        vol_score, vol_coeff = 10, 1.0
    else:
        atr14 = sum(tr[-14:]) / 14
        atr60 = sum(tr[-60:]) / 60
        ratio = atr14 / atr60 if atr60 else 1.0
        vol_score, vol_coeff = (20, 1.0) if ratio < .8 else (5, .5) if ratio > 1.5 else (10, .75) if ratio > 1.2 else (15, 1.0)
    ma5_vol = sum(volumes[-5:]) / 5
    limit_up_count = len(_state(context)["candidates"].get("yje", []))
    micro = 2 if volumes[-1] > ma5_vol * 1.5 and limit_up_count < 30 else 5 if volumes[-1] > ma5_vol * 1.5 else 10 if volumes[-1] > ma5_vol * 1.2 else 3 if volumes[-1] < ma5_vol * .7 else 8
    down_days = sum(
        closes[index] < closes[index - 1] and volumes[index] > volumes[index - 1] * 1.1
        for index in range(max(1, len(closes) - 3), len(closes))
    )
    ratio = min((trend + momentum + vol_score + micro) / 100 * vol_coeff, .3) if down_days >= 2 else (trend + momentum + vol_score + micro) / 100 * vol_coeff
    state["risk_ratio"] = max(0.0, min(1.0, ratio))
    _log(context, f"【微观结构仓位诊断】趋势{trend:.0f}/40 动量{momentum}/30 波动{vol_score}/20 微观{micro}/10 | 综合{trend + momentum + vol_score + micro:.0f} | 系数{state['risk_ratio']:.2%}")
    _log(context, f"【微观结构风控】仓位系数: {state['risk_ratio']:.2%} | 最大持仓: {max(1, int(MAX_POSITIONS * state['risk_ratio']))}只")


def _confirm_auction(context, *_args):
    state = _state(context)
    snapshot = context.four_mode_snapshot(context.now.date())
    state["snapshot"] = snapshot
    state["selection_ready"] = snapshot.get("state") == "ready"
    if not state["selection_ready"]:
        _log(context, f"【竞价确认错误】数据未完成：{'; '.join(snapshot.get('data_gaps') or [])}", "WARNING")
        return
    state["candidates"] = {
        mode: list(snapshot.get("modes", {}).get(mode, {}).get("candidates") or [])
        for mode in MODES
    }
    held = list(_held(context))
    session_symbols = list(dict.fromkeys([*held, *[str(row["symbol"]) for mode in MODES for row in state["candidates"][mode]]]))
    if session_symbols:
        context.set_universe(session_symbols)
    benchmark = context.current_bars().get("000001.SH")
    benchmark_change = (
        (_bar_price(benchmark) / float(benchmark.previous_close) - 1) * 100
        if benchmark is not None and benchmark.previous_close else 0.0
    )
    _update_priority(
        context,
        benchmark_change,
        int(snapshot.get("limit_up_count") or len(state["candidates"].get("yje", []))),
    )
    _record_event(context, "auction_confirmed", count=sum(len(rows) for rows in state["candidates"].values()))
    _log(context, f"【竞价确认】四模式选股完成 | "
         f"一进二={len(state['candidates']['yje'])}只 | "
         f"弱转强={len(state['candidates']['rzq'])}只 | "
         f"趋势股={len(state['candidates']['qs'])}只 | "
         f"首板={len(state['candidates']['sb'])}只")


def _request_sell(context, symbol, reason):
    state = _state(context)
    if symbol in state["pending_sell"]:
        return False
    available = float(context.portfolio.available_positions.get(symbol, 0) or 0)
    if available <= 0:
        return False
    context.sell(symbol, quantity=available, reason=reason)
    state["pending_sell"][symbol] = reason
    _record_event(context, "sell", symbol, reason=reason)
    mode = state["position_modes"].get(symbol, "unknown")
    _log(context, f"【卖出-{MODE_LABELS.get(mode, mode)}】{symbol} | 原因:{reason}")
    return True


def _open_sell(context, *_args):
    state = _state(context)
    state["open_sell_pending"] = True


def _apply_open_sell(context, bars):
    state = _state(context)
    state.setdefault("opening_conditions", {})
    if not state.pop("open_sell_pending", False):
        return
    for symbol in list(_held(context)):
        bar = bars.get(symbol)
        if bar is None or bar.previous_close is None:
            continue
        open_pct = (_bar_open(bar) / float(bar.previous_close) - 1) * 100
        mode = state["position_modes"].get(symbol)
        if mode == "qs":
            condition = {"type": "trend_stock_dynamic", "stop_loss": -5}
        elif mode == "sb":
            condition = {"type": "sb_intraday", "stop_loss": -3}
        elif open_pct > 5:
            condition = {"type": "high_open", "open_pct": open_pct, "stop_loss": 0, "time_exit": "14:30"}
        elif open_pct >= 0:
            condition = {"type": "flat_open", "open_pct": open_pct, "stop_loss": -2, "time_exit": "10:00"}
        elif open_pct >= -2:
            condition = {"type": "low_open", "open_pct": open_pct, "stop_loss": -3, "time_exit": "10:00"}
        else:
            condition = {"type": "extreme_low", "open_pct": open_pct, "immediate_sell": True}
        state["opening_conditions"][symbol] = condition
        if condition.get("immediate_sell") and not _is_limit(bar):
            _request_sell(context, symbol, "开盘大幅低开卖出")
        elif open_pct <= float(condition.get("stop_loss", -999)) and not _is_limit(bar):
            _request_sell(context, symbol, f"开盘低开止损{condition['stop_loss']:.0f}%")


def _budget(context, mode):
    equity = float(context.portfolio.total_value or 0)
    risk = float(_state(context).get("risk_ratio") or 1)
    if mode == "sb":
        return equity * min(SB_MAX_RATIO, risk) / SB_MAX_POSITIONS
    if mode == "yje":
        return equity * risk / YJE_MAX_POSITIONS
    return equity * risk / max(1, min(MAX_POSITIONS, int(MAX_POSITIONS * risk)))


def _submit_buy(context, symbol, mode, bar):
    state = _state(context)
    if symbol in _held(context) or symbol in state.get("pending_buy_symbols", {}):
        return False
    if not bar.tradable or bar.suspended or _is_limit(bar) or _is_one_word(bar):
        return False
    price = _bar_price(bar)
    quantity = math.floor(_budget(context, mode) / price / 100) * 100 if price > 0 else 0
    if quantity <= 0:
        return False
    context.buy(symbol, quantity=quantity, reason=f"四合一-{mode}")
    state.setdefault("pending_buy_symbols", {})[symbol] = mode
    state["position_modes"][symbol] = mode
    state["position_meta"][symbol] = {"entry_date": context.now.date().isoformat(), "entry_price": price, "highest": price, "hold_days": 1}
    _record_event(context, "buy", symbol, mode=mode, price=price, quantity=quantity)
    _log(context, f"【买入成功-{MODE_LABELS.get(mode, mode)}】{symbol} | 价格:{price:.2f} | 数量:{quantity}股 | 金额:{quantity * price:.2f}")
    return True


def _sb_signal(context, symbol, bar):
    rows = _state(context)["intraday"]["bars"].get(symbol, [])
    if len(rows) < 5:
        return False, 0
    vwap = float(_state(context)["intraday"]["vwap"].get(symbol) or 0)
    closes = [float(row["close"]) for row in rows]
    volumes = [float(row["volume"]) for row in rows]
    current = _bar_price(bar)
    day_open = float(rows[0]["open"])
    rise = (current / day_open - 1) * 100 if day_open else 0
    strength = (current - min(float(row["low"]) for row in rows)) / max(max(float(row["high"]) for row in rows) - min(float(row["low"]) for row in rows), .0001) * 100
    vol_ma = sum(volumes[-4:-1]) / max(len(volumes[-4:-1]), 1)
    attack = sum(closes[i] > closes[i - 1] and volumes[i] > (sum(volumes[max(0, i - 3):i]) / max(len(volumes[max(0, i - 3):i]), 1)) * 1.3 for i in range(max(1, len(closes) - 5), len(closes)))
    score = (35 if current > vwap and (vol_ma <= 0 or volumes[-1] / vol_ma > 1.5) else 0) + (25 if strength > 50 else 0) + (30 if attack >= 3 else 0)
    return ((2 < rise < 8 and score >= 60) or (8 < rise < 9.5 and current > vwap)), score


def _buy(context, *_args):
    state = _state(context)
    state["pending_buy"] = bool(state.get("selection_ready"))


def _buy_from_bar(context, bars):
    state = _state(context)
    if not state.pop("pending_buy", False) or not state.get("selection_ready"):
        return
    held = _held(context)
    slots = max(0, min(MAX_POSITIONS, int(MAX_POSITIONS * float(state.get("risk_ratio") or 1))) - len(held))
    if slots <= 0:
        return
    bought = set()

    def buy_mode(mode, limit, available_slots):
        current_mode = sum(value == mode for value in state["position_modes"].values())
        for row in state["candidates"].get(mode, []):
            if available_slots <= 0 or current_mode >= limit:
                break
            symbol = str(row.get("symbol"))
            bar = bars.get(symbol)
            if bar is None or symbol in bought:
                continue
            if mode == "sb":
                ok, _score = _sb_signal(context, symbol, bar)
                if not ok:
                    continue
            elif mode == "yje":
                prev = _prev_close(bar, context, symbol)
                if prev is None or _is_limit(bar):
                    continue
                rise = (_bar_price(bar) / prev - 1) * 100
                # Native TickFlow has no five-level depth; retain the
                # original 8% compatibility branch without inventing L2.
                if rise < 8.0:
                    continue
            if _submit_buy(context, symbol, mode, bar):
                bought.add(symbol)
                available_slots -= 1
                current_mode += 1
        return available_slots

    priority = state.get("priority", ["yje", "sb", "rzq"])
    yje_enabled = "yje" in priority and bool(state["candidates"].get("yje"))
    sb_enabled = "sb" in priority
    if yje_enabled:
        slots = buy_mode("yje", YJE_MAX_POSITIONS, slots)
        if sb_enabled:
            slots = buy_mode("sb", SB_MAX_POSITIONS, slots)
        return

    # Original fallback branch:首板独立30%仓位 plus weak-to-strong and
    # trend slots, with trend positions capped at half of the remaining slots.
    if sb_enabled:
        slots = buy_mode("sb", SB_MAX_POSITIONS, slots)
    qs_current = sum(value == "qs" for value in state["position_modes"].values())
    max_qs = min(max(1, int(MAX_POSITIONS * float(state.get("risk_ratio") or 1) * QS_MAX_RATIO)), QS_MAX_COUNT)
    qs_slots = min(slots // 2, max(0, max_qs - qs_current))
    rzq_slots = slots - qs_slots
    slots_after_rzq = buy_mode("rzq", rzq_slots, rzq_slots)
    used_rzq = rzq_slots - slots_after_rzq
    slots -= used_rzq
    buy_mode("qs", qs_slots, qs_slots)


def _atr(context, symbol):
    rows = context.history_bars(symbol, QS_ATR_PERIOD + 1, "1d")
    if len(rows) < QS_ATR_PERIOD + 1:
        return None
    tr = []
    for index, row in enumerate(rows[1:], 1):
        previous = _bar_price(rows[index - 1])
        high = float(row.raw_high if row.raw_high is not None else row.high)
        low = float(row.raw_low if row.raw_low is not None else row.low)
        tr.append(max(high - low, abs(high - previous), abs(low - previous)))
    return sum(tr[-QS_ATR_PERIOD:]) / QS_ATR_PERIOD


def _monitor_positions(context, bars):
    state = _state(context)
    for symbol, quantity in _held(context).items():
        bar = bars.get(symbol)
        if bar is None or symbol in state["pending_sell"]:
            continue
        price = _bar_price(bar)
        meta = state["position_meta"].setdefault(symbol, {"entry_price": float(context.portfolio.avg_cost.get(symbol) or price), "highest": price, "hold_days": 1})
        cost = float(context.portfolio.avg_cost.get(symbol) or meta.get("entry_price") or price)
        meta["highest"] = max(float(meta.get("highest") or price), price)
        mode = state["position_modes"].get(symbol, "unknown")
        if mode == "qs":
            atr = _atr(context, symbol)
            stop = float(meta["highest"]) - QS_ATR_MULTIPLIER * atr if atr else 0
            if stop and price <= stop:
                _request_sell(context, symbol, "趋势股ATR14跟踪止损")
            elif int(meta.get("hold_days") or 1) >= QS_MAX_HOLD_DAYS:
                _request_sell(context, symbol, "趋势股持有满10个交易日")
            elif bar.limit_down is not None and price <= float(bar.limit_down) * 1.005 and context.now.time() >= time(14, 30):
                _request_sell(context, symbol, "趋势股尾盘跌停")
            continue
        profit = (price / cost - 1) * 100 if cost else 0
        condition = state["opening_conditions"].get(symbol, {})
        stop_loss = condition.get("stop_loss")
        if condition.get("time_exit") == "10:00" and context.now.time() >= time(10, 0) and not _is_limit(bar):
            _request_sell(context, symbol, f"{mode}开盘条件10:00时间退出")
        elif stop_loss is not None and float(stop_loss) < 0 and profit <= float(stop_loss):
            _request_sell(context, symbol, f"{mode}盘中止损{float(stop_loss):.0f}%")
        elif profit <= -3:
            _request_sell(context, symbol, f"{mode}盘中止损-3%")
        elif context.now.time() >= time(14, 30) and not _is_limit(bar):
            _request_sell(context, symbol, f"{mode}尾盘未涨停卖出")


def on_bar(context, bars):
    _append_intraday(context, bars)
    _apply_open_sell(context, bars)
    _buy_from_bar(context, bars)
    _monitor_positions(context, bars)


def _sell_30m(context, *_args):
    _state(context)["30m_checked"] = True


def _midday_update(context, *_args):
    state = _state(context)
    benchmark = context.current_bars().get("000001.SH")
    if benchmark is not None and benchmark.previous_close:
        change = (_bar_price(benchmark) / float(benchmark.previous_close) - 1) * 100
        _update_priority(context, change, int(state.get("limit_up_count") or len(state["candidates"].get("yje", []))))


def _update_priority(context, index_change: float, limit_up_count: int) -> None:
    state = _state(context)
    previous_limit_up = int(state.get("previous_limit_up_count") or limit_up_count)
    if index_change > 2 and limit_up_count > 50:
        phase, priority = "main", ["yje", "sb", "rzq"]
    elif index_change > 0 and limit_up_count > previous_limit_up:
        phase, priority = "repair", ["yje", "sb", "rzq"]
    elif index_change < -1.5 and limit_up_count < 20:
        phase, priority = "freeze", ["rzq", "yje", "sb"]
    elif index_change > 0 and limit_up_count < 30:
        phase, priority = "divergence", ["rzq", "sb", "yje"]
    else:
        phase, priority = "decline", ["rzq"]
    state["limit_up_count"] = limit_up_count
    state["previous_limit_up_count"] = limit_up_count
    state["sentiment_phase"] = phase
    state["priority"] = priority
    _log(context, f"【策略优先级】情绪阶段[{phase}] | 指数涨跌:{index_change:.2f}% | 涨停:{limit_up_count} | 优先级:{'>'.join(priority)}")


def _afternoon_sell(context, *_args):
    _state(context)["tail_check"] = True


def _trend_status(context, *_args):
    holdings = [symbol for symbol, mode in _state(context)["position_modes"].items() if mode == "qs" and symbol in _held(context)]
    _log(context, f"趋势股收盘状态：{len(holdings)} 只")


def _daily_audit(context, *_args):
    state = _state(context)
    report = {"date": context.now.date().isoformat(), "equity": float(context.portfolio.total_value), "cash": float(context.portfolio.cash), "positions": len(_held(context)), "modes": {mode: sum(value == mode for value in state["position_modes"].values()) for mode in MODES}}
    state["audit"].append(report)
    state["audit"] = state["audit"][-30:]
    _log(context, f"日终审计：持仓 {report['positions']} 只，资产 {report['equity']:.2f}")


def _daily_reset(context, *_args):
    state = _state(context)
    held = set(_held(context))
    state["pending_sell"] = {symbol: reason for symbol, reason in state["pending_sell"].items() if symbol in held}
    state["position_modes"] = {symbol: mode for symbol, mode in state["position_modes"].items() if symbol in held}
    state["position_meta"] = {symbol: meta for symbol, meta in state["position_meta"].items() if symbol in held}
    state["selection_ready"] = False
    state["pending_buy"] = False
    state["tail_check"] = False
    state["intraday"] = {"date": None, "bars": {}, "vwap": {}, "volume": {}}


def after_trading_end(context):
    _daily_audit(context)
