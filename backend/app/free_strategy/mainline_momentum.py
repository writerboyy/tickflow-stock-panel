"""Intraday mainline momentum strategy shared by the four research variants."""

from datetime import datetime, time, timedelta
from statistics import median


STRATEGY_KIND = "mainline_momentum"
try:
    ENTRY_MODEL
except NameError:
    ENTRY_MODEL = "breakout"

MAX_POSITIONS = 3
TARGET_POSITION_PCT = 0.30
ENTRY_WINDOWS = ((time(9, 50), time(11, 20)), (time(13, 5), time(14, 30)))


def _state(context):
    return context.state.setdefault("mainline_momentum", {
        "entry_model": ENTRY_MODEL,
        "snapshot": {},
        "candidate_meta": {},
        "minute_bucket": {},
        "five_bars": {},
        "session": {},
        "pending_entries": {},
        "pending_exits": {},
        "model_hit_times": {},
        "position_meta": {},
        "entry_events": [],
        "exit_events": [],
        "daily_reports": [],
    })


def initialize(context):
    instruments = [
        item for item in context.instruments("stock")
        if item.get("symbol") and bool(item.get("has_minute", True))
    ]
    if not instruments:
        raise ValueError("主线动量策略没有可用的股票分钟标的")
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
    candidates = list(snapshot.get("candidates") or [])[:30]
    state["snapshot"] = snapshot
    state["candidate_meta"] = {row["symbol"]: row for row in candidates}
    state["minute_bucket"] = {}
    state["five_bars"] = {}
    state["session"] = {}
    state["pending_entries"] = {}
    state["pending_exits"] = {}
    state["model_hit_times"] = {}

    held = [symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0]
    session_symbols = [*state["candidate_meta"], *held]
    if session_symbols:
        context.set_universe(session_symbols)
    state["dropped_holdings"] = [
        symbol for symbol in held if symbol not in state["candidate_meta"]
    ]
    for symbol in held:
        item = state["position_meta"].setdefault(symbol, {})
        item["holding_days"] = int(item.get("holding_days") or 0) + 1
    context.log(
        f"主线快照 {snapshot.get('as_of') or '无'}："
        f"一级 {len(snapshot.get('industries') or [])} 个，"
        f"二级 {len(snapshot.get('subindustries') or [])} 个，"
        f"候选 {len(candidates)} 只，覆盖率 {float(snapshot.get('coverage') or 0):.2%}"
    )


def _bucket_end(timestamp):
    current = timestamp.time().replace(second=0, microsecond=0)
    if time(9, 31) <= current <= time(11, 30):
        start = timestamp.replace(hour=9, minute=30, second=0, microsecond=0)
    elif time(13, 1) <= current <= time(15, 0):
        start = timestamp.replace(hour=13, minute=0, second=0, microsecond=0)
    else:
        return None
    elapsed = int((timestamp - start).total_seconds() // 60)
    return start + timedelta(minutes=((elapsed - 1) // 5 + 1) * 5)


def _raw(bar, field):
    value = getattr(bar, f"raw_{field}", None)
    return float(value if value is not None else getattr(bar, field))


def _append_minute(state, symbol, bar):
    end = _bucket_end(bar.timestamp)
    if end is None:
        return False
    bucket = state["minute_bucket"].get(symbol)
    completed = False
    if bucket is not None and datetime.fromisoformat(bucket["end"]) < end:
        _finish_bucket(state, symbol)
        bucket = None
        completed = True
    if bucket is None:
        bucket = {
            "end": end.isoformat(),
            "open": _raw(bar, "open"),
            "high": _raw(bar, "high"),
            "low": _raw(bar, "low"),
            "close": _raw(bar, "close"),
            "volume": 0.0,
            "amount": 0.0,
            "limit_up": bar.limit_up,
        }
        state["minute_bucket"][symbol] = bucket
    bucket["high"] = max(float(bucket["high"]), _raw(bar, "high"))
    bucket["low"] = min(float(bucket["low"]), _raw(bar, "low"))
    bucket["close"] = _raw(bar, "close")
    bucket["volume"] += float(bar.volume or 0)
    bucket["amount"] += float(bar.amount or 0)
    bucket["limit_up"] = bar.limit_up if bar.limit_up is not None else bucket["limit_up"]
    if bar.timestamp >= end:
        _finish_bucket(state, symbol)
        completed = True
    return completed


def _finish_bucket(state, symbol):
    bucket = state["minute_bucket"].pop(symbol, None)
    if bucket is None or float(bucket["volume"]) <= 0:
        return
    session = state["session"].setdefault(symbol, {
        "open": float(bucket["open"]),
        "high": float(bucket["high"]),
        "amount": 0.0,
        "volume": 0.0,
    })
    session["high"] = max(float(session["high"]), float(bucket["high"]))
    session["amount"] += float(bucket["amount"])
    session["volume"] += float(bucket["volume"])
    bucket["vwap"] = (
        session["amount"] / (session["volume"] * 100.0)
        if session["volume"] > 0 else 0.0
    )
    rows = state["five_bars"].setdefault(symbol, [])
    rows.append(bucket)
    if len(rows) > 60:
        del rows[:-60]


def _in_entry_window(timestamp):
    current = timestamp.time()
    return any(start <= current <= end for start, end in ENTRY_WINDOWS)


def _return(rows, bars):
    if len(rows) <= bars or float(rows[-bars - 1]["close"]) <= 0:
        return None
    return float(rows[-1]["close"]) / float(rows[-bars - 1]["close"]) - 1


def _industry_metrics(state, symbol):
    meta = state["candidate_meta"].get(symbol, {})
    l1_key, l2_key = meta.get("l1_key"), meta.get("l2_key")
    l1_rows, l2_rows = [], []
    l1_above = l2_above = l1_total = l2_total = 0
    for candidate, candidate_meta in state["candidate_meta"].items():
        rows = state["five_bars"].get(candidate) or []
        if not rows:
            continue
        current = rows[-1]
        above = float(current["close"]) > float(current.get("vwap") or 0)
        ret15 = _return(rows, 3)
        if candidate_meta.get("l1_key") == l1_key:
            l1_total += 1
            l1_above += int(above)
            if ret15 is not None:
                l1_rows.append(ret15)
        if candidate_meta.get("l2_key") == l2_key:
            l2_total += 1
            l2_above += int(above)
            if ret15 is not None:
                l2_rows.append(ret15)
    own = _return(state["five_bars"].get(symbol) or [], 3)
    l2_median = median(l2_rows) if l2_rows else 0.0
    return {
        "l1_breadth": l1_above / l1_total if l1_total else 0.0,
        "l2_breadth": l2_above / l2_total if l2_total else 0.0,
        "ret15": own,
        "l2_ret15": l2_median,
        "excess15": own - l2_median if own is not None else None,
    }


def _common_gate(state, symbol, metrics):
    rows = state["five_bars"].get(symbol) or []
    meta = state["candidate_meta"].get(symbol) or {}
    if len(rows) < 4 or not meta:
        return False
    current = rows[-1]
    previous_close = float(meta.get("previous_raw_close") or 0)
    if previous_close <= 0:
        return False
    gap = float(state["session"][symbol]["open"]) / previous_close - 1
    if gap > 0.07 or float(current["close"]) <= float(current.get("vwap") or 0):
        return False
    limit_up = current.get("limit_up")
    if limit_up is not None and float(current["close"]) >= float(limit_up) - 0.005:
        return False
    return metrics["l1_breadth"] >= 0.55 and metrics["l2_breadth"] >= 0.60


def _model_hits(state, symbol, metrics):
    rows = state["five_bars"].get(symbol) or []
    if not _common_gate(state, symbol, metrics):
        return {"breakout": False, "pullback": False, "resonance": False}
    current = rows[-1]
    previous = rows[:-1]
    prior_high = max(float(row["high"]) for row in previous[-4:])
    previous_amounts = [float(row["amount"]) for row in previous[-6:] if float(row["amount"]) > 0]
    amount_ratio = (
        float(current["amount"]) / median(previous_amounts)
        if previous_amounts and median(previous_amounts) > 0 else 0.0
    )
    five_return = float(current["close"]) / float(current["open"]) - 1
    breakout = (
        float(current["close"]) > prior_high
        and five_return >= 0.006
        and amount_ratio >= 1.5
        and float(metrics.get("excess15") or 0) >= 0.008
    )
    session = state["session"][symbol]
    touched = any(
        float(row["low"]) <= float(row.get("vwap") or 0) * 1.003
        and float(row["close"]) >= float(row.get("vwap") or 0) * 0.995
        for row in rows[-3:]
    )
    impulse_amount = max((float(row["amount"]) for row in previous), default=0.0)
    pullback = (
        float(session["high"]) / float(session["open"]) - 1 >= 0.02
        and touched
        and float(current["close"]) > float(previous[-1]["high"])
        and impulse_amount > 0
        and float(current["amount"]) <= impulse_amount * 0.8
    )
    high30 = max(float(row["high"]) for row in previous[-6:])
    resonance = (
        metrics["l2_breadth"] >= 0.65
        and metrics["l1_breadth"] >= 0.55
        and float(metrics.get("excess15") or 0) >= 0.01
        and float(current["close"]) >= float(current["vwap"]) * 1.003
        and float(current["close"]) > high30
        and amount_ratio >= 1.2
    )
    return {"breakout": breakout, "pullback": pullback, "resonance": resonance}


def _sync_positions(context, state):
    positions = context.portfolio.positions
    stale_before = context.now - timedelta(minutes=10)
    for symbol, pending in list(state["pending_entries"].items()):
        submitted = datetime.fromisoformat(str(pending.get("timestamp")))
        if float(positions.get(symbol, 0)) <= 0 and submitted < stale_before:
            state["pending_entries"].pop(symbol, None)
    for symbol, quantity in positions.items():
        if quantity <= 0 or symbol in state["position_meta"]:
            continue
        pending = state["pending_entries"].pop(symbol, {})
        meta = dict(pending.get("meta") or state["candidate_meta"].get(symbol) or {})
        meta.update({
            "entry_date": context.now.date().isoformat(),
            "holding_days": 0,
            "peak": float(context.portfolio.avg_cost.get(symbol) or 0),
        })
        state["position_meta"][symbol] = meta
    for symbol in list(state["position_meta"]):
        if float(positions.get(symbol, 0)) <= 0 and symbol not in state["pending_entries"]:
            state["position_meta"].pop(symbol, None)
            state["pending_exits"].pop(symbol, None)


def _combined_model_hit(state, symbol, hits, timestamp):
    history = state["model_hit_times"].setdefault(symbol, {})
    cutoff = timestamp - timedelta(minutes=10)
    for model, hit in hits.items():
        if hit:
            history[model] = timestamp.isoformat()
    history = {
        model: occurred_at for model, occurred_at in history.items()
        if datetime.fromisoformat(occurred_at) >= cutoff
    }
    state["model_hit_times"][symbol] = history
    return len(history) >= 2


def _exit_positions(context, state, completed_symbols):
    current_time = context.now.time()
    if current_time < time(9, 50):
        return
    for symbol, quantity in list(context.portfolio.positions.items()):
        if quantity <= 0 or symbol in state["pending_exits"]:
            continue
        rows = state["five_bars"].get(symbol) or []
        if not rows or symbol not in completed_symbols:
            continue
        current = rows[-1]
        price = float(current["close"])
        info = state["position_meta"].setdefault(symbol, {})
        cost = float(context.portfolio.avg_cost.get(symbol) or 0)
        peak = max(float(info.get("peak") or cost), price)
        info["peak"] = peak
        below = int(info.get("below_vwap") or 0)
        below = below + 1 if price < float(current.get("vwap") or 0) else 0
        info["below_vwap"] = below
        metrics = _industry_metrics(state, symbol) if symbol in state["candidate_meta"] else {"excess15": -1.0}
        reasons = []
        if symbol in state.get("dropped_holdings", []):
            reasons.append("主线掉出")
        if cost > 0 and price / cost - 1 <= -0.06:
            reasons.append("止损6%")
        if cost > 0 and peak / cost - 1 >= 0.08 and price / peak - 1 <= -0.05:
            reasons.append("移动止盈")
        if below >= 3 and float(metrics.get("excess15") or 0) <= -0.008:
            reasons.append("分时转弱")
        if int(info.get("holding_days") or 0) >= 5 and current_time >= time(14, 45):
            reasons.append("持有满5日")
        available = float(context.portfolio.available_positions.get(symbol, 0))
        if not reasons or available <= 0:
            continue
        reason = "、".join(reasons)
        context.sell(symbol, quantity=available, reason=reason)
        state["pending_exits"][symbol] = context.now.isoformat()
        state["exit_events"].append({
            "timestamp": context.now.isoformat(), "symbol": symbol,
            "price": price, "reason": reason,
        })


def _entry_candidates(context, state, completed_symbols):
    if not _in_entry_window(context.now):
        return
    held = {symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0}
    slots = MAX_POSITIONS - len(held) - len(state["pending_entries"])
    if slots <= 0:
        return
    held_l1 = {
        state["position_meta"].get(symbol, {}).get("l1_key")
        for symbol in held
    }
    pending_l1 = {
        item.get("meta", {}).get("l1_key") for item in state["pending_entries"].values()
    }
    signals = []
    for symbol in completed_symbols:
        if symbol in held or symbol in state["pending_entries"] or symbol not in state["candidate_meta"]:
            continue
        meta = state["candidate_meta"][symbol]
        if meta.get("l1_key") in held_l1 or meta.get("l1_key") in pending_l1:
            continue
        rows = state["five_bars"].get(symbol) or []
        if not rows:
            continue
        metrics = _industry_metrics(state, symbol)
        hits = _model_hits(state, symbol, metrics)
        matched = hits.get(ENTRY_MODEL, False)
        if ENTRY_MODEL == "combined":
            matched = _combined_model_hit(state, symbol, hits, context.now)
        if not matched:
            continue
        current = rows[-1]
        score = float(meta.get("stock_score") or 0) + float(metrics.get("excess15") or 0) * 100
        signals.append((score, symbol, meta, metrics, hits, current))
    signals.sort(reverse=True, key=lambda item: (item[0], item[1]))
    for score, symbol, meta, metrics, hits, current in signals[:slots]:
        context.order_target_percent(symbol, TARGET_POSITION_PCT)
        payload = {
            "model": ENTRY_MODEL,
            "symbol": symbol,
            "price": float(current["close"]),
            "score": score,
            "l1_name": meta.get("l1_name"),
            "l2_name": meta.get("l2_name"),
            "l1_breadth": metrics["l1_breadth"],
            "l2_breadth": metrics["l2_breadth"],
            "excess15": metrics["excess15"],
            "hits": hits,
            "as_of": state["snapshot"].get("as_of"),
        }
        state["pending_entries"][symbol] = {"timestamp": context.now.isoformat(), "meta": meta}
        state["entry_events"].append({"timestamp": context.now.isoformat(), **payload})
        context.emit_signal(
            "mainline_momentum_entry",
            payload,
            event_id=f"mainline:{ENTRY_MODEL}:{symbol}:{context.now.isoformat()}",
        )
        held_l1.add(meta.get("l1_key"))


def on_bar(context, bars):
    state = _state(context)
    _sync_positions(context, state)
    completed = set()
    for symbol, bar in bars.items():
        if symbol not in context.universe:
            continue
        if _append_minute(state, symbol, bar):
            completed.add(symbol)
    if not completed:
        return
    _exit_positions(context, state, completed)
    _entry_candidates(context, state, completed)


def after_trading_end(context):
    state = _state(context)
    _sync_positions(context, state)
    state["daily_reports"].append({
        "date": context.now.date().isoformat(),
        "model": ENTRY_MODEL,
        "as_of": state.get("snapshot", {}).get("as_of"),
        "candidate_count": len(state.get("candidate_meta", {})),
        "holdings": [
            symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0
        ],
        "entry_count": sum(
            str(item.get("timestamp", "")).startswith(context.now.date().isoformat())
            for item in state["entry_events"]
        ),
    })
