"""Morning large-turnover first-board strategy for minute backtests."""
from datetime import time
import math


STRATEGY_KIND = "large_amount_first_board"
MIN_CUMULATIVE_AMOUNT = 1_000_000_000.0
MIN_MARKET_CAP = 10_000_000_000.0
MAX_POSITIONS = 5
TARGET_POSITION_PCT = 0.18
ENTRY_END = time(11, 30)
EXIT_TIME = time(14, 55)


def _state(context):
    return context.state.setdefault("large_amount_first_board", {
        "snapshot": {},
        "candidate_meta": {},
        "cumulative_amount": {},
        "first_touch": {},
        "pending_entries": {},
        "pending_exits": {},
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
        raise ValueError("大成交首板策略没有可用的股票分钟标的")
    context.set_universe([str(instruments[0]["symbol"])])
    context.require_limit_board_snapshot(
        lookback_days=30,
        min_cumulative_amount=MIN_CUMULATIVE_AMOUNT,
    )
    _state(context)


def before_trading_start(context):
    state = _state(context)
    snapshot = context.limit_board_snapshot(context.now.date())
    candidates = list(snapshot.get("candidates") or [])
    state["snapshot"] = snapshot
    state["candidate_meta"] = {row["symbol"]: row for row in candidates}
    state["cumulative_amount"] = {}
    state["first_touch"] = {}
    state["pending_entries"] = {}
    state["pending_exits"] = {}
    held = [symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0]
    session_symbols = [*state["candidate_meta"], *held]
    if session_symbols:
        context.set_universe(session_symbols)
    for symbol in held:
        info = state["position_meta"].setdefault(symbol, {})
        info["holding_days"] = int(info.get("holding_days") or 0) + 1
    context.log(
        f"上午首板扫描 {snapshot.get('as_of') or '无'}：候选 {len(candidates)} 只"
    )


def _raw(bar, field):
    value = getattr(bar, f"raw_{field}", None)
    return float(value if value is not None else getattr(bar, field))


def _sync_positions(context, state):
    positions = context.portfolio.positions
    for symbol, quantity in positions.items():
        if quantity <= 0 or symbol in state["position_meta"]:
            continue
        pending = state["pending_entries"].pop(symbol, {})
        state["position_meta"][symbol] = {
            "entry_date": context.now.date().isoformat(),
            "holding_days": 0,
            **dict(pending.get("meta") or {}),
        }
    for symbol in list(state["position_meta"]):
        if float(positions.get(symbol, 0)) <= 0 and symbol not in state["pending_entries"]:
            state["position_meta"].pop(symbol, None)
            state["pending_exits"].pop(symbol, None)


def _factor_score(meta, cumulative_amount):
    ret5 = float(meta.get("ret5_d1") or 0)
    ret20 = float(meta.get("ret20_d1") or 0)
    expansion = float(meta.get("amount_expansion_d1") or 0)
    score = min(max(-ret5, 0), 0.10) * 300
    score += min(max(-ret20, 0), 0.20) * 75
    score += min(max(expansion - 1, 0), 2.0) * 7.5
    score += 10 if bool(meta.get("above_ma20_d1")) else 0
    score += min(max(cumulative_amount / MIN_CUMULATIVE_AMOUNT - 1, 0), 2.0) * 5
    return round(score, 2)


def _passes_daily_gate(meta):
    return bool(
        int(meta.get("prior_limit_close_5d") or 0) == 0
        and float(meta.get("ret5_d1") or 0) <= -0.05
        and float(meta.get("market_cap_d1") or 0) >= MIN_MARKET_CAP
    )


def _entry_candidates(context, state, bars):
    if not time(9, 30) <= context.now.time() <= ENTRY_END:
        return
    held = {symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0}
    slots = MAX_POSITIONS - len(held) - len(state["pending_entries"])
    if slots <= 0:
        return
    signals = []
    for symbol, bar in bars.items():
        meta = state["candidate_meta"].get(symbol)
        if meta is None or symbol in held or symbol in state["pending_entries"]:
            continue
        cumulative = float(state["cumulative_amount"].get(symbol) or 0) + float(bar.amount or 0)
        state["cumulative_amount"][symbol] = cumulative
        limit_price = float(bar.limit_up or meta.get("limit_price") or 0)
        if limit_price <= 0 or _raw(bar, "high") < limit_price - 0.005:
            continue
        if symbol in state["first_touch"]:
            continue
        state["first_touch"][symbol] = {
            "timestamp": context.now.isoformat(),
            "cumulative_amount": cumulative,
        }
        if cumulative < MIN_CUMULATIVE_AMOUNT or not _passes_daily_gate(meta):
            continue
        signals.append((_factor_score(meta, cumulative), symbol, meta, cumulative, limit_price))

    signals.sort(reverse=True, key=lambda item: (item[0], item[1]))
    for score, symbol, meta, cumulative, limit_price in signals[:slots]:
        equity = float(context.portfolio.total_value)
        quantity = math.floor(equity * TARGET_POSITION_PCT / limit_price / 100) * 100
        if quantity <= 0:
            continue
        context.buy(symbol, quantity=quantity, reason="上午大成交首板触板")
        payload = {
            "symbol": symbol,
            "name": meta.get("name"),
            "price": limit_price,
            "cumulative_amount": cumulative,
            "score": score,
            "ret5_d1": meta.get("ret5_d1"),
            "ret20_d1": meta.get("ret20_d1"),
            "above_ma20_d1": meta.get("above_ma20_d1"),
            "amount_expansion_d1": meta.get("amount_expansion_d1"),
            "market_cap_d1": meta.get("market_cap_d1"),
            "prior_limit_close_5d": meta.get("prior_limit_close_5d"),
            "as_of": state["snapshot"].get("as_of"),
        }
        state["pending_entries"][symbol] = {
            "timestamp": context.now.isoformat(),
            "meta": meta,
        }
        state["entry_events"].append({"timestamp": context.now.isoformat(), **payload})
        context.emit_signal(
            "large_amount_first_board_entry",
            payload,
            event_id=f"first-board:{symbol}:{context.now.isoformat()}",
        )


def _exit_positions(context, state):
    if context.now.time() < EXIT_TIME:
        return
    for symbol, quantity in list(context.portfolio.positions.items()):
        info = state["position_meta"].get(symbol) or {}
        available = float(context.portfolio.available_positions.get(symbol, 0))
        if (
            quantity <= 0
            or available <= 0
            or symbol in state["pending_exits"]
            or int(info.get("holding_days") or 0) < 5
        ):
            continue
        context.sell(symbol, quantity=available, reason="持有至D+5")
        state["pending_exits"][symbol] = context.now.isoformat()
        state["exit_events"].append({
            "timestamp": context.now.isoformat(),
            "symbol": symbol,
            "reason": "持有至D+5",
        })


def on_bar(context, bars):
    state = _state(context)
    _sync_positions(context, state)
    _exit_positions(context, state)
    _entry_candidates(context, state, bars)


def after_trading_end(context):
    state = _state(context)
    _sync_positions(context, state)
    state["daily_reports"].append({
        "date": context.now.date().isoformat(),
        "as_of": state.get("snapshot", {}).get("as_of"),
        "candidate_count": len(state.get("candidate_meta", {})),
        "first_touch_count": len(state.get("first_touch", {})),
        "holdings": [
            symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0
        ],
    })
