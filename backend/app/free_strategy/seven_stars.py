"""七星高照 ETF 轮动策略的 TickFlow 原生实现。"""
from __future__ import annotations

import math
from typing import Any


SEVEN_STARS_ETF_POOL = [
    "518880.SH",
    "159980.SZ",
    "159985.SZ",
    "501018.SH",
    "161226.SZ",
    "159981.SZ",
    "513100.SH",
    "159509.SZ",
    "513290.SH",
    "513500.SH",
    "159529.SZ",
    "513400.SH",
    "513520.SH",
    "513030.SH",
    "513080.SH",
    "513310.SH",
    "513730.SH",
    "159792.SZ",
    "513130.SH",
    "513050.SH",
    "159920.SZ",
    "513690.SH",
    "510300.SH",
    "510500.SH",
    "510050.SH",
    "510210.SH",
    "159915.SZ",
    "588080.SH",
    "512100.SH",
    "563360.SH",
    "563300.SH",
    "512890.SH",
    "159967.SZ",
    "512040.SH",
    "159201.SZ",
    "511380.SH",
    "511010.SH",
    "511220.SH",
]
EXCLUDED_ETFS = {"511010.SH", "511220.SH", "511380.SH"}
T0_ETFS = [
    "159509.SZ",
    "159529.SZ",
    "159792.SZ",
    "159920.SZ",
    "159981.SZ",
    "159985.SZ",
    "161226.SZ",
    "501018.SH",
    "511010.SH",
    "511220.SH",
    "511380.SH",
    "513030.SH",
    "513050.SH",
    "513080.SH",
    "513100.SH",
    "513130.SH",
    "513290.SH",
    "513310.SH",
    "513400.SH",
    "513500.SH",
    "513520.SH",
    "513690.SH",
    "513730.SH",
    "518880.SH",
]
PROFIT_PROTECTION_TIMES = [
    "09:45",
    "10:00",
    "10:15",
    "10:30",
    "10:45",
    "11:00",
    "11:15",
    "13:01",
    "13:15",
    "13:30",
    "13:45",
    "14:00",
    "14:15",
    "14:30",
    "14:45",
    "14:56",
]
LOOKBACK_DAYS = 25
SHORT_LOOKBACK_DAYS = 10
PROFIT_PROTECTION_THRESHOLD = 0.05
PREMIUM_THRESHOLD = 0.20
MIN_TRADE_VALUE = 5_000.0
TRADE_CAPITAL_LIMIT = 100_000.0
SLIPPAGE_BPS = 0.5
PRICE_TICK = 0.001
COMMISSION_PCT = 0.0002
MIN_COMMISSION = 5.0


def _state(context) -> dict[str, Any]:
    return context.state["seven_stars"]


def initialize(context) -> None:
    context.set_universe(SEVEN_STARS_ETF_POOL)
    context.require_history("1d", bars=45)
    context.require_extra_history("unit_net_value")
    context.state.setdefault("seven_stars", {
        "rankings_date": None,
        "rankings": [],
        "reentry_block_date": None,
        "reentry_blocked_today": [],
        "target": [],
        "held_before_sell": [],
        "decision": {},
        "intraday": {"date": None, "bars": {}, "volume": {}},
        "trade_capital": TRADE_CAPITAL_LIMIT,
        "entry_prices": {},
        "daily_reports": [],
    })
    context.schedule(_check_positions, "09:10")
    context.schedule(_sell_targets, "13:09")
    context.schedule(_buy_targets, "13:10")
    for at in PROFIT_PROTECTION_TIMES:
        context.schedule(_profit_protection_check, at)
    context.log("七星高照 ETF 轮动策略已初始化：盈利保护、量价过滤与净值溢价过滤已启用")


def before_trading_start(context) -> None:
    _state(context)["intraday"] = {
        "date": context.now.date().isoformat(),
        "bars": {},
        "volume": {},
    }
    _reset_reentry_blocklist(context)


def on_bar(context, bars) -> None:
    intraday = _state(context)["intraday"]
    today = context.now.date().isoformat()
    if intraday.get("date") != today:
        intraday = {"date": today, "bars": {}, "volume": {}}
        _state(context)["intraday"] = intraday
    for symbol, value in bars.items():
        intraday["bars"][symbol] = {
            "close": _bar_price(value),
            "high": _bar_price(value, "high"),
            "suspended": bool(getattr(value, "suspended", False)),
            "tradable": bool(getattr(value, "tradable", True)),
            "limit_up": getattr(value, "limit_up", None),
            "limit_down": getattr(value, "limit_down", None),
        }
        intraday["volume"][symbol] = (
            float(intraday["volume"].get(symbol, 0.0))
            + float(getattr(value, "volume", 0.0))
        )


def after_trading_end(context) -> None:
    state = _state(context)
    report = {
        "date": context.now.date().isoformat(),
        "target": list(state.get("target", [])),
        "holdings": _held_symbols(context),
        "equity": float(context.portfolio.total_value),
        "cash": float(context.portfolio.cash),
        "candidates": [
            {"symbol": row["symbol"], "score": row["score"]}
            for row in state.get("rankings", [])[:10]
        ],
        "decision": dict(state.get("decision", {})),
    }
    state["daily_reports"].append(report)
    if len(state["daily_reports"]) > 320:
        del state["daily_reports"][:-320]


def _bar_price(bar: Any, field: str = "close") -> float:
    if isinstance(bar, dict):
        return float(bar[field])
    execution_price = getattr(bar, "execution_price", None)
    if callable(execution_price):
        return float(execution_price(field))
    raw = getattr(bar, f"raw_{field}", None)
    return float(raw if raw is not None else getattr(bar, field))


def _current_bar(context, symbol: str) -> Any | None:
    return _state(context)["intraday"].get("bars", {}).get(symbol)


def _stale_bar(context, symbol: str) -> Any | None:
    daily = list(context.history_bars(symbol, count=1, timeframe="1d"))
    if not daily:
        return None
    price = _bar_price(daily[-1])
    return {
        "close": price,
        "high": price,
        "suspended": False,
        "tradable": True,
        "limit_up": None,
        "limit_down": None,
    }


def _held_symbols(context) -> list[str]:
    return [
        symbol
        for symbol, quantity in context.portfolio.positions.items()
        if float(quantity) > 0 and symbol in SEVEN_STARS_ETF_POOL
    ]


def _available_quantity(context, symbol: str) -> float:
    available = getattr(context.portfolio, "available_positions", {})
    return float(available.get(symbol, context.portfolio.positions.get(symbol, 0.0)))


def _reset_reentry_blocklist(context) -> None:
    state = _state(context)
    today = context.now.date().isoformat()
    if state.get("reentry_block_date") != today:
        state["reentry_block_date"] = today
        state["reentry_blocked_today"] = []


def _block_reentry(context, symbol: str) -> None:
    _reset_reentry_blocklist(context)
    blocked = _state(context)["reentry_blocked_today"]
    if symbol not in blocked:
        blocked.append(symbol)


def _profit_triggered(context, symbol: str, current_bar: Any | None = None) -> bool:
    daily = list(context.history_bars(symbol, count=1, timeframe="1d"))
    current = current_bar or _current_bar(context, symbol) or _stale_bar(context, symbol)
    if not daily or current is None:
        return False
    previous_high = _bar_price(daily[-1], "high")
    current_price = _bar_price(current)
    return previous_high > 0 and current_price <= previous_high * (1 - PROFIT_PROTECTION_THRESHOLD)


def _profit_protection_check(context) -> None:
    _reset_reentry_blocklist(context)
    for symbol in _held_symbols(context):
        if not _profit_triggered(context, symbol):
            continue
        if _available_quantity(context, symbol) <= 0:
            continue
        if _exit_position(context, symbol):
            _block_reentry(context, symbol)
            context.log(f"七星盈利保护：{symbol} 从前一交易日高点回撤达到 5%，卖出并禁止当日回补")


def _weighted_momentum(prices: list[float], lookback: int) -> tuple[float | None, float | None, float | None]:
    if len(prices) < lookback + 1:
        return None, None, None
    values = prices[-(lookback + 1):]
    if any(price <= 0 for price in values):
        return None, None, None
    y = [math.log(price) for price in values]
    weights = [1 + index / lookback for index in range(len(y))]
    regression_weights = [weight * weight for weight in weights]
    total_weight = sum(regression_weights)
    x_mean = sum(index * weight for index, weight in enumerate(regression_weights)) / total_weight
    y_mean = sum(value * weight for value, weight in zip(y, regression_weights)) / total_weight
    variance = sum(weight * (index - x_mean) ** 2 for index, weight in enumerate(regression_weights))
    if variance <= 0:
        return None, None, None
    slope = sum(
        weight * (index - x_mean) * (value - y_mean)
        for index, (value, weight) in enumerate(zip(y, regression_weights))
    ) / variance
    intercept = y_mean - slope * x_mean
    arithmetic_mean = sum(y) / len(y)
    residual = sum(
        weight * (value - (slope * index + intercept)) ** 2
        for index, (value, weight) in enumerate(zip(y, weights))
    )
    total = sum(weight * (value - arithmetic_mean) ** 2 for value, weight in zip(y, weights))
    r_squared = 1 - residual / total if total else 0.0
    annualized = math.exp(slope * 250) - 1
    return annualized * r_squared, annualized, r_squared


def _short_annualized_return(prices: list[float], lookback: int) -> float | None:
    if len(prices) < lookback + 1:
        return None
    start = prices[-(lookback + 1)]
    end = prices[-1]
    if start <= 0 or end <= 0:
        return None
    return (end / start) ** (250 / lookback) - 1


def _metric_for(context, symbol: str) -> dict[str, Any] | None:
    daily = list(context.history_bars(symbol, count=45, timeframe="1d"))
    current_bar = _current_bar(context, symbol)
    if len(daily) < LOOKBACK_DAYS or current_bar is None:
        return None
    if current_bar.get("suspended", False) or not current_bar.get("tradable", True):
        return None
    if _profit_triggered(context, symbol, current_bar):
        return None

    prices = [_bar_price(item) for item in daily]
    prices.append(_bar_price(current_bar))
    score, annualized, r_squared = _weighted_momentum(prices, LOOKBACK_DAYS)
    short_annualized = _short_annualized_return(prices, SHORT_LOOKBACK_DAYS)
    if score is None or annualized is None or short_annualized is None:
        return None
    if not (0 < score < 100) or short_annualized < 0:
        return None
    if min(prices[-index] / prices[-index - 1] for index in range(1, 4)) < 0.97:
        return None

    daily_volumes = [float(getattr(item, "volume", 0.0)) for item in daily[-5:]]
    average_volume = sum(daily_volumes) / len(daily_volumes) if daily_volumes else 0.0
    current_volume = float(
        _state(context)["intraday"].get("volume", {}).get(symbol, 0.0)
    )
    volume_ratio = current_volume / average_volume if average_volume > 0 else None
    if volume_ratio is not None and volume_ratio > 2 and annualized > 1:
        return None
    return {
        "symbol": symbol,
        "score": score,
        "annualized_return": annualized,
        "r_squared": r_squared,
        "short_annualized": short_annualized,
        "current_price": _bar_price(current_bar),
        "volume_ratio": volume_ratio,
    }


def _rank_candidates(context) -> list[dict[str, Any]]:
    result = []
    for symbol in SEVEN_STARS_ETF_POOL:
        if symbol in EXCLUDED_ETFS:
            continue
        metric = _metric_for(context, symbol)
        if metric is not None:
            result.append(metric)
    result.sort(key=lambda item: item["score"], reverse=True)
    return result


def _cached_rankings(context) -> list[dict[str, Any]]:
    state = _state(context)
    today = context.now.date().isoformat()
    if state.get("rankings_date") != today:
        state["rankings"] = _rank_candidates(context)
        state["rankings_date"] = today
    return state["rankings"]


def _previous_daily_row(context, symbol: str) -> Any | None:
    rows = list(context.history_bars(symbol, count=1, timeframe="1d"))
    return rows[-1] if rows else None


def _passes_premium_filter(context, symbol: str) -> bool | None:
    previous = _previous_daily_row(context, symbol)
    timestamp = getattr(previous, "timestamp", None) if previous is not None else None
    if previous is None or timestamp is None:
        return None
    previous_day = timestamp.date()
    nav_rows = context.extra_history(
        "unit_net_value",
        symbol,
        count=1,
        end_date=previous_day,
    )
    if not nav_rows or nav_rows[-1].get("date") != previous_day.isoformat():
        return None
    nav = float(nav_rows[-1].get("value") or 0.0)
    if nav <= 0:
        return None
    premium = (_bar_price(previous) - nav) / nav
    return premium <= PREMIUM_THRESHOLD


def _check_positions(context) -> None:
    _reset_reentry_blocklist(context)
    for symbol in _held_symbols(context):
        context.log(
            f"七星持仓：{symbol} 数量={context.portfolio.positions[symbol]:g} "
            f"成本={context.portfolio.avg_cost.get(symbol, 0.0):.3f}"
        )


def _execution_price(price: float, side: str) -> float:
    adjusted = price * (1 + SLIPPAGE_BPS / 10_000 * (1 if side == "buy" else -1))
    return math.floor(adjusted / PRICE_TICK + 0.5 + 1e-10) * PRICE_TICK


def _commission(price: float, quantity: float) -> float:
    return max(MIN_COMMISSION, price * quantity * COMMISSION_PCT)


def _exit_position(context, symbol: str) -> bool:
    current = _current_bar(context, symbol) or _stale_bar(context, symbol)
    quantity = float(context.portfolio.positions.get(symbol, 0.0))
    if current is None or quantity <= 0 or _available_quantity(context, symbol) <= 0:
        return False
    price = _bar_price(current)
    limit_down = current.get("limit_down")
    if current.get("suspended") or not current.get("tradable", True):
        return False
    if limit_down is not None and price <= float(limit_down) + 0.005:
        return False
    state = _state(context)
    entry_price = float(state["entry_prices"].get(symbol, context.portfolio.avg_cost.get(symbol, price)))
    fill_price = _execution_price(price, "sell")
    state["trade_capital"] = (
        float(state["trade_capital"])
        + (fill_price - entry_price) * quantity
        - _commission(fill_price, quantity)
    )
    state["entry_prices"].pop(symbol, None)
    context.order_target(symbol, 0)
    return True


def _effective_trade_capital(context) -> float:
    state = _state(context)
    value = float(state["trade_capital"])
    for symbol in _held_symbols(context):
        current = _current_bar(context, symbol)
        if current is None:
            continue
        quantity = float(context.portfolio.positions[symbol])
        entry_price = float(state["entry_prices"].get(symbol, context.portfolio.avg_cost.get(symbol, 0.0)))
        value += (_bar_price(current) - entry_price) * quantity
    return value


def _sell_targets(context) -> None:
    state = _state(context)
    rankings = _cached_rankings(context)
    raw_targets = [row["symbol"] for row in rankings[:1]]
    held = _held_symbols(context)
    state["held_before_sell"] = held
    sold = set()
    for symbol in held:
        if symbol not in raw_targets and _available_quantity(context, symbol) > 0:
            if _exit_position(context, symbol):
                sold.add(symbol)
    for symbol in held:
        if symbol in sold or _available_quantity(context, symbol) <= 0:
            continue
        if _passes_premium_filter(context, symbol) is False:
            _exit_position(context, symbol)


def _eligible_targets(context, rankings: list[dict[str, Any]]) -> list[str]:
    _reset_reentry_blocklist(context)
    blocked = set(_state(context).get("reentry_blocked_today", []))
    for row in rankings:
        symbol = row["symbol"]
        if symbol in blocked:
            continue
        if _profit_triggered(context, symbol):
            continue
        if _passes_premium_filter(context, symbol) is True:
            return [symbol]
    return []


def _buy_targets(context) -> None:
    state = _state(context)
    rankings = _cached_rankings(context)
    targets = _eligible_targets(context, rankings)
    state["target"] = targets
    held = _held_symbols(context)
    if not targets and held:
        state["target"] = held
        _emit_decision(context, held, reason="hold_without_target")
        return
    if targets and any(symbol not in targets for symbol in held):
        context.log("七星仍有非目标持仓未卖出，本次不新增仓位")
        _emit_decision(context, targets)
        return
    for symbol in targets:
        current = _current_bar(context, symbol)
        if current is None:
            continue
        price = _bar_price(current)
        if price <= 0:
            continue
        current_quantity = float(context.portfolio.positions.get(symbol, 0.0))
        current_value = current_quantity * price
        target_value = _effective_trade_capital(context)
        target_quantity = math.floor(target_value / price / 100) * 100
        trade_value = abs(target_quantity - current_quantity) * price
        if current_quantity > 0 and abs(current_value - target_value) <= target_value * 0.05:
            continue
        if 0 < trade_value < MIN_TRADE_VALUE:
            continue
        if current.get("suspended") or not current.get("tradable", True):
            continue
        limit_up = current.get("limit_up")
        if target_quantity > current_quantity and limit_up is not None and price >= float(limit_up) - 0.005:
            continue
        context.order_target(symbol, target_quantity)
        if current_quantity == 0 and target_quantity > 0:
            entry_price = _execution_price(price, "buy")
            state["entry_prices"][symbol] = entry_price
            state["trade_capital"] = float(state["trade_capital"]) - _commission(
                entry_price,
                target_quantity,
            )
    _emit_decision(context, targets)


def _emit_decision(context, targets: list[str], *, reason: str | None = None) -> None:
    state = _state(context)
    previous = list(state.get("held_before_sell", []))
    if not targets and not previous:
        decision_type = "empty"
        reason = reason or "no_eligible_target"
    elif targets == previous:
        decision_type = "hold"
        reason = reason or "hold_top_rank"
    else:
        decision_type = "rebalance"
        reason = reason or ("ranked_target" if targets else "exit_without_target")
    day = context.now.date().isoformat()
    decision = {
        "date": day,
        "reason": reason,
        "target": list(targets),
        "holding": previous,
    }
    state["decision"] = decision
    context.emit_signal(
        "daily_decision",
        {
            "strategy": "seven_stars",
            "trading_date": day,
            "decision": decision_type,
            "target_symbols": list(targets),
            "holding_symbols": previous,
            "candidates": [
                {"symbol": row["symbol"], "score": row["score"]}
                for row in state.get("rankings", [])[:10]
            ],
            "reason": reason,
        },
        event_id=f"seven_stars:{day}:decision",
    )
