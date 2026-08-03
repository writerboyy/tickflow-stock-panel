"""高息低价绩优小市值策略的 TickFlow 原生实现。"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any


STRATEGY_KIND = "performance_small_cap"
STOCK_COUNT = 10
MAX_STOCK_PRICE = 9.0
SMALLCAP_INDEX_SIZE = 400
SMALLCAP_INDEX_THRESHOLD = 18.72
BAN_TRADE_DAYS = 5
INDEX_SYMBOL = "399303.SZ"
INDEX_HISTORY_BARS = (12 + 26 + 9) * 5


def _state(context) -> dict[str, Any]:
    return context.state["performance_small_cap"]


def _normalize_symbol(raw: Any) -> str:
    symbol = str(raw).strip().upper()
    for source, target in {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}.items():
        if symbol.endswith(source):
            return f"{symbol[:-len(source)]}{target}"
    return symbol


def _bar_price(bar: Any, field: str = "close") -> float:
    execution_price = getattr(bar, "execution_price", None)
    if callable(execution_price):
        return float(execution_price(field))
    raw = getattr(bar, f"raw_{field}", None)
    return float(raw if raw is not None else getattr(bar, field))


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _one_year_before(day: date) -> date:
    try:
        return day.replace(year=day.year - 1)
    except ValueError:
        return day.replace(year=day.year - 1, day=28)


def _instrument_records(context, asset_type: str = "stock") -> list[dict[str, Any]]:
    return list(getattr(context, "instruments", lambda _asset=None: [])(asset_type))


def _name_on(item: dict[str, Any], day: date) -> str:
    name = str(item.get("name") or "")
    for change in item.get("name_changes") or []:
        change_date = _parse_date(change.get("date"))
        if change_date is None:
            continue
        if day < change_date:
            return str(change.get("before") or name)
        name = str(change.get("after") or name)
    return name


def _held_symbols(context) -> list[str]:
    return [
        symbol
        for symbol, quantity in context.portfolio.positions.items()
        if float(quantity) > 0
    ]


def _held_scope(context, _timestamp: datetime) -> list[str]:
    return _held_symbols(context)


def _current_bars(context) -> dict[str, Any]:
    loader = getattr(context, "current_bars", None)
    return dict(loader()) if callable(loader) else {}


def _previous_trading_date(context, records: list[dict[str, Any]]) -> date:
    explicit = _parse_date(getattr(context, "previous_date", None))
    if explicit is not None:
        return explicit
    batch_loader = getattr(context, "history_batch", None)
    if callable(batch_loader):
        sample = [
            str(item.get("symbol") or "")
            for item in records[:128]
            if item.get("symbol")
        ]
        history = batch_loader(sample, count=1, timeframe="1d") if sample else {}
        dates = [
            values[-1].date
            for values in history.values()
            if values and values[-1].date < context.now.date()
        ]
        if dates:
            return max(dates)
    for item in records:
        symbol = str(item.get("symbol") or "")
        if not symbol:
            continue
        rows = context.history_bars(symbol, count=1, timeframe="1d")
        if rows:
            return rows[-1].date
    return context.now.date() - timedelta(days=1)


def _is_first_trading_day_of_month(context, previous_date: date) -> bool:
    current = context.now.date()
    return previous_date.month != current.month or previous_date.year != current.year


def _is_kcbj(symbol: str) -> bool:
    code = symbol.split(".", 1)[0]
    return code.startswith(("4", "8", "68")) or symbol.endswith(".BJ")


def _valid_name(item: dict[str, Any], day: date) -> bool:
    name = _name_on(item, day)
    upper = name.upper()
    return "ST" not in upper and "*" not in name and "退" not in name


def _tradable_at_snapshot(symbol: str, bar: Any, *, allow_held: bool = False) -> bool:
    if bar is None:
        return False
    if bool(getattr(bar, "suspended", False)) or not bool(getattr(bar, "tradable", True)):
        return False
    price = _bar_price(bar)
    if price <= 0:
        return False
    limit_up = getattr(bar, "limit_up", None)
    limit_down = getattr(bar, "limit_down", None)
    if not allow_held and limit_up is not None and price >= float(limit_up) - 0.005:
        return False
    if limit_down is not None and price <= float(limit_down) + 0.005:
        return False
    return bool(symbol)


def _valuation_market_caps(context, symbols: list[str], previous_date: date) -> dict[str, float]:
    loader = getattr(context, "valuation_market_caps", None)
    if not callable(loader):
        return {}
    try:
        return {
            str(symbol): float(value)
            for symbol, value in loader(symbols, previous_date).items()
            if value is not None and float(value) > 0
        }
    except (TypeError, ValueError):
        return {}


def _eligible_records(context, previous_date: date) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in _instrument_records(context):
        symbol = str(item.get("symbol") or "")
        if not symbol or _is_kcbj(symbol):
            continue
        result.append(item)
    return result


def _financially_qualified(context, symbols: list[str], previous_date: date) -> list[str]:
    loader = getattr(context, "financial_snapshot", None)
    if not callable(loader):
        return []
    snapshot = loader(symbols, previous_date)
    result: list[str] = []
    for symbol in symbols:
        row = snapshot.get(symbol) or {}
        revenue = row.get("revenue")
        net_income = row.get("net_income")
        attributable = row.get("net_income_attributable")
        roe = row.get("roe")
        roa = row.get("roa")
        if (
            _positive(attributable)
            and _positive(net_income)
            and revenue is not None
            and float(revenue) > 100_000_000
            and _positive(roe)
            and _positive(roa)
        ):
            result.append(symbol)
    return result


def _positive(value: Any) -> bool:
    try:
        return value is not None and math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _dividend_ratio_ranked(context, symbols: list[str], previous_date: date) -> list[str]:
    loader = getattr(context, "dividend_ratio_ranked", None)
    if callable(loader):
        ranked = loader(symbols, previous_date)
        if ranked is not None:
            return list(ranked)
    history = context.history_batch(symbols, count=260, timeframe="1d")
    valuation_caps = _valuation_market_caps(context, symbols, previous_date)
    ranked: list[tuple[float, str]] = []
    for symbol in symbols:
        values = history.get(symbol, [])
        if not values:
            continue
        latest = values[-1]
        market_cap = valuation_caps.get(symbol)
        if market_cap is None:
            continue
        one_year = _one_year_before(previous_date)
        dividend = 0.0
        for bar in values:
            if bar.date < one_year or bar.date > previous_date:
                continue
            total_shares = float(getattr(bar, "total_shares", 0.0) or 0.0)
            cash = float(getattr(bar, "cash_dividend", 0.0) or 0.0)
            if total_shares > 0 and cash > 0:
                dividend += cash * total_shares
        if dividend <= 0:
            continue
        ranked.append((dividend / market_cap, symbol))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    cutoff = int(len(ranked) * 0.25)
    return [symbol for _ratio, symbol in ranked[:cutoff]]


def _candidate_symbols(context, previous_date: date) -> list[str]:
    state = _state(context)
    cache_key = previous_date.isoformat()
    if state.get("candidate_cache_key") == cache_key:
        return list(state.get("candidate_cache", []))

    by_symbol = {str(item["symbol"]): item for item in _eligible_records(context, previous_date)}
    symbols = list(by_symbol)
    symbols = _dividend_ratio_ranked(context, symbols, previous_date)
    symbols = _financially_qualified(context, symbols, previous_date)
    history = context.history_batch(symbols, count=1, timeframe="1d")
    valuation_caps = _valuation_market_caps(context, symbols, previous_date)
    held = set(_held_symbols(context))
    candidates: list[tuple[float, str]] = []
    for symbol in symbols:
        latest = (history.get(symbol) or [None])[-1]
        if latest is None:
            continue
        price = _bar_price(latest)
        if symbol not in held and price >= MAX_STOCK_PRICE:
            continue
        market_cap = valuation_caps.get(symbol)
        if market_cap is None:
            continue
        candidates.append((market_cap, symbol))
    candidates.sort(key=lambda item: (item[0], item[1]))
    result = [symbol for _market_cap, symbol in candidates]
    state["candidate_cache_key"] = cache_key
    state["candidate_cache"] = result
    return result


def _select_stocks(context, *, require_snapshot: bool) -> list[str]:
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    state = _state(context)
    snapshot_time = context.now.strftime("%H:%M") if require_snapshot else ""
    cache_key = (previous_date.isoformat(), bool(require_snapshot), snapshot_time)
    if state.get("selection_cache_key") == cache_key:
        return list(state.get("selection_cache", []))

    symbols = _candidate_symbols(context, previous_date)
    if require_snapshot:
        current = _current_bars(context)
        by_symbol = {str(item.get("symbol") or ""): item for item in records}
        current_day = context.now.date()
        symbols = [
            symbol
            for symbol in symbols
            if _valid_name(by_symbol.get(symbol, {}), current_day)
            and _tradable_at_snapshot(symbol, current.get(symbol))
        ]
    selected = symbols[:STOCK_COUNT]
    state["selection_cache_key"] = cache_key
    state["selection_cache"] = selected
    return selected


def _selection_pool(context) -> list[str]:
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    return _candidate_symbols(context, previous_date)


def _selection_symbols(context, _timestamp: datetime) -> list[str]:
    return list(dict.fromkeys([*_held_symbols(context), *_selection_pool(context)]))


def _held_and_selection_symbols(context, timestamp: datetime) -> list[str]:
    state = _state(context)
    if not state.get("high_limit_list") and timestamp.strftime("%H:%M") == "14:00":
        return _held_symbols(context)
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    if timestamp.strftime("%H:%M") == "09:30" and not _should_monthly_adjust(context, previous_date):
        return _held_symbols(context)
    return _selection_symbols(context, timestamp)


def initialize(context) -> None:
    records = _instrument_records(context)
    symbols = [
        str(item["symbol"])
        for item in records
        if item.get("symbol") and bool(item.get("has_minute", True))
    ]
    context.set_universe(symbols)
    context.require_market_history(asset_type="index", timeframe="1d", bars=INDEX_HISTORY_BARS)
    context.state.setdefault("performance_small_cap", {
        "sorted_stocks": [],
        "just_sold": [],
        "high_limit_list": [],
        "risk_control_executed": False,
        "today_trade_allowed": True,
        "smallcap_index_value": None,
        "ban_trade_start_date": None,
        "first_rebalance_done": False,
        "candidate_cache_key": None,
        "candidate_cache": [],
        "selection_cache_key": None,
        "selection_cache": [],
        "daily_reports": [],
        "decision": {},
    })
    context.schedule(_prepare_stock_list, "09:00", symbols=_held_scope)
    context.schedule(_analyze_smallcap_index, "09:30", symbols=[])
    context.schedule(_check_smallcap_timing, "09:30", symbols=_held_scope)
    context.schedule(_dapan, "09:30", symbols=_held_scope)
    context.schedule(_monthly_adjustment, "09:30", symbols=_held_and_selection_symbols)
    context.schedule(_check_limit_up_and_buy, "14:00", symbols=_held_and_selection_symbols)
    context.log("绩优小市值策略已初始化：高股息/绩优过滤，低价小市值月度调仓")


def after_trading_end(context) -> None:
    state = _state(context)
    report = {
        "date": context.now.date().isoformat(),
        "target": list(state.get("sorted_stocks", [])),
        "holdings": _held_symbols(context),
        "equity": float(context.portfolio.total_value),
        "cash": float(context.portfolio.cash),
        "smallcap_index_value": state.get("smallcap_index_value"),
        "today_trade_allowed": bool(state.get("today_trade_allowed", True)),
        "decision": dict(state.get("decision", {})),
    }
    state["daily_reports"].append(report)
    if len(state["daily_reports"]) > 320:
        del state["daily_reports"][:-320]


def _prepare_stock_list(context) -> None:
    state = _state(context)
    state["just_sold"] = []
    state["risk_control_executed"] = False
    state["candidate_cache_key"] = None
    state["selection_cache_key"] = None
    held = _held_symbols(context)
    history = context.history_batch(held, count=1, timeframe="1d") if held else {}
    state["high_limit_list"] = [
        symbol for symbol in held
        if history.get(symbol) and _is_limit_up(history[symbol][-1])
    ]


def _is_limit_up(bar: Any) -> bool:
    limit_up = getattr(bar, "limit_up", None)
    return limit_up is not None and _bar_price(bar) >= float(limit_up) - 0.005


def _analyze_smallcap_index(context) -> None:
    state = _state(context)
    value = _smallcap_index_value(context)
    state["smallcap_index_value"] = value


def _smallcap_index_value(context) -> float | None:
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    cutoff = previous_date - timedelta(days=240)
    by_symbol = {}
    for item in records:
        symbol = str(item.get("symbol") or "")
        listing_date = _parse_date(item.get("listing_date"))
        if (
            not symbol
            or _is_kcbj(symbol)
            or listing_date is None
            or listing_date > cutoff
            or not _valid_name(item, previous_date)
        ):
            continue
        by_symbol[symbol] = item
    loader = getattr(context, "smallcap_index_value", None)
    if callable(loader):
        value = loader(list(by_symbol), previous_date)
        if value is not None:
            return float(value)
    history = context.history_batch(list(by_symbol), count=1, timeframe="1d")
    valuation_caps = _valuation_market_caps(context, list(by_symbol), previous_date)
    ranked: list[tuple[float, str, Any]] = []
    for symbol in by_symbol:
        values = history.get(symbol, [])
        if not values:
            continue
        bar = values[-1]
        market_cap = valuation_caps.get(symbol)
        if market_cap is None:
            continue
        ranked.append((market_cap, symbol, bar))
    ranked.sort(key=lambda item: (item[0], item[1]))
    closes = [float(bar.close) for _market_cap, _symbol, bar in ranked[:SMALLCAP_INDEX_SIZE] if float(bar.close) > 0]
    if not closes:
        return None
    return round(sum(closes) / len(closes), 4)


def _ban_period_ended(context) -> bool:
    state = _state(context)
    raw = state.get("ban_trade_start_date")
    if not raw:
        return False
    try:
        started = date.fromisoformat(str(raw))
    except ValueError:
        state["ban_trade_start_date"] = None
        return False
    records = _instrument_records(context)
    symbols = [str(item["symbol"]) for item in records[:16] if item.get("symbol")]
    history = context.history_batch(symbols, count=20, timeframe="1d")
    trading_days = {
        bar.date
        for values in history.values()
        for bar in values
        if started <= bar.date < context.now.date()
    }
    if len(trading_days) >= BAN_TRADE_DAYS:
        state["ban_trade_start_date"] = None
        return True
    return False


def _check_smallcap_timing(context) -> None:
    state = _state(context)
    if state.get("risk_control_executed"):
        return
    ban_ended = _ban_period_ended(context)
    value = state.get("smallcap_index_value")
    if value is None:
        return
    if float(value) > SMALLCAP_INDEX_THRESHOLD:
        for symbol in list(_held_symbols(context)):
            _close_position(context, symbol)
        state["today_trade_allowed"] = False
        state["risk_control_executed"] = True
        state["ban_trade_start_date"] = context.now.date().isoformat()
        _emit_decision(context, [], "smallcap_index_risk")
    else:
        state["today_trade_allowed"] = True
        if ban_ended:
            _execute_recovery_buying(context)


def _ema(values: list[float | None], span: int) -> list[float | None]:
    if not values:
        return []
    alpha = 2 / (span + 1)
    result: list[float | None] = []
    current: float | None = None
    observations = 0
    for value in values:
        if value is None or not math.isfinite(float(value)):
            result.append(None)
            continue
        current = value if current is None else alpha * value + (1 - alpha) * current
        observations += 1
        result.append(current if observations >= span - 1 else None)
    return result


def _macd(values: list[float]) -> tuple[list[float | None], list[float | None], list[float | None]]:
    ema_fast = _ema(values, 12)
    ema_slow = _ema(values, 26)
    dif = [
        fast - slow if fast is not None and slow is not None else None
        for fast, slow in zip(ema_fast, ema_slow)
    ]
    dea = _ema(dif, 9)
    macd = [
        (dif_value - dea_value) * 2 if dif_value is not None and dea_value is not None else None
        for dif_value, dea_value in zip(dif, dea)
    ]
    return dif, dea, macd


def _detect_divergences(context) -> tuple[bool, bool]:
    rows = context.market_history_bars(INDEX_SYMBOL, count=INDEX_HISTORY_BARS, timeframe="1d")
    if len(rows) < INDEX_HISTORY_BARS:
        return False, False
    closes = [float(row.close) for row in rows]
    dif, _dea, macd = _macd(closes)
    dead = [
        index for index in range(1, len(macd))
        if macd[index] is not None
        and macd[index - 1] is not None
        and macd[index] < 0 < macd[index - 1]
    ]
    gold = [
        index for index in range(1, len(macd))
        if macd[index] is not None
        and macd[index - 1] is not None
        and macd[index] > 0 > macd[index - 1]
    ]
    top = False
    bottom = False
    if len(dead) >= 2:
        previous, current = dead[-2], dead[-1]
        if (
            closes[previous] < closes[current]
            and dif[previous] is not None
            and dif[current] is not None
            and dif[previous] > dif[current] > 0
            and macd[-2] is not None
            and macd[-1] is not None
            and macd[-2] > 0 > macd[-1]
        ):
            recent = [value for value in dif[-10:] if value is not None]
            prior = [value for value in dif[-20:-10] if value is not None]
            top = bool(recent and prior and sum(recent) / len(recent) < sum(prior) / len(prior))
    if len(gold) >= 2:
        previous, current = gold[-2], gold[-1]
        if (
            closes[previous] > closes[current]
            and dif[previous] is not None
            and dif[current] is not None
            and dif[previous] < dif[current] < 0
            and macd[-2] is not None
            and macd[-1] is not None
            and macd[-2] < 0 < macd[-1]
        ):
            recent = [value for value in dif[-10:] if value is not None]
            prior = [value for value in dif[-20:-10] if value is not None]
            bottom = bool(recent and prior and sum(recent) / len(recent) > sum(prior) / len(prior))
    return top, bottom


def _dapan(context) -> None:
    state = _state(context)
    if state.get("risk_control_executed"):
        return
    if state.get("ban_trade_start_date") and not state.get("today_trade_allowed", True):
        if not _ban_period_ended(context):
            return
    top_divergence, bottom_divergence = _detect_divergences(context)
    if top_divergence:
        state["today_trade_allowed"] = False
        state["ban_trade_start_date"] = context.now.date().isoformat()
        state["risk_control_executed"] = True
        for symbol in list(_held_symbols(context)):
            _close_position(context, symbol)
        _emit_decision(context, [], "index_top_divergence")
    if bottom_divergence:
        state["bottom_divergence_seen"] = context.now.isoformat()


def _should_monthly_adjust(context, previous_date: date) -> bool:
    state = _state(context)
    if not state.get("first_rebalance_done"):
        return True
    return _is_first_trading_day_of_month(context, previous_date)


def _monthly_adjustment(context) -> None:
    state = _state(context)
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    if not _should_monthly_adjust(context, previous_date):
        return
    state["first_rebalance_done"] = True
    if not state.get("today_trade_allowed", True):
        return
    target = _select_stocks(context, require_snapshot=True)
    state["sorted_stocks"] = target
    current = _current_bars(context)
    for symbol in list(_held_symbols(context)):
        if symbol not in target:
            _close_position(context, symbol)
    _buy_missing_targets(context, target, current)
    _emit_decision(context, target, "monthly_adjustment")


def _close_position(context, symbol: str) -> bool:
    quantity = float(context.portfolio.positions.get(symbol, 0.0))
    if quantity <= 0:
        return False
    context.order_target(symbol, 0)
    state = _state(context)
    if symbol not in state["just_sold"]:
        state["just_sold"].append(symbol)
    return True


def _open_position(context, symbol: str, current: dict[str, Any]) -> bool:
    bar = current.get(symbol)
    if not _tradable_at_snapshot(symbol, bar):
        return False
    context.order_cash_weight(symbol, 1.0)
    return True


def _buy_missing_targets(
    context,
    target: list[str],
    current: dict[str, Any],
) -> list[str]:
    held = _held_symbols(context)
    submitted: list[str] = []
    just_sold = set(_state(context).get("just_sold", []))
    for symbol in target:
        if symbol in held or symbol in just_sold:
            continue
        bar = current.get(symbol)
        if bar is not None and _open_position(context, symbol, current):
            submitted.append(symbol)
    return submitted


def _execute_recovery_buying(context) -> None:
    target = _select_stocks(context, require_snapshot=True)
    _state(context)["sorted_stocks"] = target
    submitted = _buy_missing_targets(context, target, _current_bars(context))
    if submitted:
        _emit_decision(context, target, "recovery_buy")


def _check_limit_up_and_buy(context) -> None:
    state = _state(context)
    if state.get("risk_control_executed"):
        return
    current = _current_bars(context)
    sold: list[str] = []
    for symbol in list(state.get("high_limit_list", [])):
        bar = current.get(symbol)
        if symbol not in context.portfolio.positions or bar is None:
            continue
        limit_up = getattr(bar, "limit_up", None)
        if limit_up is not None and _bar_price(bar) < float(limit_up) - 0.005:
            if _close_position(context, symbol):
                sold.append(symbol)
                state["high_limit_list"].remove(symbol)
    if not state.get("today_trade_allowed", True) or not sold:
        return
    target = _select_stocks(context, require_snapshot=True)
    state["sorted_stocks"] = target
    submitted = _buy_missing_targets(context, target, current)
    if submitted or sold:
        _emit_decision(context, target, "limit_up_replacement")


def _emit_decision(context, target: list[str], reason: str) -> None:
    day = context.now.date().isoformat()
    decision = {
        "date": day,
        "reason": reason,
        "target": list(target),
        "holding": _held_symbols(context),
    }
    _state(context)["decision"] = decision
    context.emit_signal(
        "daily_decision",
        {
            "strategy": "performance_small_cap",
            "trading_date": day,
            "decision": "rebalance" if target else "risk_off",
            "target_symbols": list(target),
            "holding_symbols": _held_symbols(context),
            "reason": reason,
        },
        event_id=f"performance_small_cap:{day}:{reason}",
    )
