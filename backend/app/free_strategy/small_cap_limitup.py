"""涨停基因小市值策略的 TickFlow 原生实现。"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


STOCK_COUNT = 6
INITIAL_POOL_SIZE = 1_000
HISTORY_DAYS = 750
LIANBAN_DAYS = 500
ST_STATUS_DAYS = 120
FRESHNESS_EXCLUDE_PCT = 0.10
NO_TRADING_MONTHS = {1, 4}
STOPLOSS_LIMIT = 0.91
MARKET_STOPLOSS_LIMIT = 0.93
TRADE_CAPITAL_LIMIT = 130_000.0
INDUSTRY_DATA_ERROR = "涨停基因小市值策略缺少 EasyTDX 申万行业快照，无法执行行业去重"


def _state(context) -> dict[str, Any]:
    return context.state["small_cap_limitup"]


def _bar_price(bar: Any, field: str = "close") -> float:
    execution_price = getattr(bar, "execution_price", None)
    if callable(execution_price):
        return float(execution_price(field))
    raw = getattr(bar, f"raw_{field}", None)
    return float(raw if raw is not None else getattr(bar, field))


def _adjusted_price(bar: Any, field: str = "close") -> float:
    return float(getattr(bar, field))


def _held_symbols(context) -> list[str]:
    return [
        symbol
        for symbol, quantity in context.portfolio.positions.items()
        if float(quantity) > 0
    ]


def _available_quantity(context, symbol: str) -> float:
    available = getattr(context.portfolio, "available_positions", {})
    return float(available.get(symbol, context.portfolio.positions.get(symbol, 0.0)))


def _current_bars(context) -> dict[str, Any]:
    loader = getattr(context, "current_bars", None)
    return dict(loader()) if callable(loader) else {}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _instrument_records(context) -> list[dict[str, Any]]:
    return list(getattr(context, "instruments", lambda _asset=None: [])("stock"))


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


def _held_and_candidates(context, _timestamp: datetime) -> list[str]:
    return list(dict.fromkeys([
        *_held_symbols(context),
        *_state(context).get("stock_list_cache", []),
    ]))


def _is_weekly_rebalance_day(context, timestamp: datetime | None = None) -> bool:
    current = timestamp or context.now
    state = _state(context)
    cache = state.get("weekly_rebalance_check", {})
    if cache.get("date") == current.date().isoformat():
        return bool(cache.get("value"))
    week_start = current.date() - timedelta(days=current.weekday())
    symbols = [
        str(item.get("symbol") or "")
        for item in _instrument_records(context)
        if item.get("symbol")
    ]
    symbols.sort(key=lambda symbol: (symbol != "000001.SZ", symbol))
    history = context.history_batch(symbols[:16], count=5, timeframe="1d")
    trading_days = {
        bar.date
        for values in history.values()
        for bar in values
        if week_start <= bar.date < current.date()
    }
    result = len(trading_days) == 1
    state["weekly_rebalance_check"] = {
        "date": current.date().isoformat(),
        "value": result,
    }
    return result


def _weekly_selection_symbols(context, timestamp: datetime) -> list[str]:
    if _is_weekly_rebalance_day(context, timestamp) and timestamp.month not in NO_TRADING_MONTHS:
        return list(dict.fromkeys([
            *_held_symbols(context),
            *_selection_pool_symbols(context),
        ]))
    return _held_symbols(context)


def initialize(context) -> None:
    records = _instrument_records(context)
    symbols = [
        str(item["symbol"])
        for item in records
        if item.get("symbol") and bool(item.get("has_minute", True))
    ]
    context.set_universe(symbols)
    context.state.setdefault("small_cap_limitup", {
        "hold_list": [],
        "yesterday_high_limit": [],
        "target_list": [],
        "not_buy_again": [],
        "loss_black": {},
        "reason_to_sell": "",
        "no_trading_today": False,
        "no_trading_hold": False,
        "stock_list_cache_date": None,
        "stock_list_cache": [],
        "selection_scope_key": None,
        "selection_scope_symbols": [],
        "weekly_rebalance_check": {},
        "trade_capital_limit": TRADE_CAPITAL_LIMIT,
        "daily_reports": [],
        "decision": {},
    })
    context.schedule(_prepare_stock_list, "09:05", symbols=_held_and_candidates)
    context.schedule(_sell_stocks, "10:00", symbols=_held_and_candidates)
    context.schedule(_weekly_sell, "10:15", symbols=_weekly_selection_symbols)
    context.schedule(_weekly_buy, "10:30", symbols=_held_and_candidates)
    context.schedule(_trade_afternoon, "14:20", symbols=_afternoon_selection_symbols)
    context.schedule(_close_account, "14:50", symbols=_held_and_candidates)
    context.schedule(_trade_afternoon, "14:55", symbols=_afternoon_selection_symbols)
    context.log("涨停基因小市值策略已初始化：全市场日线选股，定时点按需读取分钟快照")


def after_trading_end(context) -> None:
    state = _state(context)
    report = {
        "date": context.now.date().isoformat(),
        "target": list(state.get("target_list", [])),
        "holdings": _held_symbols(context),
        "equity": float(context.portfolio.total_value),
        "cash": float(context.portfolio.cash),
        "decision": dict(state.get("decision", {})),
    }
    state["daily_reports"].append(report)
    if len(state["daily_reports"]) > 320:
        del state["daily_reports"][:-320]


def _is_limit_up(bar: Any) -> bool:
    limit_up = getattr(bar, "limit_up", None)
    return limit_up is not None and _bar_price(bar) >= float(limit_up) - 0.005


def _limit_price(reference: float, pct: float) -> float:
    return float(Decimal(str(reference * (1 + pct))).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    ))


def _price_limit_regime(bar: Any) -> bool | None:
    previous_close = float(getattr(bar, "previous_close", 0.0) or 0.0)
    if previous_close <= 0:
        return None
    upper = _limit_price(previous_close, 0.05)
    lower = _limit_price(previous_close, -0.05)
    high = _bar_price(bar, "high")
    low = _bar_price(bar, "low")
    if high > upper + 0.005 or low < lower - 0.005:
        return False
    if high >= upper - 0.005 or low <= lower + 0.005:
        return True
    return None


def _is_historical_st(values: list[Any]) -> bool:
    state = False
    st_observations = 0
    last_normal_index: int | None = None
    last_normal_date: date | None = None
    for index, bar in enumerate(values):
        regime = _price_limit_regime(bar)
        if regime is None:
            continue
        state = regime
        if regime:
            st_observations += 1
        else:
            st_observations = 0
            last_normal_index = index
            last_normal_date = getattr(bar, "date", None)
    if (
        not state
        and last_normal_index is not None
        and last_normal_date is not None
        and last_normal_date.month == 4
        and last_normal_date.day >= 20
        and len(values) - last_normal_index - 1 >= 40
    ):
        return True
    return state and st_observations >= 2


def _prepare_stock_list(context) -> None:
    state = _state(context)
    held = _held_symbols(context)
    state["hold_list"] = held
    history = context.history_batch(held, count=1, timeframe="1d") if held else {}
    state["yesterday_high_limit"] = [
        symbol for symbol in held
        if history.get(symbol) and _is_limit_up(history[symbol][-1])
    ]
    state["no_trading_today"] = context.now.month in NO_TRADING_MONTHS
    if not state["no_trading_today"]:
        state["no_trading_hold"] = False


def _previous_trading_date(context, records: list[dict[str, Any]]) -> date:
    for item in records:
        symbol = str(item.get("symbol") or "")
        if not symbol:
            continue
        rows = context.history_bars(symbol, count=1, timeframe="1d")
        if rows:
            return rows[-1].date
    return context.now.date() - timedelta(days=1)


def _loss_blacklisted(context, symbol: str) -> bool:
    raw = _state(context).get("loss_black", {}).get(symbol)
    if not raw:
        return False
    try:
        stopped_at = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    return context.now - stopped_at < timedelta(days=20)


def _selection_pool_symbols(context) -> list[str]:
    records = _instrument_records(context)
    previous_date = _previous_trading_date(context, records)
    state = _state(context)
    active_loss_black = tuple(sorted(
        symbol for symbol in state.get("loss_black", {})
        if _loss_blacklisted(context, symbol)
    ))
    cache_key = (previous_date.isoformat(), active_loss_black)
    if state.get("selection_scope_key") == cache_key:
        return list(state.get("selection_scope_symbols", []))

    cutoff = previous_date - timedelta(days=375)
    eligible_records = []
    for item in records:
        symbol = str(item.get("symbol") or "")
        code = symbol.split(".", 1)[0]
        listing_date = _parse_date(item.get("listing_date"))
        if not symbol or listing_date is None or listing_date > cutoff:
            continue
        if code.startswith(("4", "8", "68")) or symbol.endswith(".BJ"):
            continue
        name = _name_on(item, previous_date)
        if "ST" in name.upper() or "*" in name or "退" in name:
            continue
        if _loss_blacklisted(context, symbol):
            continue
        eligible_records.append(item)

    latest_history = context.history_batch(
        [str(item["symbol"]) for item in eligible_records],
        count=1,
        timeframe="1d",
    )
    candidates: list[tuple[float, str]] = []
    for item in eligible_records:
        symbol = str(item["symbol"])
        values = latest_history.get(symbol, [])
        if not values:
            continue
        latest = values[-1]
        previous_close = _bar_price(latest)
        total_shares = float(getattr(latest, "total_shares", 0.0) or 0.0)
        if previous_close <= 0 or total_shares <= 0:
            continue
        candidates.append((previous_close * total_shares, symbol))
    candidates.sort(key=lambda value: (value[0], value[1]))
    has_name_history = {
        str(item["symbol"]): bool(item.get("name_changes"))
        for item in eligible_records
    }
    symbols: list[str] = []
    for offset in range(0, len(candidates), INITIAL_POOL_SIZE):
        batch = [
            symbol
            for _market_cap, symbol in candidates[offset:offset + INITIAL_POOL_SIZE]
        ]
        status_symbols = [symbol for symbol in batch if not has_name_history[symbol]]
        status_history = (
            context.history_batch(
                status_symbols,
                count=ST_STATUS_DAYS,
                timeframe="1d",
            )
            if status_symbols else {}
        )
        for symbol in batch:
            if not has_name_history[symbol] and _is_historical_st(status_history.get(symbol, [])):
                continue
            symbols.append(symbol)
            if len(symbols) == INITIAL_POOL_SIZE:
                break
        if len(symbols) == INITIAL_POOL_SIZE:
            break
    state["selection_scope_key"] = cache_key
    state["selection_scope_symbols"] = list(symbols)
    return symbols


def _afternoon_selection_symbols(context, _timestamp: datetime) -> list[str]:
    return list(dict.fromkeys([
        *_held_symbols(context),
        *_selection_pool_symbols(context),
    ]))


def _eligible_market_records(context) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    records = _instrument_records(context)
    bars = _current_bars(context)
    state = _state(context)
    selection_scope_ready = state.get("selection_scope_key") is not None
    selection_scope = set(state.get("selection_scope_symbols", []))
    previous_date = _previous_trading_date(context, records)
    cutoff = previous_date - timedelta(days=375)
    held = set(_held_symbols(context))
    candidates: list[tuple[float, str, dict[str, Any], Any]] = []
    for item in records:
        symbol = str(item.get("symbol") or "")
        if selection_scope_ready and symbol not in selection_scope:
            continue
        code = symbol.split(".", 1)[0]
        name = _name_on(item, previous_date)
        listing_date = _parse_date(item.get("listing_date"))
        bar = bars.get(symbol)
        if not symbol or bar is None or listing_date is None or listing_date > cutoff:
            continue
        if code.startswith(("4", "8", "68")) or symbol.endswith(".BJ"):
            continue
        if "ST" in name.upper() or "*" in name or "退" in name:
            continue
        if bool(getattr(bar, "suspended", False)) or not bool(getattr(bar, "tradable", True)):
            continue
        if _loss_blacklisted(context, symbol):
            continue
        price = _bar_price(bar)
        limit_up = getattr(bar, "limit_up", None)
        limit_down = getattr(bar, "limit_down", None)
        if symbol not in held and limit_up is not None and price >= float(limit_up) - 0.005:
            continue
        if symbol not in held and limit_down is not None and price <= float(limit_down) + 0.005:
            continue
        previous_close = float(getattr(bar, "previous_close", 0.0) or 0.0)
        total_shares = float(getattr(bar, "total_shares", 0.0) or 0.0)
        if previous_close <= 0 or total_shares <= 0:
            continue
        candidates.append((previous_close * total_shares, symbol, item, bar))

    candidates.sort(key=lambda value: (value[0], value[1]))
    result: list[dict[str, Any]] = []
    for offset in range(0, len(candidates), INITIAL_POOL_SIZE):
        batch = candidates[offset:offset + INITIAL_POOL_SIZE]
        status_symbols = [
            symbol
            for _market_cap, symbol, item, _bar in batch
            if not item.get("name_changes")
        ]
        status_history = (
            context.history_batch(
                status_symbols,
                count=ST_STATUS_DAYS,
                timeframe="1d",
            )
            if status_symbols else {}
        )
        for market_cap, symbol, item, _bar in batch:
            if not item.get("name_changes") and _is_historical_st(status_history.get(symbol, [])):
                continue
            result.append({
                **item,
                "symbol": symbol,
                "market_cap": market_cap,
            })
            if len(result) == INITIAL_POOL_SIZE:
                return result, bars
    return result, bars


def _historical_st_mask(values: list[Any]) -> list[bool]:
    result = [False] * len(values)
    state = False
    observations = 0
    pending_start: int | None = None
    last_normal_index: int | None = None
    last_normal_date: date | None = None
    for index, bar in enumerate(values):
        regime = _price_limit_regime(bar)
        if regime is False:
            state = False
            observations = 0
            pending_start = None
            last_normal_index = index
            last_normal_date = getattr(bar, "date", None)
        elif regime is True:
            if observations == 0:
                pending_start = index
            observations += 1
            if not state and observations >= 2:
                state = True
                start = pending_start if pending_start is not None else index
                for pending_index in range(start, index + 1):
                    result[pending_index] = True
        elif (
            not state
            and last_normal_index is not None
            and last_normal_date is not None
            and last_normal_date.month == 4
            and last_normal_date.day >= 20
            and index - last_normal_index >= 40
        ):
            state = True
            for pending_index in range(last_normal_index + 1, index + 1):
                result[pending_index] = True
        if state:
            result[index] = True
    return result


def _history_limit_flags(
    values: list[Any],
    *,
    infer_historical_st: bool = True,
) -> list[bool]:
    five_pct_flags = [
        (
            (previous_close := float(getattr(bar, "previous_close", 0.0) or 0.0)) > 0
            and math.isclose(
                _bar_price(bar),
                _limit_price(previous_close, 0.05),
                rel_tol=0.0,
                abs_tol=0.005,
            )
        )
        for bar in values
    ]
    historical_st = (
        _historical_st_mask(values)
        if infer_historical_st else [False] * len(values)
    )
    result = []
    for index, bar in enumerate(values):
        close = _bar_price(bar)
        previous_close = float(getattr(bar, "previous_close", 0.0) or 0.0)
        limit_up = getattr(bar, "limit_up", None)
        is_limit = _is_limit_up(bar) or (
            historical_st[index] and five_pct_flags[index]
        )
        if limit_up is None and previous_close > 0:
            is_limit = any(
                math.isclose(
                    close,
                    _limit_price(previous_close, pct),
                    rel_tol=0.0,
                    abs_tol=0.005,
                )
                for pct in (0.05, 0.10, 0.20, 0.30)
            )
        result.append(is_limit)
    return result


def _rank_history_candidates(
    history: dict[str, list[Any]],
    bars: dict[str, Any],
    reliable_limit_symbols: set[str] | None = None,
) -> list[str]:
    reliable = reliable_limit_symbols or set()
    eligible: list[tuple[str, int, list[Any], list[bool]]] = []
    for symbol, values in history.items():
        values = list(values)[-HISTORY_DAYS:]
        flags = _history_limit_flags(
            values,
            infer_historical_st=symbol not in reliable,
        )
        recent = flags[-LIANBAN_DAYS:]
        if not any(recent[index] and recent[index - 1] for index in range(1, len(recent))):
            continue
        latest = max(index for index, value in enumerate(recent) if value)
        eligible.append((symbol, len(recent) - 1 - latest, values, flags))
    eligible.sort(key=lambda item: (item[1], item[0]))
    eligible = eligible[int(len(eligible) * FRESHNESS_EXCLUDE_PCT):]

    ranked: list[tuple[float, str]] = []
    for symbol, _freshness, values, flags in eligible:
        latest_limit = max(index for index, value in enumerate(flags) if value)
        start_low = None
        for bar in reversed(values[:latest_limit + 1]):
            if _adjusted_price(bar) < _adjusted_price(bar, "open"):
                start_low = _adjusted_price(bar, "low")
                break
        current = bars.get(symbol)
        if current is None or not start_low or start_low <= 0:
            continue
        current_price = _adjusted_price(current)
        if current_price > 0:
            ranked.append((current_price / start_low, symbol))
    ranked.sort(key=lambda item: (item[0], item[1]))
    return [symbol for _bias, symbol in ranked]


def _select_industries(
    ranked: list[str],
    records: list[dict[str, Any]],
) -> list[str]:
    industry_by_symbol = {
        str(item.get("symbol")): str(item.get("industry_sw") or "").strip()
        for item in records
    }
    missing = [symbol for symbol in ranked if not industry_by_symbol.get(symbol)]
    if missing:
        raise ValueError(
            f"{INDUSTRY_DATA_ERROR}；缺失标的: {', '.join(missing[:3])}"
        )
    selected: list[str] = []
    seen: set[str] = set()
    for symbol in ranked:
        industry = industry_by_symbol[symbol]
        if industry in seen:
            continue
        seen.add(industry)
        selected.append(symbol)
        if len(selected) == 10:
            break
    return selected[:STOCK_COUNT * 2]


def _get_stock_list(context) -> list[str]:
    state = _state(context)
    records = _instrument_records(context)
    if not any(str(item.get("industry_sw") or "").strip() for item in records):
        raise ValueError(INDUSTRY_DATA_ERROR)
    previous_date = _previous_trading_date(context, records)
    cache_date = previous_date.isoformat()
    if state.get("stock_list_cache_date") == cache_date:
        return list(state.get("stock_list_cache", []))
    initial, bars = _eligible_market_records(context)
    symbols = [str(item["symbol"]) for item in initial]
    history = context.history_batch(symbols, count=HISTORY_DAYS, timeframe="1d")
    reliable_limit_symbols = {
        str(item["symbol"])
        for item in initial
        if item.get("name_changes")
    }
    ranked = _rank_history_candidates(history, bars, reliable_limit_symbols)
    final = _select_industries(ranked, initial)
    state["stock_list_cache_date"] = cache_date
    state["stock_list_cache"] = list(final)
    context.log(f"小市值候选：{final}")
    return final


def _close_position(context, symbol: str) -> bool:
    quantity = float(context.portfolio.positions.get(symbol, 0.0))
    bar = _current_bars(context).get(symbol)
    if quantity <= 0 or _available_quantity(context, symbol) <= 0 or bar is None:
        return False
    price = _bar_price(bar)
    limit_down = getattr(bar, "limit_down", None)
    if bool(getattr(bar, "suspended", False)) or not bool(getattr(bar, "tradable", True)):
        return False
    if limit_down is not None and price <= float(limit_down) + 0.005:
        return False
    context.order_target(symbol, 0)
    return True


def _open_position(context, symbol: str, target_value: float) -> bool:
    bar = _current_bars(context).get(symbol)
    if bar is None or bool(getattr(bar, "suspended", False)):
        return False
    price = _bar_price(bar)
    if price <= 0:
        return False
    limit_up = getattr(bar, "limit_up", None)
    if limit_up is not None and price >= float(limit_up) - 0.005:
        return False
    target_quantity = math.floor(target_value / price / 100) * 100
    if target_quantity <= 0:
        target_quantity = 100
    current = float(context.portfolio.positions.get(symbol, 0.0))
    if target_quantity <= current:
        return False
    context.order_target(symbol, target_quantity)
    return True


def _buy_security(
    context,
    target_list: list[str],
    *,
    held_after_sells: list[str] | None = None,
) -> list[str]:
    held = list(held_after_sells if held_after_sells is not None else _held_symbols(context))
    slots = max(0, STOCK_COUNT - len(held))
    if slots == 0:
        return []
    target_value = float(context.portfolio.total_value) / STOCK_COUNT
    submitted: list[str] = []
    held_set = set(held)
    for symbol in target_list:
        if symbol in held_set or symbol in submitted:
            continue
        if _open_position(context, symbol, target_value):
            submitted.append(symbol)
        if len(submitted) >= slots:
            break
    return submitted


def _weekly_sell(context) -> None:
    if not _is_weekly_rebalance_day(context) or _state(context).get("no_trading_today"):
        return
    state = _state(context)
    state["not_buy_again"] = []
    target = _get_stock_list(context)
    state["target_list"] = target
    current = _current_bars(context)
    yesterday_limit = set(state.get("yesterday_high_limit", []))
    for symbol in list(state.get("hold_list", [])):
        bar = current.get(symbol)
        if symbol in target or symbol in yesterday_limit or bar is None:
            continue
        limit_up = getattr(bar, "limit_up", None)
        if limit_up is None or _bar_price(bar) < float(limit_up) - 0.005:
            _close_position(context, symbol)


def _weekly_buy(context) -> None:
    if not _is_weekly_rebalance_day(context) or _state(context).get("no_trading_today"):
        return
    state = _state(context)
    state["not_buy_again"] = []
    target = _get_stock_list(context)
    state["target_list"] = target
    submitted = _buy_security(context, target)
    state["not_buy_again"] = list(dict.fromkeys([*_held_symbols(context), *submitted]))
    _emit_decision(context, target, "weekly_rebalance")


def _market_down_ratio(context) -> float:
    symbols = [
        str(item["symbol"])
        for item in _instrument_records(context)
        if str(item.get("symbol") or "").startswith("002")
    ]
    history = context.history_batch(symbols, count=1, timeframe="1d")
    ratios = []
    for values in history.values():
        if not values:
            continue
        previous_open = _bar_price(values[-1], "open")
        previous_close = _bar_price(values[-1])
        if previous_open > 0 and previous_close > 0:
            ratios.append(previous_close / previous_open)
    return sum(ratios) / len(ratios) if ratios else 1.0


def _sell_stocks(context) -> None:
    if _state(context).get("no_trading_today"):
        return
    state = _state(context)
    if _market_down_ratio(context) <= MARKET_STOPLOSS_LIMIT:
        state["reason_to_sell"] = "stoploss"
        for symbol in _held_symbols(context):
            _close_position(context, symbol)
        return
    bars = _current_bars(context)
    for symbol in _held_symbols(context):
        bar = bars.get(symbol)
        avg_cost = float(context.portfolio.avg_cost.get(symbol, 0.0))
        if bar is None or avg_cost <= 0 or _bar_price(bar) >= avg_cost * STOPLOSS_LIMIT:
            continue
        if _close_position(context, symbol):
            state["reason_to_sell"] = "stoploss"
            state["loss_black"][symbol] = context.now.isoformat()
            state["stock_list_cache_date"] = None


def _turnover_sell_symbols(context, held: list[str]) -> list[str]:
    current = _current_bars(context)
    daily = context.history_batch(held, count=20, timeframe="1d") if held else {}
    result: list[str] = []
    for symbol in held:
        bar = current.get(symbol)
        if bar is None or _available_quantity(context, symbol) <= 0:
            continue
        limit_up = getattr(bar, "limit_up", None)
        if limit_up is not None and _bar_price(bar) >= float(limit_up) * 0.97:
            continue
        float_shares = float(getattr(bar, "float_shares", 0.0) or 0.0)
        if float_shares <= 0:
            continue
        session_volume = getattr(bar, "session_volume", None)
        if session_volume is None:
            minute_rows = context.history_bars(symbol, count=260, timeframe="1m")
            session_volume = sum(
                float(item.volume) for item in minute_rows if item.date == context.now.date()
            )
        intraday_volume = float(session_volume)
        turnover = intraday_volume * 100 / float_shares
        daily_rows = daily.get(symbol, [])
        average = (
            sum(float(item.volume) * 100 / float_shares for item in daily_rows) / len(daily_rows)
            if daily_rows else 0.0
        )
        if average <= 0:
            continue
        if average < 0.003 or (turnover > 0.10 and turnover / average > 2):
            result.append(symbol)
    return result


def _trade_afternoon(context) -> None:
    state = _state(context)
    if state.get("no_trading_today"):
        return
    held = _held_symbols(context)
    bars = _current_bars(context)
    sold: list[str] = []
    for symbol in state.get("yesterday_high_limit", []):
        bar = bars.get(symbol)
        if symbol not in held or bar is None:
            continue
        limit_up = getattr(bar, "limit_up", None)
        if limit_up is not None and _bar_price(bar) < float(limit_up) - 0.005:
            if _close_position(context, symbol):
                sold.append(symbol)
    for symbol in _turnover_sell_symbols(context, [s for s in held if s not in sold]):
        if _close_position(context, symbol):
            sold.append(symbol)
    if not sold:
        state["reason_to_sell"] = ""
        return
    state["reason_to_sell"] = "limitup"
    remaining = [symbol for symbol in held if symbol not in sold]
    blocked = set(state.get("not_buy_again", []))
    target = [symbol for symbol in _get_stock_list(context) if symbol not in blocked]
    submitted = _buy_security(context, target[:STOCK_COUNT], held_after_sells=remaining)
    state["not_buy_again"] = list(dict.fromkeys([*state.get("not_buy_again", []), *submitted]))
    state["reason_to_sell"] = ""
    _emit_decision(context, target, "afternoon_replacement")


def _close_account(context) -> None:
    state = _state(context)
    if not state.get("no_trading_today") or state.get("no_trading_hold"):
        return
    for symbol in _held_symbols(context):
        _close_position(context, symbol)
    state["no_trading_hold"] = True


def _emit_decision(context, target: list[str], reason: str) -> None:
    day = context.now.date().isoformat()
    holdings = _held_symbols(context)
    decision = {
        "date": day,
        "reason": reason,
        "target": list(target),
        "holding": holdings,
    }
    _state(context)["decision"] = decision
    context.emit_signal(
        "daily_decision",
        {
            "strategy": "small_cap_limitup",
            "trading_date": day,
            "decision": "rebalance",
            "target_symbols": list(target),
            "holding_symbols": holdings,
            "reason": reason,
        },
        event_id=f"small_cap_limitup:{day}:{reason}",
    )
