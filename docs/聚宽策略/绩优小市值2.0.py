# -*- coding: utf-8 -*-
"""绩优小市值 2.0：聚宽可直接回测版本。

核心规则与 TickFlow 的 performance_small_cap revision 12 对齐：
1. 近一年现金分红率前 25%，再过滤正利润、正 ROE/ROA 和营业收入门槛。
2. 按总市值升序取 5 只，非持仓股票收盘价必须低于 6 元。
3. 月初调仓；涨停打开后在 14:00 卖出并补入下一只候选。
4. 400 只最小市值股票均价高于 18.72 时清仓，连续 5 个交易日后恢复。
5. 399303 指数出现 MACD 顶背离时清仓。

聚宽没有 TickFlow 的大小盘成交占比缓存，因此这里使用回测中已经走过的
5 日成交额滚动序列计算 97%/70% 分位阈值；该扩展信号可通过
g.enable_style_liquidity_timing 关闭，不影响主策略回测。
"""

import datetime

import numpy as np
import pandas as pd
from jqdata import *


def initialize(context):
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    set_benchmark("399303.XSHE")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0001,
            close_commission=0.0001,
            min_commission=5,
        ),
        type="stock",
    )

    g.stock_num = 5
    g.max_stock_price = 6.0
    g.smallcap_index_size = 400
    g.smallcap_index_threshold = 18.72
    g.ban_trade_period = 5
    g.index_symbol = "399303.XSHE"
    g.index_history_bars = (12 + 26 + 9) * 5

    # 2.0 的扩展择时。聚宽全市场成交额扫描较重，必要时可改为 False。
    g.enable_style_liquidity_timing = True
    g.style_entry_quantile = 0.97
    g.style_recovery_quantile = 0.70
    g.style_min_history_days = 250
    g.style_ratio_history = []
    g.style_signal_cache = {}
    g.style_liquidity_active = False
    g.style_liquidity_signal = None

    g.sorted_stocks = []
    g.just_sold = []
    g.high_limit_list = []
    g.risk_control_executed = False
    g.today_trade_allowed = True
    g.smallcap_index_value = None
    g.ban_trade_start_date = None
    g.first_rebalance_done = False
    g.candidate_cache_key = None
    g.candidate_cache = []
    g.selection_cache_key = None
    g.selection_cache = []

    log.set_level("order", "error")
    log.set_level("system", "error")
    log.set_level("history", "error")

    run_daily(prepare_stock_list, "09:00:00")
    run_daily(analyze_smallcap_index, "09:30:05")
    run_daily(check_smallcap_timing, "09:30:10")
    run_daily(dapan, "09:30:15")
    run_monthly(monthly_adjustment, 1, "09:30:20")
    run_daily(check_limit_up_and_buy, "14:00:00")


def prepare_stock_list(context):
    g.just_sold = []
    g.risk_control_executed = False
    g.candidate_cache_key = None
    g.selection_cache_key = None

    g.high_limit_list = []
    holdings = list(context.portfolio.positions.keys())
    if not holdings:
        return
    prices = get_price(
        holdings,
        end_date=context.previous_date,
        frequency="daily",
        fields=["close", "high_limit"],
        count=1,
        panel=False,
        fill_paused=False,
    )
    if prices is None or prices.empty:
        return
    g.high_limit_list = prices.loc[
        prices["close"] >= prices["high_limit"] - 0.005, "code"
    ].tolist()


def monthly_adjustment(context):
    if not _should_monthly_adjust(context):
        return
    g.first_rebalance_done = True
    if not g.today_trade_allowed:
        return

    target = select_stocks(context, require_snapshot=True)
    g.sorted_stocks = target
    current_data = get_current_data()

    # 先卖后买，避免用尚未成交的卖出收入预估现金。
    for security in list(context.portfolio.positions.keys()):
        if security not in target:
            close_position(context, security)
    buy_missing_targets(context, target, current_data)


def _should_monthly_adjust(context):
    if not g.first_rebalance_done:
        return True
    previous = context.previous_date
    current = context.current_dt.date()
    return previous.month != current.month or previous.year != current.year


def select_stocks(context, require_snapshot=False):
    previous_date = context.previous_date
    cache_key = (str(previous_date), bool(require_snapshot), context.current_dt.strftime("%H:%M"))
    if g.selection_cache_key == cache_key:
        return list(g.selection_cache)

    candidates = candidate_symbols(context)
    if require_snapshot:
        current_data = get_current_data()
        candidates = [
            security
            for security in candidates
            if _valid_current_stock(current_data, security)
        ]
    selected = candidates[: g.stock_num]
    g.selection_cache_key = cache_key
    g.selection_cache = list(selected)
    return selected


def candidate_symbols(context):
    previous_date = context.previous_date
    cache_key = str(previous_date)
    if g.candidate_cache_key == cache_key:
        return list(g.candidate_cache)

    stocks = get_all_securities("stock", date=previous_date)
    if stocks is None or stocks.empty:
        g.candidate_cache_key = cache_key
        g.candidate_cache = []
        return []

    stocks = stocks[
        ~stocks.index.to_series().map(_is_kcbj)
    ]
    if stocks.empty:
        return []
    cutoff = previous_date - datetime.timedelta(days=240)
    stocks = stocks[stocks["start_date"].map(lambda day: _listed_by(day, cutoff))]
    stocks = stocks[stocks["display_name"].map(_valid_name)]
    universe = list(stocks.index)

    # 先做分红率前 25% 筛选，再做绩优财务筛选，与 2.0 原策略顺序一致。
    universe = dividend_ratio_top_quartile(context, universe, previous_date)
    universe = financially_qualified(universe, previous_date)
    if not universe:
        g.candidate_cache_key = cache_key
        g.candidate_cache = []
        return []

    market_caps = market_cap_map(universe, previous_date)
    prices = close_map(universe, previous_date)
    holdings = set(context.portfolio.positions.keys())
    ranked = []
    for security in universe:
        market_cap = market_caps.get(security)
        price = prices.get(security)
        if market_cap is None or price is None or price <= 0:
            continue
        if security not in holdings and price >= g.max_stock_price:
            continue
        ranked.append((float(market_cap), security))
    ranked.sort(key=lambda item: (item[0], item[1]))
    result = [security for _cap, security in ranked]
    g.candidate_cache_key = cache_key
    g.candidate_cache = list(result)
    return result


def dividend_ratio_top_quartile(context, stocks, previous_date):
    if not stocks:
        return []
    time0 = previous_date - datetime.timedelta(days=365)
    caps = get_fundamentals(
        query(valuation.code, valuation.market_cap).filter(
            valuation.code.in_(stocks)
        ),
        date=previous_date,
    )
    if caps is None or caps.empty:
        return []
    caps = caps.dropna(subset=["code", "market_cap"]).set_index("code")
    if caps.empty:
        return []

    dividends = finance.run_query(
        query(
            finance.STK_XR_XD.code,
            finance.STK_XR_XD.bonus_amount_rmb,
        ).filter(
            finance.STK_XR_XD.a_registration_date >= time0,
            finance.STK_XR_XD.a_registration_date <= previous_date,
            finance.STK_XR_XD.code.in_(list(caps.index)),
        )
    )
    if dividends is None or dividends.empty:
        return []
    dividends = dividends.groupby("code")["bonus_amount_rmb"].sum().to_frame("dividend")
    ranked = dividends.join(caps[["market_cap"]], how="inner")
    ranked = ranked[(ranked["dividend"] > 0) & (ranked["market_cap"] > 0)]
    if ranked.empty:
        return []
    ranked["dividend_ratio"] = ranked["dividend"] / 1e8 / ranked["market_cap"]
    ranked = ranked.sort_values(
        ["dividend_ratio"], ascending=False,
    )
    cutoff = int(len(ranked) * 0.25)
    return list(ranked.index[:cutoff])


def financially_qualified(stocks, previous_date):
    if not stocks:
        return []
    df = get_fundamentals(
        query(valuation.code).filter(
            valuation.code.in_(stocks),
            income.np_parent_company_owners > 0,
            income.net_profit > 0,
            income.operating_revenue > 1e8,
            indicator.roe > 0,
            indicator.roa > 0,
        ),
        date=previous_date,
    )
    if df is None or df.empty:
        return []
    return list(df["code"])


def market_cap_map(stocks, previous_date):
    if not stocks:
        return {}
    df = get_valuation(
        stocks,
        end_date=previous_date,
        count=1,
        fields=["market_cap"],
    )
    if df is None or df.empty:
        return {}
    return {
        row.code: float(row.market_cap)
        for row in df.itertuples()
        if pd.notna(row.market_cap) and float(row.market_cap) > 0
    }


def close_map(stocks, previous_date):
    if not stocks:
        return {}
    df = get_price(
        stocks,
        end_date=previous_date,
        frequency="daily",
        fields=["close"],
        count=1,
        panel=False,
        fill_paused=False,
    )
    if df is None or df.empty:
        return {}
    return {
        row.code: float(row.close)
        for row in df.itertuples()
        if pd.notna(row.close) and float(row.close) > 0
    }


def _valid_current_stock(current_data, security):
    try:
        data = current_data[security]
        return (
            not data.paused
            and not data.is_st
            and "ST" not in data.name.upper()
            and "*" not in data.name
            and "退" not in data.name
            and data.last_price > 0
            and data.last_price > data.low_limit
            and data.last_price < data.high_limit
        )
    except Exception:
        return False


def open_position(context, security, value):
    order = order_target_value(security, value)
    return order is not None and getattr(order, "filled", 0) > 0


def close_position(context, security):
    position = context.portfolio.positions.get(security)
    if position is None:
        return False
    order = order_target_value(security, 0)
    if security not in g.just_sold:
        g.just_sold.append(security)
    return order is not None and getattr(order, "filled", 0) > 0


def buy_missing_targets(context, target, current_data):
    held = set(context.portfolio.positions.keys())
    just_sold = set(g.just_sold)
    candidates = [
        security for security in target
        if security not in held and security not in just_sold
        and _valid_current_stock(current_data, security)
    ]
    slots = min(len(candidates), max(0, g.stock_num - len(held)))
    cursor = 0
    while slots > 0 and context.portfolio.available_cash > 0 and cursor < len(candidates):
        candidate = candidates[cursor]
        cursor += 1
        value = context.portfolio.available_cash / float(slots)
        if open_position(context, candidate, value):
            slots -= 1
        else:
            # 委托失败时跳过该股，避免在同一回调里无限重试。
            slots = min(slots, len(candidates))


def check_limit_up_and_buy(context):
    if g.risk_control_executed:
        return
    current_data = get_current_data()
    sold = []
    for security in list(g.high_limit_list):
        if security not in context.portfolio.positions:
            continue
        data = _current_data_for(current_data, security)
        if data is None or data.last_price >= data.high_limit - 0.005:
            continue
        if close_position(context, security):
            sold.append(security)
            g.high_limit_list.remove(security)

    if sold and g.today_trade_allowed:
        target = select_stocks(context, require_snapshot=True)
        g.sorted_stocks = target
        buy_missing_targets(context, target, current_data)


def analyze_smallcap_index(context):
    g.smallcap_index_value = calculate_smallcap_index(context)


def calculate_smallcap_index(context):
    previous_date = context.previous_date
    stocks = get_all_securities("stock", date=previous_date)
    if stocks is None or stocks.empty:
        return None
    cutoff = previous_date - datetime.timedelta(days=240)
    stocks = stocks[
        stocks["start_date"].map(lambda day: _listed_by(day, cutoff))
        & ~stocks.index.to_series().map(_is_kcbj)
        & stocks["display_name"].map(_valid_name)
    ]
    if stocks.empty:
        return None
    symbols = list(stocks.index)
    caps = market_cap_map(symbols, previous_date)
    ranked = sorted(caps.items(), key=lambda item: (item[1], item[0]))
    symbols = [security for security, _cap in ranked[: g.smallcap_index_size]]
    prices = close_map(symbols, previous_date)
    values = [price for price in prices.values() if price > 0]
    return round(float(np.mean(values)), 4) if values else None


def check_smallcap_timing(context):
    if g.risk_control_executed:
        return

    style_signal = calculate_style_liquidity_signal(context)
    if style_signal is not None:
        apply_style_liquidity_timing(context, style_signal)
        return

    ban_ended = ban_period_ended(context)
    value = g.smallcap_index_value
    if value is None:
        return
    if value > g.smallcap_index_threshold:
        for security in list(context.portfolio.positions.keys()):
            close_position(context, security)
        g.today_trade_allowed = False
        g.risk_control_executed = True
        g.ban_trade_start_date = context.current_dt.date()
    else:
        g.today_trade_allowed = True
        if ban_ended:
            execute_recovery_buying(context)


def ban_period_ended(context):
    if g.ban_trade_start_date is None:
        return False
    days = get_trade_days(
        start_date=g.ban_trade_start_date,
        end_date=context.current_dt.date(),
    )
    if len(days) - 1 >= g.ban_trade_period:
        g.ban_trade_start_date = None
        return True
    return False


def execute_recovery_buying(context):
    target = select_stocks(context, require_snapshot=True)
    g.sorted_stocks = target
    buy_missing_targets(context, target, get_current_data())


def calculate_style_liquidity_signal(context):
    if not g.enable_style_liquidity_timing:
        return None
    previous_date = context.previous_date
    cache_key = str(previous_date)
    if cache_key in g.style_signal_cache:
        return g.style_signal_cache[cache_key]

    try:
        ratio = calculate_style_liquidity_ratio(previous_date)
    except Exception as exc:
        log.warn("大小盘成交占比计算失败，回退微盘指数风控: %s" % exc)
        g.style_signal_cache[cache_key] = None
        return None
    if ratio is None:
        g.style_signal_cache[cache_key] = None
        return None

    history = list(g.style_ratio_history)
    risk_off = g.style_liquidity_active
    entry_threshold = None
    recovery_threshold = None
    if len(history) >= g.style_min_history_days:
        entry_threshold = float(np.quantile(history, g.style_entry_quantile))
        recovery_threshold = float(np.quantile(history, g.style_recovery_quantile))
        if not risk_off and ratio >= entry_threshold:
            risk_off = True
        elif risk_off and ratio <= recovery_threshold:
            risk_off = False
    g.style_ratio_history.append(float(ratio))
    # 保留最近 10 年交易日，避免长回测状态无限增长。
    if len(g.style_ratio_history) > 2500:
        del g.style_ratio_history[:-2500]

    signal = {
        "date": cache_key,
        "risk_off": risk_off,
        "cap_ratio": float(ratio),
        "entry_threshold": entry_threshold,
        "recovery_threshold": recovery_threshold,
    }
    g.style_signal_cache[cache_key] = signal
    return signal


def calculate_style_liquidity_ratio(previous_date):
    stocks = get_all_securities("stock", date=previous_date)
    if stocks is None or stocks.empty:
        return None
    stocks = stocks[
        ~stocks.index.to_series().map(_is_kcbj)
        & stocks["display_name"].map(_valid_name)
    ]
    symbols = list(stocks.index)
    if not symbols:
        return None

    caps = get_valuation(
        symbols,
        end_date=previous_date,
        count=1,
        fields=["market_cap"],
    )
    amounts = get_price(
        symbols,
        end_date=previous_date,
        frequency="daily",
        count=5,
        fields=["money"],
        panel=False,
        fill_paused=False,
    )
    if caps is None or caps.empty or amounts is None or amounts.empty:
        return None
    caps = caps.dropna(subset=["code", "market_cap"])
    amounts = amounts.dropna(subset=["code", "money"])
    amount_map = amounts.groupby("code")["money"].sum()
    rows = [
        (row.code, float(row.market_cap), float(amount_map.get(row.code, 0)))
        for row in caps.itertuples()
        if float(row.market_cap) > 0 and float(amount_map.get(row.code, 0)) > 0
    ]
    if len(rows) < 20:
        return None
    rows.sort(key=lambda item: (item[1], item[0]))
    group_size = max(1, int(len(rows) * 0.1))
    large_amount = sum(item[2] for item in rows[-group_size:])
    small_amount = sum(item[2] for item in rows[:group_size])
    if small_amount <= 0:
        return None
    return large_amount / small_amount


def apply_style_liquidity_timing(context, signal):
    was_active = g.style_liquidity_active
    risk_off = bool(signal.get("risk_off", False))
    g.style_liquidity_active = risk_off
    g.style_liquidity_signal = signal
    if risk_off:
        if not was_active:
            for security in list(context.portfolio.positions.keys()):
                close_position(context, security)
        g.today_trade_allowed = False
        g.risk_control_executed = True
    else:
        g.today_trade_allowed = True
        if was_active:
            execute_recovery_buying(context)


def dapan(context):
    if g.risk_control_executed:
        return
    if g.ban_trade_start_date is not None and not g.today_trade_allowed:
        if not ban_period_ended(context):
            return

    top_divergence, _bottom_divergence = detect_divergences(g.index_symbol, context)
    if top_divergence:
        g.today_trade_allowed = False
        g.risk_control_executed = True
        g.ban_trade_start_date = context.current_dt.date()
        for security in list(context.portfolio.positions.keys()):
            close_position(context, security)


def detect_divergences(stock, context):
    rows = attribute_history(
        stock,
        g.index_history_bars,
        fields=["close"],
        skip_paused=True,
        df=True,
    )
    if rows is None or len(rows) < g.index_history_bars:
        return False, False
    close = rows["close"].astype(float)
    dif, _dea, macd = macd_values(close)
    dead = [
        i for i in range(1, len(macd))
        if macd.iloc[i] < 0 < macd.iloc[i - 1]
    ]
    gold = [
        i for i in range(1, len(macd))
        if macd.iloc[i] > 0 > macd.iloc[i - 1]
    ]
    top = False
    bottom = False
    if len(dead) >= 2:
        previous, current = dead[-2], dead[-1]
        if (
            close.iloc[previous] < close.iloc[current]
            and dif.iloc[previous] > dif.iloc[current] > 0
            and macd.iloc[-2] > 0 > macd.iloc[-1]
        ):
            top = dif.iloc[-10:].mean() < dif.iloc[-20:-10].mean()
    if len(gold) >= 2:
        previous, current = gold[-2], gold[-1]
        if (
            close.iloc[previous] > close.iloc[current]
            and dif.iloc[previous] < dif.iloc[current] < 0
            and macd.iloc[-2] < 0 < macd.iloc[-1]
        ):
            bottom = dif.iloc[-10:].mean() > dif.iloc[-20:-10].mean()
    return bool(top), bool(bottom)


def macd_values(close, short=12, long=26, signal=9):
    fast = close.ewm(span=short, min_periods=short - 1, adjust=False).mean()
    slow = close.ewm(span=long, min_periods=long - 1, adjust=False).mean()
    dif = fast - slow
    dea = dif.ewm(span=signal, min_periods=signal - 1, adjust=False).mean()
    return dif, dea, (dif - dea) * 2


def _is_kcbj(security):
    code = str(security).split(".")[0]
    return code.startswith(("4", "8", "68"))


def _valid_name(name):
    name = str(name or "")
    upper = name.upper()
    return "ST" not in upper and "*" not in name and "退" not in name


def _listed_by(value, cutoff):
    try:
        return pd.Timestamp(value).date() <= cutoff
    except (TypeError, ValueError):
        return False


def _current_data_for(current_data, security):
    try:
        return current_data[security]
    except Exception:
        return None


def after_trading_end(context):
    if context.current_dt.day == 1 or g.style_liquidity_signal is not None:
        log.info(
            "绩优小市值2.0 | 持仓=%s | 微盘均价=%s | 风控=%s | 成交占比=%s"
            % (
                list(context.portfolio.positions.keys()),
                g.smallcap_index_value,
                not g.today_trade_allowed,
                g.style_liquidity_signal,
            )
        )
