"""五福 ETF 策略的 TickFlow 自由策略适配。

聚宽原版依赖指数、ETF NAV 与行业分类数据。本实现保留其交易核心：全 ETF
池、三状态、加权动量、成交量与趋势过滤、相关性守卫、防频换、午盘卖买分离、
分钟止损和组合回撤风控。TickFlow 尚未提供 ETF NAV，溢价过滤按原计划显式跳过。
"""
from __future__ import annotations

import math
from typing import Any


WUFU_ETF_POOL = [
    "159206.SZ", "159218.SZ", "159227.SZ", "159256.SZ", "159323.SZ", "159326.SZ",
    "159363.SZ", "159502.SZ", "159509.SZ", "159516.SZ", "159518.SZ", "159529.SZ",
    "159566.SZ", "159583.SZ", "159605.SZ", "159611.SZ", "159638.SZ", "159667.SZ",
    "159732.SZ", "159755.SZ", "159766.SZ", "159819.SZ", "159825.SZ", "159840.SZ",
    "159851.SZ", "159852.SZ", "159865.SZ", "159869.SZ", "159870.SZ", "159883.SZ",
    "159892.SZ", "159915.SZ", "159920.SZ", "159928.SZ", "159949.SZ", "159967.SZ",
    "159980.SZ", "159981.SZ", "159985.SZ", "159992.SZ", "159995.SZ", "159998.SZ",
    "161226.SZ", "164824.SZ", "501018.SH", "510050.SH", "510300.SH", "510500.SH",
    "510760.SH", "510880.SH", "510900.SH", "511220.SH", "511380.SH", "512010.SH",
    "512050.SH", "512070.SH", "512100.SH", "512170.SH", "512200.SH", "512400.SH",
    "512480.SH", "512660.SH", "512670.SH", "512690.SH", "512710.SH", "512800.SH",
    "512880.SH", "512890.SH", "512980.SH", "513030.SH", "513080.SH", "513090.SH",
    "513100.SH", "513120.SH", "513180.SH", "513190.SH", "513290.SH", "513310.SH",
    "513330.SH", "513350.SH", "513360.SH", "513400.SH", "513500.SH", "513520.SH",
    "513630.SH", "513730.SH", "513750.SH", "513920.SH", "513970.SH", "515030.SH",
    "515050.SH", "515120.SH", "515170.SH", "515210.SH", "515220.SH", "515400.SH",
    "515790.SH", "515880.SH", "515980.SH", "516150.SH", "516160.SH", "516190.SH",
    "516510.SH", "516520.SH", "517520.SH", "518880.SH", "520830.SH", "560860.SH",
    "561330.SH", "561360.SH", "561980.SH", "562500.SH", "562590.SH", "562800.SH",
    "563300.SH", "588080.SH", "588170.SH", "588200.SH", "588220.SH", "588790.SH",
]
DEFENSIVE_ETF = "511880.SH"
NO_TICKFLOW_MINUTE = ("161226.SZ", "164824.SZ", "501018.SH")
WUFU_MINUTE_POOL = [symbol for symbol in WUFU_ETF_POOL if symbol not in NO_TICKFLOW_MINUTE]
REGIME_PROXIES = ["510300.SH", "510500.SH", "159915.SZ", "512100.SH", "563300.SH", "510050.SH"]


def _state(context) -> dict[str, Any]:
    return context.state["five_fortunes"]


def initialize(context) -> None:
    context.state.setdefault("five_fortunes", {
        "daily": {},
        "intraday": {"date": None, "close": {}, "volume": {}, "amount": {}},
        "regime": "震荡期",
        "regime_pending": None,
        "regime_pending_days": 0,
        "target": [],
        "candidate_rows": [],
        "rank_streak": {},
        "rebuy_cooldown": {},
        "risk_mode": None,
        "position_scale": 1.0,
        "peak_equity": context.portfolio.total_value,
        "daily_reports": [],
        "nav_filter": "skipped_no_data",
    })
    context.schedule(_morning_regime, "09:40")
    context.schedule(_risk_monitor, "10:31")
    context.schedule(_prepare_and_sell, "13:10")
    context.schedule(_buy_targets, "13:11")
    context.log(
        "五福 TickFlow 适配已初始化：ETF NAV/溢价过滤无数据，已跳过；"
        "161226.SZ、164824.SZ、501018.SH 无 TickFlow 分钟K，未参与回测"
    )


def on_session_start(context) -> None:
    state = _state(context)
    state["intraday"] = {"date": context.now.date().isoformat(), "close": {}, "volume": {}, "amount": {}}
    state["risk_mode"] = None
    state["position_scale"] = 1.0


def on_bar(context, bars) -> None:
    state = _state(context)
    intraday = state["intraday"]
    day = context.now.date().isoformat()
    if intraday.get("date") != day:
        intraday = {"date": day, "close": {}, "volume": {}, "amount": {}}
        state["intraday"] = intraday
    for symbol, bar in bars.items():
        intraday["close"][symbol] = bar.close
        intraday["volume"][symbol] = float(intraday["volume"].get(symbol, 0.0)) + bar.volume
        intraday["amount"][symbol] = float(intraday["amount"].get(symbol, 0.0)) + bar.amount
    _minute_stop_loss(context, bars)


def after_trading_end(context) -> None:
    state = _state(context)
    intraday = state["intraday"]
    day = intraday.get("date")
    if not day:
        return
    daily = state["daily"]
    for symbol, close in intraday["close"].items():
        series = daily.setdefault(symbol, [])
        if series and series[-1]["date"] == day:
            continue
        series.append({
            "date": day,
            "close": float(close),
            "volume": float(intraday["volume"].get(symbol, 0.0)),
            "amount": float(intraday["amount"].get(symbol, 0.0)),
        })
        if len(series) > 320:
            del series[:-320]
    for symbol, remaining in list(state["rebuy_cooldown"].items()):
        if remaining <= 1:
            state["rebuy_cooldown"].pop(symbol, None)
        else:
            state["rebuy_cooldown"][symbol] = remaining - 1
    report = _daily_report(context)
    state["daily_reports"].append(report)
    if len(state["daily_reports"]) > 320:
        del state["daily_reports"][:-320]
    context.log(
        f"五福日终 {day}：状态={report['regime']}，目标={','.join(report['target']) or '空仓'}，"
        f"候选={len(report['candidates'])}，资产={report['equity']:.2f}"
    )


def _morning_regime(context) -> None:
    state = _state(context)
    daily = state["daily"]
    above_ma10 = 0
    below_ma20 = 0
    available = 0
    for symbol in REGIME_PROXIES:
        closes = [row["close"] for row in daily.get(symbol, [])]
        if len(closes) < 20:
            continue
        available += 1
        if closes[-1] > sum(closes[-10:]) / 10:
            above_ma10 += 1
        if closes[-1] < sum(closes[-20:]) / 20:
            below_ma20 += 1
    raw = "震荡期"
    if available == len(REGIME_PROXIES):
        if below_ma20 >= 4:
            raw = "走弱期"
        elif above_ma10 >= 4:
            raw = "正常期"
    current = state["regime"]
    if raw == current:
        state["regime_pending"] = None
        state["regime_pending_days"] = 0
    elif state["regime_pending"] == raw:
        state["regime_pending_days"] += 1
    else:
        state["regime_pending"] = raw
        state["regime_pending_days"] = 1
    if state["regime_pending_days"] >= 2:
        state["regime"] = raw
        state["regime_pending"] = None
        state["regime_pending_days"] = 0
    context.log(
        f"五福状态：指标={raw}，生效={state['regime']}，"
        f"MA10上方={above_ma10}/{available}，MA20下方={below_ma20}/{available}"
    )


def _prepare_and_sell(context) -> None:
    state = _state(context)
    rows = _rank_candidates(context)
    state["candidate_rows"] = rows
    targets = _choose_targets(context, rows)
    state["target"] = targets
    held = _held_symbols(context)
    for symbol in held:
        if symbol not in targets:
            context.order_target_percent(symbol, 0.0)
    context.log(
        f"五福 13:10：状态={state['regime']}，候选={len(rows)}，"
        f"目标={','.join(targets) or '空仓'}，卖出={','.join(symbol for symbol in held if symbol not in targets) or '无'}"
    )


def _buy_targets(context) -> None:
    state = _state(context)
    targets = state.get("target", [])
    if not targets:
        return
    allocation = min(0.95, max(0.0, float(state.get("position_scale", 1.0)) * 0.95)) / len(targets)
    for symbol in targets:
        context.order_target_percent(symbol, allocation)
    context.log(f"五福 13:11：买入目标={','.join(targets)}，单标的目标仓位={allocation:.0%}")


def _risk_monitor(context) -> None:
    state = _state(context)
    equity = context.portfolio.total_value
    state["peak_equity"] = max(float(state.get("peak_equity", equity)), equity)
    drawdown = 1 - equity / state["peak_equity"] if state["peak_equity"] else 0.0
    held = _held_symbols(context)
    if drawdown >= 0.20:
        state["risk_mode"] = "flat"
        state["target"] = []
        for symbol in held:
            context.order_target_percent(symbol, 0.0)
        context.log(f"五福风控：回撤{drawdown:.2%}，全部清仓")
    elif drawdown >= 0.12:
        state["risk_mode"] = "defensive"
        state["target"] = [DEFENSIVE_ETF]
        for symbol in held:
            if symbol != DEFENSIVE_ETF:
                context.order_target_percent(symbol, 0.0)
        context.log(f"五福风控：回撤{drawdown:.2%}，切换防御ETF")
    elif drawdown >= 0.10:
        state["position_scale"] = 0.5
        context.log(f"五福风控：回撤{drawdown:.2%}，仓位缩放至50%")


def _minute_stop_loss(context, bars) -> None:
    state = _state(context)
    threshold = 0.95 if state["regime"] == "走弱期" else 0.91
    for symbol in _held_symbols(context):
        bar = bars.get(symbol)
        cost = context.portfolio.avg_cost.get(symbol, 0.0)
        if bar is None or cost <= 0 or bar.close >= cost * threshold:
            continue
        context.order_target_percent(symbol, 0.0)
        state["rebuy_cooldown"][symbol] = 2
        context.log(f"五福止损：{symbol} 现价{bar.close:.4f} < 成本{cost:.4f}×{threshold:.0%}，冷却2日")


def _rank_candidates(context) -> list[dict[str, Any]]:
    state = _state(context)
    daily = state["daily"]
    intraday = state["intraday"]
    regime = state["regime"]
    rows = []
    for symbol in WUFU_ETF_POOL:
        history = daily.get(symbol, [])
        current = intraday["close"].get(symbol)
        if current is None or len(history) < 61:
            continue
        closes = [row["close"] for row in history] + [float(current)]
        volumes = [row["volume"] for row in history]
        metric = _metric_for(symbol, closes, volumes, float(intraday["volume"].get(symbol, 0.0)), context)
        if metric is None:
            continue
        metric["regime"] = regime
        if _passes_filters(metric, regime):
            rows.append(metric)
    rows.sort(key=lambda row: row["score"], reverse=True)
    return rows[:10]


def _metric_for(symbol: str, closes: list[float], volumes: list[float], today_volume: float, context) -> dict[str, Any] | None:
    score, annualized, r2 = _weighted_momentum(closes, 25)
    short_score, _, _ = _weighted_momentum(closes, 21)
    if score is None or short_score is None:
        return None
    current = closes[-1]
    ma10 = sum(closes[-11:-1]) / 10
    volume_ratio = _projected_volume_ratio(volumes, today_volume, context)
    laplace_value, laplace_slope = _laplace(closes[-31:-1], 0.05)
    gaussian_value, gaussian_slope = _gaussian(closes[-31:-1])
    day_ratios = [closes[-index] / closes[-index - 1] for index in range(1, 4)]
    return {
        "symbol": symbol,
        "score": score,
        "annualized_return": annualized,
        "r2": r2,
        "short_score": short_score,
        "close": current,
        "ma10": ma10,
        "volume_ratio": volume_ratio,
        "day_ratios": day_ratios,
        "laplace_value": laplace_value,
        "laplace_slope": laplace_slope,
        "gaussian_value": gaussian_value,
        "gaussian_slope": gaussian_slope,
        "history": closes[-61:],
    }


def _passes_filters(metric: dict[str, Any], regime: str) -> bool:
    if not (0 < metric["score"] <= 5):
        return False
    if regime != "走弱期" and metric["r2"] <= (0.39 if regime == "正常期" else 0.4):
        return False
    if regime == "走弱期" and metric["close"] <= metric["ma10"] * 1.0001:
        return False
    if metric["volume_ratio"] is None or metric["volume_ratio"] >= 1.9:
        return False
    if min(metric["day_ratios"]) < 0.97:
        return False
    if regime == "正常期" and not (metric["close"] > metric["laplace_value"] and metric["laplace_slope"] > 0.0022):
        return False
    if regime == "走弱期" and not (metric["close"] > metric["laplace_value"] and metric["laplace_slope"] > 0.001):
        return False
    if regime == "震荡期":
        if not (0 < metric["short_score"] <= 6 and metric["score"] > 0 and metric["short_score"] > 0):
            return False
        if not (metric["close"] > metric["gaussian_value"] and metric["gaussian_slope"] > 0.0013):
            return False
    return True


def _choose_targets(context, rows: list[dict[str, Any]]) -> list[str]:
    state = _state(context)
    if state.get("risk_mode") == "flat":
        return []
    if state.get("risk_mode") == "defensive":
        return [DEFENSIVE_ETF]
    if not rows:
        return [DEFENSIVE_ETF] if _has_price(state, DEFENSIVE_ETF) else []
    held = _held_symbols(context)
    top = rows[0]["symbol"]
    target = top
    if held:
        current = held[0]
        eligible = {row["symbol"] for row in rows}
        if current in eligible and current != top:
            streak = int(state["rank_streak"].get(current, 0)) + 1
            state["rank_streak"][current] = streak
            if streak < 5:
                target = current
            else:
                state["rank_streak"][current] = 0
        else:
            state["rank_streak"][current] = 0
        if current != target:
            target = _low_correlation_target(current, rows) or current
    if state["rebuy_cooldown"].get(target, 0) > 0:
        fallback = next((row["symbol"] for row in rows if state["rebuy_cooldown"].get(row["symbol"], 0) <= 0), None)
        target = fallback or (current if held else DEFENSIVE_ETF)
    return [target]


def _low_correlation_target(current: str, rows: list[dict[str, Any]]) -> str | None:
    current_row = next((row for row in rows if row["symbol"] == current), None)
    if current_row is None:
        return rows[0]["symbol"]
    for row in rows:
        if row["symbol"] == current:
            continue
        correlation = _correlation(current_row["history"], row["history"])
        if correlation is None or correlation < 0.85:
            return row["symbol"]
    return None


def _held_symbols(context) -> list[str]:
    return [symbol for symbol, quantity in context.portfolio.positions.items() if quantity > 0]


def _has_price(state: dict[str, Any], symbol: str) -> bool:
    return symbol in state["intraday"].get("close", {})


def _weighted_momentum(prices: list[float], lookback: int) -> tuple[float | None, float | None, float | None]:
    if len(prices) < lookback + 1 or any(price <= 0 for price in prices[-(lookback + 1):]):
        return None, None, None
    values = [math.log(price) for price in prices[-(lookback + 1):]]
    weights = [(1 + index / lookback) ** 2 for index in range(len(values))]
    total_weight = sum(weights)
    x_mean = sum(index * weight for index, weight in enumerate(weights)) / total_weight
    y_mean = sum(value * weight for value, weight in zip(values, weights)) / total_weight
    var_x = sum(weight * (index - x_mean) ** 2 for index, weight in enumerate(weights))
    slope = sum(weight * (index - x_mean) * (value - y_mean) for index, (value, weight) in enumerate(zip(values, weights))) / var_x
    predicted = [y_mean + slope * (index - x_mean) for index in range(len(values))]
    residual = sum(weight * (value - fit) ** 2 for value, fit, weight in zip(values, predicted, weights))
    total = sum(weight * (value - y_mean) ** 2 for value, weight in zip(values, weights))
    r2 = 1 - residual / total if total else 0.0
    annualized = math.exp(slope * 250) - 1
    return annualized * r2, annualized, r2


def _projected_volume_ratio(volumes: list[float], today_volume: float, context) -> float | None:
    if len(volumes) < 5 or any(volume <= 0 for volume in volumes[-5:]):
        return None
    now = context.now
    elapsed = (now.hour - 9) * 60 + now.minute - 30
    if now.hour >= 13:
        elapsed -= 90
    elapsed = max(1, min(240, elapsed))
    return today_volume * (240 / elapsed) / (sum(volumes[-5:]) / 5)


def _laplace(prices: list[float], smoothing: float) -> tuple[float, float]:
    alpha = 1 - math.exp(-smoothing)
    value = prices[0]
    previous = value
    for price in prices[1:]:
        previous, value = value, alpha * price + (1 - alpha) * value
    return value, value - previous


def _gaussian(prices: list[float]) -> tuple[float, float]:
    def smooth(values: list[float]) -> float:
        weights = [math.exp(-(index ** 2) / (2 * 1.2 ** 2)) for index in range(len(values))]
        return sum(value * weight for value, weight in zip(reversed(values), weights)) / sum(weights)
    current = smooth(prices)
    previous = smooth(prices[:-1])
    return current, (current - previous) / previous if previous else 0.0


def _correlation(left: list[float], right: list[float]) -> float | None:
    n = min(len(left), len(right), 60)
    if n < 3:
        return None
    a = [left[-index] / left[-index - 1] - 1 for index in range(1, n)]
    b = [right[-index] / right[-index - 1] - 1 for index in range(1, n)]
    mean_a = sum(a) / len(a)
    mean_b = sum(b) / len(b)
    denominator = math.sqrt(sum((value - mean_a) ** 2 for value in a) * sum((value - mean_b) ** 2 for value in b))
    return sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b)) / denominator if denominator else None


def _daily_report(context) -> dict[str, Any]:
    state = _state(context)
    candidates = [{key: row[key] for key in ("symbol", "score", "r2", "short_score", "volume_ratio", "close")} for row in state["candidate_rows"]]
    return {
        "date": context.now.date().isoformat(),
        "regime": state["regime"],
        "target": list(state.get("target", [])),
        "candidates": candidates,
        "holdings": _held_symbols(context),
        "equity": float(context.portfolio.total_value),
        "cash": float(context.portfolio.cash),
        "position_scale": float(state.get("position_scale", 1.0)),
        "nav_filter": "skipped_no_data",
    }
