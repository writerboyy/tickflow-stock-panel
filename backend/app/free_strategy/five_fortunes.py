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
    context.set_universe([*WUFU_MINUTE_POOL, DEFENSIVE_ETF])
    context.require_history(timeframe="1d", bars=61)
    context.state.setdefault("five_fortunes", {
        "daily": {},
        "intraday": {"date": None, "close": {}, "volume": {}, "amount": {}},
        "regime": "震荡期",
        "raw_regime": "震荡期",
        "regime_pending": None,
        "regime_pending_days": 0,
        "regime_last_change_date": None,
        "regime_changed_today": False,
        "target": [],
        "candidate_rows": [],
        "filtered_rows": [],
        "all_metric_rows": [],
        "liquidity_pool": list(WUFU_MINUTE_POOL),
        "liquidity_threshold": None,
        "liquidity_divisor": 20_000,
        "rank_streak": {},
        "rebuy_cooldown": {},
        "risk_mode": None,
        "position_scale": 1.0,
        "peak_equity": context.portfolio.total_value,
        "risk_action_date": None,
        "risk_actions": [],
        "filter_fail_streak": 0,
        "filter_fail_last_date": None,
        "decision": {},
        "correlation_decisions": [],
        "daily_reports": [],
        "nav_filter": "skipped_no_data",
        "excluded_no_minute_symbols": list(NO_TICKFLOW_MINUTE),
        "liquidity_scope": "configured_universe",
        "warmup_rows": 0,
        "warmup_ready_symbols": 0,
        "warmup_required_days": 61,
    })
    context.schedule(_morning_regime, "09:40")
    context.schedule(_risk_monitor, "10:31")
    context.schedule(_prepare_and_sell, "13:10")
    context.schedule(_buy_targets, "13:11")
    context.log(
        "五福 TickFlow 适配已初始化：ETF NAV/溢价过滤无数据，已跳过；"
        "161226.SZ、164824.SZ、501018.SH 无 TickFlow 分钟K，未参与回测"
    )


def before_trading_start(context) -> None:
    state = _state(context)
    if not state["daily"]:
        loaded = 0
        ready = 0
        for symbol in context.universe:
            bars = context.history_bars(symbol, count=61, timeframe="1d")
            if not bars:
                continue
            state["daily"][symbol] = [
                {
                    "date": bar.date.isoformat(),
                    "close": float(bar.close),
                    "volume": float(bar.volume),
                    "amount": float(bar.amount),
                }
                for bar in bars
            ]
            loaded += len(bars)
            ready += len(bars) >= 61
        state["warmup_rows"] = loaded
        state["warmup_ready_symbols"] = ready
        level = "INFO" if ready == len(context.universe) else "WARNING"
        context.log(
            f"五福日线预热：载入 {loaded} 根日K，"
            f"{ready}/{len(context.universe)} 只标的满足 61 根要求；"
            "不足的标的将等待正式回测数据累积",
            level=level,
        )
    state["intraday"] = {"date": context.now.date().isoformat(), "close": {}, "volume": {}, "amount": {}}
    state["risk_mode"] = None
    state["position_scale"] = 1.0
    state["regime_changed_today"] = False
    state["decision"] = {"date": context.now.date().isoformat(), "reason": "pending"}


# 旧快照仍可导入，新策略使用标准生命周期名。
on_session_start = before_trading_start


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
    state["raw_regime"] = raw
    current = state["regime"]
    day = context.now.date().isoformat()
    if state.get("regime_last_change_date") is None:
        state["regime"] = raw
        state["regime_last_change_date"] = day
        state["regime_changed_today"] = True
        state["regime_pending"] = None
        state["regime_pending_days"] = 0
    elif raw == current:
        state["regime_pending"] = None
        state["regime_pending_days"] = 0
    elif state["regime_pending"] == raw:
        state["regime_pending_days"] += 1
    else:
        state["regime_pending"] = raw
        state["regime_pending_days"] = 1
    if state["regime_pending_days"] >= 2:
        state["regime"] = raw
        state["regime_last_change_date"] = day
        state["regime_changed_today"] = True
        state["regime_pending"] = None
        state["regime_pending_days"] = 0
    context.log(
        f"五福状态：指标={raw}，生效={state['regime']}，"
        f"MA10上方={above_ma10}/{available}，MA20下方={below_ma20}/{available}"
    )


def _prepare_and_sell(context) -> None:
    state = _state(context)
    filtered_rows = _rank_candidates(context)
    candidate_rows = _candidate_pool(filtered_rows, state["regime"])
    state["filtered_rows"] = filtered_rows
    state["candidate_rows"] = candidate_rows
    targets = _choose_targets(context, candidate_rows, filtered_rows)
    held = _held_symbols(context)
    filtered = {row["symbol"] for row in filtered_rows}
    all_metrics = {row["symbol"] for row in state.get("all_metric_rows", [])}
    filter_fail = [symbol for symbol in held if symbol not in filtered and symbol in all_metrics and symbol not in targets]
    day = context.now.date().isoformat()
    if filter_fail:
        if state.get("filter_fail_last_date") != day:
            state["filter_fail_streak"] = int(state.get("filter_fail_streak", 0)) + 1
            state["filter_fail_last_date"] = day
        if state["filter_fail_streak"] >= 4 and _has_price(state, DEFENSIVE_ETF):
            targets = [DEFENSIVE_ETF]
            state["filter_fail_streak"] = 0
            state["filter_fail_last_date"] = None
            state["decision"]["reason"] = "four_day_filter_fail_defensive"
            context.log("五福 S2：连续4个交易日发生filter_fail卖出，强制切换防御ETF")
    else:
        state["filter_fail_streak"] = 0
        state["filter_fail_last_date"] = None
    state["target"] = targets
    state["decision"].update({
        "target": list(targets),
        "filtered_count": len(filtered_rows),
        "candidate_count": len(candidate_rows),
        "filter_fail_symbols": filter_fail,
        "regime": state["regime"],
        "raw_regime": state.get("raw_regime"),
        "regime_changed": bool(state.get("regime_changed_today")),
    })
    for symbol in held:
        if symbol not in targets:
            context.order_target_percent(symbol, 0.0)
    context.log(
        f"五福 13:10：状态={state['regime']}，过筛={len(filtered_rows)}，候选={len(candidate_rows)}，"
        f"目标={','.join(targets) or '空仓'}，卖出={','.join(symbol for symbol in held if symbol not in targets) or '无'}"
    )


def _buy_targets(context) -> None:
    state = _state(context)
    targets = state.get("target", [])
    if not targets:
        return
    allocation = min(0.95, max(0.0, float(state.get("position_scale", 1.0)) * 0.95)) / len(targets)
    held = set(_held_symbols(context))
    submitted = []
    for symbol in targets:
        if symbol in held and state.get("position_scale", 1.0) >= 1.0:
            continue
        context.order_target_percent(symbol, allocation)
        submitted.append(symbol)
    if submitted:
        context.log(f"五福 13:11：买入目标={','.join(submitted)}，单标的目标仓位={allocation:.0%}")
    else:
        context.log(f"五福 13:11：当前持仓已是目标 {','.join(targets)}，不重复调仓")


def _risk_monitor(context) -> None:
    state = _state(context)
    equity = context.portfolio.total_value
    state["peak_equity"] = max(float(state.get("peak_equity", equity)), equity)
    drawdown = 1 - equity / state["peak_equity"] if state["peak_equity"] else 0.0
    day = context.now.date().isoformat()
    if state.get("risk_action_date") == day:
        return
    if state["regime"] == "走弱期":
        half_threshold, defensive_threshold, flat_threshold = 0.05, 0.08, 0.12
    else:
        half_threshold, defensive_threshold, flat_threshold = 0.10, 0.12, 0.20
    held = _held_symbols(context)
    action = None
    if drawdown >= flat_threshold:
        action = "flat"
        state["risk_mode"] = "flat"
        state["target"] = []
        for symbol in held:
            context.order_target_percent(symbol, 0.0)
            state["rebuy_cooldown"][symbol] = max(3, int(state["rebuy_cooldown"].get(symbol, 0)))
        context.log(f"五福风控：回撤{drawdown:.2%}≥{flat_threshold:.0%}，全部清仓")
    elif drawdown >= defensive_threshold:
        action = "defensive"
        state["risk_mode"] = "defensive"
        state["target"] = [DEFENSIVE_ETF]
        for symbol in held:
            if symbol != DEFENSIVE_ETF:
                context.order_target_percent(symbol, 0.0)
                state["rebuy_cooldown"][symbol] = max(3, int(state["rebuy_cooldown"].get(symbol, 0)))
        context.log(f"五福风控：回撤{drawdown:.2%}≥{defensive_threshold:.0%}，切换防御ETF")
    elif drawdown >= half_threshold:
        action = "half"
        state["position_scale"] = 0.5
        for symbol in held:
            context.order_target(symbol, context.portfolio.positions.get(symbol, 0.0) * 0.5)
        context.log(f"五福风控：回撤{drawdown:.2%}≥{half_threshold:.0%}，可卖持仓减半")
    if action:
        state["risk_action_date"] = day
        state["peak_equity"] = equity
        state["risk_actions"].append({
            "date": day,
            "regime": state["regime"],
            "action": action,
            "drawdown": drawdown,
            "thresholds": {
                "half": half_threshold,
                "defensive": defensive_threshold,
                "flat": flat_threshold,
            },
        })


def _minute_stop_loss(context, bars) -> None:
    state = _state(context)
    threshold = 0.95 if state["regime"] == "走弱期" else 0.91
    for symbol in _held_symbols(context):
        bar = bars.get(symbol)
        cost = context.portfolio.avg_cost.get(symbol, 0.0)
        current = bar.execution_price("close") if bar is not None else None
        if current is None or cost <= 0 or current >= cost * threshold:
            continue
        context.order_target_percent(symbol, 0.0)
        # 当日盘中+随后2个交易日均禁止买回；日终会先递减一次。
        state["rebuy_cooldown"][symbol] = 3
        context.log(f"五福止损：{symbol} 现价{current:.4f} < 成本{cost:.4f}×{threshold:.0%}，冷却2日")


def _rank_candidates(context) -> list[dict[str, Any]]:
    state = _state(context)
    daily = state["daily"]
    intraday = state["intraday"]
    regime = state["regime"]
    rows = []
    liquidity_pool = _liquidity_pool(state, regime)
    for symbol in liquidity_pool:
        history = daily.get(symbol, [])
        current = intraday["close"].get(symbol)
        if current is None or len(history) < 61:
            continue
        closes = [row["close"] for row in history] + [float(current)]
        volumes = [row["volume"] for row in history]
        metric = _metric_for(
            symbol, closes, volumes,
            float(intraday["volume"].get(symbol, 0.0)), context, regime,
        )
        if metric is None:
            continue
        metric["regime"] = regime
        rows.append(metric)
    state["all_metric_rows"] = rows
    filtered = []
    for metric in rows:
        if _passes_filters(metric, regime):
            filtered.append(metric)
    filtered.sort(key=lambda row: row["score"], reverse=True)
    return filtered


def _metric_for(
    symbol: str,
    closes: list[float],
    volumes: list[float],
    today_volume: float,
    context,
    regime: str,
) -> dict[str, Any] | None:
    score, annualized, r2 = _weighted_momentum(closes, 25)
    short_score, _, _ = _weighted_momentum(closes, 21)
    if score is None or short_score is None:
        return None
    current = closes[-1]
    ma10 = sum(closes[-10:]) / 10
    volume_ratio = _projected_volume_ratio(volumes, today_volume, context)
    laplace_s = 0.12 if regime == "走弱期" else (0.06 if regime == "正常期" else 0.05)
    laplace_value, laplace_slope = _laplace(closes, laplace_s)
    gaussian_value, gaussian_slope = _gaussian(closes)
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
        "laplace_s": laplace_s,
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


def _liquidity_pool(state: dict[str, Any], regime: str) -> list[str]:
    """按原策略近3个交日成交额构建当日可选池。

    TickFlow 自由策略只回放本次配置的 ETF 池，因此全市场成交额分子以
    配置池口径计算；口径会在结果元数据中显式记录。
    """
    divisor = 3_000 if regime == "走弱期" else 20_000
    daily = state["daily"]
    amount_by_symbol: dict[str, float] = {}
    total_by_date: dict[str, float] = {}
    for symbol in WUFU_MINUTE_POOL:
        rows = daily.get(symbol, [])[-3:]
        if len(rows) < 3:
            continue
        amount_by_symbol[symbol] = sum(float(row.get("amount", 0.0)) for row in rows) / 3
        for row in rows:
            day = str(row["date"])
            total_by_date[day] = total_by_date.get(day, 0.0) + float(row.get("amount", 0.0))
    if not amount_by_symbol or not total_by_date:
        state["liquidity_pool"] = list(WUFU_MINUTE_POOL)
        state["liquidity_threshold"] = None
        state["liquidity_divisor"] = divisor
        return state["liquidity_pool"]
    latest_days = sorted(total_by_date)[-3:]
    average_total = sum(total_by_date[day] for day in latest_days) / len(latest_days)
    threshold = average_total / divisor
    pool = [symbol for symbol in WUFU_MINUTE_POOL if amount_by_symbol.get(symbol, 0.0) > threshold]
    state["liquidity_pool"] = pool
    state["liquidity_threshold"] = threshold
    state["liquidity_divisor"] = divisor
    return pool


def _candidate_pool(filtered_rows: list[dict[str, Any]], regime: str) -> list[dict[str, Any]]:
    top = filtered_rows[:10]
    if not top:
        return []
    ratio = 1.0 if regime == "走弱期" else 0.9
    threshold = float(top[0]["score"]) * ratio
    return [row for row in top if float(row["score"]) >= threshold]


def _choose_targets(
    context,
    rows: list[dict[str, Any]],
    filtered_rows: list[dict[str, Any]] | None = None,
) -> list[str]:
    state = _state(context)
    state["decision"].setdefault("reason", "ranked_target")
    if state.get("risk_mode") == "flat":
        state["decision"]["reason"] = "drawdown_flat"
        return []
    if state.get("risk_mode") == "defensive":
        state["decision"]["reason"] = "drawdown_defensive"
        return [DEFENSIVE_ETF]
    if not rows:
        state["decision"]["reason"] = "no_candidate_defensive"
        return [DEFENSIVE_ETF] if _has_price(state, DEFENSIVE_ETF) else []
    held = _held_symbols(context)
    top = rows[0]["symbol"]
    target = top
    if held:
        current = held[0]
        eligible = {row["symbol"] for row in rows}
        ranked = filtered_rows or rows
        rank = next((index + 1 for index, row in enumerate(ranked) if row["symbol"] == current), None)
        if current in eligible and current != top:
            streak = int(state["rank_streak"].get(current, 0)) + 1
            state["rank_streak"][current] = streak
            if streak < 5:
                target = current
                state["decision"]["reason"] = "anti_churn_hold"
            else:
                state["rank_streak"][current] = 0
        else:
            state["rank_streak"][current] = 0
        if current != target:
            if current not in eligible and state.get("regime_changed_today"):
                target = current
                state["decision"]["reason"] = "regime_change_hold"
            else:
                target = _low_correlation_target(context, current, rows) or current
                state["decision"]["reason"] = "low_correlation_switch" if target != current else "high_correlation_hold"
        if target != current:
            target = _apply_correlation_hold_guard(context, current, target) or current
        state["decision"].update({"held": current, "held_rank": rank})
    if state["rebuy_cooldown"].get(target, 0) > 0:
        fallback = next((row["symbol"] for row in rows if state["rebuy_cooldown"].get(row["symbol"], 0) <= 0), None)
        target = fallback or (current if held else DEFENSIVE_ETF)
        state["decision"]["reason"] = "rebuy_cooldown_fallback"
    return [target]


def _low_correlation_target(context, current: str, rows: list[dict[str, Any]]) -> str | None:
    state_rows = _state(context).get("all_metric_rows", rows)
    current_row = next((row for row in state_rows if row["symbol"] == current), None)
    if current_row is None:
        return rows[0]["symbol"] if rows else None
    for row in rows:
        if row["symbol"] == current:
            continue
        correlation = _adjusted_correlation(current_row["history"], row["history"])
        if correlation is None or correlation < 0.85:
            return row["symbol"]
    return None


def _apply_correlation_hold_guard(context, current: str, target: str) -> str:
    state = _state(context)
    metrics = state.get("all_metric_rows", [])
    current_row = next((row for row in metrics if row["symbol"] == current), None)
    target_row = next((row for row in metrics if row["symbol"] == target), None)
    if current_row is None or target_row is None:
        return target
    pair_corr = _adjusted_correlation(current_row["history"], target_row["history"])
    momentum = current_row.get("score")
    blocked = False
    reason = "low_correlation_allow"
    if pair_corr is not None and pair_corr >= 0.88 and (momentum is None or float(momentum) <= 8.0):
        blocked, reason = True, "high_pair_overlay"
    elif pair_corr is not None and pair_corr > 0.85 and momentum is not None and float(momentum) <= 7.0:
        blocked, reason = True, "correlation_hold_guard"
    record = {
        "date": context.now.date().isoformat(),
        "held": current,
        "target": target,
        "p_adj": pair_corr,
        "held_momentum": momentum,
        "blocked": blocked,
        "reason": reason,
    }
    state["correlation_decisions"].append(record)
    if len(state["correlation_decisions"]) > 320:
        del state["correlation_decisions"][:-320]
    state["decision"]["correlation"] = record
    if blocked:
        state["decision"]["reason"] = reason
        momentum_text = "N/A" if momentum is None else f"{float(momentum):.4f}"
        context.log(
            f"五福相关性守卫：{current}→{target} P_adj={pair_corr:.4f}，持仓动量={momentum_text}，拦截换仓"
        )
        return current
    return target


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
        weights = [math.exp(-((index + 1) ** 2) / (2 * 1.2 ** 2)) for index in range(len(values))]
        weights.reverse()
        return sum(value * weight for value, weight in zip(values, weights)) / sum(weights)
    current = smooth(prices)
    previous = smooth(prices[:-1])
    return current, (current - previous) / previous if previous else 0.0


def _adjusted_correlation(left: list[float], right: list[float]) -> float | None:
    """P_adj = Pearson(log return) * exp(-MAE(cumulative return)) * volatility ratio."""
    n = min(len(left), len(right), 60)
    if n < 3:
        return None
    prices_left = [float(value) for value in left[-n:]]
    prices_right = [float(value) for value in right[-n:]]
    if any(value <= 0 for value in prices_left + prices_right):
        return None
    returns_left = [math.log(current / previous) for previous, current in zip(prices_left, prices_left[1:])]
    returns_right = [math.log(current / previous) for previous, current in zip(prices_right, prices_right[1:])]
    mean_left = sum(returns_left) / len(returns_left)
    mean_right = sum(returns_right) / len(returns_right)
    variance_left = sum((value - mean_left) ** 2 for value in returns_left)
    variance_right = sum((value - mean_right) ** 2 for value in returns_right)
    denominator = math.sqrt(variance_left * variance_right)
    if not denominator:
        return None
    base = sum(
        (left_value - mean_left) * (right_value - mean_right)
        for left_value, right_value in zip(returns_left, returns_right)
    ) / denominator
    cumulative_left = [value / prices_left[0] - 1 for value in prices_left]
    cumulative_right = [value / prices_right[0] - 1 for value in prices_right]
    mae = sum(abs(left_value - right_value) for left_value, right_value in zip(cumulative_left, cumulative_right)) / n
    std_left = math.sqrt(variance_left / len(returns_left))
    std_right = math.sqrt(variance_right / len(returns_right))
    volatility_ratio = min(std_left, std_right) / max(std_left, std_right) if max(std_left, std_right) else 0.0
    return base * math.exp(-mae) * volatility_ratio


def _daily_report(context) -> dict[str, Any]:
    state = _state(context)
    candidate_keys = (
        "symbol", "score", "r2", "short_score", "volume_ratio", "close",
        "laplace_s", "laplace_slope", "gaussian_slope",
    )
    candidates = [{key: row[key] for key in candidate_keys} for row in state["candidate_rows"]]
    filter_rejections: dict[str, int] = {}
    for row in state.get("all_metric_rows", []):
        for reason in _filter_failures(row, state["regime"]):
            filter_rejections[reason] = filter_rejections.get(reason, 0) + 1
    return {
        "date": context.now.date().isoformat(),
        "regime": state["regime"],
        "raw_regime": state.get("raw_regime"),
        "regime_changed": bool(state.get("regime_changed_today")),
        "target": list(state.get("target", [])),
        "candidates": candidates,
        "filtered_count": len(state.get("filtered_rows", [])),
        "candidate_count": len(candidates),
        "liquidity_pool_count": len(state.get("liquidity_pool", [])),
        "liquidity_threshold": state.get("liquidity_threshold"),
        "liquidity_divisor": state.get("liquidity_divisor"),
        "filter_rejections": filter_rejections,
        "filter_fail_streak": int(state.get("filter_fail_streak", 0)),
        "decision": dict(state.get("decision", {})),
        "risk_action": state.get("risk_actions", [])[-1] if state.get("risk_action_date") == context.now.date().isoformat() else None,
        "holdings": _held_symbols(context),
        "equity": float(context.portfolio.total_value),
        "cash": float(context.portfolio.cash),
        "position_scale": float(state.get("position_scale", 1.0)),
        "nav_filter": "skipped_no_data",
    }


def _filter_failures(metric: dict[str, Any], regime: str) -> list[str]:
    failures = []
    if not (0 < metric["score"] <= 5):
        failures.append("momentum")
    if regime != "走弱期" and metric["r2"] <= (0.39 if regime == "正常期" else 0.4):
        failures.append("r2")
    if regime == "走弱期" and metric["close"] <= metric["ma10"] * 1.0001:
        failures.append("ma10")
    if metric["volume_ratio"] is None or metric["volume_ratio"] >= 1.9:
        failures.append("volume")
    if min(metric["day_ratios"]) < 0.97:
        failures.append("loss")
    if regime == "正常期" and not (metric["close"] > metric["laplace_value"] and metric["laplace_slope"] > 0.0022):
        failures.append("laplace")
    if regime == "走弱期" and not (metric["close"] > metric["laplace_value"] and metric["laplace_slope"] > 0.001):
        failures.append("laplace")
    if regime == "震荡期":
        if not (0 < metric["short_score"] <= 6 and metric["score"] > 0):
            failures.append("short_momentum")
        if not (metric["close"] > metric["gaussian_value"] and metric["gaussian_slope"] > 0.0013):
            failures.append("gaussian")
    return failures
