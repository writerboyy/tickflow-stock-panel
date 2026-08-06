"""五福 ETF 策略的 TickFlow 原生实现。"""
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
WUFU_MINUTE_POOL = WUFU_ETF_POOL
WUFU_DYNAMIC_POOL_EXCLUSIONS = {"159814.SZ"}
CANDIDATE_SCORE_RATIO = 0.9
ETF_PRICE_TICK = 0.001
MOMENTUM_SCORE_MAX = 5.0
DECISION_REASON_LABELS = {
    "ranked_target": "选择综合排名最高的候选标的",
    "hold_top_rank": "当前持仓仍为排名最高候选，继续持有",
    "no_candidate_defensive": "无合格候选，切换防御标的",
    "anti_churn_hold": "排名变化未满足连续确认，继续持有",
    "filter_fail_switch": "当前持仓未通过当日筛选，切换至排名最高的候选标的",
    "candidate_pool_exit_switch": "当前持仓未进入当日候选范围，切换至排名更高的候选标的",
    "rank_lag_switch": "当前持仓连续未重返首位，切换至排名更高的候选标的",
    "regime_change_hold": "市场状态刚切换，暂不调仓",
    "low_correlation_switch": "候选标的与当前持仓相关性较低，执行切换",
    "high_correlation_hold": "候选标的与当前持仓高度相关，继续持有",
    "high_pair_overlay": "高相关组合保护生效，继续持有",
    "correlation_hold_guard": "相关性换仓保护生效，继续持有",
    "four_day_filter_fail_defensive": "连续四个交易日未通过筛选，切换防御标的",
    "data_unavailable_hold": "净值数据未更新到所需交易日，保留当前持仓",
}
CORRELATION_REASON_LABELS = {
    "low_correlation_allow": "修正相关性较低，允许换仓",
    "high_pair_overlay": "高相关组合保护生效，拦截换仓",
    "correlation_hold_guard": "相关性换仓保护生效，拦截换仓",
}
WUFU_GROUP_NAME_OVERRIDES = {
    "161226.SZ": "国投白银LOF",
    "513000.SH": "225ETF",
    "513350.SH": "油气ETF",
    "515030.SH": "新汽车",
    "516190.SH": "文娱ETF",
    "516080.SH": "创新医药",
    "520500.SH": "恒生新药",
    "561100.SH": "电子龙头",
    "561980.SH": "芯片设备",
    "588020.SH": "科创50E",
    "588710.SH": "科半导体",
    "588760.SH": "AI科创",
    "588790.SH": "科创智能",
    "588830.SH": "科创新能",
    "588890.SH": "科创芯",
    "588990.SH": "科芯片",
    "589680.SH": "科创综Z",
    "589720.SH": "科创新药",
    "589800.SH": "科创综合",
}
GLOBAL_ETF_POOL = [
    "518880.SH", "501018.SH", "161226.SZ", "159985.SZ", "159980.SZ",
    "513310.SH", "159518.SZ", "159509.SZ", "513100.SH", "513520.SH",
    "513500.SH", "159502.SZ", "513400.SH", "513030.SH", "513290.SH",
    "520830.SH", "159529.SZ", "164824.SZ", "513080.SH", "513730.SH",
    "511380.SH", "511220.SH", "510050.SH", "563300.SH", "159928.SZ",
    "510300.SH", "510500.SH", "512100.SH", "159915.SZ", "513180.SH",
    "159920.SZ",
]
REGIME_PROXIES = [
    "000300.SH", "399101.SZ", "399006.SZ", "000510.SH", "000852.SH", "399303.SZ",
]
REGIME_FALLBACK_PROXIES = [
    "510300.SH", "510500.SH", "159915.SZ", "512100.SH", "563300.SH", "510050.SH",
]
LIQUIDITY_CALENDAR_SYMBOL = "510300.SH"

FUND_COMPANIES = (
    "易方达", "广发", "华夏", "华安", "嘉实", "富国", "招商", "鹏华", "南方", "汇添富", "国泰", "平安",
    "银华", "天弘", "建信", "工银", "华泰柏瑞", "博时", "景顺长城", "景顺", "华宝", "申万菱信", "万家", "中欧",
    "兴证全球", "浙商", "诺安", "前海开源", "泰康", "泰达宏利", "农银汇理", "交银", "东方红", "财通", "华商",
    "国联", "永赢", "金鹰", "德邦", "创金合信", "西部利得", "圆信永丰", "泓德", "汇安", "诺德", "恒生前海",
    "华润元大", "大成", "海富通", "摩根", "华泰", "中信", "中银", "兴全", "国信", "长城", "中金", "浙商证券",
    "东海", "东吴", "浦银安盛", "信达澳亚", "中加", "中航", "中融", "中邮", "中庚", "中信保诚", "中信建投",
    "中银国际", "中银证券", "九泰", "交银施罗德", "光大保德信", "兴银", "农银", "国投瑞银", "国海富兰克林",
    "国联安", "国金", "太平", "方正富邦", "民生加银", "汇丰晋信", "银河", "长信", "长安", "长盛", "长江证券", "鹏扬",
)
NOISE_WORDS = (
    "6666", "8888", "9999", "A类", "AH", "B", "BS", "C", "C类", "CS", "DB", "E", "E类",
    "ETF", "ETF基金", "ETF联接", "FG", "G60", "GF", "GT", "HGS", "LOF", "LOF基金", "LOF联接",
    "SG", "SZ", "TF", "TK", "WJ", "YH", "ZS", "ZZ", "板块", "策略", "产业", "场内", "场外", "低波",
    "基本面", "基金", "精选", "联接", "联接基金", "量化", "龙头", "民企", "民营", "国企", "央企", "智能",
    "全指", "上市开放式", "指基", "指增", "指数", "指数A", "指数C", "指数ETF", "指数基金", "主题", "增强",
    "上海", "黄", "30", "50", "100", "300", "500", "1000", "2000", "大", "新", "四川", "浙江", "湖北",
)
EXCLUDE_KEYWORDS = (
    "300现金流", "800现金流", "全指现金", "现金全指", "自由现金流", "现金流",
    "基准国债", "中银现金", "现金指数", "可转债", "政金债", "企业债", "公司债", "城投债",
    "美元债", "科创债", "信用债", "利率债", "国开债", "短融", "转债", "双债", "国债", "地债",
    "新综债", "城投", "科债", "货币", "现金", "快线", "快钱", "ESG", "MSCI", "A500", "A100", "A50",
    "沪深", "中证", "上证", "深证", "深成", "深100", "1000", "2000", "800", "500", "300", "200", "180", "100", "50", "30", "MS", "债",
)
SPECIAL_GROUPS = (
    ("香港组", ("恒生", "恒指", "港股通", "港股", "H股", "香港", "港", "HKC", "HK", "HGS", "H", "中概", "HS科技"),
     ("恒生", "恒指", "港股通", "港股", "H股", "香港", "港", "HKC", "HK", "HGS", "H", "中概", "HS")),
    ("科创组", ("科创", "科创板", "科综", "KC", "K C", "双创", "科创创业", "创创"),
     ("科创", "科创板", "科综", "KC", "K C", "双创", "科创创业", "创创",
      "债券", "债汇", "债指", "债沪", "债易", "债基", "债兴", "债摩", "债", "AAA")),
    ("创业组", ("创业板", "创业", "创板", "创成长"), ("创业板", "创业", "创板", "创成长")),
    ("美指组", ("标普", "纳指", "纳斯达克"), ("标普", "纳指", "纳斯达克")),
)


def _state(context) -> dict[str, Any]:
    return context.state["five_fortunes"]


def decision_reason_payload(decision: dict[str, Any]) -> dict[str, Any]:
    reason_code = str(decision.get("reason") or "ranked_target")
    if reason_code == "pending":
        reason_code = "ranked_target"
    if reason_code == "low_correlation_switch" and not decision.get("trigger_reason"):
        if decision.get("filter_fail_symbols"):
            reason_code = "filter_fail_switch"
        elif decision.get("held_rank") is None:
            reason_code = "filter_fail_switch"
        elif int(decision.get("held_rank") or 0) > int(decision.get("candidate_count") or 0):
            reason_code = "candidate_pool_exit_switch"
        else:
            reason_code = "rank_lag_switch"
    trigger_code = str(decision.get("trigger_reason") or reason_code)
    payload: dict[str, Any] = {
        "reason_code": reason_code,
        "reason": DECISION_REASON_LABELS.get(reason_code, reason_code),
        "trigger_reason_code": trigger_code,
        "trigger_reason": DECISION_REASON_LABELS.get(trigger_code, trigger_code),
    }
    correlation = decision.get("correlation")
    if isinstance(correlation, dict):
        correlation_code = str(correlation.get("reason") or "")
        payload["correlation_check"] = {
            "adjusted_correlation": correlation.get("p_adj"),
            "result": "blocked" if correlation.get("blocked") else "passed",
            "reason_code": correlation_code,
            "reason": CORRELATION_REASON_LABELS.get(correlation_code, correlation_code),
        }
    return payload


def _dynamic_group(name: str) -> str | None:
    if any(keyword in name for keyword in EXCLUDE_KEYWORDS):
        return None
    group_name = "普通组"
    remove_words: tuple[str, ...] = ()
    for candidate_name, keywords, candidate_remove_words in SPECIAL_GROUPS:
        if any(keyword in name for keyword in keywords):
            group_name = candidate_name
            remove_words = candidate_remove_words
            break
    cleaned = name
    for word in sorted(FUND_COMPANIES, key=len, reverse=True):
        cleaned = cleaned.replace(word, "")
    for word in sorted(remove_words, key=len, reverse=True):
        cleaned = cleaned.replace(word, "")
    for word in sorted(NOISE_WORDS, key=len, reverse=True):
        cleaned = cleaned.replace(word, "")
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    return f"{group_name}:{cleaned[:2]}"


def _market_catalog(context) -> tuple[list[str], dict[str, str], dict[str, str], set[str]]:
    instruments = getattr(context, "instruments", lambda _asset=None: [])("etf")
    if not instruments:
        instruments = [
            {"symbol": symbol, "name": symbol, "asset_type": "etf", "has_minute": True}
            for symbol in [*WUFU_MINUTE_POOL, DEFENSIVE_ETF]
        ]
    names = {str(item["symbol"]): str(item.get("name") or item["symbol"]) for item in instruments}
    minute_symbols = {
        str(item["symbol"])
        for item in instruments
        if bool(item.get("has_minute", True))
    }
    dynamic_groups = {
        symbol: group
        for symbol, name in names.items()
        if symbol in minute_symbols and symbol not in WUFU_DYNAMIC_POOL_EXCLUSIONS
        and (group := _dynamic_group(WUFU_GROUP_NAME_OVERRIDES.get(symbol, name))) is not None
    }
    return list(names), names, dynamic_groups, minute_symbols


def initialize(context) -> None:
    market_symbols, instrument_names, dynamic_groups, minute_symbols = _market_catalog(context)
    fixed_pool = [symbol for symbol in WUFU_ETF_POOL if symbol in minute_symbols]
    global_pool = [symbol for symbol in GLOBAL_ETF_POOL if symbol in minute_symbols]
    context.set_universe([*fixed_pool, DEFENSIVE_ETF])
    context.require_history(timeframe="1d", bars=61)
    context.require_market_history(asset_type="etf", timeframe="1d", bars=61)
    context.require_market_history(asset_type="index", timeframe="1d", bars=21)
    context.require_extra_history("unit_net_value")
    context.state.setdefault("five_fortunes", {
        "daily": {},
        "intraday": {
            "date": None, "close": {}, "raw_close": {}, "volume": {}, "amount": {},
            "last_volume": {}, "limit_up": {}, "limit_down": {}, "suspended": {}, "tradable": {},
        },
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
        "nav_unavailable_symbols": [],
        "liquidity_pool": list(fixed_pool),
        "normal_liquidity_pool": list(fixed_pool),
        "weak_liquidity_pool": list(global_pool),
        "subscription_pool": list(fixed_pool),
        "liquidity_threshold": None,
        "normal_liquidity_threshold": None,
        "weak_liquidity_threshold": None,
        "liquidity_divisor": 20_000,
        "fixed_pool": fixed_pool,
        "global_pool": global_pool,
        "dynamic_pool": [],
        "dynamic_pool_ready": False,
        "dynamic_groups": dynamic_groups,
        "instrument_names": instrument_names,
        "market_symbols": market_symbols,
        "market_instrument_count": len(market_symbols),
        "rank_streak": {},
        "rebuy_cooldown": {},
        "position_scale": 1.0,
        "peak_equity": context.portfolio.total_value,
        "risk_action_date": None,
        "risk_actions": [],
        "filter_fail_streak": 0,
        "filter_fail_last_date": None,
        "decision": {},
        "correlation_decisions": [],
        "daily_reports": [],
        "nav_filter": "unit_net_value",
        "excluded_no_minute_symbols": [symbol for symbol in WUFU_ETF_POOL if symbol not in minute_symbols],
        "liquidity_scope": "all_market_etf",
        "warmup_rows": 0,
        "warmup_ready_symbols": 0,
        "warmup_required_days": 61,
    })
    context.schedule(_morning_regime, "09:40")
    context.schedule(_risk_monitor, "10:31")
    context.schedule(_prepare_and_sell, "13:10")
    context.schedule(_buy_targets, "13:11")
    context.log(
        "五福原生策略已初始化：启用 ETF 净值与溢价过滤；"
        f"全市场ETF={len(market_symbols)}只，动态候选={len(dynamic_groups)}只，"
        f"无分钟K的固定池标的={len([symbol for symbol in WUFU_ETF_POOL if symbol not in minute_symbols])}只"
    )


def _history_rows(context, symbol: str, count: int) -> list[dict[str, Any]]:
    bars = context.market_history_bars(symbol, count=count, timeframe="1d")
    if not bars:
        bars = context.history_bars(symbol, count=count, timeframe="1d")
    return _bars_to_history_rows(bars)


def _history_rows_batch(context, symbols: list[str], count: int) -> dict[str, list[dict[str, Any]]]:
    history = context.market_history_batch(symbols, count=count, timeframe="1d")
    market_history_enabled = bool(context.market_history_metadata.get("enabled"))
    missing = (
        [symbol for symbol in symbols if symbol not in history]
        if market_history_enabled else list(symbols)
    )
    fallback = (
        context.history_batch(missing, count=count, timeframe="1d")
        if missing else {}
    )
    return {
        symbol: _bars_to_history_rows(
            history.get(symbol) or fallback.get(symbol) or []
        )
        for symbol in symbols
    }


def _bars_to_history_rows(bars) -> list[dict[str, Any]]:
    if not bars:
        return []

    def raw_close(bar) -> float:
        execution_price = getattr(bar, "execution_price", None)
        return float(
            execution_price("close")
            if callable(execution_price)
            else getattr(bar, "raw_close", None) or bar.close
        )

    scales = [raw_close(bar) / float(bar.close) if float(bar.close) > 0 else 1.0 for bar in bars]
    aligned_closes = [0.0] * len(bars)
    correction = 1.0
    latest_scale = scales[-1]
    for index in range(len(bars) - 1, -1, -1):
        if index < len(bars) - 1:
            observed_ratio = scales[index] / scales[index + 1] if scales[index + 1] > 0 else 1.0
            split_ratio = float(getattr(bars[index + 1], "split_ratio", 1.0) or 1.0)
            if split_ratio <= 1:
                nearest = round(observed_ratio)
                if nearest >= 2 and abs(observed_ratio - nearest) / nearest <= 0.02:
                    split_ratio = float(nearest)
            if split_ratio > 1:
                correction *= observed_ratio / split_ratio
        aligned_closes[index] = float(bars[index].close) * latest_scale * correction
    return [
        {
            "date": bar.date.isoformat(),
            "close": aligned_closes[index],
            "volume": float(bar.volume),
            "amount": float(bar.amount),
        }
        for index, bar in enumerate(bars)
    ]


def _refresh_liquidity_pools(context) -> None:
    state = _state(context)
    amount_by_symbol: dict[str, float] = {}
    history = _history_rows_batch(
        context,
        list(dict.fromkeys([LIQUIDITY_CALENDAR_SYMBOL, *state["market_symbols"]])),
        5,
    )
    market_days = [row["date"] for row in history.get(LIQUIDITY_CALENDAR_SYMBOL, [])[-3:]]
    total_by_date = {day: 0.0 for day in market_days}
    for symbol in state["market_symbols"]:
        rows = [row for row in history.get(symbol, []) if row["date"] in total_by_date]
        if not rows:
            continue
        amount_by_symbol[symbol] = sum(float(row["amount"]) for row in rows) / 3
        for row in rows:
            day = str(row["date"])
            total_by_date[day] += float(row["amount"])

    if not amount_by_symbol or not total_by_date:
        normal_pool = list(state["fixed_pool"])
        weak_pool = list(state["global_pool"])
        normal_threshold = weak_threshold = None
        dynamic_pool: list[str] = []
    else:
        average_market_amount = sum(total_by_date.values()) / len(total_by_date)
        normal_threshold = average_market_amount / 20_000
        weak_threshold = average_market_amount / 3_000
        filtered_fixed = [
            symbol for symbol in state["fixed_pool"]
            if amount_by_symbol.get(symbol, 0.0) > normal_threshold
        ]
        best_by_group: dict[str, tuple[str, float]] = {}
        for symbol, group in state["dynamic_groups"].items():
            amount = amount_by_symbol.get(symbol, 0.0)
            if amount <= normal_threshold:
                continue
            current = best_by_group.get(group)
            if current is None or amount > current[1]:
                best_by_group[group] = (symbol, amount)
        dynamic_pool = [
            symbol
            for symbol, _ in sorted(best_by_group.values(), key=lambda item: item[1], reverse=True)[:100]
        ]
        normal_pool = sorted(
            set(filtered_fixed)
            | (set(dynamic_pool) if state.get("dynamic_pool_ready") else set())
        )
        weak_pool = [
            symbol for symbol in state["global_pool"]
            if amount_by_symbol.get(symbol, 0.0) > weak_threshold
        ]

    state["dynamic_pool"] = dynamic_pool
    state["normal_liquidity_pool"] = normal_pool
    state["weak_liquidity_pool"] = weak_pool
    state["normal_liquidity_threshold"] = normal_threshold
    state["weak_liquidity_threshold"] = weak_threshold
    regime = state.get("regime", "震荡期")
    state["liquidity_pool"] = list(weak_pool if regime == "走弱期" else normal_pool)
    state["liquidity_threshold"] = weak_threshold if regime == "走弱期" else normal_threshold
    state["liquidity_divisor"] = 3_000 if regime == "走弱期" else 20_000
    held = _held_symbols(context)
    subscription = sorted(set(normal_pool) | set(weak_pool) | set(held) | {DEFENSIVE_ETF})
    state["subscription_pool"] = subscription
    context.set_universe(subscription)
    context.log(
        f"五福ETF池：全市场={len(state['market_symbols'])}，固定={len(state['fixed_pool'])}，"
        f"动态={len(dynamic_pool)}，正常/震荡={len(normal_pool)}，走弱={len(weak_pool)}"
    )


def before_trading_start(context) -> None:
    state = _state(context)
    if not state["daily"]:
        loaded = 0
        ready = 0
        history = _history_rows_batch(context, context.universe, 61)
        for symbol in context.universe:
            rows = history.get(symbol, [])
            if not rows:
                continue
            state["daily"][symbol] = rows
            loaded += len(rows)
            ready += len(rows) >= 61
        state["warmup_rows"] = loaded
        state["warmup_ready_symbols"] = ready
        level = "INFO" if ready == len(context.universe) else "WARNING"
        context.log(
            f"五福日线预热：载入 {loaded} 根日K，"
            f"{ready}/{len(context.universe)} 只标的满足 61 根要求；"
            "不足的标的将等待正式回测数据累积",
            level=level,
        )
    _refresh_liquidity_pools(context)
    state["intraday"] = {
        "date": context.now.date().isoformat(),
        "close": {},
        "raw_close": {},
        "volume": {},
        "amount": {},
        "last_volume": {},
        "limit_up": {},
        "limit_down": {},
        "suspended": {},
        "tradable": {},
    }
    state["position_scale"] = 1.0
    state["regime_changed_today"] = False
    state["decision"] = {"date": context.now.date().isoformat(), "reason": "pending"}


def on_bar(context, bars) -> None:
    state = _state(context)
    intraday = state["intraday"]
    day = context.now.date().isoformat()
    if intraday.get("date") != day:
        intraday = {
            "date": day, "close": {}, "raw_close": {}, "volume": {}, "amount": {},
            "last_volume": {}, "limit_up": {}, "limit_down": {}, "suspended": {}, "tradable": {},
        }
        state["intraday"] = intraday
    for symbol, bar in bars.items():
        raw_close = bar.execution_price("close")
        intraday["close"][symbol] = raw_close
        intraday["raw_close"][symbol] = raw_close
        intraday["volume"][symbol] = float(intraday["volume"].get(symbol, 0.0)) + bar.volume
        intraday["amount"][symbol] = float(intraday["amount"].get(symbol, 0.0)) + bar.amount
        intraday["last_volume"][symbol] = float(bar.volume)
        intraday["limit_up"][symbol] = bar.limit_up
        intraday["limit_down"][symbol] = bar.limit_down
        intraday["suspended"][symbol] = bool(bar.suspended)
        intraday["tradable"][symbol] = bool(bar.tradable)
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
    state["dynamic_pool_ready"] = True
    if len(state["daily_reports"]) > 320:
        del state["daily_reports"][:-320]
    context.log(
        f"五福日终 {day}：状态={report['regime']}，目标={','.join(report['target']) or '空仓'}，"
        f"候选={len(report['candidates'])}，资产={report['equity']:.2f}"
    )


def _morning_regime(context) -> None:
    state = _state(context)
    def breadth(symbols: list[str]) -> tuple[int, int, int]:
        above_ma10 = 0
        below_ma20 = 0
        available = 0
        for symbol in symbols:
            closes = [row["close"] for row in _history_rows(context, symbol, 20)]
            if len(closes) < 20:
                continue
            available += 1
            if closes[-1] > sum(closes[-10:]) / 10:
                above_ma10 += 1
            if closes[-1] < sum(closes[-20:]) / 20:
                below_ma20 += 1
        return above_ma10, below_ma20, available

    above_ma10, below_ma20, available = breadth(REGIME_PROXIES)
    regime_source = "index"
    if available != len(REGIME_PROXIES):
        above_ma10, below_ma20, available = breadth(REGIME_FALLBACK_PROXIES)
        regime_source = "etf_fallback"
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
    if state["regime"] == "走弱期":
        state["liquidity_pool"] = list(state.get("weak_liquidity_pool", []))
        state["liquidity_threshold"] = state.get("weak_liquidity_threshold")
        state["liquidity_divisor"] = 3_000
    else:
        state["liquidity_pool"] = list(state.get("normal_liquidity_pool", []))
        state["liquidity_threshold"] = state.get("normal_liquidity_threshold")
        state["liquidity_divisor"] = 20_000
    context.log(
        f"五福状态：指标={raw}，生效={state['regime']}，"
        f"MA10上方={above_ma10}/{available}，MA20下方={below_ma20}/{available}，数据源={regime_source}"
    )


def _prepare_and_sell(context) -> None:
    state = _state(context)
    filtered_rows = _rank_candidates(context)
    candidate_rows = _candidate_pool(filtered_rows, state["regime"])
    state["filtered_rows"] = filtered_rows
    state["candidate_rows"] = candidate_rows
    print(
        "五福候选排名："
        + (
            "；".join(
                f"{rank}. {row['symbol']}（评分 {float(row['score']):.4f}）"
                for rank, row in enumerate(candidate_rows[:10], start=1)
            )
            or "无"
        )
    )
    held = _held_symbols(context)
    nav_unavailable = list(state.get("nav_unavailable_symbols", []))
    targets = (
        list(held)
        if nav_unavailable
        else _choose_targets(context, candidate_rows, filtered_rows)
    )
    filtered = {row["symbol"] for row in filtered_rows}
    all_metrics = {row["symbol"] for row in state.get("all_metric_rows", [])}
    filter_fail = [] if nav_unavailable else [
        symbol for symbol in held
        if symbol not in filtered and symbol in all_metrics and symbol not in targets
    ]
    day = context.now.date().isoformat()
    if nav_unavailable:
        state["decision"]["reason"] = "data_unavailable_hold"
        state["decision"]["trigger_reason"] = "data_unavailable_hold"
        state["decision"]["missing_inputs"] = list(nav_unavailable)
        context.log(
            "五福净值数据未更新到所需交易日，本次保留持仓且不切换防御标的"
        )
    elif filter_fail:
        if state.get("filter_fail_last_date") != day:
            state["filter_fail_streak"] = int(state.get("filter_fail_streak", 0)) + 1
            state["filter_fail_last_date"] = day
        if state["filter_fail_streak"] >= 4 and _has_price(state, DEFENSIVE_ETF):
            targets = [DEFENSIVE_ETF]
            state["filter_fail_streak"] = 0
            state["filter_fail_last_date"] = None
            state["decision"]["reason"] = "four_day_filter_fail_defensive"
            state["decision"]["trigger_reason"] = "four_day_filter_fail_defensive"
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
        if symbol not in targets and _can_trade(state, symbol):
            context.order_target_percent(symbol, 0.0)
    decision_type = (
        "empty" if not targets and not held
        else "hold" if targets == held
        else "rebalance"
    )
    context.emit_signal(
        "daily_decision",
        {
            "strategy": "five_fortunes",
            "trading_date": day,
            "decision": decision_type,
            "regime": state["regime"],
            "raw_regime": state.get("raw_regime"),
            "target_symbols": list(targets),
            "holding_symbols": list(held),
            "candidates": [
                {"symbol": row["symbol"], "score": row.get("score")}
                for row in candidate_rows[:10]
            ],
            **decision_reason_payload(state["decision"]),
        },
        event_id=f"five_fortunes:{day}:decision",
    )
    context.log(
        f"五福 13:10：状态={state['regime']}，过筛={len(filtered_rows)}，候选={len(candidate_rows)}，"
        f"目标={','.join(targets) or '空仓'}，卖出={','.join(symbol for symbol in held if symbol not in targets) or '无'}"
    )


def _buy_targets(context) -> None:
    state = _state(context)
    targets = state.get("target", [])
    if not targets:
        return
    scale = min(1.0, max(0.0, float(state.get("position_scale", 1.0))))
    available_cash = float(context.portfolio.cash) * scale
    held = set(_held_symbols(context))
    if any(symbol not in targets for symbol in held):
        context.log("五福 13:11：非目标持仓尚未卖出，不新增仓位")
        return
    submitted = []
    targets_to_buy = [
        symbol for symbol in targets
        if symbol not in held and state["rebuy_cooldown"].get(symbol, 0) <= 0
    ]
    for index, symbol in enumerate(targets_to_buy):
        if not _can_trade(state, symbol):
            continue
        raw_price = state["intraday"].get("raw_close", {}).get(symbol)
        if raw_price is None or raw_price <= 0:
            continue
        remaining = len(targets_to_buy) - index
        target_value = math.floor(available_cash / remaining)
        estimated_price = float(raw_price) * (1 + 0.0001 + 0.0001)
        target_quantity = math.floor(target_value / estimated_price / 100) * 100
        if target_quantity <= 0:
            continue
        context.order_target(symbol, target_quantity)
        available_cash -= target_value
        submitted.append(symbol)
    if submitted:
        context.log(f"五福 13:11：买入目标={','.join(submitted)}，仓位系数={scale:.0%}")
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
    sellable = [symbol for symbol in held if _can_trade(state, symbol)]
    if drawdown >= flat_threshold and sellable:
        action = "flat"
        for symbol in sellable:
            context.order_target_percent(symbol, 0.0)
            state["rebuy_cooldown"][symbol] = max(3, int(state["rebuy_cooldown"].get(symbol, 0)))
        context.log(f"五福风控：回撤{drawdown:.2%}≥{flat_threshold:.0%}，全部清仓")
    elif drawdown >= defensive_threshold:
        sell_symbols = [symbol for symbol in sellable if symbol != DEFENSIVE_ETF]
        if sell_symbols:
            action = "defensive"
            for symbol in sell_symbols:
                context.order_target_percent(symbol, 0.0)
                state["rebuy_cooldown"][symbol] = max(3, int(state["rebuy_cooldown"].get(symbol, 0)))
            context.log(f"五福风控：回撤{drawdown:.2%}≥{defensive_threshold:.0%}，切换防御ETF")
    elif drawdown >= half_threshold and sellable:
        action = "half"
        for symbol in sellable:
            quantity = float(context.portfolio.positions.get(symbol, 0.0))
            target = math.floor(quantity * 0.5 / 100) * 100
            context.order_target(symbol, target)
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


def _can_trade(state: dict[str, Any], symbol: str) -> bool:
    intraday = state.get("intraday", {})
    if intraday.get("suspended", {}).get(symbol, False):
        return False
    if intraday.get("tradable", {}).get(symbol, True) is False:
        return False
    price = intraday.get("raw_close", {}).get(symbol)
    if price is None:
        return True
    limit_up = intraday.get("limit_up", {}).get(symbol)
    limit_down = intraday.get("limit_down", {}).get(symbol)
    if limit_up is not None and float(price) >= float(limit_up) - 1e-9:
        return False
    return limit_down is None or float(price) > float(limit_down) + 1e-9


def _rank_candidates(context) -> list[dict[str, Any]]:
    state = _state(context)
    intraday = state["intraday"]
    regime = state["regime"]
    rows = []
    liquidity_pool = _liquidity_pool(state, regime)
    for symbol in liquidity_pool:
        history = _history_rows(context, symbol, 61)
        current = intraday["close"].get(symbol)
        if current is None or len(history) < 61:
            continue
        closes = [row["close"] for row in history] + [float(current)]
        volumes = [row["volume"] for row in history]
        metric = _metric_for(
            symbol, closes[-46:], volumes[-45:],
            float(intraday["volume"].get(symbol, 0.0)), context, regime,
            history[-1]["date"],
            stale_quote=float(intraday.get("last_volume", {}).get(symbol, 0.0)) <= 0,
        )
        if metric is None:
            continue
        # Correlation in the source strategy is calculated through the previous
        # trading day, so the current 13:10 snapshot must not enter this series.
        metric["history"] = [float(row["close"]) for row in history[-60:]]
        metric["regime"] = regime
        rows.append(metric)
    state["all_metric_rows"] = rows
    state["nav_unavailable_symbols"] = [
        metric["symbol"]
        for metric in rows
        if metric.get("nav_available") is False
        and _passes_non_premium_filters(metric, regime)
    ]
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
    nav_date: str,
    *,
    stale_quote: bool = False,
) -> dict[str, Any] | None:
    score, annualized, r2 = _weighted_momentum(closes, 25)
    short_score, _, _ = _weighted_momentum(closes, 21)
    if score is None or short_score is None:
        return None
    entry_score = score
    if stale_quote:
        entry_score, _, _ = _weighted_momentum([*closes[:-1], closes[-1] + ETF_PRICE_TICK], 25)
    current = closes[-1]
    ma10 = sum(closes[-10:]) / 10
    volume_ratio = _projected_volume_ratio(volumes, today_volume, context)
    laplace_s = 0.12 if regime == "走弱期" else (0.06 if regime == "正常期" else 0.05)
    laplace_value, laplace_slope = _laplace(closes, laplace_s)
    gaussian_value, gaussian_slope = _gaussian(closes)
    day_ratios = [closes[-index] / closes[-index - 1] for index in range(1, 4)]
    nav_rows = context.extra_history(
        "unit_net_value", symbol, count=1, end_date=nav_date,
    )
    required_nav_date = str(nav_date)[:10]
    nav_row = (
        nav_rows[-1]
        if nav_rows and str(nav_rows[-1].get("date") or "")[:10] == required_nav_date
        else None
    )
    try:
        nav = float(nav_row.get("value")) if nav_row is not None else None
    except (TypeError, ValueError):
        nav = None
    nav_available = nav is not None and nav > 0
    premium_rate = (closes[-2] - nav) / nav * 100 if nav is not None and nav > 0 else None
    premium_limit = 8.0 if regime == "走弱期" else (10.0 if regime == "震荡期" else 30.0)
    return {
        "symbol": symbol,
        "score": score,
        "entry_score": entry_score,
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
        "premium_rate": premium_rate,
        "premium_limit": premium_limit,
        "nav_available": nav_available,
        "passed_premium": premium_rate is not None and premium_rate <= premium_limit,
        "history": closes[-61:],
    }


def _passes_filters(metric: dict[str, Any], regime: str) -> bool:
    if not _passes_non_premium_filters(metric, regime):
        return False
    return bool(metric["passed_premium"])


def _passes_non_premium_filters(metric: dict[str, Any], regime: str) -> bool:
    if not (0 < metric["score"] <= MOMENTUM_SCORE_MAX):
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
    if regime == "走弱期":
        return list(state.get("weak_liquidity_pool", []))
    return list(state.get("normal_liquidity_pool", []))


def _candidate_pool(filtered_rows: list[dict[str, Any]], regime: str) -> list[dict[str, Any]]:
    top = filtered_rows[:10]
    if not top:
        return []
    ratio = 1.0 if regime == "走弱期" else CANDIDATE_SCORE_RATIO
    threshold = float(top[0]["score"]) * ratio
    return [
        row for row in top
        if float(row["score"]) >= threshold
    ]


def _choose_targets(
    context,
    rows: list[dict[str, Any]],
    filtered_rows: list[dict[str, Any]] | None = None,
) -> list[str]:
    state = _state(context)
    decision = state["decision"]
    if decision.get("reason") in {None, "", "pending"}:
        decision["reason"] = "ranked_target"
    decision.setdefault("trigger_reason", decision["reason"])
    if not rows:
        decision["reason"] = "no_candidate_defensive"
        decision["trigger_reason"] = "no_candidate_defensive"
        return [DEFENSIVE_ETF] if _has_price(state, DEFENSIVE_ETF) else []
    held = _held_symbols(context)
    current = held[0] if held else None
    entry_rows = [
        row for row in rows
        if row["symbol"] == current
        or float(row.get("entry_score") or row["score"]) <= MOMENTUM_SCORE_MAX
    ]
    if not entry_rows:
        entry_rows = rows
    top = entry_rows[0]["symbol"]
    target = top
    if held:
        eligible = {row["symbol"] for row in rows}
        ranked = filtered_rows or rows
        rank = next((index + 1 for index, row in enumerate(ranked) if row["symbol"] == current), None)
        if current == top:
            decision["reason"] = "hold_top_rank"
            decision["trigger_reason"] = "hold_top_rank"
        elif current in eligible:
            decision["trigger_reason"] = "rank_lag_switch"
            streak = int(state["rank_streak"].get(current, 0)) + 1
            state["rank_streak"][current] = streak
            if streak < 5:
                target = current
                decision["reason"] = "anti_churn_hold"
            else:
                state["rank_streak"][current] = 0
                decision["reason"] = "rank_lag_switch"
        else:
            state["rank_streak"][current] = 0
            switch_reason = "filter_fail_switch" if rank is None else "candidate_pool_exit_switch"
            decision["reason"] = switch_reason
            decision["trigger_reason"] = switch_reason
        if current != target:
            if current not in eligible and state.get("regime_changed_today"):
                target = current
                decision["reason"] = "regime_change_hold"
            else:
                target = _low_correlation_target(context, current, entry_rows) or current
                if target == current:
                    decision["reason"] = "high_correlation_hold"
        if target != current:
            target = _apply_correlation_hold_guard(context, current, target) or current
        decision.update({"held": current, "held_rank": rank})
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
    linear_weights = [1 + index / lookback for index in range(len(values))]
    regression_weights = [weight**2 for weight in linear_weights]
    total_weight = sum(regression_weights)
    x_mean = sum(index * weight for index, weight in enumerate(regression_weights)) / total_weight
    y_mean = sum(value * weight for value, weight in zip(values, regression_weights)) / total_weight
    var_x = sum(weight * (index - x_mean) ** 2 for index, weight in enumerate(regression_weights))
    slope = sum(
        weight * (index - x_mean) * (value - y_mean)
        for index, (value, weight) in enumerate(zip(values, regression_weights))
    ) / var_x
    predicted = [y_mean + slope * (index - x_mean) for index in range(len(values))]
    residual = sum(
        weight * (value - fit) ** 2
        for value, fit, weight in zip(values, predicted, linear_weights)
    )
    arithmetic_mean = sum(values) / len(values)
    total = sum(
        weight * (value - arithmetic_mean) ** 2
        for value, weight in zip(values, linear_weights)
    )
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
        "laplace_s", "laplace_slope", "gaussian_slope", "premium_rate",
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
        "nav_filter": state["nav_filter"],
    }


def _filter_failures(metric: dict[str, Any], regime: str) -> list[str]:
    failures = []
    if not (0 < metric["score"] <= MOMENTUM_SCORE_MAX):
        failures.append("momentum")
    if regime != "走弱期" and metric["r2"] <= (0.39 if regime == "正常期" else 0.4):
        failures.append("r2")
    if regime == "走弱期" and metric["close"] <= metric["ma10"] * 1.0001:
        failures.append("ma10")
    if metric["volume_ratio"] is None or metric["volume_ratio"] >= 1.9:
        failures.append("volume")
    if min(metric["day_ratios"]) < 0.97:
        failures.append("loss")
    if not metric["passed_premium"]:
        failures.append("premium")
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
