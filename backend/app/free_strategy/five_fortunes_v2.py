"""五福 2.0 ETF 策略的 TickFlow 原生实现。"""
from __future__ import annotations

import math
from typing import Any


WUFU_ETF_POOL = [
    "159206.SZ", "159218.SZ", "159227.SZ", "159256.SZ", "159323.SZ", "159326.SZ",
    "159363.SZ", "159502.SZ", "159509.SZ", "159516.SZ", "159518.SZ", "159529.SZ",
    "159566.SZ", "159583.SZ", "159605.SZ", "159611.SZ", "159638.SZ", "159667.SZ",
    "159732.SZ", "159755.SZ", "159766.SZ", "159819.SZ", "159825.SZ", "159840.SZ",
    "159851.SZ", "159852.SZ", "159865.SZ", "159869.SZ", "159870.SZ", "159883.SZ",
    "159892.SZ", "159915.SZ", "159928.SZ", "159949.SZ", "159967.SZ",
    "159980.SZ", "159981.SZ", "159985.SZ", "159992.SZ", "159995.SZ", "159998.SZ",
    "161226.SZ", "501018.SH", "510300.SH", "510500.SH",
    "510760.SH", "510880.SH", "510900.SH", "511380.SH", "512010.SH",
    "512050.SH", "512070.SH", "512100.SH", "512170.SH", "512200.SH", "512400.SH",
    "512480.SH", "512660.SH", "512670.SH", "512690.SH", "512710.SH", "512800.SH",
    "512880.SH", "512890.SH", "512980.SH", "513030.SH", "513090.SH",
    "513100.SH", "513120.SH", "513180.SH", "513190.SH", "513290.SH", "513310.SH",
    "513330.SH", "513350.SH", "513360.SH", "513400.SH", "513500.SH", "513520.SH",
    "513630.SH", "513750.SH", "513920.SH", "513970.SH", "515030.SH",
    "515050.SH", "515120.SH", "515170.SH", "515210.SH", "515220.SH", "515400.SH",
    "515790.SH", "515880.SH", "515980.SH", "516150.SH", "516160.SH", "516190.SH",
    "516510.SH", "516520.SH", "517520.SH", "518880.SH", "520830.SH", "560860.SH",
    "561330.SH", "561360.SH", "561980.SH", "562500.SH", "562590.SH", "562800.SH",
    "563300.SH", "588080.SH", "588170.SH", "588200.SH", "588220.SH", "588790.SH",
]
DEFENSIVE_ETF = "511880.SH"
WUFU_MINUTE_POOL = WUFU_ETF_POOL
WUFU_DYNAMIC_POOL_EXCLUSIONS = {"159814.SZ"}
WUFU_FIXED_FUND_FALLBACKS = {
    "161226.SZ": "国投白银LOF",
    "501018.SH": "南方原油LOF",
}
CANDIDATE_SCORE_RATIO = 0.9
MOMENTUM_SCORE_MAX = 5.0
WUFU2_VERSION = "2.0"
WEAK_REGIME_PROXIES = ["000300.SH", "399101.SZ", "399006.SZ", "000510.SH"]
CHOPPY_PROXIES = WEAK_REGIME_PROXIES
LIQUIDITY_THRESHOLD_DIVISOR = 15_000
DYNAMIC_POOL_TOP_N = 150
VOLUME_RATIO_LIMIT = 1.8
TREND_LOOKBACK_MINUTES = 30
TREND_SLOPE_THRESHOLD = 0.001
TREND_R2_THRESHOLD = 0.3
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
    "trend_pending": "目标标的盘中趋势未确认，等待复检",
    "trend_confirmed": "盘中趋势确认后买入",
    "force_buy": "14:55 强制买入待确认标的",
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
    "588040.SH": "科创板指",
    "588700.SH": "科创生物",
    "588710.SH": "科半导体",
    "588760.SH": "AI科创",
    "588790.SH": "科创智能",
    "588830.SH": "科创新能",
    "588860.SH": "科创医药",
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
    "520830.SH", "159529.SZ",
]
REGIME_PROXIES = [
    "000300.SH", "399101.SZ", "399006.SZ", "000510.SH", "000852.SH", "399303.SZ",
]
REGIME_FALLBACK_PROXIES = [
    "510300.SH", "512100.SH", "159915.SZ", "563300.SH",
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
    return context.state["five_fortunes_v2"]


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
    market_symbols = list(names)
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
    # The provider ETF dimension excludes LOFs, while the source strategy keeps
    # these two funds in its fixed/global pools and local fund bars cover them.
    for symbol, name in WUFU_FIXED_FUND_FALLBACKS.items():
        names.setdefault(symbol, name)
        minute_symbols.add(symbol)
    return market_symbols, names, dynamic_groups, minute_symbols


def initialize(context) -> None:
    market_symbols, instrument_names, dynamic_groups, minute_symbols = _market_catalog(context)
    fixed_pool = [symbol for symbol in WUFU_ETF_POOL if symbol in minute_symbols]
    global_pool = [symbol for symbol in GLOBAL_ETF_POOL if symbol in minute_symbols]
    context.set_universe([*fixed_pool, DEFENSIVE_ETF])
    context.require_history(timeframe="1d", bars=61)
    context.require_market_history(asset_type="etf", timeframe="1d", bars=61)
    context.require_market_history(asset_type="index", timeframe="1d", bars=21)
    context.require_extra_history("unit_net_value")
    context.state.setdefault("five_fortunes_v2", {
        "daily": {},
        "intraday": {
            "date": None, "close": {}, "raw_close": {}, "volume": {}, "amount": {},
            "minute_closes": {},
            "last_volume": {}, "limit_up": {}, "limit_down": {}, "suspended": {}, "tradable": {},
        },
        "version": WUFU2_VERSION,
        "regime": "正常期",
        "raw_regime": "正常期",
        "regime_pending": None,
        "regime_pending_days": 0,
        "regime_last_change_date": None,
        "regime_changed_today": False,
        "is_a_share_weak": False,
        "weak_start_date": None,
        "weak_days_count": 0,
        "max_weak_days": 20,
        "weak_enter_streak": 0,
        "weak_exit_streak": 0,
        "weak_confirm_days": 1,
        "is_choppy": False,
        "weak_momentum_lookback": 25,
        "weak_momentum_lookback_base": 25,
        "weak_momentum_lookback_short": 23,
        "r2_high_streak": 0,
        "r2_low_streak": 0,
        "r2_dyn_switch_count": 0,
        "pending_buy_etfs": [],
        "minute_closes": {},
        "target": [],
        "candidate_rows": [],
        "filtered_rows": [],
        "all_metric_rows": [],
        "liquidity_pool": list(fixed_pool),
        "normal_liquidity_pool": list(fixed_pool),
        "weak_liquidity_pool": list(global_pool),
        "subscription_pool": list(fixed_pool),
        "liquidity_threshold": None,
        "normal_liquidity_threshold": None,
        "weak_liquidity_threshold": None,
        "liquidity_divisor": LIQUIDITY_THRESHOLD_DIVISOR,
        "fixed_pool": fixed_pool,
        "global_pool": global_pool,
        "dynamic_pool": [],
        "dynamic_pool_ready": True,
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
    context.schedule(_morning_pipeline, "09:00")
    context.schedule(_check_weak_period_daily, "09:40")
    context.schedule(_prepare_and_sell, "13:10")
    for retry_at in ("13:40", "14:00", "14:10", "14:30", "14:40"):
        context.schedule(_retry_pending_buys, retry_at)
    context.schedule(_force_buy_pending, "14:55")
    context.schedule(_reset_daily_flags, "15:10")
    context.log(
        "五福2.0 原生策略已初始化：弱市简化过滤，趋势确认买入；"
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
    market_symbols = set(state["market_symbols"])
    liquidity_symbols = list(dict.fromkeys([
        *state["market_symbols"], *state["fixed_pool"], *state["global_pool"],
    ]))
    history = _history_rows_batch(
        context,
        list(dict.fromkeys([LIQUIDITY_CALENDAR_SYMBOL, *liquidity_symbols])),
        5,
    )
    market_days = [row["date"] for row in history.get(LIQUIDITY_CALENDAR_SYMBOL, [])[-3:]]
    total_by_date = {day: 0.0 for day in market_days}
    for symbol in liquidity_symbols:
        rows = [row for row in history.get(symbol, []) if row["date"] in total_by_date]
        if not rows:
            continue
        amount_by_symbol[symbol] = sum(float(row["amount"]) for row in rows) / 3
        if symbol not in market_symbols:
            continue
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
        normal_threshold = average_market_amount / LIQUIDITY_THRESHOLD_DIVISOR
        weak_threshold = normal_threshold
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
            for symbol, _ in sorted(best_by_group.values(), key=lambda item: item[1], reverse=True)[:DYNAMIC_POOL_TOP_N]
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
    state["liquidity_divisor"] = LIQUIDITY_THRESHOLD_DIVISOR
    held = _held_symbols(context)
    subscription = sorted(
        set(normal_pool)
        | set(weak_pool)
        | set(held)
        | set(WUFU_FIXED_FUND_FALLBACKS)
        | {DEFENSIVE_ETF}
    )
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
        "minute_closes": {},
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
            "minute_closes": {},
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
        closes = intraday["minute_closes"].setdefault(symbol, [])
        closes.append(raw_close)
        if len(closes) > 240:
            del closes[:-240]
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


def _morning_pipeline(context) -> None:
    _refresh_liquidity_pools(context)
    state = _state(context)
    state["peak_equity"] = max(float(state.get("peak_equity", 0.0)), float(context.portfolio.total_value))


def _proxy_votes(context, symbols: list[str], lookback: int = 10) -> tuple[int, int, int]:
    above = 0
    below = 0
    available = 0
    for symbol in symbols:
        closes = [float(row["close"]) for row in _history_rows(context, symbol, lookback)]
        if len(closes) < lookback:
            continue
        available += 1
        ma_value = sum(closes[-lookback:]) / lookback
        if closes[-1] > ma_value:
            above += 1
        elif closes[-1] < ma_value:
            below += 1
    return above, below, available


def _check_choppy_market(context) -> bool:
    state = _state(context)
    choppy_count = 0
    available = 0
    for symbol in CHOPPY_PROXIES:
        closes = [float(row["close"]) for row in _history_rows(context, symbol, 11)]
        if len(closes) < 11 or closes[0] <= 0:
            continue
        available += 1
        if abs(closes[-1] / closes[0] - 1) < 0.010:
            choppy_count += 1
    was_choppy = bool(state.get("is_choppy", False))
    state["is_choppy"] = choppy_count >= 3
    if state["is_choppy"] != was_choppy:
        action = "进入" if state["is_choppy"] else "退出"
        context.log(f"五福2.0震荡检测：{action}震荡模式，横盘指数={choppy_count}/{available}")
    return bool(state["is_choppy"])


def _adjust_weak_momentum_lookback(context) -> None:
    state = _state(context)
    r2_values = []
    for symbol in state.get("global_pool", []):
        closes = [float(row["close"]) for row in _history_rows(context, symbol, 26)]
        if len(closes) < 26:
            continue
        _, _, r2 = _weighted_momentum(closes, 25)
        if r2 is not None:
            r2_values.append(float(r2))
    if not r2_values:
        return
    pool_r2 = sum(r2_values) / len(r2_values)
    if pool_r2 > 0.4:
        state["r2_high_streak"] = int(state.get("r2_high_streak", 0)) + 1
        state["r2_low_streak"] = 0
    elif pool_r2 < 0.38:
        state["r2_low_streak"] = int(state.get("r2_low_streak", 0)) + 1
        state["r2_high_streak"] = 0
    else:
        state["r2_high_streak"] = 0
        state["r2_low_streak"] = 0
    old = int(state.get("weak_momentum_lookback", 25))
    new = old
    if old == 25 and int(state.get("r2_high_streak", 0)) >= 2:
        new = 23
    elif old == 23 and int(state.get("r2_low_streak", 0)) >= 2:
        new = 25
    if new != old:
        state["weak_momentum_lookback"] = new
        state["r2_dyn_switch_count"] = int(state.get("r2_dyn_switch_count", 0)) + 1
        context.log(f"五福2.0弱市动量窗口切换：{old} -> {new}，全局R2={pool_r2:.4f}")


def _set_regime_from_flags(context, above_count: int, below_count: int, available: int) -> None:
    state = _state(context)
    previous = state.get("regime", "正常期")
    raw = "走弱期" if state.get("is_a_share_weak") else ("震荡期" if state.get("is_choppy") else "正常期")
    state["raw_regime"] = raw
    day = context.now.date().isoformat()
    state["regime"] = raw
    state["regime_changed_today"] = raw != previous
    if raw != previous or state.get("regime_last_change_date") is None:
        state["regime_last_change_date"] = day
    state["regime_pending"] = None
    state["regime_pending_days"] = 0
    if state["regime"] == "走弱期":
        state["liquidity_pool"] = list(state.get("weak_liquidity_pool", []))
        state["liquidity_threshold"] = state.get("weak_liquidity_threshold")
    else:
        state["liquidity_pool"] = list(state.get("normal_liquidity_pool", []))
        state["liquidity_threshold"] = state.get("normal_liquidity_threshold")
    state["liquidity_divisor"] = LIQUIDITY_THRESHOLD_DIVISOR
    context.log(
        f"五福2.0状态：生效={state['regime']}，MA10上方={above_count}/{available}，"
        f"MA10下方={below_count}/{available}，弱市={int(bool(state.get('is_a_share_weak')))}，"
        f"震荡={int(bool(state.get('is_choppy')))}"
    )


def _check_weak_period_daily(context) -> None:
    state = _state(context)
    above_count, below_count, available = _proxy_votes(context, WEAK_REGIME_PROXIES, 10)
    if available < len(WEAK_REGIME_PROXIES):
        fallback_above, fallback_below, fallback_available = _proxy_votes(context, REGIME_FALLBACK_PROXIES, 10)
        if fallback_available:
            above_count, below_count, available = fallback_above, fallback_below, fallback_available
    weak_condition = below_count >= 3
    exit_condition = above_count >= 3
    confirm_days = int(state.get("weak_confirm_days", 1))
    state["weak_enter_streak"] = int(state.get("weak_enter_streak", 0)) + 1 if weak_condition else 0
    state["weak_exit_streak"] = int(state.get("weak_exit_streak", 0)) + 1 if exit_condition else 0
    if state.get("is_a_share_weak"):
        state["weak_days_count"] = int(state.get("weak_days_count", 0)) + 1
        if state["weak_days_count"] >= int(state.get("max_weak_days", 20)) or state["weak_exit_streak"] >= confirm_days:
            state["is_a_share_weak"] = False
            state["weak_start_date"] = None
            state["weak_days_count"] = 0
    elif state["weak_enter_streak"] >= confirm_days:
        state["is_a_share_weak"] = True
        state["weak_start_date"] = context.now.date().isoformat()
        state["weak_days_count"] = 0
        state["weak_enter_streak"] = 0
    _adjust_weak_momentum_lookback(context)
    _check_choppy_market(context)
    _set_regime_from_flags(context, above_count, below_count, available)
    _refresh_liquidity_pools(context)


def _prepare_and_sell(context) -> None:
    state = _state(context)
    filtered_rows = _rank_candidates(context)
    candidate_rows = _candidate_pool(filtered_rows, state["regime"])
    state["filtered_rows"] = filtered_rows
    state["candidate_rows"] = candidate_rows
    targets = _choose_targets(context, candidate_rows, filtered_rows)
    held = _held_symbols(context)
    day = context.now.date().isoformat()
    state["filter_fail_streak"] = 0
    state["filter_fail_last_date"] = None
    state["target"] = targets
    state["decision"].update({
        "target": list(targets),
        "filtered_count": len(filtered_rows),
        "candidate_count": len(candidate_rows),
        "filter_fail_symbols": [],
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
            "strategy": "five_fortunes_v2",
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
        event_id=f"five_fortunes_v2:{day}:decision",
    )
    context.log(
        f"五福2.0 13:10：状态={state['regime']}，过筛={len(filtered_rows)}，候选={len(candidate_rows)}，"
        f"目标={','.join(targets) or '空仓'}，卖出={','.join(symbol for symbol in held if symbol not in targets) or '无'}"
    )
    _buy_targets(context)


def _buy_targets(
    context,
    *,
    force: bool = False,
) -> None:
    state = _state(context)
    targets = state.get("target", [])
    if not targets:
        return
    held = set(_held_symbols(context))
    targets_to_buy = [
        symbol for symbol in targets
        if symbol not in held and state["rebuy_cooldown"].get(symbol, 0) <= 0
    ]
    submitted = []
    still_pending = []
    for symbol in targets_to_buy:
        if not _can_trade(state, symbol):
            continue
        if not force and not _intraday_trend_confirmed(state, symbol):
            still_pending.append(symbol)
            continue
        raw_price = state["intraday"].get("raw_close", {}).get(symbol)
        if raw_price is None or float(raw_price) <= 0:
            still_pending.append(symbol)
            continue
        context.order_cash_weight(symbol, 1.0)
        submitted.append(symbol)
    state["pending_buy_etfs"] = still_pending
    if submitted:
        state["decision"]["reason"] = "force_buy" if force else "trend_confirmed"
        context.log(f"五福2.0 买入目标={','.join(submitted)}，模式={'强制' if force else '趋势确认'}")
    if still_pending:
        state["decision"]["reason"] = "trend_pending"
        context.log(f"五福2.0 待趋势确认={','.join(still_pending)}")


def _retry_pending_buys(context) -> None:
    state = _state(context)
    pending = list(state.get("pending_buy_etfs", []))
    if not pending:
        return
    state["target"] = pending
    _buy_targets(context, force=False)


def _force_buy_pending(context) -> None:
    state = _state(context)
    pending = list(state.get("pending_buy_etfs", []))
    if not pending:
        return
    state["target"] = pending
    _buy_targets(context, force=True)


def _reset_daily_flags(context) -> None:
    state = _state(context)
    state["pending_buy_etfs"] = []


def _intraday_trend_confirmed(state: dict[str, Any], symbol: str) -> bool:
    closes = [
        float(value)
        for value in state.get("intraday", {}).get("minute_closes", {}).get(symbol, [])[-TREND_LOOKBACK_MINUTES:]
        if float(value) > 0
    ]
    if not closes:
        return True
    if len(closes) < 5:
        return False
    n = len(closes)
    weights = [0.5 + 1.5 * index / max(n - 1, 1) for index in range(n)]
    total_weight = sum(weights)
    weighted = [value / total_weight for value in weights]
    x_values = list(range(n))
    x_bar = sum(weight * x for weight, x in zip(weighted, x_values))
    y_bar = sum(weight * y for weight, y in zip(weighted, closes))
    dx = [x - x_bar for x in x_values]
    dy = [y - y_bar for y in closes]
    variance_x = sum(weight * value * value for weight, value in zip(weighted, dx))
    slope = (
        sum(weight * x_delta * y_delta for weight, x_delta, y_delta in zip(weighted, dx, dy)) / variance_x
        if variance_x else 0.0
    )
    mean_price = y_bar if y_bar > 0 else sum(closes) / n
    slope_pct = slope / mean_price * 100 if mean_price > 0 else 0.0
    intercept = y_bar - slope * x_bar
    predicted = [slope * x + intercept for x in x_values]
    ss_res = sum(weight * (actual - fitted) ** 2 for weight, actual, fitted in zip(weighted, closes, predicted))
    ss_tot = sum(weight * (actual - y_bar) ** 2 for weight, actual in zip(weighted, closes))
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return slope_pct > TREND_SLOPE_THRESHOLD and r2 > TREND_R2_THRESHOLD


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
    current_time = context.now.strftime("%H:%M")
    if not ("09:25" < current_time < "11:30" or "13:00" < current_time < "14:57"):
        return
    threshold = 0.95
    for symbol in _held_symbols(context):
        bar = bars.get(symbol)
        cost = context.portfolio.avg_cost.get(symbol, 0.0)
        current = bar.execution_price("close") if bar is not None else None
        if current is None or cost <= 0 or current > cost * threshold:
            continue
        context.order_target_percent(symbol, 0.0)
        context.log(f"五福止损：{symbol} 现价{current:.4f} <= 成本{cost:.4f}×{threshold:.0%}")


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
        )
        if metric is None:
            continue
        # Correlation in the source strategy is calculated through the previous
        # trading day, so the current 13:10 snapshot must not enter this series.
        metric["history"] = [float(row["close"]) for row in history[-60:]]
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
    nav_date: str,
) -> dict[str, Any] | None:
    state = _state(context)
    lookback = int(state.get("weak_momentum_lookback", 25)) if regime == "走弱期" else 25
    score, annualized, r2 = _weighted_momentum(closes, lookback)
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
    nav_rows = context.extra_history(
        "unit_net_value", symbol, count=1, end_date=nav_date,
    )
    nav = nav_rows[-1]["value"] if nav_rows else None
    premium_rate = (closes[-2] - nav) / nav * 100 if nav is not None and nav > 0 else None
    premium_limit = 30.0
    passed_volume_divergence, volume_divergence = _volume_price_divergence(closes[:-1], volumes)
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
        "premium_rate": premium_rate,
        "premium_limit": premium_limit,
        "passed_premium": premium_rate is None or premium_rate <= premium_limit,
        "passed_momentum": 0 <= score <= MOMENTUM_SCORE_MAX,
        "passed_r2": r2 is not None and r2 > 0.4,
        "passed_ma": current > ma10,
        "passed_volume": volume_ratio is not None and volume_ratio < VOLUME_RATIO_LIMIT,
        "passed_loss": min(day_ratios) >= 0.97,
        "passed_laplace": current > laplace_value and laplace_slope > 0.002,
        "passed_volume_divergence": passed_volume_divergence,
        "volume_divergence": volume_divergence,
        "momentum_lookback": lookback,
        "history": closes[-61:],
    }


def _passes_filters(metric: dict[str, Any], regime: str) -> bool:
    if not metric.get("passed_momentum", False):
        return False
    if not metric.get("passed_r2", False):
        return False
    if regime == "走弱期":
        return True
    if not metric.get("passed_volume", False):
        return False
    if not metric.get("passed_loss", False):
        return False
    if regime == "震荡期" and not metric.get("passed_volume_divergence", True):
        return False
    return True


def _volume_price_divergence(
    hist_closes: list[float],
    hist_volumes: list[float],
) -> tuple[bool, dict[str, Any]]:
    lookback = 5
    if len(hist_closes) < lookback + 1 or len(hist_volumes) < lookback + 1:
        return True, {"reason": "insufficient_data"}
    earlier = hist_volumes[-lookback - 1:-3]
    recent = hist_volumes[-3:]
    earlier_volume = sum(earlier) / len(earlier) if earlier else 0.0
    if earlier_volume <= 0:
        return True, {"reason": "earlier_vol_zero"}
    price_change = hist_closes[-1] / hist_closes[-lookback - 1] - 1 if hist_closes[-lookback - 1] else 0.0
    volume_change = (sum(recent) / len(recent)) / earlier_volume - 1 if recent else 0.0
    is_divergence = price_change > 0.02 and volume_change < -0.10
    return not is_divergence, {
        "reason": "divergence" if is_divergence else "ok",
        "price_change": price_change,
        "volume_change": volume_change,
        "is_divergence": is_divergence,
    }


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
    entry_rows = rows
    held_in_pool = [row for row in entry_rows if row["symbol"] in held]
    if held_in_pool:
        target = max(held_in_pool, key=lambda row: float(row.get("score") or float("-inf")))["symbol"]
        decision["reason"] = "hold_top_rank"
        decision["trigger_reason"] = "hold_top_rank"
    else:
        target = entry_rows[0]["symbol"]
        if held:
            ranked = filtered_rows or rows
            current = held[0]
            rank = next((index + 1 for index, row in enumerate(ranked) if row["symbol"] == current), None)
            decision["reason"] = "filter_fail_switch" if rank is None else "candidate_pool_exit_switch"
            decision["trigger_reason"] = decision["reason"]
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
        "passed_volume_divergence", "momentum_lookback",
    )
    candidates = [{key: row.get(key) for key in candidate_keys} for row in state["candidate_rows"]]
    filter_rejections: dict[str, int] = {}
    for row in state.get("all_metric_rows", []):
        for reason in _filter_failures(row, state["regime"]):
            filter_rejections[reason] = filter_rejections.get(reason, 0) + 1
    return {
        "date": context.now.date().isoformat(),
        "regime": state["regime"],
        "raw_regime": state.get("raw_regime"),
        "version": state.get("version", WUFU2_VERSION),
        "is_a_share_weak": bool(state.get("is_a_share_weak")),
        "is_choppy": bool(state.get("is_choppy")),
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
    if not metric.get("passed_momentum", False):
        failures.append("momentum")
    if not metric.get("passed_r2", False):
        failures.append("r2")
    if regime == "走弱期":
        return failures
    if not metric.get("passed_volume", False):
        failures.append("volume")
    if not metric.get("passed_loss", False):
        failures.append("loss")
    if regime == "震荡期" and not metric.get("passed_volume_divergence", True):
        failures.append("volume_divergence")
    return failures
