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
WUFU_GROUP_NAME_OVERRIDES = {
    "161226.SZ": "国投白银LOF",
    "513000.SH": "225ETF",
    "513350.SH": "油气ETF",
    "515030.SH": "新汽车",
    "516190.SH": "文娱ETF",
    "520500.SH": "恒生新药",
    "561100.SH": "电子龙头",
    "561980.SH": "芯片设备",
    "588710.SH": "科半导体",
    "588760.SH": "AI科创",
    "588790.SH": "科创智能",
    "588830.SH": "科创新能",
    "588890.SH": "科创芯",
    "588990.SH": "科芯片",
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
REGIME_PROXIES = ["510300.SH", "510500.SH", "159915.SZ", "512100.SH", "563300.SH", "510050.SH"]

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
        if symbol in minute_symbols
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
        "dynamic_groups": dynamic_groups,
        "instrument_names": instrument_names,
        "market_symbols": market_symbols,
        "market_instrument_count": len(market_symbols),
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
        "五福 TickFlow 适配已初始化：ETF NAV/溢价过滤无数据，已跳过；"
        f"全市场ETF={len(market_symbols)}只，动态候选={len(dynamic_groups)}只，"
        f"无分钟K的固定池标的={len([symbol for symbol in WUFU_ETF_POOL if symbol not in minute_symbols])}只"
    )


def _history_rows(context, symbol: str, count: int) -> list[dict[str, Any]]:
    market_history = getattr(context, "market_history_bars", None)
    bars = market_history(symbol, count=count, timeframe="1d") if callable(market_history) else []
    if not bars:
        bars = context.history_bars(symbol, count=count, timeframe="1d")
    return [
        {
            "date": bar.date.isoformat(),
            "close": float(bar.close),
            "volume": float(bar.volume),
            "amount": float(bar.amount),
        }
        for bar in bars
    ]


def _refresh_liquidity_pools(context) -> None:
    state = _state(context)
    amount_by_symbol: dict[str, float] = {}
    market_days = [row["date"] for row in _history_rows(context, REGIME_PROXIES[0], 3)]
    total_by_date = {day: 0.0 for day in market_days}
    for symbol in state["market_symbols"]:
        rows = [row for row in _history_rows(context, symbol, 5) if row["date"] in total_by_date]
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
        normal_pool = sorted(set(filtered_fixed) | set(dynamic_pool))
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
    subscription = sorted(set(normal_pool) | set(weak_pool) | set(REGIME_PROXIES) | set(held) | {DEFENSIVE_ETF})
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
        for symbol in context.universe:
            rows = _history_rows(context, symbol, 61)
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
    state["intraday"] = {"date": context.now.date().isoformat(), "close": {}, "volume": {}, "amount": {}}
    state["risk_mode"] = None
    state["position_scale"] = 1.0
    state["regime_changed_today"] = False
    state["decision"] = {"date": context.now.date().isoformat(), "reason": "pending"}


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
    above_ma10 = 0
    below_ma20 = 0
    available = 0
    for symbol in REGIME_PROXIES:
        closes = [row["close"] for row in _history_rows(context, symbol, 20)]
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
    allocation = min(1.0, max(0.0, float(state.get("position_scale", 1.0)))) / len(targets)
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
    if regime == "走弱期":
        return list(state.get("weak_liquidity_pool", []))
    return list(state.get("normal_liquidity_pool", []))


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
