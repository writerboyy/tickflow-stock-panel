"""Human-readable algorithm profiles for paper strategies."""
from __future__ import annotations

import ast
from typing import Any


_PROFILES: dict[str, dict[str, Any]] = {
    "seven_stars": {
        "summary": "在大类 ETF 池中选择趋势最强且交易状态正常的单一标的，下午完成轮换，并在盘中持续执行盈利保护。",
        "steps": [
            {"title": "候选计算", "detail": "使用 25 日加权对数回归动量、拟合优度和 10 日短期收益评分，过滤停牌、近期急跌、异常放量及不可交易标的。"},
            {"title": "净值与溢价过滤", "detail": "候选必须具备可用的已披露基金净值，并通过溢价率检查；净值数据不完整时保留原持仓，不强行调仓。"},
            {"title": "调仓执行", "detail": "13:09 先卖出非目标持仓，13:10 再按可用交易资金买入排名最高的合格 ETF，避免依赖尚未到账的卖出资金。"},
            {"title": "盘中保护", "detail": "多个盘中检查点监控持仓；价格较前一交易日高点回撤达到 5% 时卖出，并禁止当日重新买回。"},
        ],
    },
    "small_cap_limitup": {
        "summary": "从全市场小市值股票中寻找具有历史涨停基因的候选，结合行业分散、周度调仓和盘中止损管理组合。",
        "steps": [
            {"title": "基础股票池", "detail": "排除科创、北交所、ST、上市时间不足及数据不完整标的，并按小市值范围形成初选池。"},
            {"title": "涨停基因评分", "detail": "检查历史涨停、连板和近期交易特征，再按申万二级行业去重，最多选择 6 只股票以降低行业集中。"},
            {"title": "周度调仓", "detail": "周度调仓日先卖出不再入选且未封板的持仓，再将资金分配给新候选；选股依赖数据缺失时保持现有仓位。"},
            {"title": "盘中风控", "detail": "上午和下午检查个股及市场跌幅，触发止损后卖出并加入当日禁买名单；1 月和 4 月按策略规则进入空仓期。"},
        ],
    },
    "five_fortunes": {
        "summary": "在多品类 ETF 中按趋势质量、流动性和净值溢价进行轮动，并根据市场状态动态选择进攻或防御标的。",
        "steps": [
            {"title": "市场状态", "detail": "盘前使用宽基和风格代理判断正常、震荡或走弱环境，并为不同环境选择对应流动性池。"},
            {"title": "多因子筛选", "detail": "综合加权动量、拟合优度、均线、成交量、近期跌幅、净值溢价及价格平滑指标过滤候选。"},
            {"title": "目标选择", "detail": "优先保留仍在高分候选范围内的持仓，并通过相关性约束避免无意义切换；没有合格候选时转入防御 ETF。"},
            {"title": "卖出与买入", "detail": "先处理风险和非目标持仓，再按剩余现金买入目标；分钟行情持续检查止损及交易状态。"},
        ],
    },
    "five_fortunes_v2": {
        "summary": "五福 ETF 轮动的增强版本，在原有趋势、流动性和溢价框架上增加弱势识别、量价背离与盘中趋势确认。",
        "steps": [
            {"title": "环境识别", "detail": "通过多个指数代理投票识别走弱或震荡环境，并动态调整动量观察窗口和流动性股票池。"},
            {"title": "候选过滤", "detail": "检查动量、R²、均线、成交量、近期跌幅、净值溢价和量价背离；不同市场环境采用不同门槛。"},
            {"title": "入场确认", "detail": "目标 ETF 还需通过 30 分钟盘中趋势斜率和拟合度确认；未确认的买单进入待处理队列，盘中重试并在尾盘执行最终处理。"},
            {"title": "持仓管理", "detail": "使用相关性保护减少频繁换仓，持续执行分钟止损；没有合格进攻标的时使用防御 ETF。"},
        ],
    },
    "strong_momentum": {
        "summary": "使用前一交易日及更早数据生成强势股候选，在集合竞价和早盘关键秒点确认强度后入场，并通过实时报价管理退出。",
        "steps": [
            {"title": "盘前候选", "detail": "候选快照只使用 D-1 及以前的日线和历史名称，避免使用当日未来数据。"},
            {"title": "早盘确认", "detail": "在 09:30:16、09:31、09:32、09:37 和 10:29 检查开盘涨幅、当前涨幅及盘中抬升强度，按强度排序后分配可用现金。"},
            {"title": "实时退出", "detail": "每次报价更新都同步持仓最高价和当前价，执行 -4% 止损、+19% 止盈、炸板回撤 1.5% 及最多持有 3 个交易日等规则。"},
            {"title": "成交约束", "detail": "委托数量按 100 股整手向下取整；T+1、涨跌停、费用、滑点和不可成交判断统一交给公共交易引擎。"},
        ],
    },
    "performance_small_cap": {
        "summary": "从低价小市值股票中筛选盈利质量和分红表现较好的标的，按月调仓，并用小盘风格流动性和指数背离控制风险。",
        "steps": [
            {"title": "基本面筛选", "detail": "使用当时已披露的收入、利润、资产和财务指标数据过滤公司，避免未来函数，再结合股息表现排序。"},
            {"title": "低价小市值排序", "detail": "排除异常名称、上市时间不足和不可交易股票，限制价格不高于 6 元，按小市值和股息质量选择最多 5 只。"},
            {"title": "月度调仓", "detail": "首次运行及每月首个交易日重新选股，先卖出落选持仓，再等权补齐目标组合；数据不完整时保留持仓。"},
            {"title": "风格风控", "detail": "监控小盘流动性信号、合成小盘指数和 MACD 背离；风险关闭时清仓并暂停交易，条件恢复后再逐步买回。"},
        ],
    },
    "four_mode": {
        "summary": "同时运行一进二、弱转强、趋势股和首板四套选股逻辑，按市场风险和模式优先级统一分配仓位。",
        "steps": [
            {"title": "盘前预选", "detail": "09:05 基于历史行情和 PIT 快照生成四类候选；09:24 评估市场风险并调整可用风险仓位和模式优先级。"},
            {"title": "竞价确认", "detail": "09:25:45 使用最后一笔可见 Tick 检查集合竞价表现，剔除不符合开盘强度、封板状态和流动性要求的候选。"},
            {"title": "开盘交易", "detail": "09:27 处理计划卖出，09:28 按模式额度买入；一进二最多 4 只，趋势股最多 3 只且不超过 35% 仓位，首板最多 4 只且不超过 30%。"},
            {"title": "盘中管理", "detail": "分钟行情持续维护 VWAP、成交量和持仓状态，使用 ATR、持有天数、涨停状态及午后检查处理止损、止盈和模式切换。"},
        ],
    },
}


def _source_identity(source: str) -> tuple[str, str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return "", ""
    doc = ast.get_docstring(tree, clean=True) or ""
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id == "STRATEGY_KIND":
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                break
            return str(value), doc
    return "", doc


def _profile_key(strategy: dict[str, Any], source: str) -> str:
    kind, doc = _source_identity(source)
    if kind in _PROFILES:
        return kind
    strategy_id = str(strategy.get("id") or "")
    name = str(strategy.get("name") or "")
    marker = f"{strategy_id} {name} {doc}".lower()
    if "七星" in marker:
        return "seven_stars"
    if "涨停基因" in marker or strategy_id == "small_cap_limitup":
        return "small_cap_limitup"
    if "五福 2.0" in marker or "五福2.0" in marker or strategy_id == "five_fortunes_v2":
        return "five_fortunes_v2"
    if "五福" in marker or "etf动量轮动" in marker:
        return "five_fortunes"
    return ""


def _mode_text(account: dict[str, Any]) -> str:
    mode = str(account.get("market_mode") or "")
    return {
        "bar_1m": "按闭合 1 分钟 K 线推进策略",
        "bar_1d": "按日 K 推进策略",
        "poll_3s": "按 3 秒实时报价快照推进策略",
        "websocket": "按 WebSocket 实时报价事件推进策略",
    }.get(mode, f"按 {mode or '当前账户'} 行情模式推进策略")


def build_algorithm_profile(
    strategy: dict[str, Any],
    account: dict[str, Any],
) -> dict[str, Any]:
    """Return strategy-specific behavior plus the account's real runtime contract."""
    source = str(strategy.get("source") or "")
    _kind, doc = _source_identity(source)
    profile = _PROFILES.get(_profile_key(strategy, source))
    if profile is None:
        callback = str(account.get("execution_mode") or "full_bar")
        callback_text = {
            "quote": "每次收到报价时执行 on_quote 回调并更新订单",
            "scheduled": "只在策略注册的定时点计算信号和订单",
            "full_bar": "每根闭合 K 线执行 on_bar 回调并更新订单",
        }.get(callback, "按策略注册的行情回调执行")
        profile = {
            "summary": doc.splitlines()[0] if doc else "该策略按照源码中注册的行情回调、定时任务和交易规则运行。",
            "steps": [
                {"title": "行情输入", "detail": _mode_text(account)},
                {"title": "信号计算", "detail": callback_text},
                {"title": "订单执行", "detail": "策略生成目标仓位或买卖委托，公共引擎统一处理可成交性、费用、滑点和资金约束。"},
            ],
        }

    config = account.get("config") if isinstance(account.get("config"), dict) else {}
    risk = account.get("risk_config") if isinstance(account.get("risk_config"), dict) else {}
    schedules = [str(value) for value in account.get("scheduled_times") or []]
    settlement = {"t0": "T+0", "t1": "T+1"}.get(
        str(config.get("settlement") or "t1").lower(),
        str(config.get("settlement") or "t1").upper(),
    )
    fill_policy = "当前行情" if config.get("fill_policy") == "close" else "下一笔可成交行情"
    runtime = [
        _mode_text(account),
        f"成交使用{fill_policy}，结算规则为 {settlement}，滑点 {float(config.get('slippage_bps') or 0):g} bps",
    ]
    if schedules:
        runtime.append(f"定时触发点：{', '.join(schedules)}")
    if risk:
        runtime.append(
            "账户风控：单标的上限 "
            f"{float(risk.get('max_symbol_exposure_pct') or 0) * 100:g}%，"
            f"单日亏损锁定 {float(risk.get('daily_loss_pct') or 0) * 100:g}%，"
            f"最大回撤锁定 {float(risk.get('max_drawdown_pct') or 0) * 100:g}%"
        )
    return {
        "summary": str(profile["summary"]),
        "steps": [dict(item) for item in profile["steps"]],
        "runtime": runtime,
    }
