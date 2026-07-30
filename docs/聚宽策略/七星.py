# 克隆自聚宽文章：https://www.joinquant.com/post/69163
# 标题：【策略优化】ETF轮动策略优化-V1.7.1-跟单稳健版
# 作者：晨曦量化

# 策略名称：七星高照ETF轮动策略-V1.7.1-跟单稳健版
# 策略作者：屌丝逆袭量化
# 优化时间：2026-03-29
# 优化内容：
# 1、接入 jq_qmt 聚宽跟单桥接
# 2、补充 pandas 依赖与前一交易日兜底
# 3、增加盈利保护卖出后的当日禁回补

import numpy as np
import math
import pandas as pd
from jqdata import *
from jq_trade_capital import install_trade_capital_helpers

try:
    from jq_qmt import qmt_order, init_qmt, get_last_bridge_error

    JQ_QMT_AVAILABLE = True
    JQ_QMT_IMPORT_ERROR = ""
except Exception as e:
    qmt_order = None
    init_qmt = None
    get_last_bridge_error = None
    JQ_QMT_AVAILABLE = False
    JQ_QMT_IMPORT_ERROR = str(e)


install_trade_capital_helpers(globals())

# 策略资金配置：优先使用固定金额；若固定金额为 0，则按比例资金执行。
STRATEGY_TRADE_CAPITAL_RATIO = 1.0
STRATEGY_TRADE_CAPITAL_LIMIT = 100000
QMT_BRIDGE_STRATEGY_ID = "QX"


def _bool_cn(value):
    return "是" if bool(value) else "否"


def _is_force_enable_qmt_in_backtest():
    return bool(getattr(g, "force_enable_qmt_in_backtest", False))


def _normalize_qmt_price(price):
    try:
        price_value = float(price)
    except Exception:
        return 0.0
    return price_value if price_value > 0 else 0.0


def _init_copy_trade_bridge(context):
    g.copy_trade_strategy_id = QMT_BRIDGE_STRATEGY_ID
    g.copy_trade_enabled = False
    g.copy_trade_bridge_ready = False

    if not JQ_QMT_AVAILABLE or init_qmt is None:
        return False

    try:
        bridge_ready = init_qmt(
            context,
            strategy_id=g.copy_trade_strategy_id,
            quiet=True,
            verify_connection=True,
            force_enable_in_backtest=_is_force_enable_qmt_in_backtest(),
            state_prefix="copy_trade",
        )
    except Exception as e:
        bridge_ready = False
        log.error("QMT桥接初始化失败: {}".format(e))

    g.copy_trade_enabled = bool(bridge_ready)
    g.copy_trade_bridge_ready = bool(bridge_ready)
    return bool(bridge_ready)


def _log_copy_trade_bridge_status(context, phase):
    bridge_error = get_last_bridge_error() if callable(get_last_bridge_error) else ""
    if not bridge_error:
        bridge_error = JQ_QMT_IMPORT_ERROR
    run_type = getattr(getattr(context, "run_params", None), "type", "-")
    log.info(
        "QMT桥接状态 阶段={} 已启用={} 已就绪={} 策略={} run_type={} last_bridge_error={}".format(
            phase or "-",
            _bool_cn(getattr(g, "copy_trade_enabled", False)),
            _bool_cn(getattr(g, "copy_trade_bridge_ready", False)),
            getattr(g, "copy_trade_strategy_id", QMT_BRIDGE_STRATEGY_ID),
            run_type or "-",
            bridge_error or "-",
        )
    )


def _get_qmt_order_uuid(qmt_result):
    if isinstance(qmt_result, dict):
        return str(qmt_result.get("order_uuid") or "-")
    return "-"


def _get_qmt_result_reason(qmt_result):
    if isinstance(qmt_result, dict):
        return str(qmt_result.get("reason") or "-")
    return "-"


def _send_qmt_order_delta(security, amount, price, stage=""):
    try:
        amount_value = int(amount)
    except Exception:
        amount_value = 0
    if amount_value == 0:
        return False, {}

    strategy_id = getattr(g, "copy_trade_strategy_id", QMT_BRIDGE_STRATEGY_ID)
    price_value = _normalize_qmt_price(price)

    if not getattr(g, "copy_trade_enabled", False):
        bridge_error = get_last_bridge_error() if callable(get_last_bridge_error) else ""
        log.warning(
            "QMT信号未发送 security={} amount={} price={:.3f} strategy_id={} stage={} "
            "reason=copy_trade_disabled last_bridge_error={}".format(
                security,
                amount_value,
                price_value,
                strategy_id,
                stage or "-",
                bridge_error or JQ_QMT_IMPORT_ERROR or "-",
            )
        )
        return False, {}

    if qmt_order is None:
        log.warning(
            "QMT信号未发送 security={} amount={} price={:.3f} strategy_id={} stage={} "
            "reason=qmt_order_unavailable last_bridge_error={}".format(
                security,
                amount_value,
                price_value,
                strategy_id,
                stage or "-",
                JQ_QMT_IMPORT_ERROR or "-",
            )
        )
        return False, {}

    try:
        qmt_result = qmt_order(
            security,
            amount_value,
            price_value,
            strategy_id=strategy_id,
            return_detail=True,
        )
    except Exception as e:
        log.error(
            "QMT信号发送异常 security={} amount={} price={:.3f} strategy_id={} stage={} error={}".format(
                security,
                amount_value,
                price_value,
                strategy_id,
                stage or "-",
                e,
            )
        )
        return False, {}

    qmt_detail = qmt_result if isinstance(qmt_result, dict) else {}
    qmt_ok = bool(qmt_detail.get("ok")) if isinstance(qmt_result, dict) else bool(qmt_result)
    if not qmt_ok:
        log.warning(
            "QMT信号发送失败 security={} amount={} price={:.3f} strategy_id={} stage={} reason={} "
            "last_bridge_error={} order_uuid={}".format(
                security,
                amount_value,
                price_value,
                strategy_id,
                stage or "-",
                _get_qmt_result_reason(qmt_detail),
                get_last_bridge_error() if callable(get_last_bridge_error) else "-",
                _get_qmt_order_uuid(qmt_detail),
            )
        )
        return False, qmt_detail

    return True, qmt_detail


def initialize(context):
    """
    初始化函数：保留 1.7 原始策略框架，只增加跟单与少量稳定性修复
    """
    set_option("avoid_future_data", True)
    set_option("use_real_price", True)
    set_slippage(PriceRelatedSlippage(0.0001), type="fund")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0,
            open_commission=0.0002,
            close_commission=0.0002,
            close_today_commission=0,
            min_commission=5,
        ),
        type="fund",
    )
    set_benchmark("510300.XSHG")

    log.set_level("order", "error")
    log.set_level("system", "error")
    log.set_level("strategy", "debug")
    log.info("========== 策略初始化开始 ==========")

    if not hasattr(g, "force_enable_qmt_in_backtest"):
        g.force_enable_qmt_in_backtest = True
    # 显式跟单：不使用 hook，固定先发 QMT，再提交聚宽本地单。
    g.copy_trade_send_first = True
    g.copy_trade_send_first_on_buy = True
    g.copy_trade_send_first_on_sell = True
    _init_copy_trade_bridge(context)
    _log_copy_trade_bridge_status(context, "initialize")
    g.trade_capital_ratio = STRATEGY_TRADE_CAPITAL_RATIO  # 资金使用比例：1.0=使用全部回测资金，0.5=只使用50%
    g.trade_capital_limit = STRATEGY_TRADE_CAPITAL_LIMIT  # 固定使用资金(元)，>0 时优先生效，例如 30000 表示按 3 万元运行
    setup_trade_capital(context, "七星高照ETF轮动策略")

    g.etf_pool_bak = [
        "518880.XSHG",
        "159985.XSHE",
        "501018.XSHG",
        "161226.XSHE",
        "513100.XSHG",
        "159915.XSHE",
        "511220.XSHG",
    ]

    g.etf_pool = [
        "518880.XSHG",
        "159980.XSHE",
        "159985.XSHE",
        "501018.XSHG",
        "161226.XSHE",
        "159981.XSHE",
        "513100.XSHG",
        "159509.XSHE",
        "513290.XSHG",
        "513500.XSHG",
        "159529.XSHE",
        "513400.XSHG",
        "513520.XSHG",
        "513030.XSHG",
        "513080.XSHG",
        "513310.XSHG",
        "513730.XSHG",
        "159792.XSHE",
        "513130.XSHG",
        "513050.XSHG",
        "159920.XSHE",
        "513690.XSHG",
        "510300.XSHG",
        "510500.XSHG",
        "510050.XSHG",
        "510210.XSHG",
        "159915.XSHE",
        "588080.XSHG",
        "512100.XSHG",
        "563360.XSHG",
        "563300.XSHG",
        "512890.XSHG",
        "159967.XSHE",
        "512040.XSHG",
        "159201.XSHE",
        "511380.XSHG",
        "511010.XSHG",
        "511220.XSHG",
    ]

    g.lookback_days = 25
    g.holdings_num = 1
    g.defensive_etf = "511880.XSHG"
    g.enable_defensive_etf = False
    g.excluded_etfs = {
        "511220.XSHG",  # 城投债ETF
        "511010.XSHG",  # 国债ETF
        "511380.XSHG",  # 可转债ETF
    }
    g.min_money = 5000

    g.enable_profit_protection = True
    g.profit_protection_lookback = 1
    g.profit_protection_threshold = 0.05
    g.profit_protection_check_times = [
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

    g.loss = 0.97
    g.min_score_threshold = 0
    g.max_score_threshold = 100.0

    g.enable_volume_check = True
    g.volume_lookback = 5
    g.volume_threshold = 2
    g.volume_return_limit = 1

    g.use_short_momentum_filter = True
    g.short_lookback_days = 10
    g.short_momentum_threshold = 0.0

    g.enable_premium_filter = True
    g.premium_threshold = 0.20

    g.rankings_cache = {"date": None, "data": None}
    g.enable_reentry_block = True
    g.reentry_block_date = None
    g.reentry_blocked_today = set()

    run_daily(check_positions, time="09:10")
    run_daily(etf_sell_trade, time="13:09")
    run_daily(etf_buy_trade, time="13:10")

    for check_time in g.profit_protection_check_times:
        run_daily(profit_protection_check, time=check_time)
        log.info("已注册盈利保护检查时间：{}".format(check_time))

    log.info(
        "策略初始化完成：ETF池{}只，动量周期{}天，持仓{}只".format(
            len(g.etf_pool), g.lookback_days, g.holdings_num
        )
    )
    log.info(
        "盈利保护开关：{}，回看周期{}天，回撤阈值{:.0f}%".format(
            "开启" if g.enable_profit_protection else "关闭",
            g.profit_protection_lookback,
            g.profit_protection_threshold * 100,
        )
    )
    if g.enable_premium_filter:
        log.info("溢价率过滤已启用，阈值：{:.0f}%".format(g.premium_threshold * 100))
    else:
        log.info("溢价率过滤未启用")
    log.info("禁买ETF：{}".format(",".join(sorted(g.excluded_etfs))))
    log.info("防御ETF模式：{}".format("开启" if g.enable_defensive_etf else "关闭，空仓"))
    log.info("========== 策略初始化完成 ==========")


def after_code_changed(context):
    if not hasattr(g, "force_enable_qmt_in_backtest"):
        g.force_enable_qmt_in_backtest = True
    g.copy_trade_send_first = getattr(g, "copy_trade_send_first", True)
    g.copy_trade_send_first_on_buy = getattr(g, "copy_trade_send_first_on_buy", True)
    g.copy_trade_send_first_on_sell = getattr(g, "copy_trade_send_first_on_sell", True)
    g.trade_capital_ratio = STRATEGY_TRADE_CAPITAL_RATIO
    g.trade_capital_limit = STRATEGY_TRADE_CAPITAL_LIMIT
    _init_copy_trade_bridge(context)
    _log_copy_trade_bridge_status(context, "after_code_changed")
    refresh_trade_capital_base(context, "七星高照ETF轮动策略", reason="after_code_changed")
    setup_trade_capital(context, "七星高照ETF轮动策略")
    g.rankings_cache = {"date": None, "data": None}
    g.defensive_etf = getattr(g, "defensive_etf", "511880.XSHG")
    g.enable_defensive_etf = getattr(g, "enable_defensive_etf", False)
    g.excluded_etfs = getattr(
        g,
        "excluded_etfs",
        {
            "511220.XSHG",
            "511010.XSHG",
            "511380.XSHG",
        },
    )


def is_excluded_etf(security):
    return security in getattr(g, "excluded_etfs", set())


def is_managed_etf(security):
    defensive_etf = getattr(g, "defensive_etf", None)
    return (
        security in getattr(g, "etf_pool", [])
        or security in getattr(g, "excluded_etfs", set())
        or (defensive_etf is not None and security == defensive_etf)
    )


def reset_reentry_blocklist(context):
    today = context.current_dt.date()
    if getattr(g, "reentry_block_date", None) != today:
        g.reentry_block_date = today
        g.reentry_blocked_today = set()


def add_reentry_block(security, context):
    if not getattr(g, "enable_reentry_block", False):
        return
    reset_reentry_blocklist(context)
    g.reentry_blocked_today.add(security)


def get_prev_trade_date(context):
    trade_days = get_trade_days(end_date=context.current_dt.date(), count=2)
    if trade_days is None or len(trade_days) == 0:
        return context.current_dt.date()
    if len(trade_days) == 1:
        return trade_days[0]
    return trade_days[0]


# ==================== 盈利保护独立检查函数 ====================
def profit_protection_check(context):
    """
    独立执行的盈利保护检查函数
    遍历所有持仓，若触发盈利保护则卖出
    """
    if not g.enable_profit_protection:
        log.debug("盈利保护模块已关闭，跳过检查")
        return

    reset_reentry_blocklist(context)
    log.info("========== 盈利保护独立检查开始 ==========")
    for sec in list(context.portfolio.positions.keys()):
        if not is_managed_etf(sec):
            continue
        pos = context.portfolio.positions[sec]
        if pos.total_amount > 0:
            if check_profit_protection(sec, context):
                if smart_order_target_value(sec, 0, context):
                    add_reentry_block(sec, context)
                    log.info("🛡️ 盈利保护卖出（独立检查）：{} {}".format(sec, get_name(sec)))
    log.info("========== 盈利保护独立检查完成 ==========")


def check_profit_protection(security, context, lookback=None, threshold=None):
    """
    检查是否触发盈利保护（从最近N日最高点回撤超过阈值）
    """
    if not g.enable_profit_protection:
        return False

    lookback = lookback or g.profit_protection_lookback
    threshold = threshold or g.profit_protection_threshold

    hist = attribute_history(security, lookback, "1d", ["high"])
    if hist.empty or len(hist) < lookback:
        log.debug("{} {} 历史数据不足{}天，无法检查盈利保护".format(security, get_name(security), lookback))
        return False

    max_high = hist["high"].max()
    current_price = get_current_data()[security].last_price

    if current_price <= max_high * (1 - threshold):
        log.info(
            "🔻 {} {} 触发盈利保护：当前价{:.3f}，最近{}日最高{:.3f}，回撤{:.2f}% > {:.0f}%".format(
                security,
                get_name(security),
                current_price,
                lookback,
                max_high,
                (1 - current_price / max_high) * 100,
                threshold * 100,
            )
        )
        return True
    return False


# ==================== 溢价率获取函数 ====================
def get_premium_rate(code, date):
    """
    获取指定日期的溢价率（使用前一日净值，适合盘中判断）
    """
    price_data = get_price(
        code,
        start_date=date,
        end_date=date,
        frequency="daily",
        fields=["close"],
    )
    if price_data is None or price_data.empty:
        log.debug("{} {} 无交易价格数据".format(date, code))
        return None, None, None

    price = price_data["close"].iloc[0]

    net_data = get_extras("unit_net_value", code, start_date=date, end_date=date, df=True)
    if net_data.empty or code not in net_data.columns or pd.isna(net_data[code].iloc[0]):
        try:
            q = query(finance.FUND_NET_VALUE).filter(
                finance.FUND_NET_VALUE.code == code,
                finance.FUND_NET_VALUE.day == date,
            )
            net_df = finance.run_query(q)
            if not net_df.empty:
                net_value = net_df["net_value"].iloc[0]
            else:
                log.debug("{} {} 无净值数据".format(date, code))
                return None, None, None
        except Exception:
            log.debug("{} {} 查询净值异常".format(date, code))
            return None, None, None
    else:
        net_value = net_data[code].iloc[0]

    if not net_value:
        return None, price, net_value

    premium_rate = (price - net_value) / net_value
    return premium_rate, price, net_value


# ==================== 核心计算模块 ====================
def get_cached_rankings(context):
    today = context.current_dt.date()
    if g.rankings_cache["date"] != today:
        log.info("重新计算ETF排名...")
        ranked = get_ranked_etfs(context)
        g.rankings_cache = {"date": today, "data": ranked}
    else:
        log.debug("使用缓存的ETF排名")
    return g.rankings_cache["data"]


def get_ranked_etfs(context):
    etf_metrics = []
    for etf in g.etf_pool:
        if is_excluded_etf(etf):
            log.debug("{} {} 在禁买名单中，跳过".format(etf, get_name(etf)))
            continue
        if get_current_data()[etf].paused:
            log.debug("{} {} 停牌，跳过".format(etf, get_name(etf)))
            continue

        metrics = calculate_momentum_metrics(context, etf)
        if metrics is not None:
            if g.min_score_threshold < metrics["score"] < g.max_score_threshold:
                etf_metrics.append(metrics)
            else:
                log.debug("{} {} 得分{:.2f}超出阈值，过滤".format(etf, metrics["etf_name"], metrics["score"]))

    etf_metrics.sort(key=lambda x: x["score"], reverse=True)
    return etf_metrics


def calculate_momentum_metrics(context, etf):
    try:
        name = get_name(etf)
        lookback = max(g.lookback_days, g.short_lookback_days) + 20
        prices = attribute_history(etf, lookback, "1d", ["close", "high"])
        if len(prices) < g.lookback_days:
            log.debug("{} {} 历史数据不足{}天，跳过".format(etf, name, len(prices)))
            return None

        current_price = get_current_data()[etf].last_price
        price_series = np.append(prices["close"].values, current_price)

        if check_profit_protection(etf, context):
            log.info("🚫 {} {} 触发盈利保护，从排名中排除".format(etf, name))
            return None

        if g.enable_volume_check:
            vol_ratio = get_volume_ratio(context, etf)
            if vol_ratio is not None:
                annualized = get_annualized_returns(price_series, g.lookback_days)
                if annualized > g.volume_return_limit:
                    log.info(
                        "📉 {} {} 成交量放量{:.1f}倍，且年化{:.1f}% > 阈值{:.1f}%，过滤".format(
                            etf,
                            name,
                            vol_ratio,
                            annualized * 100,
                            g.volume_return_limit * 100,
                        )
                    )
                    return None

        if len(price_series) >= g.short_lookback_days + 1:
            short_return = price_series[-1] / price_series[-(g.short_lookback_days + 1)] - 1
            short_annualized = (1 + short_return) ** (250 / g.short_lookback_days) - 1
        else:
            short_annualized = 0

        if g.use_short_momentum_filter and short_annualized < g.short_momentum_threshold:
            log.debug(
                "{} {} 短期动量{:.1f}% < 阈值{:.1f}%，过滤".format(
                    etf,
                    name,
                    short_annualized * 100,
                    g.short_momentum_threshold * 100,
                )
            )
            return None

        recent = price_series[-(g.lookback_days + 1):]
        y = np.log(recent)
        x = np.arange(len(y))
        weights = np.linspace(1, 2, len(y))
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.exp(slope * 250) - 1

        ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
        ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0

        score = annualized_returns * r_squared

        if len(price_series) >= 4:
            day1 = price_series[-1] / price_series[-2]
            day2 = price_series[-2] / price_series[-3]
            day3 = price_series[-3] / price_series[-4]
            if min(day1, day2, day3) < g.loss:
                log.info("⚠️ {} {} 近3日有单日跌幅超{:.1f}%，直接排除".format(etf, name, (1 - g.loss) * 100))
                return None

        return {
            "etf": etf,
            "etf_name": name,
            "annualized_returns": annualized_returns,
            "r_squared": r_squared,
            "score": score,
            "current_price": current_price,
            "short_annualized": short_annualized,
        }
    except Exception as e:
        log.warning("计算{} {}时出错: {}".format(etf, get_name(etf), e))
        return None


def get_annualized_returns(price_series, lookback_days):
    recent = price_series[-(lookback_days + 1):]
    y = np.log(recent)
    x = np.arange(len(y))
    weights = np.linspace(1, 2, len(y))
    slope, _ = np.polyfit(x, y, 1, w=weights)
    return math.exp(slope * 250) - 1


def get_volume_ratio(context, security, lookback=None, threshold=None):
    lookback = lookback or g.volume_lookback
    threshold = threshold or g.volume_threshold
    try:
        name = get_name(security)
        hist = attribute_history(security, lookback, "1d", ["volume"])
        if hist.empty or len(hist) < lookback:
            return None
        avg_vol = hist["volume"].mean()

        today = context.current_dt.date()
        df_vol = get_price(
            security,
            start_date=today,
            end_date=context.current_dt,
            frequency="1m",
            fields=["volume"],
            skip_paused=False,
            fq="pre",
        )
        if df_vol is None or df_vol.empty:
            return None
        current_vol = df_vol["volume"].sum()
        ratio = current_vol / avg_vol if avg_vol > 0 else 0
        if ratio > threshold:
            log.debug("{} {} 成交量比{:.2f} > {}".format(security, name, ratio, threshold))
            return ratio
        return None
    except Exception as e:
        log.warning("成交量计算失败 {}: {}".format(security, e))
        return None


# ==================== 卖出模块 ====================
def check_positions(context):
    reset_reentry_blocklist(context)
    for sec in context.portfolio.positions:
        pos = context.portfolio.positions[sec]
        if pos.total_amount > 0:
            log.info(
                "📊 持仓：{} {} 数量{} 成本{:.3f} 现价{:.3f}".format(
                    sec, get_name(sec), pos.total_amount, pos.avg_cost, pos.price
                )
            )


def etf_sell_trade(context):
    log.info("========== 卖出操作开始 ==========")

    ranked = get_cached_rankings(context)
    target_etfs = []
    for metrics in ranked[: g.holdings_num]:
        if metrics["score"] >= g.min_score_threshold:
            target_etfs.append(metrics["etf"])

    if not target_etfs:
        log.info("今日无目标ETF，卖出后保持空仓")

    target_set = set(target_etfs)

    for sec in list(context.portfolio.positions.keys()):
        if not is_managed_etf(sec):
            continue
        if sec not in target_set:
            pos = context.portfolio.positions[sec]
            if pos.total_amount > 0:
                if smart_order_target_value(sec, 0, context):
                    log.info("📤 卖出不在目标的持仓：{} {}".format(sec, get_name(sec)))

    if g.enable_premium_filter:
        prev_date = get_prev_trade_date(context)
        for sec in list(context.portfolio.positions.keys()):
            if not is_managed_etf(sec):
                continue
            pos = context.portfolio.positions[sec]
            if pos.total_amount > 0:
                premium, _, _ = get_premium_rate(sec, prev_date)
                if premium is not None and premium > g.premium_threshold:
                    if smart_order_target_value(sec, 0, context):
                        log.info(
                            "🚨 溢价率过高 {} {} 溢价率{:.2f}% > {:.0f}%，卖出".format(
                                sec,
                                get_name(sec),
                                premium * 100,
                                g.premium_threshold * 100,
                            )
                        )

    log.info("========== 卖出操作完成 ==========")


# ==================== 买入模块 ====================
def etf_buy_trade(context):
    log.info("========== 买入操作开始 ==========")
    reset_reentry_blocklist(context)

    ranked = get_cached_rankings(context)
    log.info("=== ETF排名前5 ===")
    for i, metrics in enumerate(ranked[:5]):
        log.info(
            "排名{}: {} {} 得分{:.4f} 年化{:.2f}% R²={:.4f}".format(
                i + 1,
                metrics["etf"],
                metrics["etf_name"],
                metrics["score"],
                metrics["annualized_returns"] * 100,
                metrics["r_squared"],
            )
        )

    target_etfs = []
    prev_date = None
    if g.enable_premium_filter:
        prev_date = get_prev_trade_date(context)

    blocked = getattr(g, "reentry_blocked_today", set())

    for metrics in ranked:
        if len(target_etfs) >= g.holdings_num:
            break

        if metrics["score"] < g.min_score_threshold:
            continue

        etf = metrics["etf"]

        if etf in blocked:
            log.info("⛔ {} {} 当日已触发盈利保护，禁回补跳过".format(etf, get_name(etf)))
            continue

        if g.enable_profit_protection and check_profit_protection(etf, context):
            log.info("🚫 {} {} 触发盈利保护，从买入候选列表中排除".format(etf, get_name(etf)))
            continue

        if g.enable_premium_filter:
            premium, _, _ = get_premium_rate(etf, prev_date)
            if premium is None:
                log.info("⚠️ {} {} 无法获取溢价率，视为不合格，跳过".format(etf, get_name(etf)))
                continue
            if premium > g.premium_threshold:
                log.info(
                    "🚫 {} {} 溢价率{:.2f}% > {:.0f}%，跳过".format(
                        etf, get_name(etf), premium * 100, g.premium_threshold * 100
                    )
                )
                continue
            log.info(
                "✅ {} {} 溢价率{:.2f}% ≤ {:.0f}%，通过".format(
                    etf, get_name(etf), premium * 100, g.premium_threshold * 100
                )
            )

        target_etfs.append(etf)
        log.info("🎯 目标ETF {}: {} {} 得分{:.4f}".format(len(target_etfs), etf, metrics["etf_name"], metrics["score"]))

    if not target_etfs:
        log.info("💤 无目标ETF，且已关闭债券/防御ETF兜底，保持空仓")
        return

    current_etf_pos = [s for s in context.portfolio.positions if is_managed_etf(s)]
    to_sell = [s for s in current_etf_pos if s not in target_etfs]
    if to_sell:
        to_sell_names = [get_name(s) for s in to_sell]
        log.info("尚有持仓需要卖出：{}，等待卖出完成再买入".format(list(zip(to_sell, to_sell_names))))
        return

    total_val = get_effective_portfolio_value(context)
    if total_val <= 0:
        log.info("当前资金使用度为0，跳过买入")
        return
    target_per_etf = total_val / len(target_etfs)

    for etf in target_etfs:
        current_val = 0
        if etf in context.portfolio.positions:
            pos = context.portfolio.positions[etf]
            if pos.total_amount > 0:
                current_val = pos.total_amount * pos.price

        if abs(current_val - target_per_etf) > target_per_etf * 0.05 or current_val == 0:
            if smart_order_target_value(etf, target_per_etf, context):
                action = "买入" if current_val < target_per_etf else "调仓"
                log.info("📦 {}：{} {} 目标金额{:.2f}".format(action, etf, get_name(etf), target_per_etf))

    log.info("========== 买入操作完成 ==========")


# ==================== 辅助函数 ====================
def get_name(security):
    try:
        return get_current_data()[security].name
    except Exception:
        return "未知"


def check_defensive_etf_available(context):
    if not getattr(g, "enable_defensive_etf", False):
        return False
    data = get_current_data()
    etf = g.defensive_etf
    if data[etf].paused:
        log.debug("防御ETF {} {} 停牌".format(etf, get_name(etf)))
        return False
    if data[etf].last_price >= data[etf].high_limit:
        log.debug("防御ETF {} {} 涨停".format(etf, get_name(etf)))
        return False
    if data[etf].last_price <= data[etf].low_limit:
        log.debug("防御ETF {} {} 跌停".format(etf, get_name(etf)))
        return False
    return True


def _submit_smart_order(security, diff, price, name):
    qmt_ok, qmt_result = _send_qmt_order_delta(security, diff, price, stage="smart_order")
    if not qmt_ok:
        log.warning(
            "QMT下单失败，继续本地下单: {} {} 数量{} 原因={} order_uuid={}".format(
                security,
                name,
                diff,
                _get_qmt_result_reason(qmt_result),
                _get_qmt_order_uuid(qmt_result),
            )
        )

    order_result = order(security, diff)
    if order_result:
        log.info(
            "{} {} {} 数量{} 价格{:.3f}".format(
                "📥 买入" if diff > 0 else "📤 卖出",
                security,
                name,
                abs(diff),
                price,
            )
        )
        return True
    log.warning(
        "本地下单失败，但QMT信号已先发送: {} {} 数量{} order_uuid={}".format(
            security,
            name,
            diff,
            _get_qmt_order_uuid(qmt_result),
        )
    )
    return False


def smart_order_target_value(security, target_value, context):
    """
    智能下单：根据目标市值调整持仓，处理停牌、涨跌停、最小交易金额、T+1
    """
    data = get_current_data()
    name = get_name(security)

    if data[security].paused:
        log.info("{} {} 停牌，跳过".format(security, name))
        return False

    price = data[security].last_price
    if price == 0:
        log.info("{} {} 当前价格0，跳过".format(security, name))
        return False

    target_amount = int(target_value / price)
    target_amount = (target_amount // 100) * 100
    if target_amount <= 0 and target_value > 0:
        target_amount = 100

    cur_pos = context.portfolio.positions.get(security, None)
    cur_amount = cur_pos.total_amount if cur_pos else 0
    diff = target_amount - cur_amount

    if diff > 0:
        if data[security].last_price >= data[security].high_limit:
            log.info("{} {} 涨停，跳过买入".format(security, name))
            return False
    elif diff < 0:
        if data[security].last_price <= data[security].low_limit:
            log.info("{} {} 跌停，跳过卖出".format(security, name))
            return False

    trade_val = abs(diff) * price
    if 0 < trade_val < g.min_money:
        log.info("{} {} 交易金额{:.2f} < {}，跳过".format(security, name, trade_val, g.min_money))
        return False

    if diff < 0:
        closeable = cur_pos.closeable_amount if cur_pos else 0
        if closeable == 0:
            log.info("{} {} 当天买入不可卖出".format(security, name))
            return False
        diff = -min(abs(diff), closeable)

    if diff != 0:
        return bool(_submit_smart_order(security, diff, price, name))
    return False


def trade(context):
    """
    主交易函数，为了兼容性保留
    """
    pass
