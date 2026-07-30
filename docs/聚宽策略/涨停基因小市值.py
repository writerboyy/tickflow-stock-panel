# 克隆自聚宽文章：https://www.joinquant.com/post/70363
# 标题：年化113%-连续涨停基因的股池轮动机会 速度优化版
# 作者：不会卖股票

# 克隆自聚宽文章：https://www.joinquant.com/post/70173
# 标题：年化113%-连续涨停基因的股池轮动机会
# 作者：發蔡

# 克隆自聚宽文章：https://www.joinquant.com/post/67057
# 标题：带涨停基因的小市值 已转PT代码 并附PT回测结果 2602
# 作者：种咖啡果得咖啡因

# 克隆自聚宽文章：https://www.joinquant.com/post/64881
# 标题：【变种小狮子】带涨停基因的股池轮动V2.2(BUGFIX)
# 作者：0xtao

from jqdata import *
from jqfactor import *
import numpy as np
import pandas as pd
import datetime
from datetime import time
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

STRATEGY_TRADE_CAPITAL_RATIO = 1.0
STRATEGY_TRADE_CAPITAL_LIMIT = 130000
QMT_BRIDGE_STRATEGY_ID = "XSZ"

# 变体：500天2连板史 + 避开最热新鲜度前10%


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

#初始化函数 
def initialize(context):
    # 开启防未来函数
    set_option('avoid_future_data', True)
    # 设定基准
    set_benchmark('399101.XSHE')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 将滑点设置为0
    set_slippage(PriceRelatedSlippage(0.002), type="stock")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.0005,
            open_commission=0.0001,
            close_commission=0.0001,
            close_today_commission=0,
            min_commission=1,
        ),
        type="stock",
    )
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    if not hasattr(g, "force_enable_qmt_in_backtest"):
        g.force_enable_qmt_in_backtest = True
    g.copy_trade_send_first = True
    g.copy_trade_send_first_on_buy = True
    g.copy_trade_send_first_on_sell = True
    #初始化全局变量 bool
    g.no_trading_today_signal = False  # 是否为可交易日
    g.no_trading_months = [1, 4]  # 空仓月份，可按需修改，例如 [] / [4] / [1, 4]
    g.run_stoploss = True  # 是否进行止损
    #全局变量list
    g.hold_list = [] #当前持仓的全部股票    
    g.yesterday_HL_list = [] #记录持仓中昨日涨停的股票
    g.target_list = []
    g.not_buy_again = []
    g.filter_loss_black = True
    g.loss_black = {} # 止损后拉黑
    #全局变量
    g.stock_num = 6
    g.up_price = 20  # 设置股票单价 
    g.limit_days_window = 3 * 250 # 历史涨停的参考窗口期
    g.lianban_window = 500 # 2连板史和新鲜度的参考窗口期
    g.freshness_exclude_pct = 0.10 # 剔除最新鲜涨停股票的比例
    g.init_stock_count = 1000 # 初始股池的数量
    g.reason_to_sell = ''
    g.stoploss_strategy = 3  # 1为止损线止损，2为市场趋势止损, 3为联合1、2策略
    g.stoploss_limit = 0.91  # 止损线
    g.stoploss_market = 0.93  # 市场趋势止损参数
    
    g.HV_control = False #新增，Ture是日频判断是否放量，False则不然
    g.HV_duration = 120 #HV_control用，周期可以是240-120-60，默认比例是0.9
    g.HV_ratio = 0.9    #HV_control用
    g.stockL = []
    # g.no_trading_buy = ['600036.XSHG','518880.XSHG','600900.XSHG']  # 空仓月份持有 
    g.no_trading_buy = []  # 空仓月份持有  TODO
    g.no_trading_hold_signal = False
    g.stock_list_cache_date = None
    g.stock_list_cache = []
    g.trade_capital_ratio = STRATEGY_TRADE_CAPITAL_RATIO
    g.trade_capital_limit = STRATEGY_TRADE_CAPITAL_LIMIT
    _init_copy_trade_bridge(context)
    _log_copy_trade_bridge_status(context, "initialize")
    setup_trade_capital(context, "连扳基因小市值")
    # 设置交易运行时间
    run_daily(prepare_stock_list, '9:05')
    run_weekly(weekly_sell,2,'10:15')
    run_weekly(weekly_buy,2,'10:30')
    run_daily(sell_stocks, time='10:00') # 止损函数
    run_daily(trade_afternoon, time='14:20') #检查持仓中的涨停股是否需要卖出
    run_daily(trade_afternoon, time='14:55') #检查持仓中的涨停股是否需要卖出
    run_daily(close_account, '14:50')
    # run_weekly(print_position_info, 5, time='15:10')


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
    refresh_trade_capital_base(context, "连扳基因小市值", reason="after_code_changed")
    setup_trade_capital(context, "连扳基因小市值")


#1-1 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    #获取昨日涨停列表
    if g.hold_list != []:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit','low_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        g.yesterday_HL_list = list(df.code)
    else:
        g.yesterday_HL_list = []
    #判断今天是否为账户资金再平衡的日期
    g.no_trading_today_signal = today_is_between(context)


def get_history_highlimit(context, stock_list, days=3*250, p=0.10):
    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency="daily",
        fields=["close", "high_limit"],
        count=days,
        panel=False,
        fill_paused=False,
    )
    df = df[df["close"] == df["high_limit"]]
    grouped_result = df.groupby('code').size().reset_index(name='count')
    grouped_result = grouped_result.sort_values(by=["count"], ascending=False)
    result_list = grouped_result["code"].tolist()[:int(len(grouped_result)*p)]
    log.info(f"筛选前合计{len(grouped_result)}个， 筛选后合计{len(result_list)}个")

    return result_list
    

def filter_lianban_history_stock(context, stock_list, days=500):
    if len(stock_list) == 0:
        log.info("进入500天2连板史筛选前为空")
        return []

    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency="daily",
        fields=["close", "high_limit"],
        count=days,
        panel=False,
        fill_paused=False,
    )

    result_list = []
    for code, group in df.groupby('code'):
        group = group.sort_values('time')
        is_limit_up = group["close"] == group["high_limit"]
        prev_limit_up = is_limit_up.shift(1).fillna(False)
        if (is_limit_up & prev_limit_up).any():
            result_list.append(code)

    log.info("进入500天2连板史筛选前%d个，500天内有2连板史的股票%d个" % (len(stock_list), len(result_list)))
    return result_list


def filter_fresh_limitup_stock(context, stock_list, days=500, exclude_pct=0.10):
    if len(stock_list) == 0:
        log.info("进入新鲜度过滤前为空")
        return []

    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency="daily",
        fields=["close", "high_limit"],
        count=days,
        panel=False,
        fill_paused=False,
    )

    freshness_list = []
    for code, group in df.groupby('code'):
        group = group.sort_values('time').reset_index(drop=True)
        limit_hit_rows = group[group["close"] == group["high_limit"]]
        if not limit_hit_rows.empty:
            latest_limit_idx = limit_hit_rows.index[-1]
            days_since_limit = len(group) - 1 - latest_limit_idx
            freshness_list.append((code, days_since_limit))

    if len(freshness_list) == 0:
        log.info("500天2连板史股票0个，新鲜度剔除后合计0个")
        return []

    freshness_df = pd.DataFrame(freshness_list, columns=["code", "days_since_limit"])
    freshness_df = freshness_df.sort_values(by=["days_since_limit", "code"], ascending=[True, True])
    exclude_count = int(len(freshness_df) * exclude_pct)
    result_list = freshness_df["code"].tolist()[exclude_count:]

    log.info("500天2连板史股票%d个，新鲜度剔除前10%%=%d个，筛选后合计%d个" % (len(freshness_df), exclude_count, len(result_list)))
    return result_list
    
    
def get_start_point(context, stock_list, days=3*250):
    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency="daily",
        fields=["open", "low", "close", "high_limit"],
        count=days,
        panel=False,
        fill_paused=False,
    )
    stock_start_point = {}
    stock_price_bias = {}
    current_data = get_current_data()
    for code, group in df.groupby('code'):
        group = group.sort_values('time')
        
        # 找到所有close等于high_limit的行
        limit_hit_rows = group[group['close'] == group['high_limit']]

        if not limit_hit_rows.empty:
            # 获取最近的涨停行（时间最大的）
            latest_limit_hit = limit_hit_rows.iloc[-1]
            latest_limit_index = latest_limit_hit.name
            
            # 获取该涨停行之前的所有行（按时间倒序，便于向前查找）
            previous_rows = group[group.index <= latest_limit_index].iloc[::-1]
            
            # 寻找第一个close < open的行
            target_row = None
            for idx, row in previous_rows.iterrows():
                if row['close'] < row['open']:
                    # print(code, row['time'], row['close'])
                    stock_start_point[code] = row['low']
                    break
    
    # 计算股票当前价格与历史启动点的偏移量
    for code, start_point in stock_start_point.items():
        last_price = current_data[code].last_price
        bias = last_price / start_point
        stock_price_bias[code] = bias
    
    sorted_list = sorted(stock_price_bias.items(), key=lambda x: x[1], reverse=False)

    return [i[0] for i in sorted_list]


def get_ranked_history_candidates(context, stock_list, lianban_days=500, start_days=3 * 250, exclude_pct=0.10):
    if len(stock_list) == 0:
        log.info('历史涨停特征筛选前为空')
        return []

    history_days = max(lianban_days, start_days)
    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency='daily',
        fields=['open', 'low', 'close', 'high_limit'],
        count=history_days,
        panel=False,
        fill_paused=False,
    )
    if df is None or df.empty:
        log.info('历史行情为空')
        return []

    df = df.sort_values(['code', 'time']).reset_index(drop=True)
    df['is_limit_up'] = df['close'] == df['high_limit']

    recent_df = df.groupby('code', group_keys=False).tail(lianban_days).copy()
    recent_df['prev_limit_up'] = recent_df.groupby('code')['is_limit_up'].shift(1).fillna(False)

    lianban_codes = recent_df.loc[
        recent_df['is_limit_up'] & recent_df['prev_limit_up'], 'code'
    ].drop_duplicates().tolist()
    log.info(
        '进入500天2连板史筛选前%d个，500天内有2连板史的股票%d个'
        % (len(stock_list), len(lianban_codes))
    )
    if len(lianban_codes) == 0:
        return []

    freshness_df = recent_df[recent_df['code'].isin(lianban_codes)].copy()
    freshness_df['row_no'] = freshness_df.groupby('code').cumcount()
    freshness_df['row_count'] = freshness_df.groupby('code')['code'].transform('size')
    latest_limit_df = freshness_df[freshness_df['is_limit_up']].groupby('code', group_keys=False).tail(1).copy()
    if latest_limit_df.empty:
        log.info('500天2连板史股票0个，新鲜度剔除后合计0个')
        return []

    latest_limit_df['days_since_limit'] = latest_limit_df['row_count'] - 1 - latest_limit_df['row_no']
    latest_limit_df = latest_limit_df[['code', 'days_since_limit']].sort_values(
        by=['days_since_limit', 'code'], ascending=[True, True]
    )

    exclude_count = int(len(latest_limit_df) * exclude_pct)
    selected_codes = latest_limit_df['code'].tolist()[exclude_count:]
    log.info(
        '500天2连板史股票%d个，新鲜度剔除前10%%=%d个，筛选后合计%d个'
        % (len(latest_limit_df), exclude_count, len(selected_codes))
    )
    if len(selected_codes) == 0:
        return []

    start_df = df[df['code'].isin(selected_codes)].copy()
    start_df['bearish_low'] = np.where(start_df['close'] < start_df['open'], start_df['low'], np.nan)
    start_df['last_bearish_low'] = start_df.groupby('code')['bearish_low'].ffill()
    latest_limit_rows = start_df[start_df['is_limit_up']].groupby('code', group_keys=False).tail(1).copy()
    latest_limit_rows = latest_limit_rows[['code', 'last_bearish_low']].dropna(subset=['last_bearish_low'])
    if latest_limit_rows.empty:
        return []

    current_data = get_current_data()
    bias_list = []
    for row in latest_limit_rows.itertuples(index=False):
        last_price = current_data[row.code].last_price
        start_point = row.last_bearish_low
        if start_point and start_point > 0 and last_price and last_price > 0:
            bias_list.append((row.code, last_price / start_point))

    bias_list.sort(key=lambda x: x[1])
    return [code for code, _ in bias_list]

#1-2 选股模块
def get_stock_list(context):
    if g.stock_list_cache_date == context.previous_date:
        log.info('复用当日选股缓存: %s' % str(context.previous_date))
        return list(g.stock_list_cache)

    final_list = []
    yesterday = context.previous_date
    securities_df = get_all_securities("stock", yesterday)
    if securities_df is None or securities_df.empty:
        return []

    cutoff_date = yesterday - datetime.timedelta(days=375)
    start_dates = pd.to_datetime(securities_df['start_date']).dt.date
    initial_list = securities_df[start_dates <= cutoff_date].index.tolist()

    current_data = get_current_data()
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_st_stock(initial_list, current_data)
    initial_list = filter_paused_stock(initial_list, current_data)
    if len(initial_list) == 0:
        g.stock_list_cache_date = context.previous_date
        g.stock_list_cache = []
        return []
    
    if g.filter_loss_black:
        initial_list = filter_loss_black(context, initial_list, days=20) # 过滤最近20天被止损的股票
        if len(initial_list) == 0:
            g.stock_list_cache_date = context.previous_date
            g.stock_list_cache = []
            return []
    
    q = query(
        valuation.code,indicator.eps
        ).filter(
            valuation.code.in_(initial_list)
            ).order_by(
                valuation.market_cap.asc()
                )
    df = get_fundamentals(q)
    initial_list = df['code'].tolist()[:g.init_stock_count]
    if len(initial_list) == 0:
        g.stock_list_cache_date = context.previous_date
        g.stock_list_cache = []
        return []

    minute_close = history(1, unit='1m', field='close', security_list=initial_list)
    initial_list = filter_limitup_stock(context, initial_list, current_data, minute_close)
    initial_list = filter_limitdown_stock(context, initial_list, current_data, minute_close)
    initial_list = get_ranked_history_candidates(
        context,
        initial_list,
        g.lianban_window,
        g.limit_days_window,
        g.freshness_exclude_pct,
    )
    if len(initial_list) == 0:
        log.info('历史涨停特征筛选后为空')
        g.stock_list_cache_date = context.previous_date
        g.stock_list_cache = []
        return []

    stock_list = get_stock_industry(initial_list)
    final_list = stock_list[:g.stock_num*2]
    log.info('今日前10:%s' % final_list)
    g.stock_list_cache_date = context.previous_date
    g.stock_list_cache = list(final_list)
    
    return final_list


#1-3 整体调整持仓
def weekly_sell(context):
    if g.no_trading_today_signal == False:
        current_data = get_current_data()
        close_no_trading_hold(context)
        #获取应买入列表 
        g.not_buy_again = []
        g.target_list = get_stock_list(context)
        target_list = g.target_list[:g.stock_num*2]
        log.info(str(target_list))

        #调仓卖出
        for stock in g.hold_list:
            if (stock not in target_list) and (stock not in g.yesterday_HL_list) and (current_data[stock].last_price < current_data[stock].high_limit):
                log.info("卖出[%s]" % (stock))
                position = context.portfolio.positions[stock]
                close_position(context, position)
            else:
                pass
                log.info("已持有[%s]" % (stock))

            
            
#1-3 整体调整持仓
def weekly_buy(context):
    if g.no_trading_today_signal == False:
        current_data = get_current_data()
        #获取应买入列表 
        g.not_buy_again = []
        g.target_list = get_stock_list(context)
        target_list = g.target_list[:g.stock_num*2]
        log.info(str(target_list))

        #调仓买入
        buy_security(context,target_list)
        #记录已买入股票
        for position in list(context.portfolio.positions.values()):
            stock = position.security
            g.not_buy_again.append(stock)


#1-4 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt
    if g.yesterday_HL_list != []:
        latest_df = get_price(
            g.yesterday_HL_list,
            end_date=now_time,
            frequency='1m',
            fields=['close', 'high_limit'],
            skip_paused=False,
            fq='pre',
            count=1,
            panel=False,
            fill_paused=True,
        )
        if latest_df is None or latest_df.empty:
            return
        latest_df = latest_df.sort_values(['code', 'time']).drop_duplicates(['code'], keep='last').set_index('code')
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.yesterday_HL_list:
            if context.portfolio.positions[stock].closeable_amount > -100:
                if stock not in latest_df.index:
                    continue
                if latest_df.at[stock, 'close'] < latest_df.at[stock, 'high_limit']:
                    log.info("[%s]涨停打开，卖出" % (stock))
                    position = context.portfolio.positions[stock]
                    close_position(context, position)
                    g.reason_to_sell = 'limitup'
                    # g.limitup_cash += context.portfolio.positions[stock].total_amount
                    # g.limitup_number += 1
                else:
                    log.info("[%s]涨停，继续持有" % (stock))


#1-5 如果昨天有股票卖出或者买入失败，剩余的金额今天早上买入
def check_remain_amount(context):
    if g.reason_to_sell == 'limitup': #判断提前售出原因，如果是涨停售出则次日再次交易，如果是止损售出则不交易
        g.hold_list= []
        for position in list(context.portfolio.positions.values()):
            stock = position.security
            g.hold_list.append(stock)
        if len(g.hold_list) < g.stock_num:
            target_list = get_stock_list(context)
            #剔除本周一曾买入的股票，不再买入
            target_list = filter_not_buy_again(target_list)
            target_list = target_list[:min(g.stock_num, len(target_list))]
            log.info('有余额可用'+str(round((context.portfolio.cash),2))+'元。'+ str(target_list))
            buy_security(context,target_list)
        g.reason_to_sell = ''

    else:
        # log.info('虽然有余额（'+str(round((context.portfolio.cash),2))+'元）可用，但是为止损后余额，下周再交易')
        g.reason_to_sell = ''


#1-6 下午检查交易
def trade_afternoon(context):
    if g.no_trading_today_signal == False:
        check_limit_up(context)
        if g.HV_control == True:
            check_high_volume(context)
        huanshou(context)
        
        check_remain_amount(context)
        
        
#1-7 止盈止损
def sell_stocks(context):
    if g.run_stoploss == True:
        if g.stoploss_strategy == 1:
            for stock in context.portfolio.positions.keys():
                # 股票盈利大于等于100%则卖出
                if context.portfolio.positions[stock].price >= context.portfolio.positions[stock].avg_cost * 2:
                    smart_order_target_value(stock, 0, context)
                    log.debug("收益100%止盈,卖出{}".format(stock))
                    g.loss_black[stock] = context.current_dt
                    g.stock_list_cache_date = None

                # 止损
                elif context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:
                    smart_order_target_value(stock, 0, context)
                    log.debug("收益止损,卖出{}".format(stock))
                    g.reason_to_sell = 'stoploss'
                    g.loss_black[stock] = context.current_dt
                    g.stock_list_cache_date = None

        elif g.stoploss_strategy == 2:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)
            #down_ratio = (stock_df['close'] / stock_df['open'] < 1).sum() / len(stock_df)
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean()
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in context.portfolio.positions.keys():
                    smart_order_target_value(stock, 0, context)
        elif g.stoploss_strategy == 3:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean()
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in context.portfolio.positions.keys():
                    smart_order_target_value(stock, 0, context)
            else:
                for stock in context.portfolio.positions.keys():
                    if context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:
                        smart_order_target_value(stock, 0, context)
                        log.debug("收益止损,卖出{}".format(stock))
                        g.reason_to_sell = 'stoploss'
                        g.loss_black[stock] = context.current_dt
                        g.stock_list_cache_date = None

                        

# 3-2 调整放量股票
def check_high_volume(context):
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        if current_data[stock].paused == True:
            continue
        if current_data[stock].last_price == current_data[stock].high_limit:
            continue
        if context.portfolio.positions[stock].closeable_amount ==0:
            continue
        df_volume = get_bars(stock,count=g.HV_duration,unit='1d',fields=['volume'],include_now=True, df=True)
        if df_volume['volume'].values[-1] > g.HV_ratio*df_volume['volume'].values.max():
            position = context.portfolio.positions[stock]
            r = close_position(context, position)
            log.info(f"[{stock}]天量，卖出, close_position: {r}")
            g.reason_to_sell = 'limitup'

            
            
#2-1 过滤停牌股票
def filter_paused_stock(stock_list, current_data=None):
    if current_data is None:
        current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]



#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list, current_data=None):
    if current_data is None:
        current_data = get_current_data()
    return [stock for stock in stock_list
            if not current_data[stock].is_st
            and 'ST' not in current_data[stock].name
            and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name]


#2-3 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    return [stock for stock in stock_list if stock[0] not in ('4', '8') and stock[:2] != '68']


#2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list, current_data=None, last_prices=None):
    if len(stock_list) == 0:
        return []
    if current_data is None:
        current_data = get_current_data()
    if last_prices is None:
        last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock].iloc[-1] < current_data[stock].high_limit]


#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list, current_data=None, last_prices=None):
    if len(stock_list) == 0:
        return []
    if current_data is None:
        current_data = get_current_data()
    if last_prices is None:
        last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    return [stock for stock in stock_list if (stock in context.portfolio.positions.keys()
            or last_prices[stock].iloc[-1] > current_data[stock].low_limit) 
            ]


#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date <  datetime.timedelta(days=375)]


#2-6.5 过滤股价
def filter_highprice_stock(context,stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock].iloc[-1] <= g.up_price]


#2-7 删除本周一买入的股票
def filter_not_buy_again(stock_list):
    return [stock for stock in stock_list if stock not in g.not_buy_again]
    
# 过滤最近被止损的股票
def filter_loss_black(context, stock_list, days=20):
    result_list = []
    for stock in stock_list:
        if (
            stock in g.loss_black.keys()
            and context.current_dt - g.loss_black[stock]
            < datetime.timedelta(days=days)
        ):
            log.info(
                f"{stock}由于近期止损被过滤, 止损时间：{g.loss_black[stock]}"
            )
            continue
        result_list.append(stock)
    return result_list
    
    
# 获取股票所属行业
def get_stock_industry(stock):
    result = get_industry(security=stock)
    selected_stocks = []
    industry_list = []

    for stock_code, info in result.items():
        industry_name = info['sw_l2']['industry_name']
        if industry_name not in industry_list:
            industry_list.append(industry_name)
            selected_stocks.append(stock_code)
            # print(f"行业信息: {industry_name} (股票: {stock_code})")
            # 选取了 10 个不同行业的股票
            if len(industry_list) == 10 :
                break
    return selected_stocks

            
#换手率计算
def huanshoulv(context, stock, is_avg=False):
    if is_avg:
        # 计算平均换手率
        start_date = context.current_dt - datetime.timedelta(days=20)
        end_date = context.previous_date
        df_volume = get_price(stock,end_date=end_date, frequency='daily', fields=['volume'],count=20)
        df_cap = get_valuation(stock, end_date=end_date, fields=['circulating_cap'], count=1)
        circulating_cap = df_cap['circulating_cap'].iloc[0] if not df_cap.empty else 0
        if circulating_cap == 0:
            return 0.0
        df_volume['turnover_ratio'] = df_volume['volume'] / (circulating_cap * 10000)
        return df_volume['turnover_ratio'].mean()
    else:
        # 计算实时换手率
        date_now = context.current_dt
        df_vol = get_price(stock, start_date=date_now.date(), end_date=date_now, frequency='1m', fields=['volume'],
                           skip_paused=False, fq='pre', panel=True, fill_paused=False)
        volume = df_vol['volume'].sum()
        date_pre = context.previous_date
        df_circulating_cap = get_valuation(stock, end_date=date_pre, fields=['circulating_cap'], count=1)
        circulating_cap = df_circulating_cap['circulating_cap'].iloc[0]  if not df_circulating_cap.empty else 0
        if circulating_cap == 0:
            return 0.0
        turnover_ratio = volume / (circulating_cap * 10000)
        return turnover_ratio            


# 换手检测
def huanshou(context):
    ss = []
    current_data = get_current_data()
    shrink, expand = 0.003, 0.1
    for stock in context.portfolio.positions:
        if current_data[stock].paused == True:
            continue
        if current_data[stock].last_price >= current_data[stock].high_limit*0.97:
            continue
        if context.portfolio.positions[stock].closeable_amount ==0:
            continue
        rt = huanshoulv(context, stock, False)
        avg = huanshoulv(context, stock, True)
        if avg == 0: continue
        r = rt / avg
        action, icon = '', ''
        if avg < 0.003:
            action, icon = '缩量', '??'
        elif rt > expand and r > 2:
            action, icon = '放量', '?'
        if action:
            position = context.portfolio.positions[stock]
            r = close_position(context, position)
            log.info(f"{action} {stock} {get_security_info(stock).display_name} 换手率:{rt:.2%}→均:{avg:.2%} 倍率:{r:.1f}x {icon} close_position: {r}")
            g.reason_to_sell = 'limitup'
            
            
#3-1 交易模块-自定义下单
def smart_order_target_value(security, target_value, context):
    current_data = get_current_data()
    try:
        snapshot = current_data[security]
    except Exception:
        log.warning("下单跳过，无法获取实时数据: {}".format(security))
        return None
    if snapshot.paused:
        log.info("{} 停牌，跳过下单".format(security))
        return None

    current_price = snapshot.last_price
    if current_price is None or current_price <= 0:
        log.info("{} 当前价格无效，跳过下单".format(security))
        return None

    current_position = context.portfolio.positions.get(security)
    current_amount = current_position.total_amount if current_position is not None else 0
    target_amount = 0
    if target_value > 0:
        target_amount = int(float(target_value) / float(current_price) / 100) * 100
        if target_amount <= 0:
            target_amount = 100

    amount_diff = int(target_amount - current_amount)
    if amount_diff > 0 and current_price >= snapshot.high_limit:
        log.info("{} 涨停，跳过买入".format(security))
        return None
    if amount_diff < 0 and current_price <= snapshot.low_limit:
        log.info("{} 跌停，跳过卖出".format(security))
        return None

    if amount_diff < 0:
        closeable_amount = current_position.closeable_amount if current_position is not None else 0
        if closeable_amount <= 0:
            log.info("{} 当前不可卖出（T+1限制）".format(security))
            return None
        amount_diff = -min(abs(amount_diff), int(closeable_amount))

    if amount_diff == 0:
        return None

    qmt_ok = False
    qmt_detail = {}
    if getattr(g, "copy_trade_send_first", True):
        qmt_ok, qmt_detail = _send_qmt_order_delta(security, amount_diff, current_price, stage="smart_order")

    order_result = order(security, amount_diff)

    if not getattr(g, "copy_trade_send_first", True):
        qmt_ok, qmt_detail = _send_qmt_order_delta(security, amount_diff, current_price, stage="smart_order")

    if order_result:
        return order_result

    if qmt_ok:
        log.warning(
            "本地下单失败，但QMT信号已发送: security={} amount={} order_uuid={}".format(
                security,
                amount_diff,
                _get_qmt_order_uuid(qmt_detail),
            )
        )
    else:
        log.warning(
            "本地下单失败且QMT未成功发送: security={} amount={} reason={}".format(
                security,
                amount_diff,
                _get_qmt_result_reason(qmt_detail),
            )
        )
    return None


def order_target_value_(context, security, value):
    return smart_order_target_value(security, value, context)

#3-2 交易模块-开仓
def open_position(context, security, value):
    order = order_target_value_(context, security, value)
    if order != None and order.filled > 0:
        return True
    return False

#3-3 交易模块-平仓
def close_position(context, position):
    security = position.security
    order = order_target_value_(context, security, 0)  # 可能会因停牌失败
    if order != None:
        if order.status == OrderStatus.held and order.filled == order.amount:
            return True
    return False

#3-4 买入模块
def buy_security(context,target_list,cash=0,buy_number=0):
    #调仓买入
    position_count = len(context.portfolio.positions)
    target_num = g.stock_num
    if cash == 0:
        cash = get_effective_portfolio_value(context)
    if buy_number == 0:
        buy_number = target_num
    bought_num = 0
    print('---------------------buy_number：%s'%buy_number)
    if target_num > position_count:
        value = cash / (target_num) # - position_count
        for stock in target_list:
            position = context.portfolio.positions.get(stock)
            if position is None or position.total_amount == 0:
            #if stock not in context.portfolio.positions:
                if bought_num < buy_number:
                    if open_position(context, stock, value):
                        # log.info("买入[%s]（%s元）" % (stock,value))
                        g.not_buy_again.append(stock) #持仓清单，后续不希望再买入
                        bought_num += 1
                        if len(context.portfolio.positions) == target_num:
                            break
    # else:
    #     value = cash / target_num
    #     for stock in target_list:
    #         if context.portfolio.positions[stock].total_amount == 0:
    #             if bought_num < buy_number:
    #                 if open_position(stock, value):
    #                     log.info("买入[%s]（%s元）" % (stock,value))
    #                     g.not_buy_again.append(stock) #持仓清单，后续不希望再买入
    #                     bought_num += 1
    #                     if len(context.portfolio.positions) == target_num:
    #                         break




#4-1 判断今天是否为空仓月份
def today_is_between(context):
    no_trading_months = getattr(g, 'no_trading_months', [1, 4])
    current_month = context.current_dt.month
    return current_month in no_trading_months


#4-2 清仓后次日资金可转
def close_account(context):
    if g.no_trading_today_signal == True:
        if len(g.hold_list) != 0 and g.no_trading_hold_signal == False:
            for stock in g.hold_list:
                position = context.portfolio.positions[stock]
                if close_position(context, position):
                    log.info("卖出[%s]" % (stock))
                else:
                    log.info("卖出[%s]错误！！！！！" % (stock))
            buy_security(context, g.no_trading_buy)
            g.no_trading_hold_signal = True   
            

#4-3 清仓小市值不交易期间股票
def close_no_trading_hold(context):
    if g.no_trading_hold_signal == True:
        for stock in g.hold_list:
            position = context.portfolio.positions[stock]
            close_position(context, position)
            log.info("卖出[%s]" % (stock))
        g.no_trading_hold_signal = False



def print_position_info(context):
    print('———————————————————————————————————')
    for position in list(context.portfolio.positions.values()):
        securities=position.security
        cost=position.avg_cost
        price=position.price
        ret=100*(price/cost-1)
        value=position.value
        amount=position.total_amount    
        print('代码:{}'.format(securities))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
        print('———————————————————————————————————')
    print('余额:{}'.format(format(context.portfolio.cash,'.2f')))
    print('———————————————————————————————————————分割线————————————————————————————————————————')
    

        
