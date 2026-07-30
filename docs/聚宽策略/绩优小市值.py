# 克隆自聚宽文章：https://www.joinquant.com/post/64414
# 标题：微盘股指数择时的高息低价绩优股策略
# 作者：璐璐202006

import numpy as np
import pandas as pd
import datetime
import math
import gc
from jqdata import *
from jqfactor import *
from dateutil.relativedelta import relativedelta

# ======================================
# 初始化函数
# ======================================
def initialize(context):
    set_option('use_real_price', True)
    set_option("avoid_future_data", True)
    
    # 策略变量初始化
    g.sorted_stocks = []
    g.just_sold = []
    g.stock_num = 10
    g.high_limit_list = []
    g.max_stock_price = 9    
    g.risk_control_executed = False
    g.today_trade_allowed = True
    
    # MACD背离检测变量
    g.dbl = []
    g.dixl = []
    
    # 微盘股指数相关变量
    g.smallcap_index_threshold = 18.72
    g.smallcap_index_value = None
    g.ban_trade_start_date = None
    g.ban_trade_days = 0
    g.ban_trade_period = 5
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('history', 'error')    
    # 定时任务
    run_daily(prepare_stock_list, '09:00:00')
    run_daily(analyze_smallcap_index, '09:30:05')
    run_daily(check_smallcap_timing, '09:30:10')
    run_daily(dapan, '09:30:15')
    run_monthly(monthly_adjustment, 1, '09:30:20')
    run_daily(check_limit_up_and_buy, '14:00')

# ======================================
# 月度调仓函数
# ======================================
def monthly_adjustment(context):
    if not g.today_trade_allowed:
        return
        
    daily_my_Trader_1(context)
    
    cdata = get_current_data()
    
    # 卖出不在选股列表中的股票
    for s in list(context.portfolio.positions.keys()): 
        if s not in g.sorted_stocks:
            position = context.portfolio.positions[s]
            close_position(position)
    
    # 买入新股票
    available_cash = context.portfolio.available_cash  
    position_count = len(context.portfolio.positions)
    if len(g.sorted_stocks) > position_count and available_cash > 0:
        psize = available_cash / (len(g.sorted_stocks) - position_count)
        for s in g.sorted_stocks:
            if (s not in context.portfolio.positions 
                and cdata[s].last_price < cdata[s].high_limit
                and cdata[s].last_price > cdata[s].low_limit
                and not cdata[s].paused
                and s not in g.just_sold
            ):
                if open_position(s, psize):
                    available_cash -= psize
                if available_cash <= 0 or len(context.portfolio.positions) >= g.stock_num:
                    break

# ======================================
# 选股函数
# ======================================
def daily_my_Trader_1(context):
    yesterday = context.previous_date
    stocks = get_all_securities('stock', yesterday).index.tolist()
    stocks = filter_kcbj_stock(stocks)
    stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.25)
    stocks = get_peg(context, stocks)
    stocks = filter_all_stock(context, stocks)
    stocks = filter_highprice_stock(context, stocks)
    
    if stocks:
        df = get_fundamentals(
            query(valuation.code, valuation.market_cap)
            .filter(valuation.code.in_(stocks))
            .order_by(valuation.market_cap.asc(), valuation.code.asc())
        )
        g.sorted_stocks = list(df.code)
    else:
        g.sorted_stocks = []
    
    g.sorted_stocks = g.sorted_stocks[:g.stock_num]  
    
    return g.sorted_stocks
 
# ======================================
# 交易模块
# ======================================
def open_position(security, value):
    order = order_target_value(security, value)
    if order != None and order.filled > 0:
        return True
    return False

def close_position(position):
    security = position.security
    order = order_target_value(security, 0)
    g.just_sold.append(security)
    
    if order != None and order.filled > 0:
        return True
    return False

# ======================================
# 涨停检查和补仓函数
# ======================================
def check_limit_up_and_buy(context):
    if g.risk_control_executed:
        return
        
    current_data = get_current_data()
    sold_stocks = []
    
    if g.high_limit_list:
        for stock in g.high_limit_list.copy():
            if current_data[stock].last_price < current_data[stock].high_limit:
                if stock in context.portfolio.positions:
                    position = context.portfolio.positions[stock]
                    close_position(position)
                    sold_stocks.append(stock)
                    g.high_limit_list.remove(stock)
    
    if not g.today_trade_allowed:
        return
        
    if sold_stocks:
        daily_my_Trader_1(context)
        
        valid_stocks = [s for s in g.sorted_stocks if s not in g.just_sold]
            
        available_cash = context.portfolio.available_cash  
        position_count = len(context.portfolio.positions)
        need_buy_num = max(0, g.stock_num - position_count)
            
        if need_buy_num > 0 and available_cash > 0:
            psize = available_cash / need_buy_num
            bought = 0
            for s in valid_stocks:
                if (s not in context.portfolio.positions 
                    and current_data[s].last_price < current_data[s].high_limit
                    and current_data[s].last_price > current_data[s].low_limit
                    and not current_data[s].paused
                ):
                    success = open_position(s, psize)
                    if success:
                        available_cash -= psize
                        bought += 1
                    if bought >= need_buy_num or available_cash <= 0:
                        break

# ======================================
# 准备股票列表函数
# ======================================
def prepare_stock_list(context):
    g.just_sold = []
    g.risk_control_executed = False
    
    g.high_limit_list = []
    hold_list = list(context.portfolio.positions)
    if hold_list:
        df = get_price(hold_list, end_date=context.previous_date, frequency='daily',
                       fields=['close', 'high_limit'],
                       count=1, panel=False)
        g.high_limit_list = df[df['close'] == df['high_limit']]['code'].tolist()

# ======================================
# 大盘分析函数（MACD背离检测）
# ======================================
def dapan(context):
    if g.risk_control_executed:
        return
    
    if g.ban_trade_start_date is not None:
        trade_days = get_trade_days(start_date=g.ban_trade_start_date, end_date=context.current_dt.date())
        ban_trade_days = len(trade_days) - 1
        if ban_trade_days >= g.ban_trade_period:
            g.ban_trade_start_date = None
            g.ban_trade_days = 0
    
    if not g.today_trade_allowed and g.ban_trade_start_date is not None:
        return
    
    top_divergence, bottom_divergence = detect_divergences('399303.XSHE', context)
    
    if top_divergence:
        g.dbl.append(True)
        g.today_trade_allowed = False    
        g.ban_trade_start_date = context.current_dt.date()
        g.ban_trade_days = 0
        g.risk_control_executed = True
        
        current_data = get_current_data()
        for stock in list(context.portfolio.positions.keys()):
            if current_data[stock].last_price < current_data[stock].high_limit:
                position = context.portfolio.positions[stock]
                close_position(position)
    
    if bottom_divergence:
        g.dixl.append(True)

# ======================================
# 微盘股指数分析函数
# ======================================
def analyze_smallcap_index(context):
    smallcap_close, smallcap_date = cal_smallcap_close(context, context.current_dt)
    g.smallcap_index_value = smallcap_close
    
    return smallcap_close, None

def filter_new_stock(end_date, stock_list):
    return [stock for stock in stock_list if (end_date - datetime.timedelta(days=240)) > get_security_info(stock).start_date]

def cal_smallcap_close(context, current_dt, n=400):
    current_date = current_dt.date()
    
    trade_days = get_trade_days(end_date=current_date, count=2)
    if len(trade_days) < 2:
        return None, None
    last_traded_day = trade_days[-2]
    
    yesterday = context.previous_date
    stock_list = get_all_securities('stock', yesterday).index.tolist()
    stock_list = filter_kcbj_stock(stock_list)
    stock_list = filter_new_stock(last_traded_day, stock_list)
    stock_list = filter_all_stock(context, stock_list)
 
    if len(stock_list) == 0:
        return None, last_traded_day
    
    valuation_df = get_valuation(stock_list, count=1, end_date=last_traded_day, fields=['market_cap'])
    if valuation_df.empty:
        return None, last_traded_day
    
    valuation_df = valuation_df.dropna()
    if valuation_df.empty:
        return None, last_traded_day
        
    microcap_stocks = valuation_df.sort_values('market_cap').iloc[:n]['code'].tolist()
    
    prices = get_price(microcap_stocks, end_date=last_traded_day, frequency='1d', fields=['close'], count=1, panel=False)
    valid_prices = prices[prices['close'] > 0]['close']
    
    if not valid_prices.empty:
        gc.collect()
        return round(valid_prices.mean(), 4), last_traded_day
    return None, last_traded_day

# ======================================
# 微盘股指数择时检测函数
# ======================================
def check_smallcap_timing(context):
    if g.risk_control_executed:
        return
            
    current_date = context.current_dt
    current_data = get_current_data()
    
    ban_period_ended = False
    if g.ban_trade_start_date is not None:
        trade_days = get_trade_days(start_date=g.ban_trade_start_date, end_date=current_date.date())
        ban_trade_days = len(trade_days) - 1
        if ban_trade_days >= g.ban_trade_period:
            ban_period_ended = True
            g.ban_trade_start_date = None
            g.ban_trade_days = 0
    
    if g.smallcap_index_value is None:
        return
            
    smallcap_value = g.smallcap_index_value
    
    if smallcap_value > g.smallcap_index_threshold:
        for stock in list(context.portfolio.positions.keys()):
            if current_data[stock].last_price < current_data[stock].high_limit:
                position = context.portfolio.positions[stock]
                close_position(position)
        
        g.today_trade_allowed = False
        g.risk_control_executed = True
        
        if g.ban_trade_start_date is None:
            g.ban_trade_start_date = current_date.date()
    else:
        g.today_trade_allowed = True
        
        if ban_period_ended:
            execute_recovery_buying(context)

def execute_recovery_buying(context):
    try:
        daily_my_Trader_1(context)
        
        current_data = get_current_data()
        available_cash = context.portfolio.available_cash
        position_count = len(context.portfolio.positions)
        
        if available_cash > 0 and g.sorted_stocks:
            buy_stock_count = min(len(g.sorted_stocks), g.stock_num - position_count)
            if buy_stock_count > 0:
                psize = available_cash / buy_stock_count
                bought = 0
                
                for stock in g.sorted_stocks:
                    if (bought >= buy_stock_count or 
                        available_cash <= 0 or 
                        len(context.portfolio.positions) >= g.stock_num):
                        break
                    
                    if (stock not in context.portfolio.positions and
                        current_data[stock].last_price < current_data[stock].high_limit and
                        current_data[stock].last_price > current_data[stock].low_limit and
                        not current_data[stock].paused):
                        
                        success = open_position(stock, psize)
                        if success:
                            available_cash -= psize
                            bought += 1
    except Exception as e:
        pass

# ======================================
# MACD相关函数
# ======================================
def EMA(series, N):
    return pd.Series.ewm(series, span=N, min_periods=N-1, adjust=False).mean()

def MACD(close, SHORT=12, LONG=26, M=9):
    DIF = EMA(close, SHORT) - EMA(close, LONG)
    DEA = EMA(DIF, M)
    MACD = (DIF - DEA) * 2
    return DIF, DEA, MACD

def detect_divergences(stock, context):
    fast = 12
    slow = 26
    sign = 9
    rows = (fast + slow + sign) * 5
    
    top_divergence = False
    bottom_divergence = False
    
    try:
        grid = attribute_history(stock, rows, fields=['close'])
        if grid is None or len(grid) < rows:
            return top_divergence, bottom_divergence
    except Exception as e:
        return top_divergence, bottom_divergence
    
    try:
        grid['dif'], grid['dea'], grid['macd'] = MACD(grid.close, SHORT=fast, LONG=slow, M=sign)
        
        dead_cross = (grid['macd'] < 0) & (grid['macd'].shift(1) > 0)
        dead_cross_points = dead_cross[dead_cross].index
        
        gold_cross = (grid['macd'] > 0) & (grid['macd'].shift(1) < 0)
        gold_cross_points = gold_cross[gold_cross].index
        
        if len(dead_cross_points) >= 2:
            key2 = dead_cross_points[-2]
            key1 = dead_cross_points[-1]
            
            price_condition = grid.close[key2] < grid.close[key1]
            dif_condition = grid.dif[key2] > grid.dif[key1] > 0
            macd_condition = grid.macd.iloc[-2] > 0 > grid.macd.iloc[-1]
            
            if not (pd.isna(macd_condition) or pd.isna(dif_condition)):
                is_top_divergence = price_condition and dif_condition and macd_condition
                
                if is_top_divergence:
                    dif_values = grid['dif'].values
                    if len(dif_values) > 20:
                        recent_avg = np.mean(dif_values[-10:])
                        prev_avg = np.mean(dif_values[-20:-10])
                        top_divergence = recent_avg < prev_avg
        
        if len(gold_cross_points) >= 2:
            key2 = gold_cross_points[-2]
            key1 = gold_cross_points[-1]
            
            price_condition = grid.close[key2] > grid.close[key1]
            dif_condition = grid.dif[key2] < grid.dif[key1] < 0
            macd_condition = grid.macd.iloc[-2] < 0 < grid.macd.iloc[-1]
            
            if not (pd.isna(macd_condition) or pd.isna(dif_condition)):
                is_bottom_divergence = price_condition and dif_condition and macd_condition
                
                if is_bottom_divergence:
                    dif_values = grid['dif'].values
                    if len(dif_values) > 20:
                        recent_avg = np.mean(dif_values[-10:])
                        prev_avg = np.mean(dif_values[-20:-10])
                        bottom_divergence = recent_avg > prev_avg
    
    except Exception as e:
        pass
        
    return top_divergence, bottom_divergence

# ======================================
# 过滤和数据获取函数
# ======================================
def filter_kcbj_stock(stock_list):
    return [stock for stock in stock_list if not (stock[0] in {'4', '8'} or stock[:2] == '68')]

def filter_highprice_stock(context, stock_list):
    prices = history(1, '1d', 'close', stock_list, df=False)
    return [s for s in stock_list if s in context.portfolio.positions or prices[s][-1] < g.max_stock_price]

def filter_all_stock(context, stocks):
    curr_data = get_current_data()
    valid_stocks = []
    for stock in stocks:
        try:
            if (not curr_data[stock].paused
                and not curr_data[stock].is_st
                and 'ST' not in curr_data[stock].name
                and '*' not in curr_data[stock].name
                and '退' not in curr_data[stock].name
                and curr_data[stock].last_price < curr_data[stock].high_limit
                and curr_data[stock].last_price > curr_data[stock].low_limit
            ):
                valid_stocks.append(stock)
        except Exception as e:
            pass
    return valid_stocks

def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date
    time0 = time1 - relativedelta(years=1)
    
    cap = get_fundamentals(
        query(valuation.code, valuation.market_cap).filter(
            valuation.code.in_(stock_list)
        ), 
        date=time1
    ).set_index('code')

    df = finance.run_query(
        query(
            finance.STK_XR_XD.code,
            finance.STK_XR_XD.bonus_amount_rmb
        ).filter(
            finance.STK_XR_XD.a_registration_date >= time0,
            finance.STK_XR_XD.a_registration_date <= time1,
            finance.STK_XR_XD.code.in_(stock_list)
    ))

    if df.empty or cap.empty:
        return []

    dividend = df.groupby('code')['bonus_amount_rmb'].sum().to_frame()
    DR = dividend.join(cap, how='inner')
    DR['dividend_ratio'] = (DR['bonus_amount_rmb'] / 1e8) / DR['market_cap']

    DR = DR.sort_values(by='dividend_ratio', ascending=sort)
    final_list = DR.index[int(p1 * len(DR)) : int(p2 * len(DR))].tolist()

    return final_list

def get_factor_filter_list(context, stock_list, jqfactor, sort, quantity):
    yesterday = context.previous_date
    score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1)[jqfactor].iloc[0].tolist()
    df = pd.DataFrame(columns=['code','score'])
    df['code'] = stock_list
    df['score'] = score_list
    df = df.dropna()
    df.sort_values(by='score', ascending=sort, inplace=True)
    filter_list = list(df.code)[:quantity]
    return filter_list
    
def get_peg(context, stocks):
    query_date = context.previous_date  
    df = get_fundamentals(
        query(valuation.code).filter(
            valuation.code.in_(stocks),
            income.np_parent_company_owners > 0, 
            income.net_profit > 0, 
            income.operating_revenue > 1e8,
            indicator.roe > 0, 
            indicator.roa > 0
        ),
        date=query_date
    )
    if df is None or df.empty:
        return []
    stocks = list(df.code)
    return stocks