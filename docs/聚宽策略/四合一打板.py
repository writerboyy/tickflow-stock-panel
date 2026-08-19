# 克隆自聚宽文章：https://www.joinquant.com/post/78265
# 标题：年华90+回撤9四模式策略一进二、弱转强、趋势股、首板
# 作者：放下执念接纳万物

# 克隆自聚宽文章：https://www.joinquant.com/post/78226
# 标题：四模式-无未来-自用shi盘框架-满100删
# 作者：B1n2

# 克隆自聚宽文章：https://www.joinquant.com/post/74997
# 标题：四模式量化-无未来-26年至今收益79%回撤8.8%
# 作者：rbq2025

# 克隆自聚宽文章：https://www.joinquant.com/post/72495
# 标题：这个社区的某些人真可悲，又不了解市场，又自以为是
# 作者：末墨46

from jqdata import *
from jqfactor import *
from jqlib.technical_analysis import *
import datetime as dt
import builtins
import copy
import pandas as pd
import numpy as np
import talib
import re
import time as pytime

# =============================================================================
# 部署配置区：上线前只需检查本区；交易参数位于其后的分组配置函数。
# =============================================================================

# 实盘订单桥接。默认 False；只有完成 QMT 连接、账户和小额联调后才改为 True。
# False 时仍正常运行聚宽回测/模拟盘，但不会把聚宽订单同步到实盘账户。
ENABLE_LIVE_TRADING = False

# 一次性账户出入金事件。仅 after_code_changed 调用，默认关闭。
# 使用方法：先设置 cash（正数=入金，负数=出金），再换一个从未使用过的 id，
# 最后将 enabled 改为 True。相同 id 只处理一次；状态不确定时会停止自动重试。
CASH_EVENT_CONFIG = {
    'id': 'capital-replace-v1',
    'enabled': False,
    'cash': 0.0,
}


# ========== [配置] 四模式策略参数 ==========
def _apply_config():
    """四模式全部可调参数：仓位、模式上限、风控阈值和任务时间。"""
    # ---- 定时任务（聚宽时间字符串） ----
    g.preselect_time = '09:05:00'         # 第1步：仅使用昨日数据完成四模式静态预选
    g.market_risk_time = '09:24:00'       # 第2步：盘前市场微观结构检查
    # 竞价数据服务通常在 09:25 结束后仍需要一小段时间落库；旧版在
    # 09:25:45 查询稳定可用，保留这段缓冲后再做最终确认。
    g.select_time = '09:25:45'            # 第3步：使用最终竞价数据确认并发布候选池
    g.open_sell_time = '09:27:00'         # 第4步：集合竞价后卖出检查，必须早于09:28主买入
    g.buy_time = '09:28:00'               # 第5步：完成风控、选股和卖出检查后启动主买入流水线
    g.sell_30min_time = '10:00:00'        # 开盘30分钟卖出检查
    g.midday_update_time = '11:30:00'     # 午间情绪/优先级更新
    g.afternoon_sell_time = '14:30:00'    # 午后卖出检查
    g.qs_daily_log_time = '15:01:00'      # 收盘第1步：记录趋势股状态，必须早于日终审计
    g.position_log_time = '15:02:00'      # 收盘第2步：日终资产与持仓审计，必须早于状态重置
    g.reset_time = '15:05:00'             # 收盘第3步：完成日志和审计后重置日内状态

    # ---- 账户出入金事件 ----
    g.cash_event = copy.deepcopy(CASH_EVENT_CONFIG)  # 仅代码替换时检查；相同id不会重复执行

    # ---- 通用仓位与选股排序 ----
    g.max_positions = 10             # 四模式合计最大持仓数量
    g.position_ratio = 1.0           # 初始总仓位系数，盘前风控会动态覆盖；1.0=满仓
    g.jqfactor = 'VOL5'              # 候选排序因子
    g.sort = True                    # 因子排序方向：True=升序

    # ---- 一进二模式 ----
    g.yje_max_positions = 4          # 一进二单只资金分母：总资产×仓位系数/该值（设计持仓数）

    # ---- 弱转强/趋势股模式 ----
    g.qs_max_ratio = 0.35            # 趋势股最多占动态持仓名额比例
    g.qs_max_count = 3               # 趋势股绝对持仓上限
    g.qs_max_hold_days = 10          # 趋势股最长持有交易日
    g.qs_atr_period = 14             # 趋势股ATR周期
    g.qs_atr_multiplier = 1.5        # 趋势股ATR止损倍数

    # ---- 首板模式（独立仓位） ----
    g.sb_max_positions = 100         # 首板监控池上限
    g.sb_buy_max_positions = 4       # 首板实际买入上限
    g.sb_max_ratio = 0.30            # 首板模式资金上限

    # ---- 盘中信号与模式优先级 ----
    g.weibi_threshold = 80           # 一进二委比买入阈值（百分数，80表示80%）
    g.weibi_min_chg = 4              # 委比信号最低涨幅（保留兼容参数）
    g.priority_config = ['yje', 'sb', 'rzq']  # 初始模式优先级；选股及午间情绪更新会动态调整

    parameter_signature = (
        g.max_positions, g.position_ratio, g.jqfactor, g.sort,
        g.yje_max_positions, g.qs_max_count, g.sb_buy_max_positions,
        tuple(g.priority_config),
    )
    if getattr(g, '__fourmode_log_parameter_signature', None) != parameter_signature:
        g.__fourmode_log_parameter_signature = parameter_signature
        _strategy_log.event(
            '参数配置', '已加载',
            f'总持仓上限={g.max_positions}只 | 总仓位={g.position_ratio:.0%} | '
            f'排序={g.jqfactor}({"升序" if g.sort else "降序"}) | '
            f'模式上限=一进二{g.yje_max_positions}/趋势股{g.qs_max_count}/首板{g.sb_buy_max_positions} | '
            f'优先级={">".join(g.priority_config)}',
            icon='⚙️',
        )


def _register_tasks():
    """按配置重建四模式任务。"""
    unschedule_all()
    run_daily(prepare_stock_candidates, g.preselect_time)           # 第1步：盘前静态预选
    run_daily(check_market_risk_microstructure, g.market_risk_time) # 第2步：盘前风控
    run_daily(get_stock_list, g.select_time)                         # 第3步：竞价确认并发布候选池
    run_daily(sell_check_open, g.open_sell_time)                    # 第4步：开盘卖出检查
    run_daily(buy, g.buy_time)                                      # 第5步：风控、选股和卖出后买入
    run_daily(sell_30min_check, g.sell_30min_time)                  # 保留原调度位；当前函数为空
    run_daily(midday_sentiment_update, g.midday_update_time)        # 更新午间情绪和模式优先级
    run_daily(sell_check_afternoon, g.afternoon_sell_time)          # 尾盘卖出检查
    run_daily(log_qs_daily_status, g.qs_daily_log_time)             # 收盘第1步：记录趋势股状态
    run_daily(print_daily_audit, g.position_log_time)               # 收盘第2步：日终审计
    run_daily(daily_reset, g.reset_time)                            # 收盘第3步：重置日内状态

# QMT 对聚宽四种标准下单函数做透明包装；策略内部交易计算不变。
if ENABLE_LIVE_TRADING:
    from jqmt import (
        push_order,
        push_order_target,
        push_order_value,
        push_order_target_value,
    )

    order = push_order(order)
    order_target = push_order_target(order_target)
    order_value = push_order_value(order_value)
    order_target_value = push_order_target_value(order_target_value)


class _StrategyLogger:
    """四模式统一日志出口，统一前缀、级别和事件格式。"""

    prefix = "四模式"
    _separator_pattern = re.compile(r'[=*\-★━─_ \n\r\t]+')
    _known_icons = (
        '⚙️', '▶️', '⏸️', '📊', '🔔', '🛡️', '⚠️', '💰', '✅', '❌',
        '⭕', '🔄', '💼', 'ℹ️', '🔍', '⏱️', '🛑', '📈', '📉',
        '⬆️', '⬇️', '➡️',
    )
    _icon_rules = (
        ('⚙️', ('参数', '配置')),
        ('💼', ('日终审计', '资产', '持仓', '仓位')),
        ('🛡️', ('风控', '防御', '止损', '止盈', '风险', '顶背离')),
        ('💰', ('订单', '下单', '买入', '卖出', '成交', '调仓')),
        ('🔔', ('信号', '触发', '涨停', '开板', '选中')),
        ('📊', ('选股', '候选', '评分', '统计', '指标', '概念',
                 '过滤', '检查', '评估', '排名', '股票池', '判断')),
        ('🔄', ('更新', '重置', '状态变化', '代码变更')),
    )
    _legacy_scope_pattern = re.compile(
        r'^(\s*[^\w\u4e00-\u9fff]*\s*)四模式(?:\([^)]*\))?\s*(?:\||:|：)?\s*(.*)$',
        re.S,
    )

    def _leading_icon(self, text):
        stripped = text.lstrip()
        for icon in self._known_icons:
            if stripped.startswith(icon):
                return icon, stripped[len(icon):].lstrip()
        return '', text.strip()

    def _decorate(self, text, level):
        icon, _ = self._leading_icon(text)
        if icon:
            return text
        if level == 'error':
            return f'⭕ {text}'
        if level in ('warn', 'warning'):
            return f'⚠️ {text}'
        if '流水线' in text or '流程' in text:
            if any(word in text for word in ('启动', '开始')):
                return f'▶️ {text}'
            if any(word in text for word in ('完毕', '结束', '完成')):
                return f'⏸️ {text}'
        if any(word in text for word in ('跳过', '无需', '无候选', '未生成')):
            return f'ℹ️ {text}'
        for rule_icon, keywords in self._icon_rules:
            if any(keyword in text for keyword in keywords):
                return f'{rule_icon} {text}'
        return text

    def _message(self, message, level='info'):
        text = str(message)
        if text.strip() and self._separator_pattern.fullmatch(text):
            return None
        text = self._decorate(text, level)
        match = self._legacy_scope_pattern.match(text)
        if match:
            icon, body = match.groups()
            return f"{icon}{self.prefix} | {body}"
        icon, body = self._leading_icon(text)
        return f'{icon + " " if icon else ""}{self.prefix} | {body}'

    def _write(self, level, message):
        try:
            scoped = self._message(message, level)
            if scoped is None:
                return None
            if level in ('warn', 'warning'):
                return log.warn(scoped)
            if level == 'error':
                return log.error(scoped)
            if level == 'debug':
                return log.debug(scoped)
            return log.info(scoped)
        except Exception:
            # 聚宽日志中间层故障不得中断选股、风控或下单流程。
            return None

    def info(self, message):
        return self._write('info', message)

    def debug(self, message):
        return self._write('debug', message)

    def warning(self, message):
        return self._write('warning', message)

    warn = warning

    def error(self, message):
        return self._write('error', message)

    def event(self, category, event, detail=None, icon=None, level='info'):
        message = f"{category} | {event}" if icon is None else f"{icon} {category} | {event}"
        if detail:
            message += f" | {detail}"
        return self._write(level, message)


_strategy_log = _StrategyLogger()


def _apply_log_levels():
    """进程重启或代码替换后重新应用日志级别。"""
    log.set_level('system', 'error')
    log.set_level('order', 'error')
    log.set_level('strategy', 'info')


# 进程内只读缓存：不写入 g，不参与聚宽跨日状态序列化。
# 缓存键包含日期、字段和窗口，确保复用请求与原请求完全等价。
_RUNTIME_DATA_CACHE = {
    'trade_days': None,
    'stock_securities_date': None,
    'stock_securities': None,
    'prepared_stock_list_date': None,
    'prepared_stock_list': None,
    'security_info_date': None,
    'security_info': {},
    'daily_date': None,
    'daily_frames': {},
    'previous_close': {},
    'qs_atr': {},
    'perf_stats': {},
    'intraday_date': None,
    'intraday_highs': {},
    'intraday_last_dt': None,
}


def _reset_runtime_data_cache():
    """每次进程启动时清空临时缓存，避免进入聚宽跨日恢复链路。"""
    _RUNTIME_DATA_CACHE.clear()
    _RUNTIME_DATA_CACHE.update({
        'trade_days': None,
        'stock_securities_date': None,
        'stock_securities': None,
        'prepared_stock_list_date': None,
        'prepared_stock_list': None,
        'security_info_date': None,
        'security_info': {},
        'daily_date': None,
        'daily_frames': {},
        'previous_close': {},
        'qs_atr': {},
        'perf_stats': {},
        'intraday_date': None,
        'intraday_highs': {},
        'intraday_last_dt': None,
    })


def _cache_date_key(value):
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d')
    return str(value)[:10]


def _cached_trade_day_strings():
    cached = _RUNTIME_DATA_CACHE['trade_days']
    if cached is None:
        cached = [day.strftime('%Y-%m-%d') for day in list(get_all_trade_days())]
        _RUNTIME_DATA_CACHE['trade_days'] = cached
    return cached


def _cached_stock_securities(date):
    date_key = _cache_date_key(date)
    if _RUNTIME_DATA_CACHE['stock_securities_date'] != date_key:
        _RUNTIME_DATA_CACHE['stock_securities_date'] = date_key
        _RUNTIME_DATA_CACHE['stock_securities'] = get_all_securities('stock', date)
    return _RUNTIME_DATA_CACHE['stock_securities']


def _cached_security_info(security, date=None):
    """缓存日内重复读取的证券名称、上市日和概念等元数据。"""
    security = str(security)
    if date is not None:
        date_key = _cache_date_key(date)
        if _RUNTIME_DATA_CACHE['security_info_date'] != date_key:
            _RUNTIME_DATA_CACHE['security_info_date'] = date_key
            _RUNTIME_DATA_CACHE['security_info'] = {}
        cache_key = ('dated', date_key, security)
    else:
        cache_key = ('current', _RUNTIME_DATA_CACHE['security_info_date'], security)
    cache = _RUNTIME_DATA_CACHE['security_info']
    if cache_key not in cache:
        cache[cache_key] = (
            get_security_info(security, date)
            if date is not None else get_security_info(security)
        )
    return cache[cache_key]


def _cached_security_start_date(security, date):
    securities = _cached_stock_securities(date)
    security = str(security)
    if securities is not None and security in securities.index:
        start_date = securities.loc[security, 'start_date']
        if isinstance(start_date, dt.datetime):
            start_date = start_date.date()
        return start_date
    info = _cached_security_info(security, date)
    return info.start_date if info is not None else None


def _cached_daily_frame(security, end_date, fields, count, skip_paused=True):
    """复用同一证券、日期、字段、窗口的日线请求；返回副本防止调用方污染缓存。"""
    security = str(security)
    requested_fields = list(fields)
    count = int(count)
    date_key = _cache_date_key(end_date)
    if _RUNTIME_DATA_CACHE['daily_date'] != date_key:
        _RUNTIME_DATA_CACHE['daily_date'] = date_key
        _RUNTIME_DATA_CACHE['daily_frames'] = {}
        _RUNTIME_DATA_CACHE['previous_close'] = {}
        _RUNTIME_DATA_CACHE['qs_atr'] = {}
    key = (security, bool(skip_paused))
    entries = _RUNTIME_DATA_CACHE['daily_frames'].setdefault(key, [])
    requested_set = frozenset(requested_fields)
    for entry in entries:
        if entry['count'] >= count and requested_set.issubset(entry['fields']):
            _perf_increment('daily_cache_hits')
            return entry['frame'].loc[:, requested_fields].tail(count).copy()
    _perf_increment('single_daily_requests')
    frame = get_price(
        security, end_date=end_date, frequency='daily', fields=requested_fields,
        count=count, skip_paused=skip_paused,
    )
    if frame is not None:
        entries.append({
            'count': count,
            'fields': requested_set,
            'frame': frame.copy(),
        })
        return frame.copy()
    return frame


def _perf_increment(name, amount=1):
    """记录当前进程内的轻量性能计数，不进入聚宽持久化状态。"""
    stats = _RUNTIME_DATA_CACHE['perf_stats']
    stats[name] = stats.get(name, 0) + amount


def _store_daily_frame(security, end_date, fields, count, skip_paused, frame):
    """保存批量取数结果，供后续完全等价的单股窗口复用。"""
    if frame is None:
        return
    date_key = _cache_date_key(end_date)
    if _RUNTIME_DATA_CACHE['daily_date'] != date_key:
        _RUNTIME_DATA_CACHE['daily_date'] = date_key
        _RUNTIME_DATA_CACHE['daily_frames'] = {}
        _RUNTIME_DATA_CACHE['previous_close'] = {}
        _RUNTIME_DATA_CACHE['qs_atr'] = {}
    key = (str(security), bool(skip_paused))
    _RUNTIME_DATA_CACHE['daily_frames'].setdefault(key, []).append({
        'count': int(count),
        'fields': frozenset(fields),
        'frame': frame.copy(),
    })


def _batch_daily_frames(securities, end_date, fields, count,
                        skip_paused=True, chunk_size=300, store_cache=False):
    """分块获取日线；批量缺失或失败的股票由调用方按原单股逻辑回退。"""
    securities = list(dict.fromkeys(str(stock) for stock in securities))
    fields = list(fields)
    result = {}
    for start in range(0, len(securities), chunk_size):
        chunk = securities[start:start + chunk_size]
        _perf_increment('batch_chunks')
        _perf_increment('batch_requested_stocks', len(chunk))
        try:
            batch_fields = list(fields)
            batch_count = int(count)
            batch_skip_paused = bool(skip_paused)
            if skip_paused:
                if 'paused' not in batch_fields:
                    batch_fields.append('paused')
                batch_count = builtins.max(count + 20, count * 2)
                batch_skip_paused = False
            batch = get_price(
                chunk, end_date=end_date, frequency='daily', fields=batch_fields,
                count=batch_count, skip_paused=batch_skip_paused, panel=False,
            )
            if batch is None or batch.empty or 'code' not in batch.columns:
                continue
            for code, stock_df in batch.groupby('code', sort=False):
                code = str(code)
                stock_df = stock_df.copy()
                if 'time' in stock_df.columns:
                    stock_df = stock_df.set_index('time')
                if skip_paused and 'paused' in stock_df.columns:
                    stock_df = stock_df[stock_df['paused'] == 0]
                if len(stock_df) < count:
                    continue
                stock_df = stock_df.tail(count)
                available_fields = [field for field in fields if field in stock_df.columns]
                if len(available_fields) != len(fields):
                    continue
                stock_df = stock_df.loc[:, fields]
                result[code] = stock_df
                if store_cache:
                    _store_daily_frame(
                        code, end_date, fields, count, skip_paused, stock_df
                    )
        except Exception as exc:
            _perf_increment('batch_failed_chunks')
            _strategy_log.debug(
                f'历史日线批量获取降级 | 数量={len(chunk)} | 原因={exc}'
            )
    _perf_increment('batch_returned_stocks', len(result))
    return result


def _batch_valuation_frames(securities, date, fields, chunk_size=1000):
    """批量获取单日估值；缺失股票由调用方使用原单股接口回退。"""
    securities = list(dict.fromkeys(str(stock) for stock in securities))
    fields = list(fields)
    result = {}
    for start in range(0, len(securities), chunk_size):
        chunk = securities[start:start + chunk_size]
        _perf_increment('valuation_batch_chunks')
        _perf_increment('valuation_batch_requested', len(chunk))
        try:
            frame = get_valuation(
                chunk, start_date=date, end_date=date, fields=fields
            )
            if frame is None or frame.empty:
                continue
            if isinstance(frame.index, pd.MultiIndex):
                frame = frame.reset_index()
            else:
                frame = frame.copy()
            if 'code' not in frame.columns:
                continue
            for code, stock_frame in frame.groupby('code', sort=False):
                code = str(code)
                if stock_frame.empty or not set(fields).issubset(stock_frame.columns):
                    continue
                result[code] = stock_frame.loc[:, fields].tail(1).reset_index(drop=True)
        except Exception as exc:
            _perf_increment('valuation_batch_failed_chunks')
            _strategy_log.debug(
                f'估值批量获取降级 | 数量={len(chunk)} | 原因={exc}'
            )
    _perf_increment('valuation_batch_returned', len(result))
    return result


def _intraday_highs(context, securities):
    """增量维护持仓当日最高价；失败时按原逻辑逐股全日补取。"""
    securities = list(dict.fromkeys(str(stock) for stock in securities))
    date_key = context.current_dt.strftime('%Y-%m-%d')
    if _RUNTIME_DATA_CACHE['intraday_date'] != date_key:
        _RUNTIME_DATA_CACHE['intraday_date'] = date_key
        _RUNTIME_DATA_CACHE['intraday_highs'] = {}
        _RUNTIME_DATA_CACHE['intraday_last_dt'] = None

    highs = _RUNTIME_DATA_CACHE['intraday_highs']
    last_dt = _RUNTIME_DATA_CACHE['intraday_last_dt']
    full_day_stocks = [stock for stock in securities if stock not in highs]
    incremental_stocks = [stock for stock in securities if stock in highs]

    def update_batch(stocks, start_date):
        if not stocks:
            return set()
        updated = set()
        try:
            frame = get_price(
                stocks, start_date=start_date, end_date=context.current_dt,
                frequency='1m', fields=['high'], panel=False,
                skip_paused=False, fill_paused=False,
            )
            if frame is not None and not frame.empty and 'code' in frame.columns:
                for code, values in frame.groupby('code', sort=False):
                    code = str(code)
                    high_value = values['high'].max()
                    if pd.notna(high_value):
                        highs[code] = builtins.max(highs.get(code, high_value), high_value)
                        updated.add(code)
        except Exception as exc:
            _strategy_log.debug(f'盘中最高价批量更新降级 | 原因={exc}')
        return updated

    incremental_start = last_dt or date_key
    updated_incremental = update_batch(incremental_stocks, incremental_start)
    for stock in incremental_stocks:
        if stock in updated_incremental:
            continue
        try:
            frame = get_price(
                stock, start_date=incremental_start, end_date=context.current_dt,
                frequency='1m', fields=['high'], skip_paused=True,
            )
            if frame is not None and not frame.empty:
                high_value = frame['high'].max()
                if pd.notna(high_value):
                    highs[stock] = builtins.max(highs.get(stock, high_value), high_value)
                    updated_incremental.add(stock)
        except Exception:
            continue

    updated_full_day = update_batch(full_day_stocks, date_key)
    for stock in full_day_stocks:
        if stock in updated_full_day:
            continue
        try:
            frame = get_price(
                stock, start_date=date_key, end_date=context.current_dt,
                frequency='1m', fields=['high'], skip_paused=True,
            )
            if frame is not None and not frame.empty:
                high_value = frame['high'].max()
                if pd.notna(high_value):
                    highs[stock] = high_value
        except Exception:
            continue

    if not incremental_stocks or len(updated_incremental) == len(incremental_stocks):
        _RUNTIME_DATA_CACHE['intraday_last_dt'] = context.current_dt
    return {stock: highs[stock] for stock in securities if stock in highs}


def _cached_previous_close(security, context):
    """同一交易日昨收不变，减少盘中重复 attribute_history 请求。"""
    date_key = _cache_date_key(context.previous_date)
    if _RUNTIME_DATA_CACHE['daily_date'] != date_key:
        _RUNTIME_DATA_CACHE['daily_date'] = date_key
        _RUNTIME_DATA_CACHE['daily_frames'] = {}
        _RUNTIME_DATA_CACHE['previous_close'] = {}
        _RUNTIME_DATA_CACHE['qs_atr'] = {}
    cache = _RUNTIME_DATA_CACHE['previous_close']
    key = str(security)
    if key not in cache:
        hist = attribute_history(str(security), 1, '1d', ['close'], skip_paused=True)
        if hist is None or len(hist) == 0:
            return None
        cache[key] = hist['close'][0]
    return cache[key]

def _ensure_runtime_state():
    """补齐跨日运行状态；已有状态保留，日内状态由 daily_reset 显式清理。"""
    defaults = {
        'yje_stock_list': [], 'yje_pending_list': [], 'yje_weibi_bought': [],
        'yje_trade_stats': [], 'yje_strength_scores': {}, 'yje_auction_signals': {},
        'rzq_stock_list': [], 'qs_stock_list': [], 'qs_trade_log': [],
        'qs_daily_log': [], 'qs_combo_info': {}, 'qs_atr_stops': {},
        'qs_position_highs': {}, '_micro_data_cache': {}, '_cache_date': None,
        'sb_stock_list': [], 'sb_pending_list': [], 'sb_bought_list': [],
        'sb_weibi_bought': [], 'sb_trade_log': [], 'sb_sell_conditions': {},
        'sb_strength_scores': {},
        'last_sb_weibi_minute': -1,
        'sentiment_cache': {'phase': 'unknown', 'prev_limit_up': 0},
        'trade_stats': {}, 'emo_count': [], 'reversal': [], 'limit_up_count': 0,
        'concept_pool': [], 'position_modes': {}, 'sell_conditions': {},
        'preselection_state': {}, 'preselection_ready_date': None,
        'selection_ready_date': None,
        'failed_buy_list': [], 'opening_prices': {}, 'today_bought': {},
        'sell_skip_log_keys': {}, 'buy_skip_log_keys': {},
        'concept_implosion_log_keys': {}, 'sb_limit_log_signature': None,
        'last_check_minute': -1, 'last_weibi_check_minute': -1,
        'last_concept_check': -1, 'index_1m_cache': [], 'emergency_cut': False,
        'temp_position_ratio': 1.0,
        # 事件账本必须跨进程保留，不能在每日重置中清空。
        'cash_event_ledger': {},
    }
    for name, default in defaults.items():
        if not hasattr(g, name):
            setattr(g, name, copy.deepcopy(default))


def _migrate_runtime_state():
    """一次性迁移旧模拟盘状态，移除一进二买入阶段写入的零收益占位记录。"""
    schema_version = int(getattr(g, 'fourmode_state_schema_version', 0) or 0)
    if schema_version < 1:
        old_stats = list(getattr(g, 'yje_trade_stats', []) or [])
        g.yje_trade_stats = [
            item for item in old_stats
            if not (
                isinstance(item, dict)
                and item.get('profit_pct') == 0
                and item.get('success') is False
            )
        ]
        removed_count = len(old_stats) - len(g.yje_trade_stats)
        if removed_count:
            _strategy_log.event(
                '状态迁移', '一进二统计已修正',
                f'清理买入占位记录={removed_count}条', icon='🔄',
            )
        g.fourmode_state_schema_version = 1


def _cash_event_subportfolio(context, pindex):
    """返回出入金目标仓位；没有分仓时只允许使用 pindex=0。"""
    portfolio = context.portfolio
    subportfolios = getattr(portfolio, 'subportfolios', None)
    if subportfolios is not None:
        try:
            if len(subportfolios) > 0:
                if pindex >= len(subportfolios):
                    return None
                return subportfolios[pindex]
        except Exception:
            return None
    return portfolio if pindex == 0 else None


def apply_configured_cash_events(context):
    """代码替换回调触发的一次性出入金；普通进程重启和每日任务不会触发。"""
    events = getattr(g, 'cash_event', None)
    if not events:
        return
    if isinstance(events, dict):
        events = [events]

    ledger = getattr(g, 'cash_event_ledger', None)
    if not isinstance(ledger, dict):
        ledger = {}
        g.cash_event_ledger = ledger

    current_dt = getattr(context, 'current_dt', None)
    now_text = str(current_dt)

    for event in events:
        if not isinstance(event, dict) or not event.get('enabled', True):
            continue

        event_id = str(event.get('id', '')).strip()
        if not event_id:
            _strategy_log.warning('账户出入金事件缺少唯一 id，已跳过')
            continue

        previous = ledger.get(event_id, {})
        if not isinstance(previous, dict):
            previous = {}
        status = previous.get('status') if isinstance(previous, dict) else None
        if status == 'applied':
            _strategy_log.event(
                '账户资金', '已跳过重复事件', f'id={event_id} | 原因=该事件已执行', icon='ℹ️'
            )
            continue
        if status == 'ignored':
            _strategy_log.event(
                '账户资金', '已跳过事件', f'id={event_id} | 原因=该事件此前已忽略', icon='ℹ️'
            )
            continue
        if status == 'manual_review':
            _strategy_log.warning(
                f'账户出入金事件 {event_id} 等待人工核对，禁止自动重试'
            )
            continue
        if status == 'applying':
            _strategy_log.warning(
                f'账户出入金事件 {event_id} 处于 applying 状态，禁止自动重试，请人工核对账户后处理'
            )
            continue

        try:
            cash = float(event.get('cash', 0) or 0)
            pindex = int(event.get('pindex', 0) or 0)
        except Exception as exc:
            ledger[event_id] = {
                'status': 'manual_review', 'error': f'配置无效: {exc}', 'updated_at': now_text,
            }
            _strategy_log.error(f'账户出入金事件 {event_id} 配置无效，已暂停: {exc}')
            continue

        if not np.isfinite(cash) or cash == 0:
            ledger[event_id] = {
                'status': 'ignored', 'cash': cash, 'pindex': pindex, 'updated_at': now_text,
            }
            _strategy_log.event(
                '账户资金', '已跳过出入金', f'id={event_id} | 原因=金额为0', icon='ℹ️'
            )
            continue
        if pindex < 0:
            ledger[event_id] = {
                'status': 'manual_review', 'error': 'pindex 不能为负数', 'updated_at': now_text,
            }
            _strategy_log.error(f'账户出入金事件 {event_id} pindex 无效: {pindex}')
            continue

        subportfolio = _cash_event_subportfolio(context, pindex)
        if subportfolio is None:
            ledger[event_id] = {
                'status': 'manual_review', 'cash': cash, 'pindex': pindex,
                'error': '目标 subportfolio 不存在', 'updated_at': now_text,
            }
            _strategy_log.error(
                f'账户出入金事件 {event_id} 找不到 pindex={pindex} 的目标仓位，已暂停'
            )
            continue

        available_cash = getattr(subportfolio, 'available_cash', None)
        if cash < 0 and available_cash is not None:
            try:
                if float(available_cash) < abs(cash):
                    ledger[event_id] = dict(previous)
                    ledger[event_id].update({
                        'status': 'pending', 'cash': cash, 'pindex': pindex,
                        'updated_at': now_text,
                    })
                    _strategy_log.warning(
                        f'账户出入金事件 {event_id} 出金暂缓 | '
                        f'需要={abs(cash):.2f} | 可用资金={float(available_cash):.2f} | '
                        '下次代码替换时重试'
                    )
                    continue
            except (TypeError, ValueError):
                pass

        before_cash = None
        try:
            before_cash = float(getattr(subportfolio, 'available_cash'))
        except (TypeError, ValueError, AttributeError):
            pass

        # 先落 applying 状态：若调用后进程崩溃，下一次启动禁止盲目重复入金。
        ledger[event_id] = {
            'status': 'applying', 'cash': cash, 'pindex': pindex,
            'started_at': now_text,
        }
        try:
            inout_cash(cash, pindex=pindex)
        except Exception as exc:
            ledger[event_id] = {
                'status': 'manual_review', 'cash': cash, 'pindex': pindex,
                'error': str(exc), 'updated_at': str(getattr(context, 'current_dt', '')),
            }
            _strategy_log.error(f'账户出入金事件 {event_id} 调用失败，已暂停自动重试: {exc}')
            continue

        after_cash = None
        try:
            after_cash = float(getattr(subportfolio, 'available_cash'))
        except (TypeError, ValueError, AttributeError):
            pass

        delta_ok = True
        if before_cash is not None and after_cash is not None:
            delta_ok = abs((after_cash - before_cash) - cash) <= max(0.01, abs(cash) * 1e-6)
        if not delta_ok:
            ledger[event_id] = {
                'status': 'manual_review', 'cash': cash, 'pindex': pindex,
                'before_cash': before_cash, 'after_cash': after_cash,
                'error': '出入金后可用资金变化与配置不一致', 'updated_at': now_text,
            }
            _strategy_log.error(
                f'账户出入金事件 {event_id} 结果待核对 | '
                f'配置={cash:.2f} | 可用资金变化={(after_cash - before_cash):.2f}'
            )
            continue

        ledger[event_id] = {
            'status': 'applied', 'cash': cash, 'pindex': pindex,
            'before_cash': before_cash,
            'after_cash': after_cash, 'updated_at': now_text,
        }
        _strategy_log.event(
            '账户资金', '一次性出入金已执行',
            f'id={event_id} | 金额={cash:.2f} | '
            f'可用资金={after_cash if after_cash is not None else "未知"}',
            icon='💰',
        )


# 统一订单日志出口：推送包装在前，故这里记录的是最终送出的订单结果。
_RAW_ORDER_TARGET = order_target
_RAW_ORDER_TARGET_VALUE = order_target_value


def _order_result_text(order_obj):
    if order_obj is None or order_obj is False:
        return "未生成"
    status = getattr(order_obj, 'status', None)
    filled = getattr(order_obj, 'filled', getattr(order_obj, 'filled_amount', None))
    amount = getattr(order_obj, 'amount', None)
    details = []
    if status is not None:
        details.append(f"状态={status}")
    if amount is not None:
        details.append(f"委托={amount}")
    if filled is not None:
        details.append(f"成交={filled}")
    return ' '.join(details) or '已提交'


def _order_target_logged(security, target_amount, *args, **kwargs):
    try:
        result = _RAW_ORDER_TARGET(security, target_amount, *args, **kwargs)
        event = '已提交' if result else '未生成'
        _strategy_log.event('订单回执', event,
                            f'{security} | 目标数量={target_amount} | {_order_result_text(result)}',
                            icon='💰' if result else '❌',
                            level='info' if result else 'warning')
        return result
    except Exception as exc:
        _strategy_log.event('订单回执', '异常',
                            f'{security} | 目标数量={target_amount} | {exc}',
                            icon='⭕', level='error')
        return None


def _can_close_position(context, security, reason='', log_skip=True):
    """检查卖出前的可平仓数量；同一股票每天只记录一次跳过。"""
    security = str(security)
    position = context.portfolio.positions.get(security)
    if not position or getattr(position, 'total_amount', 0) <= 0:
        return False
    closeable_amount = getattr(position, 'closeable_amount', None)
    if closeable_amount is None:
        closeable_amount = position.total_amount
    if closeable_amount <= 0:
        if log_skip:
            log_key = f'{context.current_dt.strftime("%Y-%m-%d")}|{security}'
            if log_key not in g.sell_skip_log_keys:
                g.sell_skip_log_keys[log_key] = True
                bought_today = security in getattr(g, 'today_bought', {})
                restriction = (
                    '当日买入受T+1限制'
                    if bought_today else '当前无可卖数量，可能已有卖出委托占用'
                )
                detail = (
                    f'{security} | 持仓={int(position.total_amount)}股 | 可卖=0股 | '
                    f'{restriction} | 保留持仓及模式状态 | '
                    f'触发原因={reason or "卖出信号"}'
                )
                _strategy_log.event('交易跳过', '卖出无可用数量', detail, icon='ℹ️')
        return False
    return True


def _close_position_logged(context, security, reason='', check=True):
    """提交清仓委托；只有聚宽返回有效委托对象才报告成功。"""
    if check and not _can_close_position(context, security, reason):
        return None
    return _order_target_logged(str(security), 0)


def _order_target_value_logged(security, target_value, *args, **kwargs):
    try:
        result = _RAW_ORDER_TARGET_VALUE(security, target_value, *args, **kwargs)
        event = '已提交' if result else '未生成'
        _strategy_log.event('订单回执', event,
                            f'{security} | 目标市值={target_value:.2f} | {_order_result_text(result)}',
                            icon='💰' if result else '❌',
                            level='info' if result else 'warning')
        return result
    except Exception as exc:
        _strategy_log.event('订单回执', '异常',
                            f'{security} | 目标市值={target_value:.2f} | {exc}',
                            icon='⭕', level='error')
        return None

# ============================================================================
# 第一部分：工具函数（四模式通用）
# ============================================================================

def transform_date(date, date_type):
    if type(date) == str:
        str_date = date
        dt_date = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date = dt_date.date()
    elif type(date) == dt.datetime:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = dt_date.date()
    elif type(date) == dt.date:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    dct = {'str': str_date, 'dt': dt_date, 'd': d_date}
    return dct[date_type]

def get_shifted_date(date, days, days_type='T'):
    d_date = transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    if days_type == 'N':
        shifted_date = yesterday + dt.timedelta(days + 1)
    if days_type == 'T':
        all_trade_days = _cached_trade_day_strings()
        if str(yesterday) in all_trade_days:
            shifted_date = all_trade_days[all_trade_days.index(str(yesterday)) + days + 1]
        else:
            for i in range(100):
                last_trade_date = yesterday - dt.timedelta(i)
                if str(last_trade_date) in all_trade_days:
                    shifted_date = all_trade_days[all_trade_days.index(str(last_trade_date)) + days + 1]
                    break
    return str(shifted_date)

def filter_new_stock(initial_list, date, days=50):
    d_date = transform_date(date, 'd')
    initial_list = [str(s) for s in initial_list]
    return [stock for stock in initial_list
            if (d_date - _cached_security_start_date(stock, date)
                > dt.timedelta(days=days))]

def filter_st_stock(initial_list, date):
    str_date = transform_date(date, 'str')
    if get_shifted_date(str_date, 0, 'N') != get_shifted_date(str_date, 0, 'T'):
        str_date = get_shifted_date(str_date, -1, 'T')
    initial_list = [str(s) for s in initial_list]
    df = get_extras('is_st', initial_list, start_date=str_date, end_date=str_date, df=True)
    df = df.T
    df.columns = ['is_st']
    df = df[df['is_st'] == False]
    filter_list = list(df.index)
    return [str(s) for s in filter_list]

def filter_kcbj_stock(initial_list):
    initial_list = [str(s) for s in initial_list]
    return [stock for stock in initial_list
            if str(stock)[0] != '4' and str(stock)[0] != '8' and str(stock)[:2] != '68']

def filter_invalid_stocks(initial_list):
    """
    【新增】过滤无效/已退市股票代码（如005开头的深证基金）
    """
    initial_list = [str(s) for s in initial_list]
    filtered = []
    for stock in initial_list:
        # 过滤005开头的深证股票（多为基金或已退市）
        if '.XSHE' in stock and stock.startswith('005'):
            continue
        # 过滤其他明显无效的代码格式
        if len(stock.split('.')[0]) != 6:
            continue
        filtered.append(stock)
    return filtered

def filter_paused_stock(initial_list, date):
    initial_list = [str(s) for s in initial_list]
    df = get_price(initial_list, end_date=date, frequency='daily',
                   fields=['paused'], count=1, panel=False, fill_paused=True)
    df = df[df['paused'] == 0]
    paused_list = list(df.code)
    return [str(s) for s in paused_list]

def filter_name_stock(initial_list):
    initial_list = [str(s) for s in initial_list]
    current_data = get_current_data()
    filtered_list = []
    for s in initial_list:
        try:
            name = current_data[str(s)].name
            if 'ST' not in name and '※ST' not in name and '退' not in name:
                filtered_list.append(str(s))
        except:
            continue
    return filtered_list

def filter_stock_list(stock_list, date):
    if not stock_list:
        return []
    stock_list = [str(s) for s in stock_list]
    stock_list = filter_kcbj_stock(stock_list)
    # 【修复】预过滤无效股票，减少后续None警告
    stock_list = filter_invalid_stocks(stock_list)
    stock_list = filter_new_stock(stock_list, date)
    stock_list = filter_st_stock(stock_list, date)
    stock_list = filter_paused_stock(stock_list, date)
    stock_list = filter_name_stock(stock_list)
    return [str(s) for s in stock_list]

def prepare_stock_list(date):
    date_key = _cache_date_key(date)
    if (_RUNTIME_DATA_CACHE['prepared_stock_list_date'] == date_key
            and _RUNTIME_DATA_CACHE['prepared_stock_list'] is not None):
        return list(_RUNTIME_DATA_CACHE['prepared_stock_list'])
    initial_list = _cached_stock_securities(date).index.tolist()
    initial_list = [str(s) for s in initial_list]
    initial_list = filter_stock_list(initial_list, date)
    result = [str(s) for s in initial_list]
    _RUNTIME_DATA_CACHE['prepared_stock_list_date'] = date_key
    _RUNTIME_DATA_CACHE['prepared_stock_list'] = tuple(result)
    return result

def get_ever_hl_stock2(initial_list, date):
    initial_list = [str(s) for s in initial_list]
    df = get_price(initial_list, end_date=date, frequency='daily',
                   fields=['close', 'high', 'high_limit'], count=1,
                   panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna()
    cd1 = df['high'] == df['high_limit']
    cd2 = df['close'] != df['high_limit']
    df = df[cd1 & cd2]
    hl_list = list(df.code)
    return [str(s) for s in hl_list]

def calculate_mainline_score_optimized(stock, context):
    try:
        stock = str(stock)
        stock_info = _cached_security_info(stock)
        if stock_info and stock_info.concepts and len(stock_info.concepts) > 0:
            return 2
        else:
            return 0
    except:
        return 2

def rise_low_volume(s, context):
    s = str(s)
    hist = attribute_history(s, 106, '1d', fields=['high', 'volume'],
                             skip_paused=True, df=False)
    high_prices = hist['high'][:102]
    prev_high = high_prices[-1]
    zyts_0 = next((i - 1 for i, high in enumerate(high_prices[-3::-1], 2)
                   if high >= prev_high), 100)
    zyts = zyts_0 + 5
    if hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * 0.9:
        return True
    return False

def get_hl_stock(initial_list, date):
    initial_list = [str(s) for s in initial_list]
    df = get_price(initial_list, end_date=date, frequency='daily',
                   fields=['close', 'high_limit'], count=1,
                   panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna()
    df = df[df['close'] == df['high_limit']]
    hl_list = list(df.code)
    return [str(s) for s in hl_list]

def get_continue_count_df(hl_list, date, watch_days):
    if not hl_list:
        return pd.DataFrame(columns=['count', 'extreme_count'])
    hl_list = [str(s) for s in hl_list]
    df = get_price(hl_list, end_date=date, frequency='daily',
                   fields=['close', 'high_limit', 'low'], count=watch_days,
                   panel=False, fill_paused=False, skip_paused=False)
    if df.empty:
        return pd.DataFrame(columns=['count', 'extreme_count'])
    df = df.dropna().copy()
    results = []
    for stock in hl_list:
        stock_df = df[df['code'] == stock].copy()
        stock_df = stock_df.reset_index(drop=True)
        consecutive_count = 0
        extreme_count = 0
        for i in range(len(stock_df) - 1, -1, -1):
            row = stock_df.iloc[i]
            if abs(row['close'] - row['high_limit']) < 0.01:
                consecutive_count += 1
                if abs(row['low'] - row['high_limit']) < 0.01:
                    extreme_count += 1
            else:
                break
        if consecutive_count > 0:
            results.append({'code': str(stock), 'count': consecutive_count,
                           'extreme_count': extreme_count})
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.set_index('code')
    return result_df

def get_hot_concept(dct, ccd, date):
    concept_count = {}
    for key in dct:
        lb_count = 1
        if key in ccd.index:
            lb_count = ccd.loc[key, 'count']
        for i in dct[key]['jq_concept']:
            concept_name = i['name']
            if concept_name in concept_count:
                concept_count[concept_name] += lb_count
            else:
                if concept_name not in ['转融券标的', '融资融券', '深股通', '沪股通', '国企改革']:
                    concept_count[concept_name] = 1
    df = pd.DataFrame(list(concept_count.items()), columns=['concept_name', 'concept_count'])
    df = df.set_index('concept_name')
    df = df.sort_values(by='concept_count', ascending=False)
    concept = list(df.index)[0:5]
    _strategy_log.info('*******************************')
    _strategy_log.info(concept)
    return concept

def get_stocks_by_concept_name(concept_name):
    df_concepts = get_concepts()
    concept_code = df_concepts[df_concepts['name'] == concept_name].index
    if len(concept_code) == 0:
        _strategy_log.info(f"警告：概念 '{concept_name}' 不存在，已跳过")
        return []
    try:
        stocks = get_concept_stocks(str(concept_code[0]))
        return [str(s) for s in stocks]
    except Exception as e:
        error_type = type(e).__name__
        _strategy_log.info(f"[错误] 获取概念股票失败 | 概念: {concept_name} | 错误类型: {error_type} | 详情: {str(e)}")
        return []

def get_concept_stats_weighted(stock_list, date, context, top_n=5,
                               is_today=False, auction_data=None):
    if not stock_list:
        return []
    stock_list = [str(s) for s in stock_list]
    concept_stats = {}
    current_data = get_current_data()
    for stock in stock_list:
        try:
            stock = str(stock)
            stock_info = _cached_security_info(stock, date)
            concept_names = [c['name'] for c in stock_info.concepts if c['name'] not in
                            ['转融券标的', '融资融券', '深股通', '沪股通', '国企改革']]
            volume_money = 0
            if is_today and auction_data and stock in auction_data:
                volume_money = auction_data[stock] * current_data[stock].high_limit
            for name in concept_names:
                if name not in concept_stats:
                    concept_stats[name] = {'count': 0, 'volume': 0}
                concept_stats[name]['count'] += 1
                concept_stats[name]['volume'] += volume_money
        except:
            continue
    if not concept_stats:
        return []
    df = pd.DataFrame.from_dict(concept_stats, orient='index')
    df = df.reset_index()
    df.columns = ['concept', 'count', 'volume']
    score_map = {0: 1.0, 1: 0.8, 2: 0.7, 3: 0.6, 4: 0.5}
    df_count = df.sort_values('count', ascending=False).reset_index(drop=True).copy()
    df_count.loc[:, 'count_score'] = df_count.index.map(lambda x: score_map.get(x, 0.4))
    df_volume = df.sort_values('volume', ascending=False).reset_index(drop=True).copy()
    df_volume.loc[:, 'volume_score'] = df_volume.index.map(lambda x: score_map.get(x, 0.4))
    df = df_count.merge(df_volume[['concept', 'volume_score']], on='concept').copy()
    if df['volume'].sum() == 0:
        df.loc[:, 'total_score'] = df['count_score']
    else:
        df.loc[:, 'total_score'] = df['count_score'] * 0.5 + df['volume_score'] * 0.5
    df = df.sort_values('total_score', ascending=False)
    return list(df['concept'].head(top_n))

def get_factor_filter_df(context, stock_list, jqfactor, sort):
    stock_list = [str(s) for s in stock_list]
    if len(stock_list) != 0:
        yesterday = context.previous_date
        score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday,
                                       count=1)[jqfactor].iloc[0].tolist()
        df = pd.DataFrame(index=stock_list, data={'score': score_list}).dropna()
        df = df.sort_values(by='score', ascending=sort)
    else:
        df = pd.DataFrame(index=[], data={'score': []})
    return df

def get_concept(stock_list, date):
    dct = {}
    for stock in stock_list:
        try:
            stock_str = str(stock)
            dct[stock_str] = {}
            dct[stock_str]['jq_concept'] = _cached_security_info(stock_str, date).concepts
        except:
            continue
    return dct

def normalize_stock_code(stock):
    """
    【新增】标准化股票代码格式
    将各种输入格式转换为聚宽标准格式（000001.XSHE/600000.XSHG）
    """
    try:
        if isinstance(stock, int):
            # 纯数字，需要补零并添加后缀（默认深交所）
            code_str = f"{stock:06d}"
            if code_str.startswith('6') or code_str.startswith('9'):
                return f"{code_str}.XSHG"  # 上交所
            else:
                return f"{code_str}.XSHE"  # 深交所
        elif isinstance(stock, str):
            # 检查是否已有后缀
            if '.' in stock:
                return stock
            else:
                # 纯数字字符串，添加后缀
                if len(stock) == 6:
                    if stock.startswith('6') or stock.startswith('9'):
                        return f"{stock}.XSHG"
                    else:
                        return f"{stock}.XSHE"
                else:
                    # 尝试补零
                    code_num = int(stock)
                    code_str = f"{code_num:06d}"
                    if code_str.startswith('6') or code_str.startswith('9'):
                        return f"{code_str}.XSHG"
                    else:
                        return f"{code_str}.XSHE"
        else:
            return str(stock)
    except:
        return str(stock)

def filter_by_concept(stock_list, concept_pool):
    """
    【核心修复】根据概念池过滤股票，修复NoneType错误
    """
    if not stock_list or not concept_pool:
        return stock_list

    concept_pool_set = set(concept_pool)
    filtered = []
    error_stocks = []  # 批量记录错误，减少日志输出

    for stock in stock_list:
        try:
            # 【修复1】标准化股票代码格式
            stock = normalize_stock_code(stock)
            stock_str = str(stock)

            # 【修复2】检查get_security_info返回值，防止NoneType
            stock_info = _cached_security_info(stock_str)
            if stock_info is None:
                error_stocks.append(stock_str)
                continue

            # 【修复3】安全访问concepts属性
            if not hasattr(stock_info, 'concepts') or stock_info.concepts is None:
                error_stocks.append(stock_str)
                continue

            concepts = stock_info.concepts
            stock_concept_names = set(c['name'] for c in concepts if c.get('name'))

            if stock_concept_names & concept_pool_set:
                filtered.append(stock_str)

        except Exception as e:
            error_msg = str(e)
            # 类型错误直接跳过
            if "参数必须是个字符串" in error_msg or "must be a string" in error_msg:
                _strategy_log.warning(f"【概念过滤】{stock} 类型错误，已跳过: {error_msg}")
            # NoneType错误或其他错误也跳过（不再默认保留）
            elif "'NoneType' object has no attribute" in error_msg:
                error_stocks.append(stock)
            else:
                # 其他错误（如网络），默认跳过以确保数据质量
                _strategy_log.info(f"【概念过滤】{stock} 获取概念失败，已跳过: {error_msg}")

    # 批量输出错误日志，减少刷屏
    if error_stocks:
        _strategy_log.warning(f"【概念过滤】{len(error_stocks)}只股票获取信息失败(已退市/数据缺失): {error_stocks[:5]}...")

    return filtered

def is_one_word_limit(stock, context):
    """兼容普通权限：优先使用tick，失败则使用开盘数据判断"""
    stock = str(stock)
    current_data = get_current_data()
    if current_data[stock].day_open < current_data[stock].high_limit * 0.995:
        return False
    try:
        tick = get_current_tick(stock)
        if tick:
            if tick['b1_v'] > 10000 and tick['a1_v'] == 0:
                return True
            if current_data[stock].last_price >= current_data[stock].high_limit * 0.998 and \
               current_data[stock].day_open >= current_data[stock].high_limit * 0.995:
                return True
    except:
        if current_data[stock].day_open >= current_data[stock].high_limit * 0.995:
            return True
    return False

def record_buy_attempt(stock, mode, reason=''):
    stock = str(stock)
    mode_name = "一进二" if mode == 'yje' else (
        "弱转强" if mode == 'rzq' else (
        "首板" if mode == 'sb' else "趋势股"))
    _strategy_log.info(f"【买入失败-{mode_name}】{stock} | 原因: {reason}")
    g.failed_buy_list.append(stock)

# ============================================================================
# 第二部分：一进二新评分体系（2026-03-29更新）
# ============================================================================

def calculate_bottom_rebound_score(stock, date_str):
    """
    计算底部首板得分（前20日累计跌幅）- 15分
    超跌首板平均收益是高位首板的10倍
    """
    try:
        stock = str(stock)
        hist = _cached_daily_frame(
            stock, date_str, ['close'], 21, skip_paused=True,
        )
        if len(hist) < 21:
            return 0, 0

        price_20d_ago = hist['close'].iloc[0]
        price_now = hist['close'].iloc[-1]

        decline_pct = (price_now - price_20d_ago) / price_20d_ago * 100

        if decline_pct < -10:
            score = 15
        elif decline_pct < -5:
            score = 8
        elif decline_pct < 0:
            score = 3
        else:
            score = 0

        return score, decline_pct

    except Exception as e:
        return 0, 0

def calculate_volume_ratio_score(stock, date_str):
    """
    计算T-1日量比得分 - 20分
    量比<0.3的股票次日收益+7.44%，胜率91.7%
    """
    try:
        stock = str(stock)
        hist = _cached_daily_frame(
            stock, date_str, ['volume'], 10, skip_paused=True,
        )
        if len(hist) < 2:
            return 0, 999

        vol_t_1 = hist['volume'].iloc[-2]
        vol_t = hist['volume'].iloc[-1]

        volume_ratio = vol_t_1 / vol_t if vol_t > 0 else 999

        if volume_ratio < 0.3:
            score = 20
        elif volume_ratio < 0.5:
            score = 15
        elif volume_ratio < 1.0:
            score = 5
        else:
            score = 0

        return score, volume_ratio

    except Exception as e:
        return 0, 999

def calculate_board_quality_metrics(stock, date_str, context, include_live_auction=True):
    """
    计算封板质量指标 - 60分
    包含：封板成交占比(25) + 封板时长(10) + 封流比(10) + 封成比(5) + 一字板(5) + 价格稳定性(5)
    """
    metrics = {}
    quality_score = 0

    try:
        stock = str(stock)
        min_data = get_price(stock, start_date=date_str, end_date=date_str,
                           frequency='1m', fields=['close', 'high', 'low', 'volume', 'high_limit'],
                           skip_paused=True)

        if min_data.empty or len(min_data) < 240:
            return metrics, 0

        hist_day = attribute_history(stock, 1, '1d',
                                    ['close', 'volume', 'high_limit', 'money'],
                                    skip_paused=True)

        if hist_day.empty:
            return metrics, 0

        total_volume = hist_day['volume'][0]
        total_money = hist_day['money'][0]
        high_limit = hist_day['high_limit'][0]
        metrics['_total_volume'] = float(total_volume)

        limit_mask = min_data['close'] >= min_data['high_limit'] * 0.998
        limit_minutes = min_data[limit_mask]

        if not limit_minutes.empty:
            seal_volume = limit_minutes['volume'].sum()
            seal_volume_ratio = seal_volume / total_volume if total_volume > 0 else 1.0
        else:
            seal_volume_ratio = 1.0

        metrics['封板成交占比'] = seal_volume_ratio

        if seal_volume_ratio < 0.1:
            quality_score += 25
            metrics['封板成交占比评分'] = 25
        elif seal_volume_ratio < 0.3:
            quality_score += 15
            metrics['封板成交占比评分'] = 15
        elif seal_volume_ratio < 0.5:
            quality_score += 5
            metrics['封板成交占比评分'] = 5
        else:
            metrics['封板成交占比评分'] = 0

        if not limit_minutes.empty:
            first_limit_idx = min_data.index.get_loc(limit_minutes.index[0])
            seal_duration = max(0, len(min_data) - first_limit_idx)
        else:
            seal_duration = 0

        metrics['封板时长'] = seal_duration

        if seal_duration >= 180:
            quality_score += 10
            metrics['封板时长评分'] = 10
        elif seal_duration >= 120:
            quality_score += 6
            metrics['封板时长评分'] = 6
        elif seal_duration >= 60:
            quality_score += 3
            metrics['封板时长评分'] = 3
        else:
            metrics['封板时长评分'] = 0

        tick = None
        if include_live_auction:
            try:
                tick = get_current_tick(stock)
            except Exception:
                tick = None

        circ_cap = None
        if not include_live_auction or (tick and tick['b1_v'] is not None):
            try:
                valuation = get_valuation(
                    stock, start_date=date_str, end_date=date_str,
                    fields=['circulating_market_cap'],
                )
                if valuation is not None and not valuation.empty:
                    circ_cap = float(valuation['circulating_market_cap'].iloc[0]) * 1e8
            except Exception:
                circ_cap = None
        metrics['_circulating_market_cap'] = circ_cap

        live_score, live_metrics = _calculate_yje_live_board_score(
            tick, float(total_volume), circ_cap
        )
        quality_score += live_score
        metrics.update(live_metrics)

        first_min_close = min_data['close'].iloc[0]
        is_one_word = (first_min_close >= high_limit * 0.998)
        metrics['一字板'] = is_one_word

        if is_one_word:
            quality_score += 5
            metrics['一字板评分'] = 5
        else:
            metrics['一字板评分'] = 0

        high_price = min_data['high'].max()
        low_price = min_data['low'].min()

        if high_limit > 0:
            price_range = (high_price - low_price) / high_limit
        else:
            price_range = 0

        metrics['价格振幅'] = price_range

        if price_range < 0.02:
            quality_score += 5
            metrics['价格稳定性评分'] = 5
        elif price_range < 0.05:
            quality_score += 3
            metrics['价格稳定性评分'] = 3
        elif price_range < 0.08:
            quality_score += 1
            metrics['价格稳定性评分'] = 1
        else:
            metrics['价格稳定性评分'] = 0

        metrics['封板质量总分'] = quality_score
        return metrics, quality_score

    except Exception as e:
        return metrics, 0

def calculate_concept_score(stock, concept_pool):
    """
    计算题材热度得分 - 5分
    """
    if not concept_pool:
        return 0

    try:
        stock = str(stock)
        stock_info = get_security_info(stock)
        if stock_info is None or not hasattr(stock_info, 'concepts'):
            return 0

        concepts = stock_info.concepts
        stock_concepts = [c['name'] for c in concepts if c['name'] not in
                         ['转融券标的', '融资融券', '深股通', '沪股通', '国企改革']]

        concept_pool_list = list(concept_pool)

        in_top3 = any(c in concept_pool_list[:3] for c in stock_concepts)
        in_top5 = any(c in concept_pool_list[:5] for c in stock_concepts)

        if in_top3:
            return 5
        elif in_top5:
            return 3
        else:
            return 0

    except:
        return 0

def _calculate_yje_live_board_score(tick, total_volume, circ_cap):
    """使用最终竞价盘口补齐一进二封流比和封成比评分。"""
    metrics = {
        '封流比': None, '封流比评分': 0,
        '封成比': None, '封成比评分': 0,
    }
    score = 0
    try:
        if tick and tick['b1_v'] is not None:
            seal_amount = tick['b1_v'] * tick['b1_p']
            fengliubi = seal_amount / circ_cap if circ_cap and circ_cap > 0 else 0
            metrics['封流比'] = fengliubi
            if fengliubi > 0.05:
                metrics['封流比评分'] = 10
            elif fengliubi > 0.02:
                metrics['封流比评分'] = 6
            elif fengliubi > 0.01:
                metrics['封流比评分'] = 3
            score += metrics['封流比评分']

            fengchengbi = tick['b1_v'] / total_volume if total_volume > 0 else 0
            metrics['封成比'] = fengchengbi
            if fengchengbi > 10:
                metrics['封成比评分'] = 5
            elif fengchengbi > 3:
                metrics['封成比评分'] = 3
            elif fengchengbi > 1:
                metrics['封成比评分'] = 1
            score += metrics['封成比评分']
    except Exception:
        return 0, {
            '封流比': None, '封流比评分': 0,
            '封成比': None, '封成比评分': 0,
        }
    return score, metrics


def calculate_yje_score(stock, date_str, concept_pool, context,
                        include_live_auction=True):
    """
    计算一进二综合评分（满分100分）
    """
    total_score = 0
    details = {}

    try:
        stock = str(stock)
        board_metrics, board_score = calculate_board_quality_metrics(
            stock, date_str, context, include_live_auction=include_live_auction
        )
        total_score += board_score
        details.update(board_metrics)

        # 两个评分项共享同一份21日收盘/成交量数据，字段和历史窗口保持不变。
        _cached_daily_frame(
            stock, date_str, ['close', 'volume'], 21, skip_paused=True,
        )

        vol_score, volume_ratio = calculate_volume_ratio_score(stock, date_str)
        total_score += vol_score
        details['T-1日量比'] = volume_ratio
        details['T-1日量比评分'] = vol_score

        bottom_score, decline_pct = calculate_bottom_rebound_score(stock, date_str)
        total_score += bottom_score
        details['20日跌幅'] = decline_pct
        details['底部首板评分'] = bottom_score

        concept_score = calculate_concept_score(stock, concept_pool)
        total_score += concept_score
        details['题材热度评分'] = concept_score

        details['综合评分'] = total_score

        return total_score, details

    except Exception as e:
        return 0, {}

def check_strategy_effectiveness(context):
    if not g.yje_trade_stats:
        return 1.0, True

    current_date = context.current_dt.date()
    recent_trades = []
    for trade in g.yje_trade_stats:
        trade_date = trade['date'] if isinstance(trade['date'], dt.date) else \
            dt.datetime.strptime(trade['date'], '%Y-%m-%d').date()
        days_diff = (current_date - trade_date).days
        if 0 <= days_diff <= 3:
            recent_trades.append(trade)

    if len(recent_trades) < 3:
        _strategy_log.info(f"【策略有效性】近3日一进二交易次数{len(recent_trades)}<3，暂不评估")
        return 1.0, True

    success_count = sum(1 for t in recent_trades if t.get('success', False))
    success_rate = success_count / len(recent_trades)

    _strategy_log.info(f"【策略有效性】近3日一进二统计：成功{success_count}/{len(recent_trades)}，胜率{success_rate:.1%}")
    return success_rate, success_rate >= 0.3

def record_yje_trade_result(context, stock, profit_pct):
    stock = str(stock)
    record = {
        'date': context.current_dt.date(),
        'stock': stock,
        'profit_pct': profit_pct,
        'success': profit_pct > 0
    }
    g.yje_trade_stats.append(record)
    if len(g.yje_trade_stats) > 20:
        g.yje_trade_stats.pop(0)

def midday_sentiment_update(context):
    try:
        _strategy_log.info("========== 午间情绪更新 ==========")
        index_code = '000001.XSHG'
        morning_data = get_price(index_code,
                                 start_date=context.current_dt.strftime('%Y-%m-%d 09:30:00'),
                                 end_date=context.current_dt, # <--- 精确到当前时刻
                                 frequency='1m', fields=['close', 'volume'])
        if morning_data.empty:
            _strategy_log.warning("【午间更新】无法获取上午数据")
            return

        morning_close = morning_data['close'].iloc[-1]
        morning_high = morning_data['close'].max()
        morning_low = morning_data['close'].min()
        prev_close = attribute_history(index_code, 1, '1d', ['close'],
                                       skip_paused=True)['close'][0]

        morning_change = (morning_close - prev_close) / prev_close * 100
        morning_volatility = (morning_high - morning_low) / prev_close * 100

        _strategy_log.info(f"【午间数据】上午收盘{morning_close:.2f}，"
                 f"涨跌幅{morning_change:.2f}%，振幅{morning_volatility:.2f}%")

        if morning_change > 1.5 and morning_volatility < 2:
            g.priority_config = ["yje", "sb", "rzq"]
            _strategy_log.info("【午间调整】上午单边强势，维持一进二优先，首板次优")
        elif morning_change < -1.0 or morning_volatility > 3:
            g.priority_config = ["rzq", "yje", "sb"]
            _strategy_log.info("【午间调整】上午弱势或高波动，切换为弱转强优先")

        if morning_change < -1.5:
            g.temp_position_ratio = 0.5
            _strategy_log.warning("【午间风控】上午跌幅过大，下午仓位限制50%")

    except Exception as e:
        _strategy_log.error(f"【午间更新错误】{str(e)}")

def check_intraday_volatility(context):
    try:
        current_minute = context.current_dt.hour * 60 + context.current_dt.minute
        index_code = '000001.XSHG'
        current_data = get_current_data()
        current_price = current_data[index_code].last_price

        if current_minute % 5 != 0:
            return

        g.index_1m_cache.append({'minute': current_minute, 'price': current_price})
        if len(g.index_1m_cache) > 30:
            g.index_1m_cache.pop(0)

        target_minute = current_minute - 10
        past_records = [r for r in g.index_1m_cache if r['minute'] <= target_minute]
        if not past_records:
            return

        past_price = past_records[-1]['price']
        drop_pct = (current_price - past_price) / past_price * 100

        if drop_pct < -1.5 and not g.emergency_cut:
            _strategy_log.warning(f"【波动率突变】指数10分钟跌幅{drop_pct:.2f}%，触发紧急降仓！")
            g.emergency_cut = True
            g.temp_position_ratio = 0.3
        elif drop_pct > -0.5 and g.emergency_cut:
            _strategy_log.info(f"【波动率恢复】跌幅收窄至{drop_pct:.2f}%，解除紧急降仓")
            g.emergency_cut = False

    except Exception as e:
        _strategy_log.error(f"【波动率监控错误】{str(e)}")

# ============================================================================
# 第三部分：微观结构风控（四模式通用仓位管理）
# ============================================================================

def trend_strength_score(context):
    try:
        index_code = '000001.XSHG'
        close = attribute_history(index_code, 80, '1d', ['close'],
                                  skip_paused=True)['close']
        if len(close) < 60:
            return 20

        ma3 = close[-3:].mean()
        ma10 = close[-10:].mean()
        ma20 = close[-20:].mean()
        ma60 = close[-60:].mean()
        current = close[-1]

        bull_score = 0
        if ma3 > ma10 > ma20 > ma60:
            bull_score = 30
        elif ma3 > ma10 > ma20:
            bull_score = 20
        elif ma10 > ma20:
            bull_score = 10

        ma20_prev = close[-25:-5].mean()
        ma20_slope = (ma20 - ma20_prev) / ma20_prev * 100 if ma20_prev > 0 else 0
        slope_score = max(0, min(20, (ma20_slope + 2) * 5))

        bias_20 = (current - ma20) / ma20 * 100
        if bias_20 > 5:
            bias_score = max(0, 15 - (bias_20 - 5) * 3)
        elif bias_20 < -5:
            bias_score = min(20, abs(bias_20) * 2)
        else:
            bias_score = 15

        return bull_score + slope_score + bias_score
    except:
        return 20

def momentum_fatigue_score(context):
    try:
        index_code = '000001.XSHG'
        hist = attribute_history(index_code, 30, '1d',
                                 ['close', 'volume', 'high', 'low'],
                                 skip_paused=True)
        if len(hist) < 20:
            return 15

        price_new_high = hist['close'][-1] >= hist['close'][-10:].max() * 0.998
        vol_recent = hist['volume'][-5:].mean()
        vol_prev = hist['volume'][-10:-5].mean()
        volume_decline = vol_recent < vol_prev * 0.85

        divergence_penalty = 0
        if price_new_high and volume_decline:
            divergence_penalty = 20

        rsi = talib.RSI(hist['close'].values, timeperiod=14)
        if len(rsi) >= 5:
            rsi_divergence = (hist['close'][-1] > hist['close'][-5] and
                              rsi[-1] < rsi[-5] * 0.98)
            if rsi_divergence:
                divergence_penalty += 15

        macd, signal, hist_macd = talib.MACD(hist['close'].values,
                                             fastperiod=12, slowperiod=26,
                                             signalperiod=9)
        if len(hist_macd) >= 4 and hist_macd[-1] > 0:
            macd_fade = (hist_macd[-1] < hist_macd[-2] and
                         hist_macd[-2] < hist_macd[-3])
            if macd_fade:
                divergence_penalty += 10

        return max(0, 30 - divergence_penalty)
    except:
        return 15

def volatility_adjustment(context):
    try:
        index_code = '000001.XSHG'
        hist = attribute_history(index_code, 120, '1d',
                                 ['close', 'high', 'low'], skip_paused=True)
        if len(hist) < 60:
            return 10, 1.0

        atr14 = talib.ATR(hist['high'].values, hist['low'].values,
                          hist['close'].values, timeperiod=14)[-1]
        atr60 = talib.ATR(hist['high'].values, hist['low'].values,
                          hist['close'].values, timeperiod=60)[-1]
        atr_ratio = atr14 / atr60 if atr60 > 0 else 1.0

        if atr_ratio < 0.8:
            return 20, 1.0
        elif atr_ratio > 1.5:
            return 5, 0.5
        elif atr_ratio > 1.2:
            return 10, 0.75
        else:
            return 15, 1.0
    except:
        return 10, 1.0

def market_micro_structure(context):
    try:
        index_code = '000001.XSHG'
        vol_hist = attribute_history(index_code, 20, '1d',
                                     ['volume'], skip_paused=True)['volume']
        if len(vol_hist) < 5:
            return 5

        current_vol = vol_hist[-1]
        ma5_vol = vol_hist[-5:].mean()
        limit_up_count = getattr(g, 'limit_up_count', 0)

        if current_vol > ma5_vol * 1.5:
            if limit_up_count < 30:
                return 2
            else:
                return 5
        elif current_vol > ma5_vol * 1.2:
            return 10
        elif current_vol < ma5_vol * 0.7:
            return 3
        else:
            return 8
    except:
        return 5

def is_capital_flight(context):
    try:
        index_code = '000001.XSHG'
        hist = attribute_history(index_code, 5, '1d',
                                 ['close', 'volume'], skip_paused=True)
        if len(hist) < 4:
            return False

        down_days = 0
        for i in range(-3, 0):
            if i >= -len(hist):
                price_down = hist['close'][i] < hist['close'][i - 1]
                vol_up = hist['volume'][i] > hist['volume'][i - 1] * 1.1
                if price_down and vol_up:
                    down_days += 1
        return down_days >= 2
    except:
        return False

def calculate_position_ratio(context):
    trend_score = trend_strength_score(context)
    momentum_score = momentum_fatigue_score(context)
    vol_score, vol_coeff = volatility_adjustment(context)
    micro_score = market_micro_structure(context)

    total_score = trend_score + momentum_score + vol_score + micro_score
    base_ratio = total_score / 100.0
    adjusted_ratio = base_ratio * vol_coeff

    if is_capital_flight(context):
        adjusted_ratio = min(adjusted_ratio, 0.3)
        _strategy_log.warning("【极端风控】检测到资金连续出逃，强制限制仓位≤30%")

    if g.emergency_cut:
        adjusted_ratio = min(adjusted_ratio, g.temp_position_ratio)
        _strategy_log.warning(f"【临时降仓生效】当前仓位限制为{g.temp_position_ratio:.0%}")

    final_ratio = max(0.0, min(1.0, adjusted_ratio))

    _strategy_log.info(f"【微观结构仓位诊断】趋势{trend_score}/40 动量{momentum_score}/30 "
             f"波动{vol_score}/20 微观{micro_score}/10 | "
             f"综合{total_score} | 系数{final_ratio:.2%}")

    return final_ratio

def check_market_risk_microstructure(context):
    try:
        new_ratio = calculate_position_ratio(context)
        g.position_ratio = new_ratio
        actual_max_pos = max(1, int(g.max_positions * new_ratio))
        _strategy_log.info(f"【微观结构风控】仓位系数: {new_ratio:.2%} | 最大持仓: {actual_max_pos}只")
    except Exception as e:
        _strategy_log.error(f"【微观结构风控错误】{str(e)}，回退到默认仓位")
        g.position_ratio = 1.0

# ============================================================================
# 第四部分：首板模块（SHOB - 首板选股与买卖管理）
# ============================================================================
# ============================================================================
# 新增：三维买入确认系统（首板专用）
# ============================================================================

def check_3d_buy_signal(stock, context):
    """
    三维买入确认系统 - 首板专用版本
    返回: (是否买入, 综合得分0-100, 信号详情dict)
    """
    signals = {}

    try:
        stock = str(stock)
        current_data = get_current_data()

        # 获取基础数据
        current_price = current_data[stock].last_price
        day_open = current_data[stock].day_open
        current_pct = (current_price / day_open - 1) * 100 if day_open > 0 else 0

        # 1. 分时结构确认（突破均价线+放量）
        break_avg_line, vol_ratio = check_break_avg_line(stock, context)
        signals['break_avg'] = break_avg_line
        signals['vol_ratio'] = round(vol_ratio, 2)

        # 2. 相对强度确认（日内位置）
        self_strength, _ = calculate_intraday_strength(stock, context)
        signals['strength'] = round(self_strength, 1)

        # 3. 资金攻击波形确认（连续放量上涨）
        attack_score, large_ratio = analyze_attack_wave(stock, context, window_minutes=5)
        signals['attack'] = attack_score
        signals['large_ratio'] = round(large_ratio, 2)

        # 综合评分（首板专用权重）
        total_score = 0
        if break_avg_line and vol_ratio > 1.5:
            total_score += 35  # 突破均价线权重最高（防追高）
        if self_strength > 50:  # 处于日内上半区（强势）
            total_score += 25
        if attack_score >= 3:   # 至少3分钟连续攻击
            total_score += 30
        if large_ratio > 0.3:   # 大单占比
            total_score += 10

        # 首板买入条件：
        # 条件A：2% < 涨幅 < 8% 且 三维评分 >= 60（标准买入）
        # 条件B：涨幅 >= 8% 且 突破均价线确认（极速买入，由兜底函数处理）
        should_buy = False
        if (2.0 < current_pct < 8.0) and (total_score >= 60):
            should_buy = True
            signals['trigger'] = '3D标准'
        elif (current_pct >= 8.0) and break_avg_line:
            should_buy = True
            total_score = 85  # 强制高分
            signals['trigger'] = '8%极速'

        return should_buy, total_score, signals

    except Exception as e:
        _strategy_log.debug(f"【首板3D错误】{stock}: {str(e)}")
        return False, 0, {}

def analyze_attack_wave(stock, context, window_minutes=5):
    """
    分析资金攻击波形（最近N分钟）
    返回: (攻击得分0-10, 大单占比0-1)
    """
    try:
        stock = str(stock)
        min_data = get_price(stock, end_date=context.current_dt, frequency='1m',
                            fields=['close', 'volume', 'money'],
                            count=window_minutes+3, panel=False)

        if len(min_data) < window_minutes:
            return 0, 0

        # 计算分钟涨跌幅和量比
        min_data['pct'] = min_data['close'].pct_change()
        min_data['vol_ma3'] = min_data['volume'].rolling(window=3, min_periods=1).mean()
        min_data['vol_ratio'] = min_data['volume'] / min_data['vol_ma3'].replace(0, np.nan)

        # 识别连续攻击波形
        attack_score = 0
        consecutive = 0

        for i in range(1, len(min_data)):
            if (min_data['pct'].iloc[i] > 0.0015 and  # 上涨>0.15%
                min_data['vol_ratio'].iloc[i] > 1.3):  # 放量>30%
                consecutive += 1
                attack_score += min(consecutive, 3)  # 连续加分，单次最多3
            elif min_data['pct'].iloc[i] < -0.001:
                consecutive = max(0, consecutive - 1)  # 下跌扣分

        # 大单占比估算（用成交额/成交量比）
        recent = min_data.iloc[-3:]
        avg_price = recent['money'].sum() / (recent['volume'].sum() + 1)
        day_open = get_current_data()[stock].day_open
        price_dev = (avg_price - day_open) / day_open if day_open > 0 else 0
        large_ratio = min(max(price_dev * 8, 0), 0.8)  # 简化模型

        return min(attack_score, 10), large_ratio

    except:
        return 0, 0

def calculate_intraday_strength(stock, context):
    """
    计算日内强度（当前价格位于今日高低点的百分比位置）
    返回: (自身强度0-100, 相对强度)
    """
    try:
        stock = str(stock)
        today_str = context.current_dt.strftime('%Y-%m-%d')
        min_data = get_price(stock, start_date=today_str, end_date=context.current_dt,
                            frequency='1m', fields=['high', 'low', 'close'], panel=False)

        if len(min_data) < 3:
            return 50, 0

        day_high = min_data['high'].max()
        day_low = min_data['low'].min()
        current = get_current_data()[stock].last_price

        if day_high > day_low:
            strength = (current - day_low) / (day_high - day_low) * 100
        else:
            strength = 50

        return strength, 0

    except:
        return 50, 0

def check_break_avg_line(stock, context, lookback_minutes=8):
    """
    检查是否放量突破分时均价线（VWAP）
    返回: (是否突破, 量比)
    """
    try:
        stock = str(stock)
        min_data = get_price(stock, end_date=context.current_dt, frequency='1m',
                            fields=['close', 'volume', 'avg'], count=lookback_minutes, panel=False)

        if len(min_data) < 5:
            return False, 0

        # 寻找上穿均价线点（当前>avg，前一刻<=avg）
        for i in range(1, len(min_data)):
            curr_close = min_data['close'].iloc[i]
            curr_avg = min_data['avg'].iloc[i]
            prev_close = min_data['close'].iloc[i-1]
            prev_avg = min_data['avg'].iloc[i-1]

            if (curr_close > curr_avg) and (prev_close <= prev_avg):
                # 检查放量
                prev_vol = min_data['volume'].iloc[max(0, i-5):i].mean()
                if prev_vol > 0:
                    ratio = min_data['volume'].iloc[i] / prev_vol
                    if ratio > 1.5 and (curr_close / min_data['close'].iloc[0] - 1) < 0.06:
                        return True, ratio

        # 当前时刻检查：若已在均价线上方且持续放量，也算突破
        latest = min_data.iloc[-1]
        if latest['close'] > latest['avg']:
            avg_vol = min_data['volume'].iloc[:-1].mean()
            if avg_vol > 0:
                ratio = latest['volume'] / avg_vol
                return ratio > 1.2, ratio

        return False, 0

    except:
        return False, 0
def check_sb_history_condition(stock, date_str, history=None):
    """
    【首板-历史条件检查】
    检查近10个交易日是否有涨停记录（不含今天）
    """
    try:
        stock = str(stock)
        hist = history
        if hist is None:
            hist = get_price(stock, end_date=date_str, frequency='daily',
                             fields=['close', 'high_limit'], count=10,
                             skip_paused=True)
        if hist is None or hist.empty:
            return False

        hl_count = (hist['close'] >= hist['high_limit'] * 0.998).sum()
        if hl_count > 0:
            return False
        return True
    except Exception as e:
        _strategy_log.info(f"【首板-历史检查】{stock} 检查失败: {str(e)}")
        return False


def _sb_screen_reason_reference(hist):
    """首板原始 pandas 筛选语义，供快速路径自检和自动降级。"""
    if (hist['close'] >= hist['high_limit'] * 0.998).sum() > 0:
        return 'history_hl'
    if len(hist) < 5:
        return 'data_error'

    last_close = hist['close'].iloc[-1]
    prev_close = hist['close'].iloc[-2] if len(hist) >= 2 else last_close
    yesterday_chg = (last_close - prev_close) / prev_close * 100 if prev_close > 0 else 0
    if yesterday_chg < 2.0 or yesterday_chg > 9.0:
        return 'price_change'

    amplitude = (hist['high'].iloc[-1] - hist['low'].iloc[-1]) / prev_close * 100 if prev_close > 0 else 0
    if amplitude <= 2.0:
        return 'amplitude'

    vol_yesterday = hist['volume'].iloc[-1]
    vol_ma5 = hist['volume'].iloc[-6:-1].mean() if len(hist) >= 6 else hist['volume'].mean()
    vol_ratio = vol_yesterday / vol_ma5 if vol_ma5 > 0 else 0
    if vol_ratio <= 1.5:
        return 'volume_ratio'

    ma5 = hist['close'].iloc[-5:].mean()
    if last_close < ma5 * 0.98:
        return 'ma_fail'

    vol_ma5_recent = hist['volume'].rolling(5).mean()
    count_high_ratio = 0
    for i in range(-4, 0):
        if abs(i) <= len(hist):
            day_vol = hist['volume'].iloc[i]
            day_ma5 = vol_ma5_recent.iloc[i - 1] if i - 1 >= -len(hist) else vol_ma5
            if day_ma5 > 0 and day_vol / day_ma5 > 1.3:
                count_high_ratio += 1
    if count_high_ratio < 2:
        return 'volume_ratio_5d'

    if len(hist) >= 20:
        ma20_today = hist['close'].iloc[-20:].mean()
        ma20_yesterday = hist['close'].iloc[-21:-1].mean() if len(hist) >= 21 else ma20_today
        ma20_slope = (ma20_today - ma20_yesterday) / ma20_yesterday if ma20_yesterday > 0 else 0
        if ma20_slope < 0:
            return 'ma20_slope'

    if len(hist) >= 10:
        if hist['volume'].iloc[-5:].mean() <= hist['volume'].iloc[-10:].mean():
            return 'vol_ma5_10'

    if len(hist) >= 6:
        ma5_today = hist['close'].iloc[-5:].mean()
        ma5_yesterday = hist['close'].iloc[-6:-1].mean()
        ma5_slope = (ma5_today - ma5_yesterday) / ma5_yesterday if ma5_yesterday > 0 else 0
        if ma5_slope <= 0:
            return 'ma5_slope'

        vol_ma5_today = hist['volume'].iloc[-5:].mean()
        vol_ma5_yesterday = hist['volume'].iloc[-6:-1].mean()
        vol_ma5_slope = (
            (vol_ma5_today - vol_ma5_yesterday) / vol_ma5_yesterday
            if vol_ma5_yesterday > 0 else 0
        )
        if vol_ma5_slope <= 0:
            return 'vol_ma5_slope'

    return None


def _sb_screen_reason_fast(hist):
    """有限数值日线的 NumPy 等价路径；异常数据交回原始 pandas 路径。"""
    arrays = {}
    row_count = len(hist)
    for field in ('close', 'high', 'low', 'volume', 'high_limit'):
        raw_values = np.asarray(hist[field].values)
        if not np.issubdtype(raw_values.dtype, np.number):
            raise ValueError(f'{field}列不是原生数值类型')
        values = raw_values.astype(float, copy=False)
        if values.ndim != 1 or len(values) != row_count or not np.all(np.isfinite(values)):
            raise ValueError(f'{field}列无法使用首板NumPy快速路径')
        arrays[field] = values

    close = arrays['close']
    high = arrays['high']
    low = arrays['low']
    volume = arrays['volume']
    if np.count_nonzero(close >= arrays['high_limit'] * 0.998) > 0:
        return 'history_hl'
    if row_count < 5:
        return 'data_error'

    last_close = close[-1]
    prev_close = close[-2]
    yesterday_chg = (last_close - prev_close) / prev_close * 100 if prev_close > 0 else 0
    if yesterday_chg < 2.0 or yesterday_chg > 9.0:
        return 'price_change'
    amplitude = (high[-1] - low[-1]) / prev_close * 100 if prev_close > 0 else 0
    if amplitude <= 2.0:
        return 'amplitude'

    vol_ma5 = np.mean(volume[-6:-1]) if row_count >= 6 else np.mean(volume)
    vol_ratio = volume[-1] / vol_ma5 if vol_ma5 > 0 else 0
    if vol_ratio <= 1.5:
        return 'volume_ratio'
    if last_close < np.mean(close[-5:]) * 0.98:
        return 'ma_fail'

    count_high_ratio = 0
    for i in range(-4, 0):
        rolling_position = row_count + i - 1
        if rolling_position >= 4:
            day_ma5 = np.mean(volume[rolling_position - 4:rolling_position + 1])
            if day_ma5 > 0 and volume[i] / day_ma5 > 1.3:
                count_high_ratio += 1
    if count_high_ratio < 2:
        return 'volume_ratio_5d'

    if row_count >= 20:
        ma20_today = np.mean(close[-20:])
        ma20_yesterday = np.mean(close[-21:-1]) if row_count >= 21 else ma20_today
        ma20_slope = (ma20_today - ma20_yesterday) / ma20_yesterday if ma20_yesterday > 0 else 0
        if ma20_slope < 0:
            return 'ma20_slope'
    if row_count >= 10 and np.mean(volume[-5:]) <= np.mean(volume[-10:]):
        return 'vol_ma5_10'
    if row_count >= 6:
        ma5_today = np.mean(close[-5:])
        ma5_yesterday = np.mean(close[-6:-1])
        ma5_slope = (ma5_today - ma5_yesterday) / ma5_yesterday if ma5_yesterday > 0 else 0
        if ma5_slope <= 0:
            return 'ma5_slope'
        vol_ma5_today = np.mean(volume[-5:])
        vol_ma5_yesterday = np.mean(volume[-6:-1])
        vol_ma5_slope = (
            (vol_ma5_today - vol_ma5_yesterday) / vol_ma5_yesterday
            if vol_ma5_yesterday > 0 else 0
        )
        if vol_ma5_slope <= 0:
            return 'vol_ma5_slope'
    return None


def get_sb_candidate_pool(context, initial_list, date_str):
    """
    【首板-候选池构建】先技术面过滤，后财务面过滤
    """
    _strategy_log.debug("首板候选池 | 开始K线与财务验证")
    pool_started = pytime.perf_counter()
    pool_perf = {
        '10日日线': 0.0, '估值': 0.0, '技术财务初筛': 0.0,
        '60日成交额': 0.0, '流动性复核': 0.0,
    }
    candidates = []
    pre_liquidity_candidates = []
    rejected = {
        'history_hl': 0,
        'market_cap': 0,        # 流通市值不符
        'market_cap_total': 0,  # 总市值<50亿
        'pe_invalid': 0,        # PE<=0
        'avg_money_60d_low': 0, # 60日日均成交额<1亿
        'data_insufficient_60d': 0, # 60日数据不足
        'price_change': 0,
        'amplitude': 0,
        'volume_ratio': 0,
        'ma_fail': 0,
        'data_error': 0,
        'volume_ratio_5d': 0,
        'ma20_slope': 0,
        'vol_ma5_10': 0,
        'net_buy': 0,
        'ma5_slope': 0,
        'vol_ma5_slope': 0
    }

    history_fields = ['close', 'open', 'high', 'low', 'volume', 'money', 'high_limit']
    stage_started = pytime.perf_counter()
    history_frames = _batch_daily_frames(
        initial_list, date_str, history_fields, 10, skip_paused=True
    )
    pool_perf['10日日线'] = pytime.perf_counter() - stage_started

    valuation_fields = ['market_cap', 'circulating_market_cap', 'pe_ratio', 'turnover_ratio']
    stage_started = pytime.perf_counter()
    valuation_frames = _batch_valuation_frames(
        initial_list, context.previous_date, valuation_fields
    )
    pool_perf['估值'] = pytime.perf_counter() - stage_started

    fast_enabled = True
    fast_self_checks = 0
    fast_fallbacks = 0
    stage_started = pytime.perf_counter()
    for stock in initial_list:
        try:
            stock = str(stock)

            hist = history_frames.get(stock)
            if hist is None:
                hist = _cached_daily_frame(
                    stock, date_str, history_fields, 10, skip_paused=True
                )
            if hist is None or hist.empty:
                rejected['history_hl'] += 1
                continue

            if fast_enabled:
                try:
                    screen_reason = _sb_screen_reason_fast(hist)
                    if fast_self_checks < 3:
                        reference_reason = _sb_screen_reason_reference(hist)
                        fast_self_checks += 1
                        if screen_reason != reference_reason:
                            fast_enabled = False
                            screen_reason = reference_reason
                            _strategy_log.warning(
                                '首板NumPy筛选自检不一致，已自动降级为原始pandas路径'
                            )
                except Exception:
                    fast_fallbacks += 1
                    screen_reason = _sb_screen_reason_reference(hist)
            else:
                screen_reason = _sb_screen_reason_reference(hist)
            if screen_reason is not None:
                rejected[screen_reason] += 1
                continue

            try:
                valuation = valuation_frames.get(stock)
                if valuation is None:
                    _perf_increment('valuation_single_fallbacks')
                    valuation = get_valuation(
                        stock, start_date=context.previous_date,
                        end_date=context.previous_date, fields=valuation_fields,
                    )
                if valuation is None or valuation.empty:
                    rejected['data_error'] += 1
                    continue

                market_cap = valuation['market_cap'].iloc[0]
                if market_cap < 50:
                    rejected['market_cap_total'] += 1
                    _strategy_log.debug(f"【首板-过滤】{stock} 总市值{market_cap:.1f}亿<50亿")
                    continue

                pe_ratio = valuation['pe_ratio'].iloc[0]
                if pe_ratio <= 0:
                    rejected['pe_invalid'] += 1
                    _strategy_log.debug(f"【首板-过滤】{stock} PE={pe_ratio:.1f}（要求PE>0）")
                    continue
            except Exception as e:
                rejected['data_error'] += 1
                _strategy_log.debug(f"【首板-过滤】{stock} 估值数据获取失败: {str(e)}")
                continue

            pre_liquidity_candidates.append((stock, hist, valuation))
        except Exception as e:
            rejected['data_error'] += 1
            _strategy_log.debug(f"首板候选池 | {stock} 处理异常={str(e)}")

    pool_perf['技术财务初筛'] = pytime.perf_counter() - stage_started

    stage_started = pytime.perf_counter()
    liquidity_stocks = [item[0] for item in pre_liquidity_candidates]
    money_60d_frames = _batch_daily_frames(
        liquidity_stocks, date_str, ['money'], 60, skip_paused=True
    )
    pool_perf['60日成交额'] = pytime.perf_counter() - stage_started

    stage_started = pytime.perf_counter()
    for stock, hist, valuation in pre_liquidity_candidates:
        try:
            hist_60d = money_60d_frames.get(stock)
            if hist_60d is None:
                hist_60d = _cached_daily_frame(
                    stock, date_str, ['money'], 60, skip_paused=True
                )
            if hist_60d is None:
                rejected['data_error'] += 1
                continue
            if len(hist_60d) < 60:
                rejected['data_insufficient_60d'] += 1
                continue
            avg_money_60d = hist_60d['money'].mean()
            if avg_money_60d < 1e8:
                rejected['avg_money_60d_low'] += 1
                continue

            circ_cap = valuation['circulating_market_cap'].iloc[0]
            if circ_cap < 50 or circ_cap > 1500:
                rejected['market_cap'] += 1
                continue

            try:
                if len(hist) >= 5:
                    net_buy_estimate = sum(1 for i in range(-5, 0) if hist['close'].iloc[i] > hist['open'].iloc[i])
                    if net_buy_estimate < 1:
                        rejected['net_buy'] += 1
            except:
                pass

            candidates.append(stock)
            _store_daily_frame(stock, date_str, history_fields, 10, True, hist)
        except Exception as e:
            rejected['data_error'] += 1
            _strategy_log.debug(f"首板候选池 | {stock} 流动性复核失败={str(e)}")

    pool_perf['流动性复核'] = pytime.perf_counter() - stage_started

    _strategy_log.info(f"【首板-过滤统计】近10日涨停:{rejected['history_hl']} "
             f"流通市值:{rejected['market_cap']} 总市值<50亿:{rejected['market_cap_total']} "
             f"PE<=0:{rejected['pe_invalid']} 60日成交<1亿:{rejected['avg_money_60d_low']} "
             f"60日数据不足:{rejected['data_insufficient_60d']}")
    _strategy_log.info(f"【首板-过滤统计续】涨跌幅:{rejected['price_change']} "
             f"振幅:{rejected['amplitude']} 量比:{rejected['volume_ratio']} "
             f"均线:{rejected['ma_fail']} 4日量比:{rejected['volume_ratio_5d']} "
             f"MA20斜率:{rejected['ma20_slope']} 均量线:{rejected['vol_ma5_10']} "
             f"MA5斜率:{rejected['ma5_slope']} VOL_MA5斜率:{rejected['vol_ma5_slope']} "
             f"净买入:{rejected['net_buy']} 数据异常:{rejected['data_error']}")
    _strategy_log.info(f"【首板-候选池结果】共 {len(candidates)} 只股票进入候选池")
    pool_total = pytime.perf_counter() - pool_started
    _strategy_log.event(
        '性能统计', '首板基础池分段',
        ' | '.join([
            f"10日日线={pool_perf['10日日线']:.2f}秒",
            f"估值={pool_perf['估值']:.2f}秒",
            f"技术财务初筛={pool_perf['技术财务初筛']:.2f}秒",
            f"60日成交额={pool_perf['60日成交额']:.2f}秒",
            f"流动性复核={pool_perf['流动性复核']:.2f}秒",
            f"其他={builtins.max(0.0, pool_total - builtins.sum(pool_perf.values())):.2f}秒",
            f"总计={pool_total:.2f}秒",
            f"技术路径={'NumPy' if fast_enabled else 'pandas降级'}",
            f"自检={fast_self_checks}只",
            f"逐股回退={fast_fallbacks}只",
        ]),
        icon='⏱️',
    )
    return [str(s) for s in candidates]

def prepare_sb_stock_list(context, initial_list, date_str):
    """盘前完成首板历史、估值、流动性筛选和静态评分。"""
    _strategy_log.debug("首板模块 | 开始盘前静态评分")
    sb_started = pytime.perf_counter()
    stage_started = pytime.perf_counter()
    candidates = get_sb_candidate_pool(context, initial_list, date_str)
    pool_seconds = pytime.perf_counter() - stage_started
    if not candidates:
        _strategy_log.warning("【首板静态预选】候选池为空")
        return {}

    scores = {}
    stage_started = pytime.perf_counter()
    for stock in candidates:
        stock = str(stock)
        try:
            hist = _cached_daily_frame(
                stock, date_str, ['close', 'open', 'high', 'low', 'volume'],
                5, skip_paused=True,
            )
            if hist is None or len(hist) < 5:
                continue

            last = hist.iloc[-1]
            prev = hist.iloc[-2]
            chg_pct = (last['close'] - prev['close']) / prev['close'] * 100
            if 2 <= chg_pct <= 7:
                score_chg = 30 - abs(chg_pct - 4.5) * 2
            else:
                score_chg = max(0, 15 - abs(chg_pct - 4.5) * 3)

            vol_ma5 = hist['volume'].iloc[-5:].mean()
            vol_ratio = last['volume'] / vol_ma5 if vol_ma5 > 0 else 0
            if 1.5 <= vol_ratio <= 3.0:
                score_vol = 25 - abs(vol_ratio - 2.25) * 4
            else:
                score_vol = max(0, 15 - abs(vol_ratio - 2.25) * 3)

            if last['high'] > last['low']:
                body_ratio = (last['close'] - last['low']) / (last['high'] - last['low'])
                score_body = body_ratio * 20
            else:
                score_body = 10

            ma5 = hist['close'].iloc[-5:].mean()
            ma5_prev = hist['close'].iloc[-6:-1].mean() if len(hist) >= 6 else ma5
            ma5_slope = (ma5 - ma5_prev) / ma5_prev if ma5_prev > 0 else 0
            dist_ma5 = abs(last['close'] / ma5 - 1) * 100
            score_ma = max(0, 20 - dist_ma5 * 4) + max(0, min(5, ma5_slope * 100))
            total_score = score_chg + score_vol + score_body + score_ma

            scores[stock] = {
                'score': round(total_score, 1),
                'chg_pct': round(chg_pct, 2),
                'vol_ratio': round(vol_ratio, 2),
                'detail': (
                    f'涨幅{score_chg:.0f}+量比{score_vol:.0f}+'
                    f'实体{score_body:.0f}+均线{score_ma:.0f}'
                ),
            }
        except Exception:
            scores[stock] = {
                'score': 50, 'chg_pct': 0, 'vol_ratio': 0, 'detail': '默认50分'
            }

    score_seconds = pytime.perf_counter() - stage_started
    total_seconds = pytime.perf_counter() - sb_started
    _strategy_log.event(
        '性能统计', '首板静态预选',
        f'基础池={pool_seconds:.2f}秒 | 评分={score_seconds:.2f}秒 | '
        f'总计={total_seconds:.2f}秒 | 静态候选={len(scores)}只',
        icon='⏱️',
    )
    return scores


def finalize_sb_stock_list(context, static_scores, concept_pool):
    """竞价后仅做首板概念复核、排序和候选发布。"""
    started = pytime.perf_counter()
    scores = dict(static_scores or {})
    candidates = list(scores.keys())
    if concept_pool and candidates:
        before_count = len(candidates)
        candidates = filter_by_concept(candidates, concept_pool)
        _strategy_log.info(f"【首板-概念过滤】{before_count}只 → {len(candidates)}只")
    if not candidates:
        _strategy_log.warning("【首板选股】概念过滤后为空")
        return [], {}

    final_scores = {stock: scores[stock] for stock in candidates if stock in scores}
    sorted_stocks = sorted(
        final_scores.items(), key=lambda item: item[1]['score'], reverse=True
    )
    final_list = [
        item[0] for item in sorted_stocks[:min(g.sb_max_positions, len(sorted_stocks))]
    ]

    _strategy_log.info(
        f"【首板选股结果】监控池{len(final_list)}只（上限{g.sb_max_positions}），"
        f"买入上限{g.sb_buy_max_positions}只:"
    )
    for stock, data in sorted_stocks[:min(5, len(sorted_stocks))]:
        info = get_security_info(str(stock))
        name = info.display_name if info is not None else str(stock)
        _strategy_log.info(
            f"  {stock}({name}): 总分{data['score']:.0f}分 | "
            f"昨日涨幅{data['chg_pct']}% | 明细:{data['detail']}"
        )
    _strategy_log.event(
        '性能统计', '首板竞价确认',
        f'概念复核与排序={pytime.perf_counter() - started:.2f}秒 | '
        f'最终={len(final_list)}只',
        icon='⏱️',
    )
    return [str(stock) for stock in final_list], final_scores


def get_sb_stock_list(context, initial_list, date_str):
    """兼容入口：连续完成首板静态预选和最终复核。"""
    static_scores = prepare_sb_stock_list(context, initial_list, date_str)
    return finalize_sb_stock_list(context, static_scores, g.concept_pool)[0]

def buy_sb_mode(context):
    """【首板-买入初始化】三维系统+8%兜底双轨制"""
    current_time = context.current_dt.strftime('%H:%M:%S')
    if not g.sb_stock_list:
        _strategy_log.info("【买入-首板模式】股票池为空，跳过")
        return

    g.sb_pending_list = [str(s) for s in g.sb_stock_list]
    g.sb_bought_list = []
    g.sb_weibi_bought = []

    one_word_count = sum(1 for s in g.sb_stock_list if is_one_word_limit(str(s), context))
    tradable_count = len(g.sb_stock_list) - one_word_count

    sb_value_per = context.portfolio.total_value * g.sb_max_ratio / g.sb_buy_max_positions

    _strategy_log.info(f"========== 首板买入初始化（{current_time}）【三维系统版】==========")
    _strategy_log.info(f"【首板-策略升级】三维确认系统（突破均价线+日内强度+攻击波形）")
    _strategy_log.info(f"【首板-双轨制】2%-8%三维评分>=60买入 + 8%涨幅突破均价线兜底买入")
    _strategy_log.info(f"【首板-开盘状态】监控池{len(g.sb_stock_list)}只(上限{g.sb_max_positions}) | "
             f"可交易{tradable_count}只 | 一字板{one_word_count}只")
    _strategy_log.info(f"【首板-买入配置】最多买入{g.sb_buy_max_positions}只 | "
             f"单只金额{sb_value_per:.0f}元 | 总仓位限制30%")

def check_sb_weibi_signal(context):
    time_now = context.current_dt
    hour = time_now.hour
    minute = time_now.minute

    # 监控时段：早盘+下午至14:30
    is_morning = (hour == 9 and minute >= 25) or (hour == 10) or (hour == 11 and minute <= 30)
    is_afternoon = (hour == 13) or (hour == 14 and minute <= 30)
    if not (is_morning or is_afternoon):
        return

    if minute == g.last_sb_weibi_minute:
        return
    g.last_sb_weibi_minute = minute

    if not hasattr(g, 'sb_pending_list') or not g.sb_pending_list:
        return

    current_data = get_current_data()

    # 统计当前首板持仓
    current_sb_count = sum(1 for s in context.portfolio.positions.keys()
                          if g.position_modes.get(str(s)) == 'sb')
    available_slots = g.sb_buy_max_positions - current_sb_count

    if available_slots <= 0:
        limit_signature = (
            context.current_dt.strftime('%Y-%m-%d'),
            current_sb_count,
            g.sb_buy_max_positions,
        )
        if g.sb_limit_log_signature != limit_signature:
            g.sb_limit_log_signature = limit_signature
            _strategy_log.info(
                f"【首板-已达上限】当前持仓{current_sb_count}/{g.sb_buy_max_positions}只，停止买入"
            )
        return
    g.sb_limit_log_signature = None

    sb_value_per = context.portfolio.total_value * g.sb_max_ratio / g.sb_buy_max_positions

    # 过滤监控池（排除已持仓、已买入、失败列表）
    monitor_list = [s for s in g.sb_pending_list
                    if str(s) not in context.portfolio.positions
                    and str(s) not in getattr(g, 'sb_bought_list', [])
                    and str(s) not in getattr(g, 'sb_weibi_bought', [])
                    and str(s) not in [str(x) for x in g.failed_buy_list]]
    if not monitor_list:
        return

    _strategy_log.debug(f"【首板-三维监控-{time_now.strftime('%H:%M:%S')}】"
                        f"监控{len(monitor_list)}只 | 槽位{available_slots}/{g.sb_buy_max_positions}")

    bought_this_round = 0

    for stock in monitor_list:
        try:
            stock = str(stock)

            # 排除一字板
            if is_one_word_limit(stock, context):
                continue

            if current_sb_count >= g.sb_buy_max_positions:
                break

            current_price = current_data[stock].last_price
            day_open = current_data[stock].day_open
            rise_pct = (current_price / day_open - 1) * 100 if day_open > 0 else 0

            # 【核心】调用三维系统
            should_buy, score_3d, signals = check_3d_buy_signal(stock, context)

            # 评分明细保留在DEBUG；INFO只记录实际决策或明确跳过原因。
            if should_buy or (minute % 5 == 0 and stock == monitor_list[0]):
                _strategy_log.debug(f"【首板-3D评分】{stock}({current_data[stock].name}) | "
                        f"总分:{score_3d} | 涨幅:{rise_pct:.1f}% | "
                        f"突破:{signals.get('break_avg')}({signals.get('vol_ratio')}倍) | "
                        f"强度:{signals.get('strength')}% | "
                        f"攻击:{signals.get('attack')}分 | "
                        f"触发:{signals.get('trigger', '未触发')}")

            if should_buy:
                # 计算买入数量
                amount = int(sb_value_per / current_price / 100) * 100
                actual_value = amount * current_price
                available_cash = context.portfolio.available_cash

                if amount <= 0:
                    skip_key = f'{context.current_dt.date()}|lot_budget|{stock}'
                    if skip_key not in g.buy_skip_log_keys:
                        g.buy_skip_log_keys[skip_key] = True
                        _strategy_log.event(
                            '交易跳过', '首板单只预算不足购买一手',
                            f'{stock} | 单只预算={sb_value_per:.0f}元 | '
                            f'当前价={current_price:.2f}元 | 一手需={current_price * 100:.0f}元 | '
                            f'3D得分={score_3d}',
                            icon='ℹ️',
                        )
                    continue
                if available_cash < actual_value:
                    skip_key = f'{context.current_dt.date()}|cash|{stock}'
                    if skip_key not in g.buy_skip_log_keys:
                        g.buy_skip_log_keys[skip_key] = True
                        _strategy_log.event(
                            '交易跳过', '首板可用现金不足',
                            f'{stock} | 订单金额={actual_value:.0f}元 | '
                            f'可用现金={available_cash:.0f}元',
                            icon='ℹ️',
                        )
                    continue

                # 执行买入
                _strategy_log.info(f"【首板-三维买入】{stock}({current_data[stock].name}) | "
                        f"价格:{current_price:.2f} | 涨幅:{rise_pct:.1f}% | "
                        f"3D得分:{score_3d} | 金额:{actual_value:.0f}元")

                order_result = _order_target_value_logged(stock, sb_value_per)
                if not order_result:
                    continue
                g.position_modes[stock] = 'sb'
                g.today_bought[stock] = current_price
                g.sb_bought_list.append(stock)
                g.sb_weibi_bought.append(stock)
                current_sb_count += 1
                bought_this_round += 1

                record_sb_buy(context, stock, current_price, amount, actual_value)

                if current_sb_count >= g.sb_buy_max_positions:
                    _strategy_log.info(f"【首板-买入完成】已达上限{g.sb_buy_max_positions}只，停止监控")
                    break

        except Exception as e:
            _strategy_log.error(f"【首板-三维错误】{stock}: {str(e)}")

    if bought_this_round > 0:
        _strategy_log.info(f"【首板-本轮买入】共{bought_this_round}只，当前持仓{current_sb_count}/{g.sb_buy_max_positions}只")

def check_sb_compatibility_buy(context):
    """
    【首板-8%兜底买入】保留原策略，但增加三维确认（防诱多）
    逻辑：涨幅>8% + 突破均价线确认（极速模式，不等待完整三维评分）
    """
    time_now = context.current_dt
    hour = time_now.hour
    minute = time_now.minute

    # 监控时段与三维监控一致
    is_morning = (hour == 9 and minute >= 25) or (hour == 10) or (hour == 11 and minute <= 30)
    is_afternoon = (hour == 13) or (hour == 14 and minute <= 30)
    if not (is_morning or is_afternoon):
        return

    if not hasattr(g, 'sb_pending_list') or not g.sb_pending_list:
        return

    current_data = get_current_data()
    sb_value_per = context.portfolio.total_value * g.sb_max_ratio / g.sb_buy_max_positions

    # 统计持仓
    current_sb_count = sum(1 for s in context.portfolio.positions.keys()
                          if g.position_modes.get(str(s)) == 'sb')
    available_slots = g.sb_buy_max_positions - current_sb_count

    if available_slots <= 0:
        return

    # 过滤列表（与三维监控互斥，避免重复买入）
    compat_list = [str(s) for s in g.sb_pending_list
                   if str(s) not in context.portfolio.positions
                   and str(s) not in getattr(g, 'sb_bought_list', [])  # 已被三维买入的不再兜底
                   and str(s) not in getattr(g, 'sb_weibi_bought', [])
                   and str(s) not in [str(x) for x in g.failed_buy_list]]
    if not compat_list:
        return

    current_time = context.current_dt.strftime('%H:%M:%S')

    for stock in compat_list:
        try:
            stock = str(stock)
            if is_one_word_limit(stock, context):
                continue

            if current_sb_count >= g.sb_buy_max_positions:
                return

            current_price = current_data[stock].last_price
            day_open = current_data[stock].day_open
            rise_pct = (current_price / day_open - 1) * 100 if day_open > 0 else 0

            # 【8%兜底条件】涨幅>8% 且 未超过9.5%（避免打板失败）
            if not (8.0 < rise_pct < 9.5):
                continue

            # 【三维确认】仅检查突破均价线（极速确认，避免诱多）
            break_avg, vol_ratio = check_break_avg_line(stock, context, lookback_minutes=3)

            if not break_avg:
                _strategy_log.info(f"【首板-8%兜底跳过】{stock} 涨幅{rise_pct:.1f}%但未突破均价线（防诱多）")
                continue

            amount = int(sb_value_per / current_price / 100) * 100
            actual_value = amount * current_price

            if context.portfolio.available_cash < actual_value or amount <= 0:
                continue

            _strategy_log.info(f"【首板-8%兜底买入】{current_time} | {stock}({current_data[stock].name}) | "
                    f"涨幅:{rise_pct:.2f}% | 突破均价线确认({vol_ratio:.1f}倍) | "
                    f"金额:{actual_value:.0f}元 | 【极速模式】")

            order_result = _order_target_value_logged(stock, sb_value_per)
            if not order_result:
                continue
            g.position_modes[stock] = 'sb'
            g.today_bought[stock] = current_price
            g.sb_bought_list.append(stock)
            g.sb_weibi_bought.append(stock)  # 标记已买入，避免三维重复买
            current_sb_count += 1

            record_sb_buy(context, stock, current_price, amount, actual_value)

            # 立即检查是否达到上限
            if current_sb_count >= g.sb_buy_max_positions:
                _strategy_log.info(f"【首板-兜底完成】已达上限，停止兜底买入")
                return

        except Exception as e:
            _strategy_log.error(f"【首板-兜底错误】{stock}: {str(e)}")

def record_sb_buy(context, stock, price, amount, value):
    """【首板-买入记录】"""
    stock = str(stock)
    commission = max(value * 0.0003, 5)
    total_cost = value + commission
    record = {
        'type': 'buy',
        'date': context.current_dt.date(),
        'stock': stock,
        'name': get_security_info(stock).display_name,
        'price': round(price, 2),
        'amount': amount,
        'value': round(value, 2),
        'commission': round(commission, 2),
        'total_cost': round(total_cost, 2),
        'mode': 'sb',
        'strategy': '首板独立30%仓位'
    }
    g.sb_trade_log.append(record)
    if len(g.sb_trade_log) > 1000:
        del g.sb_trade_log[:-1000]
    _strategy_log.info(f"【首板-买入记录】{stock}({record['name']}) | "
             f"价格:{record['price']} 数量:{record['amount']}股 | "
             f"市值:{record['value']}元")

def record_sb_sell(context, stock, reason, sell_price):
    """【首板-卖出记录与执行】"""
    stock = str(stock)
    position = context.portfolio.positions.get(stock)
    if not position or position.total_amount == 0:
        return False
    if not _can_close_position(context, stock, reason):
        return False

    buy_record = None
    for log_entry in reversed(g.sb_trade_log):
        if log_entry['stock'] == stock and log_entry['type'] == 'buy':
            buy_record = log_entry
            break

    if not buy_record:
        result = _close_position_logged(context, stock, reason, check=False)
        if result:
            g.position_modes.pop(stock, None)
            g.sell_conditions.pop(stock, None)
        return bool(result)

    sell_value = sell_price * position.total_amount
    commission = max(sell_value * 0.0003, 5)
    tax = sell_value * 0.001
    total_fee = commission + tax
    net_proceeds = sell_value - total_fee
    cost_value = buy_record['total_cost']
    net_profit = net_proceeds - cost_value
    profit_pct = (net_profit / cost_value) * 100 if cost_value else 0

    hold_time = "当日"
    if buy_record['date'] != context.current_dt.date():
        hold_days = (context.current_dt.date() - buy_record['date']).days
        hold_time = f"{hold_days}日"

    record = {
        'type': 'sell',
        'date': context.current_dt.date(),
        'stock': stock,
        'name': get_security_info(stock).display_name,
        'sell_price': round(sell_price, 2),
        'amount': position.total_amount,
        'sell_value': round(sell_value, 2),
        'commission': round(commission, 2),
        'tax': round(tax, 2),
        'net_profit': round(net_profit, 2),
        'profit_pct': round(profit_pct, 2),
        'reason': reason,
        'mode': 'sb',
        'hold_time': hold_time
    }
    result = _close_position_logged(context, stock, reason, check=False)
    if not result:
        return False

    g.sb_trade_log.append(record)
    if len(g.sb_trade_log) > 1000:
        del g.sb_trade_log[:-1000]
    _strategy_log.info(f"【首板-卖出记录】{stock}({record['name']}) | 原因:{reason} | 盈亏:{profit_pct:.2f}%")
    g.position_modes.pop(stock, None)
    g.sell_conditions.pop(stock, None)
    return True

def sell_sb_realtime(context):
    """
    【首板-盘中卖出监控】修改为：尾盘未涨停 或 跌破-3%
    """
    current_data = get_current_data()
    current_time = context.current_dt
    hour = current_time.hour
    minute = current_time.minute

    for stock in list(context.portfolio.positions.keys()):
        if g.position_modes.get(str(stock)) != 'sb':
            continue
        position = context.portfolio.positions.get(str(stock))
        if not position or position.total_amount == 0:
            continue

        stock = str(stock)
        curr_price = current_data[stock].last_price
        high_limit = current_data[stock].high_limit
        cost = position.avg_cost
        profit_pct = (curr_price - cost) / cost * 100 if cost > 0 else 0

        if profit_pct <= -3.0:
            _strategy_log.info(f"【首板止损-跌破-3%】{stock} 盈亏:{profit_pct:.2f}%")
            record_sb_sell(context, stock, '盘中止损:跌破-3%', curr_price)
            continue

        if curr_price >= high_limit * 0.998:
            if stock not in g.sb_sell_conditions:
                g.sb_sell_conditions[stock] = {}
            g.sb_sell_conditions[stock]['was_limit_up'] = True
            if stock not in g.sell_conditions:
                g.sell_conditions[stock] = {}
            g.sell_conditions[stock]['is_limit_up'] = True
            _strategy_log.info(f"【首板封板确认】{stock} 成功封板，继续持有")

def print_sb_trade_summary(context):
    """【首板-交易汇总日志】"""
    if not g.sb_trade_log:
        _strategy_log.info("【首板-交易汇总】今日无首板交易记录")
        return

    _strategy_log.info("========== 首板交易汇总（独立30%仓位策略）==========")
    buy_records = [x for x in g.sb_trade_log if x['type'] == 'buy']
    sell_records = [x for x in g.sb_trade_log if x['type'] == 'sell']

    _strategy_log.info(f"总买入次数: {len(buy_records)} 次 | 总卖出次数: {len(sell_records)} 次")

    if sell_records:
        profit_count = sum(1 for x in sell_records if x['net_profit'] > 0)
        win_rate = profit_count / len(sell_records) * 100
        total_profit = sum(x['net_profit'] for x in sell_records)
        avg_profit = total_profit / len(sell_records)

        _strategy_log.info(f"胜率: {profit_count}/{len(sell_records)} = {win_rate:.1f}%")
        _strategy_log.info(f"总净盈亏: {total_profit:.2f}元")

    current_sb = [s for s in context.portfolio.positions.keys()
                  if g.position_modes.get(str(s)) == 'sb']
    _strategy_log.info(f"当前首板持仓: {len(current_sb)}/{g.sb_max_positions} 只")

# ============================================================================
# 第五部分：六维微观结构评估系统（趋势股专用）
# ============================================================================

def calculate_micro_structure(df):
    if df is None or len(df) < 20:
        return None
    df = df.copy()
    df['returns_1d'] = df['close'].pct_change()
    df['returns_5d'] = df['close'].pct_change(5)

    for period in [5, 10, 20, 60]:
        df[f'ma{period}'] = df['close'].rolling(window=period).mean()
        df[f'ma{period}_slope'] = (df[f'ma{period}'] - df[f'ma{period}'].shift(5)) / df[f'ma{period}'].shift(5)
        df[f'ma{period}_accel'] = df[f'ma{period}_slope'].diff()

    df['ma_spread_ratio'] = (abs(df['ma5'] - df['ma10']) / (df['ma10'] + 0.0001)) / \
                            (abs(df['ma10'] - df['ma20']) / (df['ma20'] + 0.0001) + 0.0001)

    ma_values = df[['ma5', 'ma10', 'ma20', 'ma60']]
    df['ma_convergence'] = ma_values.std(axis=1) / (ma_values.mean(axis=1) + 0.0001)

    deviation = (df['close'] - df['ma20']) / (df['ma20'] + 0.0001)
    df['price_ma_angle'] = np.arctan(deviation * 20) * 180 / np.pi

    df['volume_ma5'] = df['volume'].rolling(window=5).mean()
    df['volume_ma20'] = df['volume'].rolling(window=20).mean()
    df['volume_ratio'] = df['volume'] / (df['volume_ma5'] + 0.0001)
    df['volume_min_20'] = df['volume'].rolling(20).min()
    df['volume_max_20'] = df['volume'].rolling(20).max()

    df['volume_increasing'] = ((df['volume'] > df['volume'].shift(1)) &
                               (df['volume'].shift(1) > df['volume'].shift(2))).astype(int)
    df['volume_stack_days'] = df['volume_increasing'].rolling(5).sum()
    df['volume_shrink'] = df['volume'] < df['volume_ma20'] * 0.6
    df['volume_pulse'] = (df['volume_ratio'] > 3.0) & (df['volume_ratio'].shift(-1) < 1.5)

    df['volume_ma_golden'] = (df['volume_ma5'] > df['volume_ma5'].shift(1)) & \
                             (df['volume_ma5'] > df['volume_ma20']) & \
                             (df['volume_ma5'].shift(1) <= df['volume_ma20'].shift(1))
    df['volume_ratio_change'] = df['volume_ratio'].pct_change()
    df['volume_ratio_ma3'] = df['volume_ratio'].rolling(3).mean()
    df['volume_ratio_trend'] = df['volume_ratio_ma3'].diff()

    body = abs(df['close'] - df['open'])
    range_hl = df['high'] - df['low']
    df['body_ratio'] = body / (range_hl + 0.0001)

    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    df['upper_shadow_ratio'] = upper_shadow / (range_hl + 0.0001)
    df['lower_shadow_ratio'] = lower_shadow / (range_hl + 0.0001)

    df['hammer'] = (df['lower_shadow_ratio'] > 0.5) & (df['body_ratio'] < 0.3) & \
                   (df['upper_shadow_ratio'] < 0.1)
    df['engulfing'] = (df['close'] > df['open']) & \
                      (df['close'].shift(1) < df['open'].shift(1)) & \
                      (df['close'] > df['open'].shift(1)) & \
                      (df['open'] < df['close'].shift(1))
    df['close_position'] = (df['close'] - df['low']) / (range_hl + 0.0001)
    df['open_position'] = (df['open'] - df['low']) / (range_hl + 0.0001)
    df['low_open_high_close'] = (df['open_position'] < 0.3) & (df['close_position'] > 0.6)

    price_low = df['close'] == df['close'].rolling(20).min()
    volume_not_low = df['volume'] > df['volume'].rolling(20).min() * 1.2
    df['divergence_bottom'] = price_low & volume_not_low
    df['price_volume_healthy'] = (df['close'] > df['close'].shift(1)) & \
                                  (df['volume'] > df['volume'].shift(1))

    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 0.0001)
    df['rsi_14'] = 100 - (100 / (1 + rs))

    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_dif'] = exp1 - exp2
    df['macd_dea'] = df['macd_dif'].ewm(span=9, adjust=False).mean()
    df['macd'] = 2 * (df['macd_dif'] - df['macd_dea'])

    return df


def calculate_micro_structures(history_frames):
    """按有效K线序号批量计算趋势指标，公式与逐股版本保持一致。"""
    valid_frames = {
        str(stock): frame for stock, frame in history_frames.items()
        if frame is not None and len(frame) >= 20
    }
    if not valid_frames:
        return {}

    stocks = list(valid_frames.keys())
    max_count = builtins.max(len(frame) for frame in valid_frames.values())
    wide_index = pd.RangeIndex(max_count)

    def _wide(field):
        return pd.DataFrame({
            stock: pd.Series(
                valid_frames[stock][field].values,
                index=range(max_count - len(valid_frames[stock]), max_count),
            )
            for stock in stocks
        }, index=wide_index, dtype=float)

    open_wide = _wide('open')
    high_wide = _wide('high')
    low_wide = _wide('low')
    close = _wide('close')
    volume = _wide('volume')

    ma5 = close.rolling(window=5).mean()
    ma10 = close.rolling(window=10).mean()
    ma20 = close.rolling(window=20).mean()
    ma60 = close.rolling(window=60).mean()
    ma5_slope = (ma5 - ma5.shift(5)) / ma5.shift(5)
    ma10_slope = (ma10 - ma10.shift(5)) / ma10.shift(5)
    ma20_slope = (ma20 - ma20.shift(5)) / ma20.shift(5)

    ma_spread_ratio = (abs(ma5 - ma10) / (ma10 + 0.0001)) / \
                      (abs(ma10 - ma20) / (ma20 + 0.0001) + 0.0001)
    ma_mean = (ma5 + ma10 + ma20 + ma60) / 4.0
    ma_std = np.sqrt(
        ((ma5 - ma_mean) ** 2 + (ma10 - ma_mean) ** 2 +
         (ma20 - ma_mean) ** 2 + (ma60 - ma_mean) ** 2) / 3.0
    )
    deviation = (close - ma20) / (ma20 + 0.0001)

    volume_ma5 = volume.rolling(window=5).mean()
    volume_ma20 = volume.rolling(window=20).mean()
    volume_ratio = volume / (volume_ma5 + 0.0001)
    volume_min_20 = volume.rolling(20).min()
    volume_increasing = ((volume > volume.shift(1)) &
                         (volume.shift(1) > volume.shift(2))).astype(int)

    body = abs(close - open_wide)
    range_hl = high_wide - low_wide
    body_ratio = body / (range_hl + 0.0001)
    upper_shadow = high_wide - np.maximum(close, open_wide)
    lower_shadow = np.minimum(close, open_wide) - low_wide
    upper_shadow_ratio = upper_shadow / (range_hl + 0.0001)
    lower_shadow_ratio = lower_shadow / (range_hl + 0.0001)
    close_position = (close - low_wide) / (range_hl + 0.0001)
    open_position = (open_wide - low_wide) / (range_hl + 0.0001)

    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 0.0001)
    rsi_14 = 100 - (100 / (1 + rs))
    macd_dif = (close.ewm(span=12, adjust=False).mean() -
                close.ewm(span=26, adjust=False).mean())
    macd_dea = macd_dif.ewm(span=9, adjust=False).mean()

    indicators = {
        'returns_1d': close.pct_change(),
        'returns_5d': close.pct_change(5),
        'ma5': ma5,
        'ma10': ma10,
        'ma20': ma20,
        'ma60': ma60,
        'ma5_slope': ma5_slope,
        'ma10_slope': ma10_slope,
        'ma20_slope': ma20_slope,
        'ma_spread_ratio': ma_spread_ratio,
        'ma_convergence': ma_std / (ma_mean + 0.0001),
        'price_ma_angle': np.arctan(deviation * 20) * 180 / np.pi,
        'volume_ma5': volume_ma5,
        'volume_ma20': volume_ma20,
        'volume_ratio': volume_ratio,
        'volume_min_20': volume_min_20,
        'volume_stack_days': volume_increasing.rolling(5).sum(),
        'volume_pulse': ((volume_ratio > 3.0) & (volume_ratio.shift(-1) < 1.5)),
        'body_ratio': body_ratio,
        'upper_shadow_ratio': upper_shadow_ratio,
        'lower_shadow_ratio': lower_shadow_ratio,
        'hammer': ((lower_shadow_ratio > 0.5) & (body_ratio < 0.3) &
                   (upper_shadow_ratio < 0.1)),
        'engulfing': ((close > open_wide) &
                      (close.shift(1) < open_wide.shift(1)) &
                      (close > open_wide.shift(1)) &
                      (open_wide < close.shift(1))),
        'close_position': close_position,
        'low_open_high_close': ((open_position < 0.3) & (close_position > 0.6)),
        'divergence_bottom': ((close == close.rolling(20).min()) &
                              (volume > volume_min_20 * 1.2)),
        'price_volume_healthy': ((close > close.shift(1)) &
                                 (volume > volume.shift(1))),
        'rsi_14': rsi_14,
        'macd': 2 * (macd_dif - macd_dea),
    }

    result = {}
    for stock in stocks:
        source_tail = valid_frames[stock].iloc[-10:].copy()
        derived_tail = pd.DataFrame(
            {name: values[stock].iloc[-10:].values
             for name, values in indicators.items()},
            index=source_tail.index,
        )
        result[stock] = pd.concat([source_tail, derived_tail], axis=1)
    return result


def _micro_structure_frames_match(reference, candidate):
    """抽样校验批量指标；任一字段不一致时由调用方自动逐股降级。"""
    if (reference is None or candidate is None or len(candidate) == 0 or
            len(reference) < len(candidate)):
        return False
    reference = reference.iloc[-len(candidate):]
    if not set(candidate.columns).issubset(reference.columns):
        return False
    bool_fields = {
        'volume_pulse', 'hammer', 'engulfing', 'low_open_high_close',
        'divergence_bottom', 'price_volume_healthy',
    }
    for field in candidate.columns:
        reference_values = reference[field].values
        candidate_values = candidate[field].values
        if field in bool_fields:
            if not np.array_equal(reference_values, candidate_values):
                return False
            continue
        try:
            if not np.allclose(
                reference_values.astype(float), candidate_values.astype(float),
                rtol=1e-12, atol=1e-12, equal_nan=True,
            ):
                return False
        except (TypeError, ValueError):
            if not np.array_equal(reference_values, candidate_values):
                return False
    return True

def identify_shakeout(df):
    if len(df) < 10:
        return False

    latest = df.iloc[-1]
    recent = df.iloc[-6:].copy()
    recent['ma5_above_ma20'] = (recent['ma5'] > recent['ma20']).astype(int)
    cross_count = abs(recent['ma5_above_ma20'].diff()).sum()
    frequent_cross = cross_count >= 2

    rsi_recent = df['rsi_14'].iloc[-5:]
    rsi_chaotic = (40 < latest['rsi_14'] < 60) and (rsi_recent.std() < 5)

    recent_close = df['close'].iloc[-10:]
    price_range = (recent_close.max() - recent_close.min()) / (latest['close'] + 0.0001) * 100
    price_sideways = price_range < 4

    is_shakeout = frequent_cross and rsi_chaotic and price_sideways
    if is_shakeout:
        _strategy_log.info(f"【震仓识别】均线交叉{cross_count}次, RSI混沌{rsi_chaotic}, 振幅{price_range:.2f}%")

    return is_shakeout

class ComboEvaluator:
    COMBO_CONFIG = {
        'combo_1': {'name': '吸筹期地量地价', 'priority': 4, 'target_adjust': -1},
        'combo_2': {'name': '洗盘期缩量回调', 'priority': 3, 'target_adjust': 0},
        'combo_3': {'name': '突破型放量启动', 'priority': 1, 'target_adjust': 2},
        'combo_4': {'name': '主升浪回踩低吸', 'priority': 2, 'target_adjust': 1},
        'combo_5': {'name': '极端超跌反弹', 'priority': 5, 'target_adjust': 1},
        'combo_6': {'name': '标准多头排列', 'priority': 6, 'target_adjust': 0}
    }

    @staticmethod
    def _safe_bool(val):
        try:
            if pd.isna(val):
                return False
            if hasattr(val, '__iter__') and not isinstance(val, (str, bytes)):
                if len(val) == 0:
                    return False
                val = val.iloc[0] if hasattr(val, 'iloc') else val[0]
            return bool(val)
        except:
            return False

    @staticmethod
    def _calculate_score(core_dict, edge_dict):
        try:
            core_bool_list = [bool(ComboEvaluator._safe_bool(v)) for v in core_dict.values()]
            edge_bool_list = [bool(ComboEvaluator._safe_bool(v)) for v in edge_dict.values()]

            core_met = sum(core_bool_list)
            edge_met = sum(edge_bool_list)

            core_count = len(core_bool_list)
            edge_count = len(edge_bool_list)
            if core_count == 0:
                return False, 0.0, 0, 0

            core_pass = (core_met == core_count)
            edge_ratio = edge_met / edge_count if edge_count > 0 else 0.0

            weighted_score = (core_met / core_count * 3 + edge_ratio * 1) / 4 * 100
            return core_pass, float(weighted_score), int(core_met), int(edge_met)
        except Exception as e:
            _strategy_log.error(f"【六维评估】评分计算错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_1(latest, prev):
        try:
            core = {
                'ma20_shake': abs(float(latest['close']) - float(latest['ma20'])) / float(latest['ma20']) < 0.03,
                'rsi_low': float(latest['rsi_14']) < 40,
                'volume_dry': (float(latest['volume']) < float(latest['volume_ma20']) * 0.6) or \
                               (abs(float(latest['volume']) - float(latest['volume_min_20'])) < 0.01)
            }
            edge = {
                'slope_up': float(latest['ma20_slope']) > float(prev['ma20_slope']) if not pd.isna(prev['ma20_slope']) else False,
                'convergence': float(latest['ma_convergence']) < 0.03,
                'hammer': bool(latest['hammer']),
                'divergence': bool(latest['divergence_bottom']),
                'close_pos': float(latest['close_position']) > 0.6,
                'pv_healthy': bool(latest['price_volume_healthy'])
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_1错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_2(latest, prev):
        try:
            deviation_ma10 = (float(latest['close']) - float(latest['ma10'])) / float(latest['ma10'])
            core = {
                'touch_ma10': (-0.05 < deviation_ma10 < -0.02),
                'above_ma20': float(latest['close']) > float(latest['ma20']),
                'rsi_neutral': 45 < float(latest['rsi_14']) < 60
            }
            edge = {
                'ma_bull': (float(latest['ma5']) > float(latest['ma20'])) and \
                           (float(latest['ma10']) > float(latest['ma20'])),
                'volume_shrink': float(latest['volume_ratio']) < 0.7,
                'long_shadow': float(latest['lower_shadow_ratio']) > float(latest['body_ratio']) * 1.5,
                'no_pulse': not bool(latest['volume_pulse']),
                'close_pos': float(latest['close_position']) > 0.5,
                'slope_diff': float(latest['ma5_slope']) > float(latest['ma10_slope'])
                if not pd.isna(latest['ma5_slope']) and not pd.isna(latest['ma10_slope']) else False
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_2错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_3(latest, prev):
        try:
            core = {
                'breakout': float(latest['returns_1d']) > 0.04,
                'volume_surge': float(latest['volume_ratio']) > 1.3,
                'ma_converge': float(latest['ma_convergence']) < 0.025,
                'price_angle': float(latest['price_ma_angle']) > 25
            }
            edge = {
                'ma_spread': float(latest['ma_spread_ratio']) > 1.8,
                'body_strong': float(latest['body_ratio']) > 0.5,
                'engulfing': bool(latest['engulfing']),
                'close_high': float(latest['close_position']) > 0.8,
                'vol_stack': int(latest['volume_stack_days']) >= 2,
                'no_pump': not bool(latest['volume_pulse'])
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_3错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_4(latest, prev):
        try:
            core = {
                'bull_arrange': (float(latest['close']) > float(latest['ma5']) >
                                 float(latest['ma10']) > float(latest['ma20'])),
                'touch_ma10': abs((float(latest['close']) - float(latest['ma10'])) /
                                  float(latest['ma10'])) < 0.03,
                'ma20_up': float(latest['ma20_slope']) > 0,
                'vol_shrink': float(latest['volume_ratio']) < 0.8
            }
            edge = {
                'lower_shadow': float(latest['lower_shadow_ratio']) > 0.35,
                'close_pos': float(latest['close_position']) > 0.55,
                'ma_spread': float(latest['ma_spread_ratio']) > 1.3,
                'above_ma20_safe': float(latest['close']) > float(latest['ma20']) * 1.02,
                'vol_healthy': float(latest['volume']) > float(latest['volume_ma20']) * 0.4,
                'slope_accelerate': float(latest['ma5_slope']) > -0.02
                if not pd.isna(latest['ma5_slope']) else False
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_4错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_5(latest, prev):
        try:
            core = {
                'deep_fall': (float(latest['returns_5d']) < -0.06) or \
                             (float(latest['rsi_14']) < 28),
                'volume_freeze': (abs(float(latest['volume']) - float(latest['volume_min_20'])) < 0.01) or \
                                 (float(latest['volume']) < float(latest['volume_ma20']) * 0.5),
                'far_ma60': (float(latest['close']) - float(latest['ma60'])) / float(latest['ma60']) < -0.06
            }
            edge = {
                'ma_converge': float(latest['ma_convergence']) < 0.04,
                'rebound': float(latest['returns_1d']) > 0,
                'hammer_engulf': bool(latest['hammer']) or \
                                 (bool(latest['engulfing']) and float(latest['returns_1d']) > 0.03),
                'divergence': bool(latest['divergence_bottom']),
                'low_open_high': bool(latest['low_open_high_close']),
                'ma_slope_recover': float(latest['ma20_slope']) > float(prev['ma20_slope'])
                if not pd.isna(prev['ma20_slope']) else False
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_5错误: {str(e)}")
            return False, 0.0, 0, 0

    @staticmethod
    def evaluate_combo_6(latest, prev):
        """
        【新增】标准多头排列评估（备用方案）
        """
        try:
            core = {
                'ma_bull': (float(latest['ma5']) > float(latest['ma10']) >
                           float(latest['ma20'])),
                'price_above_ma5': float(latest['close']) > float(latest['ma5']),
                'ma20_up': float(latest['ma20_slope']) > 0,
                'volume_healthy': float(latest['volume_ratio']) > 1.0
            }
            edge = {
                'close_pos': float(latest['close_position']) > 0.5,
                'body_strong': float(latest['body_ratio']) > 0.4,
                'no_long_shadow': float(latest['upper_shadow_ratio']) < 0.3,
                'macd_positive': float(latest['macd']) > 0,
                'volume_ma_up': float(latest['volume_ma5']) > float(latest['volume_ma20']),
                'rsi_healthy': 40 < float(latest['rsi_14']) < 70
            }
            core_pass, weighted_score, core_met, edge_met = ComboEvaluator._calculate_score(core, edge)
            return core_pass and weighted_score >= 90, weighted_score, core_met, edge_met
        except Exception as e:
            _strategy_log.error(f"【六维评估】combo_6错误: {str(e)}")
            return False, 0.0, 0, 0

# ============================================================================
# 第六部分：趋势股管理（独立模块，使用六维评估）
# ============================================================================
def calculate_atr_for_qs(stock, context):
    """
    【新增】计算趋势股ATR（14日周期）
    """
    try:
        stock = str(stock)
        date_key = _cache_date_key(context.previous_date)
        if _RUNTIME_DATA_CACHE['daily_date'] != date_key:
            _RUNTIME_DATA_CACHE['daily_date'] = date_key
            _RUNTIME_DATA_CACHE['daily_frames'] = {}
            _RUNTIME_DATA_CACHE['previous_close'] = {}
            _RUNTIME_DATA_CACHE['qs_atr'] = {}
        cache_key = (stock, int(g.qs_atr_period))
        if cache_key in _RUNTIME_DATA_CACHE['qs_atr']:
            return _RUNTIME_DATA_CACHE['qs_atr'][cache_key]
        # 获取15日数据（14日ATR需要15日数据计算）
        hist = attribute_history(stock, g.qs_atr_period + 1, '1d',
                                 ['high', 'low', 'close'], skip_paused=True)
        if len(hist) < g.qs_atr_period + 1:
            return None

        # 使用talib计算ATR
        atr = talib.ATR(hist['high'].values,
                       hist['low'].values,
                       hist['close'].values,
                       timeperiod=g.qs_atr_period)[-1]
        _RUNTIME_DATA_CACHE['qs_atr'][cache_key] = atr
        return atr
    except:
        return None
def prepare_qs_stock_list(context, date_str):
    """盘前计算趋势股日线指标、震仓信号和全部静态组合匹配。"""
    try:
        qs_started = pytime.perf_counter()
        qs_perf = {
            '候选准备': 0.0, '批量日线': 0.0, '指标计算': 0.0,
            '震仓识别': 0.0, '组合评估': 0.0, '概念复核': 0.0,
        }
        calculated_count = 0
        cached_count = 0
        indicator_mode = '批量向量'
        validation_count = 0
        _strategy_log.debug("趋势股模块 | 开始盘前六维静态评估（昨日数据）")

        candidate_started = pytime.perf_counter()
        initial_list = prepare_stock_list(date_str)

        # 获取中证1000成分股作为股票池（代码000852.XSHG）
        try:
            zz1000_stocks = get_index_stocks('000852.XSHG', date_str)
            zz1000_set = set([normalize_stock_code(s) for s in zz1000_stocks])
            initial_set = set([normalize_stock_code(s) for s in initial_list])
            candidates = list(initial_set & zz1000_set)
            _strategy_log.info(f"【趋势股-中证1000过滤】初始{len(initial_list)}只 → 中证1000交集{len(candidates)}只")
        except Exception as e:
            _strategy_log.error(f"【趋势股中证1000获取错误】{str(e)}，回退到初始列表")
            candidates = [normalize_stock_code(s) for s in initial_list]

        if not candidates:
            _strategy_log.warning("【趋势股选股】市值过滤后无候选股票")
            return []

        qs_perf['候选准备'] = pytime.perf_counter() - candidate_started
        _strategy_log.debug(
            f"趋势股基础池 | 保留={len(candidates)}只 | 概念过滤=组合匹配后复核"
        )

        if not hasattr(g, '_micro_data_cache'):
            g._micro_data_cache = {}
            g._cache_date = date_str
        if g._cache_date != date_str:
            g._micro_data_cache = {}
            g._cache_date = date_str

        static_candidates = []
        evaluator = ComboEvaluator()
        history_fields = ['open', 'high', 'low', 'close', 'volume', 'money']
        uncached_candidates = [
            str(stock) for stock in candidates
            if str(stock) not in g._micro_data_cache
        ]
        batch_started = pytime.perf_counter()
        history_frames = _batch_daily_frames(
            uncached_candidates, date_str, history_fields, 80, skip_paused=True
        )
        qs_perf['批量日线'] = pytime.perf_counter() - batch_started

        valid_history_frames = {}
        fallback_started = pytime.perf_counter()
        for stock in uncached_candidates:
            try:
                raw_frame = history_frames.get(stock)
                if raw_frame is None:
                    raw_frame = _cached_daily_frame(
                        stock, date_str, history_fields, 80, skip_paused=True
                    )
                if raw_frame is not None and len(raw_frame) >= 65:
                    valid_history_frames[stock] = raw_frame
            except Exception as history_error:
                _strategy_log.debug(
                    f"趋势股历史数据跳过 | {stock} | 原因={str(history_error)}"
                )
        qs_perf['批量日线'] += pytime.perf_counter() - fallback_started

        indicator_started = pytime.perf_counter()
        calculated_frames = {}
        try:
            calculated_frames = calculate_micro_structures(valid_history_frames)
            for validation_stock in list(valid_history_frames.keys())[:3]:
                reference_frame = calculate_micro_structure(
                    valid_history_frames[validation_stock]
                )
                if not _micro_structure_frames_match(
                        reference_frame, calculated_frames.get(validation_stock)):
                    raise ValueError(f'{validation_stock} 指标字段不一致')
                validation_count += 1
        except Exception as batch_error:
            indicator_mode = '逐股降级'
            _strategy_log.info(f"趋势股指标向量化降级 | 原因={str(batch_error)}")
            calculated_frames = {}
            for stock, frame in valid_history_frames.items():
                try:
                    calculated_frame = calculate_micro_structure(frame)
                    if calculated_frame is not None:
                        calculated_frames[stock] = calculated_frame
                except Exception as indicator_error:
                    _strategy_log.debug(
                        f"趋势股指标计算跳过 | {stock} | 原因={str(indicator_error)}"
                    )
        qs_perf['指标计算'] = pytime.perf_counter() - indicator_started
        calculated_frames = {
            stock: frame for stock, frame in calculated_frames.items()
            if frame is not None
        }
        calculated_count = len(calculated_frames)
        g._micro_data_cache.update(calculated_frames)

        for i, stock in enumerate(candidates):
            if i % 100 == 0 and i > 0:
                _strategy_log.debug(f"【趋势股-评估进度】已检查 {i}/{len(candidates)} 只…")

            try:
                stock = str(stock)
                if stock in g._micro_data_cache:
                    if stock not in calculated_frames:
                        cached_count += 1
                    df = g._micro_data_cache[stock]
                else:
                    continue

                latest = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else latest

                shakeout_started = pytime.perf_counter()
                is_shakeout = identify_shakeout(df)
                qs_perf['震仓识别'] += pytime.perf_counter() - shakeout_started
                if is_shakeout:
                    continue

                combo_started = pytime.perf_counter()
                combo_checks = [
                    ('combo_3', evaluator.evaluate_combo_3),
                    ('combo_4', evaluator.evaluate_combo_4),
                    ('combo_2', evaluator.evaluate_combo_2),
                    ('combo_1', evaluator.evaluate_combo_1),
                    ('combo_5', evaluator.evaluate_combo_5)
                ]

                combo_matches = []
                for combo_type, eval_func in combo_checks:
                    try:
                        passed, score, core_met, edge_met = eval_func(latest, prev)
                        if passed:
                            cfg = evaluator.COMBO_CONFIG[combo_type]
                            combo_matches.append({
                                'combo_type': combo_type,
                                'combo_name': cfg['name'],
                                'score': float(score),
                                'core_met': int(core_met),
                                'edge_met': int(edge_met),
                                'priority': cfg['priority'],
                                'target_adjust': cfg['target_adjust'],
                            })
                    except Exception:
                        continue

                fallback = None
                try:
                    ma5 = float(latest['ma5'])
                    ma10 = float(latest['ma10'])
                    ma20 = float(latest['ma20'])
                    vol_ratio = float(latest['volume_ratio'])
                    if (ma5 > ma10 > ma20) and (vol_ratio > 1.2):
                        fallback = {
                            'static_edge_met': int(builtins.sum([
                                float(latest['volume_ma5']) > float(latest['volume_ma20']),
                                float(latest['close']) > ma5,
                                float(latest['upper_shadow_ratio']) < 0.3,
                                float(latest['macd']) > 0,
                            ])),
                        }
                except Exception:
                    fallback = None

                if combo_matches or fallback is not None:
                    static_candidates.append({
                        'stock': stock,
                        'pre_close': float(latest['close']),
                        'combo_matches': combo_matches,
                        'fallback': fallback,
                    })
                qs_perf['组合评估'] += pytime.perf_counter() - combo_started
            except Exception:
                continue

        qs_total = pytime.perf_counter() - qs_started
        qs_known = builtins.sum(qs_perf.values())
        _strategy_log.event(
            '性能统计', '趋势股静态预选分段',
            ' | '.join([
                f"候选准备={qs_perf['候选准备']:.2f}秒",
                f"批量日线={qs_perf['批量日线']:.2f}秒",
                f"指标计算={qs_perf['指标计算']:.2f}秒",
                f"震仓识别={qs_perf['震仓识别']:.2f}秒",
                f"组合评估={qs_perf['组合评估']:.2f}秒",
                f"概念复核={qs_perf['概念复核']:.2f}秒",
                f"其他={builtins.max(0.0, qs_total - qs_known):.2f}秒",
                f"总计={qs_total:.2f}秒",
                f"指标模式={indicator_mode}",
                f"向量自检={validation_count}只",
                f"指标新算={calculated_count}只",
                f"指标缓存={cached_count}只",
                f"静态候选={len(static_candidates)}只",
            ]),
            icon='⏱️',
        )

        return static_candidates
    except Exception as e:
        _strategy_log.error(f"【趋势股静态预选重大错误】{str(e)}")
        return []


def finalize_qs_stock_list(context, static_candidates, concept_pool,
                           auction_snapshot=None):
    """使用最终开盘价和竞价概念确认趋势股候选。"""
    started = pytime.perf_counter()
    evaluator = ComboEvaluator()
    current_data = get_current_data()
    selected_stocks = []

    for item in static_candidates or []:
        stock = str(item['stock'])
        try:
            pre_close = float(item['pre_close'])
            auction_values = (auction_snapshot or {}).get(stock)
            if auction_values and auction_values.get('current') is not None:
                day_open = float(auction_values['current'])
            else:
                day_open = current_data[stock].day_open
            open_chg_pct = (day_open / pre_close - 1) * 100 if pre_close > 0 else 0
            selected = None

            for combo in item.get('combo_matches', []):
                combo_type = combo['combo_type']
                if combo_type == 'combo_3':
                    if not (3 <= open_chg_pct <= 7):
                        _strategy_log.debug(
                            f"【趋势股-Combo3开盘不符】{stock} 开盘{open_chg_pct:.2f}%"
                        )
                        continue
                elif open_chg_pct <= 1:
                    _strategy_log.debug(
                        f"【趋势股-开盘不符】{stock} 开盘{open_chg_pct:.2f}%"
                    )
                    continue
                selected = dict(combo)
                selected['stock'] = stock
                selected['open_chg'] = open_chg_pct
                break

            fallback = item.get('fallback')
            if selected is None and fallback is not None and open_chg_pct > 1.5:
                edge_met = int(fallback.get('static_edge_met', 0))
                edge_met += int(open_chg_pct < 5.0)
                if edge_met >= 4:
                    cfg = evaluator.COMBO_CONFIG['combo_6']
                    selected = {
                        'stock': stock,
                        'combo_type': 'combo_6',
                        'combo_name': cfg['name'],
                        'score': 90.0 + float(edge_met) * 2,
                        'core_met': 3,
                        'edge_met': edge_met,
                        'priority': cfg['priority'],
                        'target_adjust': cfg['target_adjust'],
                        'open_chg': open_chg_pct,
                    }

            if selected is not None:
                selected_stocks.append(selected)
        except Exception:
            continue

    if concept_pool and selected_stocks:
        matched_codes = [item['stock'] for item in selected_stocks]
        concept_codes = set(filter_by_concept(matched_codes, concept_pool))
        selected_stocks = [
            item for item in selected_stocks if item['stock'] in concept_codes
        ]
        _strategy_log.info(
            f"【趋势股-概念过滤】{len(matched_codes)}只 → {len(selected_stocks)}只 | "
            f"概念池: {concept_pool}"
        )

    selected_stocks.sort(key=lambda item: (item['priority'], -item['score']))
    final_items = selected_stocks[:g.qs_max_count]
    final_list = [item['stock'] for item in final_items]
    combo_info = {item['stock']: item for item in final_items}

    _strategy_log.info(f"【趋势股-六维评估结果】选中 {len(final_list)}/{g.qs_max_count} 只:")
    for item in selected_stocks[:min(5, len(selected_stocks))]:
        info = get_security_info(str(item['stock']))
        name = info.display_name if info is not None else str(item['stock'])
        _strategy_log.info(
            f"  {item['stock']}({name}): {item['combo_name']} | "
            f"得分:{item['score']:.0f} | 开盘:{item['open_chg']:.2f}%"
        )

    combo_stats = {}
    for item in selected_stocks:
        combo_stats[item['combo_name']] = combo_stats.get(item['combo_name'], 0) + 1
    _strategy_log.info(f"【趋势股-组合分布】{combo_stats}")
    _strategy_log.event(
        '性能统计', '趋势股竞价确认',
        f'开盘与概念复核={pytime.perf_counter() - started:.2f}秒 | '
        f'静态候选={len(static_candidates or [])}只 | 最终={len(final_list)}只',
        icon='⏱️',
    )
    return [str(stock) for stock in final_list], combo_info


def get_qs_stock_list(context, date_str):
    """兼容入口：连续完成趋势股静态预选和竞价确认。"""
    static_candidates = prepare_qs_stock_list(context, date_str)
    final_list, combo_info = finalize_qs_stock_list(
        context, static_candidates, g.concept_pool
    )
    g.qs_combo_info = combo_info
    return final_list

def get_qs_hold_days_and_target(stock, context):
    stock = str(stock)
    buy_record = None
    for log_entry in reversed(g.qs_trade_log):
        if log_entry['stock'] == stock and log_entry['type'] == 'buy':
            buy_record = log_entry
            break

    if not buy_record:
        return 1, 6, -5

    buy_date = buy_record['date']
    current_date = context.current_dt.date()

    all_trade_days = _cached_trade_day_strings()
    if str(buy_date) in all_trade_days and str(current_date) in all_trade_days:
        buy_idx = all_trade_days.index(str(buy_date))
        current_idx = all_trade_days.index(str(current_date))
        hold_days = current_idx - buy_idx + 1
    else:
        hold_days = 1

    base_targets = {1: 6, 2: 12, 3: 18, 4: 22, 5: 35, 6: 35, 7: 35, 8: 35, 9: 35, 10: 35}
    base_target = base_targets.get(hold_days, 35)

    combo_info = getattr(g, 'qs_combo_info', {}).get(stock, {})
    adjust = combo_info.get('target_adjust', 0)
    adjusted_target = base_target + adjust

    return hold_days, adjusted_target, -5

def record_qs_buy(context, stock, price, amount, value):
    stock = str(stock)
    commission = max(value * 0.0003, 5)
    total_cost = value + commission
    record = {
        'type': 'buy', 'date': context.current_dt.date(), 'stock': stock,
        'name': get_security_info(stock).display_name,
        'price': round(price, 2), 'amount': amount, 'value': round(value, 2),
        'commission': round(commission, 2), 'total_cost': round(total_cost, 2), 'mode': 'qs'
    }
    g.qs_trade_log.append(record)
    if len(g.qs_trade_log) > 1000:
        del g.qs_trade_log[:-1000]
    _strategy_log.info(f"【趋势股-买入记录】{stock}({record['name']}) | "
             f"价格:{record['price']} 数量:{record['amount']} | "
             f"市值:{record['value']}元")
        # 【新增】初始化ATR止损
    try:
        atr = calculate_atr_for_qs(stock, context)
        if atr:
            initial_stop = record['price'] - (atr * g.qs_atr_multiplier)
            g.qs_atr_stops[stock] = initial_stop
            g.qs_position_highs[stock] = record['price']
            _strategy_log.info(f"【趋势股-ATR止损初始化】{stock} | ATR14:{atr:.3f} | "
                     f"止损价:{initial_stop:.2f} | "
                     f"下行空间:{(record['price']-initial_stop)/record['price']*100:.2f}%")
    except Exception as e:
        _strategy_log.warning(f"【趋势股-ATR初始化失败】{stock}: {str(e)}")

def record_qs_sell(context, stock, reason, sell_price):
    stock = str(stock)
    position = context.portfolio.positions.get(stock)
    if not position or position.total_amount == 0:
        return False
    if not _can_close_position(context, stock, reason):
        return False

    buy_record = None
    for log_entry in reversed(g.qs_trade_log):
        if log_entry['stock'] == stock and log_entry['type'] == 'buy':
            buy_record = log_entry
            break

    if not buy_record:
        return False

    sell_value = sell_price * position.total_amount
    commission = max(sell_value * 0.0003, 5)
    tax = sell_value * 0.001
    total_fee = commission + tax
    net_proceeds = sell_value - total_fee
    cost_value = buy_record['total_cost']
    net_profit = net_proceeds - cost_value
    profit_pct = (net_profit / cost_value) * 100 if cost_value else 0
    hold_days, _, _ = get_qs_hold_days_and_target(stock, context)

    record = {
        'type': 'sell', 'date': context.current_dt.date(), 'stock': stock,
        'name': get_security_info(stock).display_name,
        'sell_price': round(sell_price, 2), 'amount': position.total_amount,
        'sell_value': round(sell_value, 2), 'commission': round(commission, 2),
        'tax': round(tax, 2), 'net_profit': round(net_profit, 2),
        'profit_pct': round(profit_pct, 2), 'hold_days': hold_days,
        'reason': reason, 'mode': 'qs'
    }
    result = _close_position_logged(context, stock, reason, check=False)
    if not result:
        return False

    g.qs_trade_log.append(record)
    if len(g.qs_trade_log) > 1000:
        del g.qs_trade_log[:-1000]
    _strategy_log.info(f"【趋势股-卖出记录】{stock}({record['name']}) | 原因:{reason} | "
             f"持仓:{hold_days}天 | 净盈亏:{record['net_profit']}元({record['profit_pct']}%)")
    g.position_modes.pop(stock, None)
    g.sell_conditions.pop(stock, None)
    # 【新增】清理ATR止损相关存储
    g.qs_atr_stops.pop(stock, None)
    g.qs_position_highs.pop(stock, None)
    return True

def print_qs_trade_summary(context):
    if not g.qs_trade_log:
        return
    _strategy_log.info("========== 趋势股交易汇总 ==========")
    sell_records = [x for x in g.qs_trade_log if x['type'] == 'sell']
    if sell_records:
        profit_count = sum(1 for x in sell_records if x['net_profit'] > 0)
        win_rate = (profit_count / len(sell_records)) * 100
        total_net_profit = sum(x['net_profit'] for x in sell_records)
        _strategy_log.info(f"胜率:{profit_count}/{len(sell_records)}={win_rate:.1f}% | "
                 f"总盈亏:{total_net_profit:.2f}元")
    _strategy_log.info("=====================================")

def log_qs_daily_status(context):
    current_data = get_current_data()
    for stock in list(context.portfolio.positions.keys()):
        if g.position_modes.get(str(stock)) != 'qs':
            continue
        position = context.portfolio.positions.get(str(stock))
        if position.total_amount == 0:
            continue
        stock = str(stock)
        current_price = current_data[stock].last_price
        cost_price = position.avg_cost
        profit_pct = (current_price - cost_price) / cost_price * 100 if cost_price else 0
        hold_days, target, stop = get_qs_hold_days_and_target(stock, context)
        combo_name = g.qs_combo_info.get(stock, {}).get('combo_name', '未知')
        _strategy_log.info(f"【趋势股-每日持仓】{stock} 持仓第{hold_days}天 | "
                 f"类型:{combo_name} | 成本:{cost_price:.2f} 现价:{current_price:.2f} | "
                 f"盈亏:{profit_pct:.2f}% | 止盈:{target}% 止损:{stop}%")

# ============================================================================
# 第七部分：概念与选股（四模式统一入口）
# ============================================================================

def prepare_yesterday_concept_pool(context, initial_list, date_str, hl_yesterday=None):
    """盘前仅使用已收盘数据生成昨日热点概念。"""
    if hl_yesterday is None:
        hl_yesterday = get_hl_stock(initial_list, date_str)
    return get_concept_stats_weighted(
        hl_yesterday, date_str, context, top_n=4, is_today=False
    )


def _fetch_auction_snapshot(context, securities):
    """批量读取最终竞价并整理为每只股票一条记录。"""
    date_now = context.current_dt.strftime("%Y-%m-%d")
    auction_start = date_now + ' 09:15:00'
    auction_end = date_now + ' 09:25:00'
    try:
        auction_data = get_call_auction(
            list(securities), start_date=auction_start, end_date=auction_end,
            fields=['time', 'current', 'volume'],
        )
    except Exception as e:
        _strategy_log.info(f"【竞价快照】批量获取失败: {str(e)}")
        return {}, False
    if auction_data is None:
        _strategy_log.warning(
            f"【竞价快照】接口返回None | 查询区间={auction_start}~{auction_end}"
        )
        return {}, False
    if auction_data.empty:
        _strategy_log.warning(
            f"【竞价快照】返回空表 | 查询区间={auction_start}~{auction_end} | "
            f"请求股票数={len(securities)}"
        )
        return {}, False
    if 'code' not in auction_data.columns:
        _strategy_log.warning(
            f"【竞价快照】返回字段缺少code | columns={list(auction_data.columns)}"
        )
        return {}, False

    snapshot = {}
    for code, stock_data in auction_data.groupby('code', sort=False):
        if stock_data.empty:
            continue
        last = stock_data.iloc[-1]
        snapshot[str(code)] = {
            'current': last.get('current'),
            'volume': last.get('volume'),
            'time': last.get('time'),
        }
    return snapshot, bool(snapshot)


def finalize_concept_filter_pool(context, initial_list, date_str, concept_yesterday):
    """使用一份全市场竞价快照生成今日概念，并供各模式共同复用。"""
    auction_snapshot, auction_available = _fetch_auction_snapshot(context, initial_list)
    concept_today = []
    if auction_available:
        hl_auction = []
        auction_vol_data = {}
        current_data = get_current_data()
        for stock, values in auction_snapshot.items():
            try:
                current_price = values['current']
                if current_price >= current_data[stock].high_limit * 0.998:
                    hl_auction.append(stock)
                    auction_vol_data[stock] = values['volume']
            except Exception:
                continue
        try:
            concept_today = get_concept_stats_weighted(
                hl_auction, date_str, context, top_n=4, is_today=True,
                auction_data=auction_vol_data,
            )
        except Exception as exc:
            _strategy_log.info(f"【概念过滤】今日竞价概念统计失败: {str(exc)}")
            concept_today = []
    else:
        _strategy_log.info("【概念过滤】最终竞价数据不可用，仅使用昨日数据")

    concept_pool = list(set(concept_yesterday or []) | set(concept_today))
    _strategy_log.info(f"【概念池构建】昨日涨停概念前4: {concept_yesterday}")
    _strategy_log.info(f"【概念池构建】今日竞价概念前4: {concept_today}")
    _strategy_log.info(f"【概念池构建】合并概念池: {concept_pool}")
    _strategy_log.info(
        f"【竞价快照】覆盖={len(auction_snapshot)}/{len(initial_list)}只 | "
        f"状态={'可用' if auction_available else '不可用'}"
    )
    return concept_pool, auction_snapshot, auction_available


def get_concept_filter_pool(context):
    """兼容入口：连续生成昨日和今日竞价概念池。"""
    date_str = transform_date(context.previous_date, 'str')
    initial_list = prepare_stock_list(date_str)
    concept_yesterday = prepare_yesterday_concept_pool(context, initial_list, date_str)
    concept_pool, _, _ = finalize_concept_filter_pool(
        context, initial_list, date_str, concept_yesterday
    )
    g.concept_pool = concept_pool
    return concept_pool

def _current_trade_date(context):
    return context.current_dt.strftime('%Y-%m-%d')


def _clear_daily_candidate_lists():
    g.yje_stock_list = []
    g.yje_pending_list = []
    g.yje_weibi_bought = []
    g.rzq_stock_list = []
    g.qs_stock_list = []
    g.sb_stock_list = []
    g.sb_pending_list = []
    g.sb_bought_list = []
    g.sb_weibi_bought = []
    g.yje_strength_scores = {}
    g.sb_strength_scores = {}


def _selection_ready_for_today(context):
    return getattr(g, 'selection_ready_date', None) == _current_trade_date(context)


def prepare_stock_candidates(context):
    """
    盘前静态预选：只读取昨日及更早数据，不读取今日竞价或开盘价。
    """
    selection_started = pytime.perf_counter()
    _RUNTIME_DATA_CACHE['perf_stats'] = {}
    perf_stats = _RUNTIME_DATA_CACHE['perf_stats']
    trade_date = _current_trade_date(context)
    g.preselection_state = {}
    g.preselection_ready_date = None
    g.selection_ready_date = None
    g.concept_pool = []
    _clear_daily_candidate_lists()

    stage_started = pytime.perf_counter()
    date = context.previous_date
    date = transform_date(date, 'str')
    date_1 = get_shifted_date(date, -1, 'T')
    date_2 = get_shifted_date(date, -2, 'T')

    initial_list = prepare_stock_list(date)
    if not initial_list:
        _strategy_log.error("【盘前预选错误】初始股票池为空")
        return
    perf_stats['基础池'] = pytime.perf_counter() - stage_started

    stage_started = pytime.perf_counter()
    try:
        hl_list = get_hl_stock(initial_list, date)
    except Exception as exc:
        _strategy_log.error(f"【盘前预选】昨日涨停数据获取失败: {str(exc)}")
        hl_list = []
    try:
        concept_yesterday = prepare_yesterday_concept_pool(
            context, initial_list, date, hl_yesterday=hl_list
        )
    except Exception as exc:
        _strategy_log.error(f"【盘前预选】昨日概念池获取失败: {str(exc)}")
        concept_yesterday = []
    perf_stats['昨日概念'] = pytime.perf_counter() - stage_started

    # ===== 模块一：一进二静态预选 =====
    stage_started = pytime.perf_counter()
    _strategy_log.event('盘前预选', '四模式静态计算开始', '仅使用已收盘数据', icon='▶️')
    _strategy_log.info(f"【一进二-初始】昨日涨停股: {len(hl_list)}只")
    g.limit_up_count = len(hl_list)

    yje_static_scores = {}
    if hl_list:
        _strategy_log.debug("一进二开始计算不含竞价盘口和今日概念的静态评分")
        for stock in hl_list:
            stock = str(stock)
            score, details = calculate_yje_score(
                stock, date, [], context, include_live_auction=False
            )
            yje_static_scores[stock] = {'score': score, 'details': details}
    perf_stats['一进二'] = pytime.perf_counter() - stage_started

    # ===== 模块二：弱转强选股 =====
    stage_started = pytime.perf_counter()
    _strategy_log.debug("弱转强条件：6日涨幅<50%，昨日跌幅<2%，竞价2-6%")
    try:
        h1_list = get_ever_hl_stock2(initial_list, date)
    except Exception as exc:
        _strategy_log.error(f"【弱转强静态预选】炸板数据获取失败: {str(exc)}")
        h1_list = []
    _strategy_log.info(f"【弱转强-初始】昨日炸板股: {len(h1_list)}只")

    if len(h1_list) == 0:
        _strategy_log.warning("【弱转强】昨日无炸板股")
        rzq_static_candidates = []
    else:
        try:
            hl_prev_list = get_hl_stock(initial_list, date_1)
            hl_prev2_list = get_hl_stock(initial_list, date_2) if date_2 else []
        except Exception as exc:
            _strategy_log.error(f"【弱转强静态预选】历史涨停数据获取失败: {str(exc)}")
            hl_prev_list = []
            hl_prev2_list = []
        elements_to_remove = set(hl_prev_list + hl_prev2_list)
        reversal_candidates = [str(s) for s in h1_list if str(s) not in elements_to_remove]

        _strategy_log.info(f"【弱转强-候选】去除前1-2日涨停后: {len(reversal_candidates)}只")

        rzq_static_candidates = []
        rejected = {k: 0 for k in [
            'data_insufficient', 'ma_fail', 'increase_too_high', 'kline_bad',
            'volume_fail', 'market_cap_fail', 'low_volume', 'auction_data_missing',
            'auction_too_low', 'auction_too_high',
            'no_concept', 'both_conditions_fail', 'limit_up_count_invalid']}

        for s in reversal_candidates:
            try:
                s = str(s)
                price_data = attribute_history(
                    s, 10, '1d',
                    fields=['open', 'close', 'volume', 'money', 'high_limit'],
                    skip_paused=True,
                )
                if len(price_data) < 6:
                    rejected['data_insufficient'] += 1
                    continue

                ma5 = price_data['close'][-5:].mean()
                ma10 = price_data['close'][-10:].mean()
                last_close = price_data['close'][-1]

                prev_6_data = price_data.loc[:, ['close', 'high_limit']].tail(6)
                if len(prev_6_data) < 6:
                    rejected['data_insufficient'] += 1
                    continue

                prev_limit_up_count = sum(
                    1 for i in range(5)
                    if prev_6_data['close'].iloc[i] >= prev_6_data['high_limit'].iloc[i] * 0.998)

                distance_to_ma5 = abs(last_close - ma5) / ma5

                if not (prev_limit_up_count in [1, 2, 3] or distance_to_ma5 <= 0.1):
                    rejected['both_conditions_fail'] += 1
                    rejected['limit_up_count_invalid'] += 1
                    _strategy_log.debug(f"【弱转强-过滤】{s} 涨停次数{prev_limit_up_count}次(要求1-3次)，距离5日线{distance_to_ma5:.2%}")
                    continue

                _strategy_log.debug(f"【弱转强-通过】{s} 涨停次数{prev_limit_up_count}次，距离5日线{distance_to_ma5:.2%}")

                if not (last_close > ma10):
                    rejected['ma_fail'] += 1
                    continue

                increase_ratio = (last_close - price_data['close'][-6]) / price_data['close'][-6]
                if increase_ratio > 0.50:
                    rejected['increase_too_high'] += 1
                    continue

                prev_day_data = price_data.loc[:, ['open', 'close', 'volume', 'money']].tail(1)
                if len(prev_day_data) < 1:
                    rejected['data_insufficient'] += 1
                    continue

                open_close_ratio = ((prev_day_data['close'].iloc[-1] - prev_day_data['open'].iloc[-1]) /
                                    prev_day_data['open'].iloc[-1])
                if open_close_ratio <=-0:
                    rejected['kline_bad'] += 1
                    continue

                if prev_day_data['money'].iloc[-1] < 1e8 or prev_day_data['money'].iloc[-1] > 60e8:
                    rejected['volume_fail'] += 1
                    continue

                valuation_data = get_valuation(s, start_date=context.previous_date,
                                               end_date=context.previous_date,
                                               fields=['market_cap', 'circulating_market_cap'])
                if valuation_data.empty:
                    rejected['market_cap_fail'] += 1
                    continue

                if (valuation_data['market_cap'].iloc[-1] < 30 or
                        valuation_data['circulating_market_cap'].iloc[-1] > 1500):
                    rejected['market_cap_fail'] += 1
                    continue

                mainline_score = calculate_mainline_score_optimized(s, context)
                if mainline_score == 0:
                    rejected['no_concept'] += 1
                    _strategy_log.debug(f"【弱转强-无概念】{s} 无有效概念，但仍保留")

                rzq_static_candidates.append({
                    'stock': s,
                    'yesterday_close': float(prev_day_data['close'].iloc[-1]),
                    'prev_limit_up_count': int(prev_limit_up_count),
                })
                info = get_security_info(s)
                name = info.display_name if info is not None else s
                _strategy_log.debug(
                    f"【弱转强-静态通过】{s}({name}) 涨停次数:{prev_limit_up_count}次"
                )

            except Exception as e:
                _strategy_log.error(f"【弱转强-错误】处理{s}出错: {str(e)}")

        rejected_labels = {
            'data_insufficient': '数据不足', 'ma_fail': '均线不符',
            'increase_too_high': '六日涨幅过大', 'kline_bad': 'K线不符',
            'volume_fail': '成交额不符', 'market_cap_fail': '市值不符',
            'low_volume': '缩量', 'auction_data_missing': '竞价缺失',
            'auction_too_low': '竞价低开', 'auction_too_high': '竞价高开',
            'no_concept': '无概念但放行', 'both_conditions_fail': '形态不符',
            'limit_up_count_invalid': '涨停次数不符',
        }
        rejected_detail = ' | '.join(
            f'{rejected_labels[name]}={count}'
            for name, count in rejected.items() if count
        ) or '无淘汰记录'
        _strategy_log.info(
            f"【弱转强-静态筛选统计】候选={len(rzq_static_candidates)}只 | {rejected_detail}"
        )

    perf_stats['弱转强'] = pytime.perf_counter() - stage_started

    # ===== 模块三：趋势股选股 =====
    stage_started = pytime.perf_counter()
    qs_batch_requested_before = perf_stats.get('batch_requested_stocks', 0)
    qs_batch_returned_before = perf_stats.get('batch_returned_stocks', 0)
    qs_single_before = perf_stats.get('single_daily_requests', 0)
    _strategy_log.debug("趋势股进入六维静态评估（昨日数据）")
    try:
        qs_static_candidates = prepare_qs_stock_list(context, date)
    except Exception as exc:
        _strategy_log.error(f"【趋势股静态预选错误】{str(exc)}")
        qs_static_candidates = []
    qs_static_candidates = list(qs_static_candidates or [])
    # 竞价确认只需要紧凑的组合结果，不持久化数百只股票的指标DataFrame。
    g._micro_data_cache = {}
    g._cache_date = date
    perf_stats['趋势股'] = pytime.perf_counter() - stage_started
    perf_stats['qs_batch_requested'] = perf_stats.get('batch_requested_stocks', 0) - qs_batch_requested_before
    perf_stats['qs_batch_returned'] = perf_stats.get('batch_returned_stocks', 0) - qs_batch_returned_before
    perf_stats['qs_single_fallbacks'] = perf_stats.get('single_daily_requests', 0) - qs_single_before

    # ===== 模块四：首板选股 =====
    stage_started = pytime.perf_counter()
    sb_batch_requested_before = perf_stats.get('batch_requested_stocks', 0)
    sb_batch_returned_before = perf_stats.get('batch_returned_stocks', 0)
    sb_single_before = perf_stats.get('single_daily_requests', 0)
    _strategy_log.debug("首板进入盘前市值、PE、60日成交额过滤")
    try:
        sb_static_scores = prepare_sb_stock_list(context, initial_list, date)
    except Exception as exc:
        _strategy_log.error(f"【首板静态预选错误】{str(exc)}")
        sb_static_scores = {}
    sb_static_scores = dict(sb_static_scores or {})
    perf_stats['首板'] = pytime.perf_counter() - stage_started
    perf_stats['sb_batch_requested'] = perf_stats.get('batch_requested_stocks', 0) - sb_batch_requested_before
    perf_stats['sb_batch_returned'] = perf_stats.get('batch_returned_stocks', 0) - sb_batch_returned_before
    perf_stats['sb_single_fallbacks'] = perf_stats.get('single_daily_requests', 0) - sb_single_before

    perf_stats['总计'] = pytime.perf_counter() - selection_started
    _strategy_log.event(
        '性能统计', '四模式盘前静态预选',
        f"基础池={perf_stats.get('基础池', 0):.2f}秒 | "
        f"昨日概念={perf_stats.get('昨日概念', 0):.2f}秒 | "
        f"一进二={perf_stats.get('一进二', 0):.2f}秒 | "
        f"弱转强={perf_stats.get('弱转强', 0):.2f}秒 | "
        f"趋势股={perf_stats.get('趋势股', 0):.2f}秒 | "
        f"首板={perf_stats.get('首板', 0):.2f}秒 | "
        f"总计={perf_stats.get('总计', 0):.2f}秒",
        icon='⏱️',
    )
    _strategy_log.event(
        '性能统计', '盘前历史数据命中',
        f"趋势股批量={perf_stats.get('qs_batch_returned', 0)}/"
        f"{perf_stats.get('qs_batch_requested', 0)}只 | "
        f"趋势股逐股回退={perf_stats.get('qs_single_fallbacks', 0)}次 | "
        f"首板批量={perf_stats.get('sb_batch_returned', 0)}/"
        f"{perf_stats.get('sb_batch_requested', 0)}只 | "
        f"首板逐股回退={perf_stats.get('sb_single_fallbacks', 0)}次 | "
        f"估值批量={perf_stats.get('valuation_batch_returned', 0)}/"
        f"{perf_stats.get('valuation_batch_requested', 0)}只 | "
        f"估值逐股回退={perf_stats.get('valuation_single_fallbacks', 0)}次 | "
        f"日线缓存命中={perf_stats.get('daily_cache_hits', 0)}次 | "
        f"日线失败分块={perf_stats.get('batch_failed_chunks', 0)}个 | "
        f"估值失败分块={perf_stats.get('valuation_batch_failed_chunks', 0)}个",
        icon='📊',
    )
    g.preselection_state = {
        'schema': 1,
        'trade_date': trade_date,
        'source_date': date,
        'initial_list': [str(stock) for stock in initial_list],
        'hl_list': [str(stock) for stock in hl_list],
        'concept_yesterday': [str(item) for item in concept_yesterday],
        'yje_static_scores': yje_static_scores,
        'rzq_static_candidates': rzq_static_candidates,
        'qs_static_candidates': qs_static_candidates,
        'sb_static_scores': sb_static_scores,
    }
    g.preselection_ready_date = trade_date
    _strategy_log.event(
        '盘前预选', '静态计算完成',
        f'一进二静态={len(yje_static_scores)}只 | '
        f'弱转强静态={len(rzq_static_candidates)}只 | '
        f'趋势股静态={len(qs_static_candidates)}只 | '
        f'首板静态={len(sb_static_scores)}只 | '
        f'总计={perf_stats.get("总计", 0):.2f}秒',
        icon='✅',
    )


def finalize_yje_stock_list(context, static_scores, concept_pool, date_str):
    """竞价后补齐一进二盘口评分、今日概念评分并排序。"""
    started = pytime.perf_counter()
    scores = {}
    candidates = list((static_scores or {}).keys())
    if concept_pool and candidates:
        before_count = len(candidates)
        candidates = filter_by_concept(candidates, concept_pool)
        _strategy_log.info(f"【一进二-题材过滤后】{before_count}只 → {len(candidates)}只")

    for stock in candidates:
        stock = str(stock)
        try:
            static_data = static_scores[stock]
            details = copy.deepcopy(static_data.get('details', {}))
            tick = None
            if details.get('_total_volume') is not None:
                try:
                    tick = get_current_tick(stock)
                except Exception:
                    tick = None
            circ_cap = details.get('_circulating_market_cap')
            if (details.get('_total_volume') is not None and circ_cap is None and
                    tick and tick['b1_v'] is not None):
                try:
                    valuation = get_valuation(
                        stock, start_date=date_str, end_date=date_str,
                        fields=['circulating_market_cap'],
                    )
                    if valuation is not None and not valuation.empty:
                        circ_cap = float(
                            valuation['circulating_market_cap'].iloc[0]
                        ) * 1e8
                except Exception:
                    circ_cap = None
            live_score, live_metrics = _calculate_yje_live_board_score(
                tick,
                float(details.get('_total_volume', 0) or 0),
                circ_cap,
            )
            concept_score = calculate_concept_score(stock, concept_pool)
            score = float(static_data.get('score', 0)) + live_score + concept_score
            details.update(live_metrics)
            details['封板质量总分'] = (
                float(details.get('封板质量总分', 0)) + live_score
            )
            details['题材热度评分'] = concept_score
            details['综合评分'] = score
            details.pop('_total_volume', None)
            details.pop('_circulating_market_cap', None)
            scores[stock] = {'score': score, 'details': details}
        except Exception as exc:
            _strategy_log.debug(f"【一进二-竞价评分】{stock}失败: {str(exc)}")

    sorted_stocks = sorted(
        scores.items(), key=lambda item: item[1]['score'], reverse=True
    )
    _strategy_log.info("【一进二评分TOP5】：")
    for stock, data in sorted_stocks[:5]:
        info = get_security_info(stock)
        name = info.display_name if info is not None else stock
        details = data['details']
        _strategy_log.info(
            f" {stock}({name}): 总分{data['score']:.0f}分 | "
            f"封板成交占比:{details.get('封板成交占比', 0):.1%} | "
            f"T-1日量比:{details.get('T-1日量比', 0):.2f} | "
            f"20日跌幅:{details.get('20日跌幅', 0):.1f}%"
        )
    final_list = [str(item[0]) for item in sorted_stocks[:10]]
    _strategy_log.event(
        '性能统计', '一进二竞价确认',
        f'概念、盘口与排序={pytime.perf_counter() - started:.2f}秒 | '
        f'最终={len(final_list)}只',
        icon='⏱️',
    )
    return final_list, scores


def finalize_rzq_stock_list(context, static_candidates, auction_snapshot,
                            auction_available):
    """竞价后只执行弱转强的最终竞价/开盘条件。"""
    started = pytime.perf_counter()
    candidates = list(static_candidates or [])
    rejected = {key: 0 for key in [
        'auction_data_missing', 'auction_too_low', 'auction_too_high',
    ]}

    if candidates and not auction_available:
        # 全市场快照失败时只对弱转强候选重试，避免退回逐股请求。
        target_snapshot, target_available = _fetch_auction_snapshot(
            context, [item['stock'] for item in candidates]
        )
        if target_available:
            auction_snapshot = target_snapshot
            auction_available = True

    final_list = []
    current_data = get_current_data()
    for item in candidates:
        stock = str(item['stock'])
        yesterday_close = float(item['yesterday_close'])
        try:
            if auction_available:
                values = auction_snapshot.get(stock)
                if not values or values.get('current') is None:
                    rejected['auction_data_missing'] += 1
                    _strategy_log.debug(f"【弱转强-竞价缺失】{stock} 无竞价数据")
                    continue
                open_chg_pct = (
                    float(values['current']) - yesterday_close
                ) / yesterday_close
                # 保留原竞价分支的实际阈值：0%<竞价涨幅<=6%。
                if open_chg_pct <= 0.00:
                    rejected['auction_too_low'] += 1
                    _strategy_log.debug(f"【弱转强-竞价低开】{stock} 涨幅{open_chg_pct:.2%}")
                    continue
                if open_chg_pct > 0.06:
                    rejected['auction_too_high'] += 1
                    _strategy_log.debug(f"【弱转强-竞价高开】{stock} 涨幅{open_chg_pct:.2%}")
                    continue
                _strategy_log.debug(f"【弱转强-竞价通过】{stock} 涨幅{open_chg_pct:.2%}")
            else:
                day_open = current_data[stock].day_open
                open_chg_pct = (day_open - yesterday_close) / yesterday_close
                if not (0.02 <= open_chg_pct <= 0.06):
                    _strategy_log.debug(f"【弱转强-开盘不符】{stock} 涨幅{open_chg_pct:.2%}")
                    continue
            final_list.append(stock)
        except Exception as exc:
            _strategy_log.debug(f"【弱转强-竞价确认】{stock}失败: {str(exc)}")

    detail = ' | '.join(
        f'{label}={count}' for label, count in [
            ('竞价缺失', rejected['auction_data_missing']),
            ('竞价低开', rejected['auction_too_low']),
            ('竞价高开', rejected['auction_too_high']),
        ] if count
    ) or '无淘汰记录'
    _strategy_log.info(
        f"【弱转强-竞价确认】候选={len(final_list)}只 | {detail} | "
        f"耗时={pytime.perf_counter() - started:.2f}秒"
    )
    return final_list


def get_stock_list(context):
    """竞价后确认并原子发布四模式当天候选池。"""
    selection_started = pytime.perf_counter()
    trade_date = _current_trade_date(context)
    if _selection_ready_for_today(context):
        _strategy_log.info(f"【竞价确认】{trade_date}候选池已发布，跳过重复执行")
        return
    state = getattr(g, 'preselection_state', None)
    source_date = transform_date(context.previous_date, 'str')
    if not isinstance(state, dict) or state.get('schema') != 1 or \
            state.get('trade_date') != trade_date or \
            state.get('source_date') != source_date or \
            getattr(g, 'preselection_ready_date', None) != trade_date:
        _clear_daily_candidate_lists()
        g.concept_pool = []
        g.selection_ready_date = None
        _strategy_log.error(
            f"【竞价确认错误】盘前静态预选未就绪，日期={trade_date}，跳过新候选发布"
        )
        return

    initial_list = [str(stock) for stock in state.get('initial_list', [])]
    date_str = source_date
    try:
        stage_started = pytime.perf_counter()
        concept_pool, auction_snapshot, auction_available = finalize_concept_filter_pool(
            context, initial_list, date_str, state.get('concept_yesterday', [])
        )
        concept_seconds = pytime.perf_counter() - stage_started

        stage_started = pytime.perf_counter()
        yje_stock_list, yje_scores = finalize_yje_stock_list(
            context, state.get('yje_static_scores', {}), concept_pool, date_str
        )
        yje_seconds = pytime.perf_counter() - stage_started

        stage_started = pytime.perf_counter()
        rzq_stock_list = finalize_rzq_stock_list(
            context, state.get('rzq_static_candidates', []),
            auction_snapshot, auction_available,
        )
        rzq_seconds = pytime.perf_counter() - stage_started

        stage_started = pytime.perf_counter()
        qs_stock_list, qs_combo_info = finalize_qs_stock_list(
            context, state.get('qs_static_candidates', []), concept_pool,
            auction_snapshot=auction_snapshot,
        )
        qs_seconds = pytime.perf_counter() - stage_started

        stage_started = pytime.perf_counter()
        sb_stock_list, sb_scores = finalize_sb_stock_list(
            context, state.get('sb_static_scores', {}), concept_pool
        )
        sb_seconds = pytime.perf_counter() - stage_started

        # 情绪与优先级仍在所有模式确认后更新，保持原有切换时点。
        g.limit_up_count = len(state.get('hl_list', []))
        update_strategy_priority(context)

        # 所有模式均确认成功后再一次性发布，避免盘中读到半成品。
        g.concept_pool = concept_pool
        g.yje_stock_list = [str(stock) for stock in yje_stock_list]
        g.yje_pending_list = list(g.yje_stock_list)
        g.yje_weibi_bought = []
        g.yje_strength_scores = yje_scores
        g.rzq_stock_list = [str(stock) for stock in rzq_stock_list]
        g.qs_stock_list = [str(stock) for stock in qs_stock_list]
        g.qs_combo_info = qs_combo_info
        g.sb_stock_list = [str(stock) for stock in sb_stock_list]
        g.sb_strength_scores = sb_scores
        g.sb_pending_list = []
        g.sb_bought_list = []
        g.sb_weibi_bought = []
        g.selection_ready_date = trade_date

        total_seconds = pytime.perf_counter() - selection_started
        _strategy_log.event(
            '候选生成', '四模式选股完成',
            f"一进二={len(yje_stock_list)}只 | 弱转强={len(rzq_stock_list)}只 | "
            f"趋势股={len(qs_stock_list)}只 | 首板={len(sb_stock_list)}只 | "
            f"优先级={' > '.join(g.priority_config)} | 就绪={trade_date}",
            icon='✅',
        )
        _strategy_log.event(
            '性能统计', '四模式竞价确认分段',
            f"概念池={concept_seconds:.2f}秒 | 一进二={yje_seconds:.2f}秒 | "
            f"弱转强={rzq_seconds:.2f}秒 | 趋势股={qs_seconds:.2f}秒 | "
            f"首板={sb_seconds:.2f}秒 | 总计={total_seconds:.2f}秒",
            icon='⏱️',
        )
    except Exception as exc:
        _clear_daily_candidate_lists()
        g.concept_pool = []
        g.selection_ready_date = None
        _strategy_log.error(f"【竞价确认重大错误】{str(exc)}，当天跳过新候选发布")

def update_sentiment_phase(context):
    try:
        success_rate, is_valid = check_strategy_effectiveness(context)
        if not is_valid:
            _strategy_log.warning(f"【策略有效性】近3日一进二成功率{success_rate:.1%}<30%，暂停一进二！")
            return 'low_success_rate'

        limit_up_today = getattr(g, 'limit_up_count', 0)
        limit_up_yesterday = g.sentiment_cache.get('prev_limit_up', 0)

        index_data = attribute_history('000001.XSHG', 2, '1d', ['close'], skip_paused=True)
        if len(index_data) < 2:
            return 'unknown'

        pct_change = (index_data['close'][-1] / index_data['close'][-2] - 1) * 100

        if pct_change > 2 and limit_up_today > 50:
            phase = 'main'
        elif pct_change > 0 and limit_up_today > limit_up_yesterday:
            phase = 'repair'
        elif pct_change < -1.5 and limit_up_today < 20:
            phase = 'freeze'
        elif pct_change > 0 and limit_up_today < 30:
            phase = 'divergence'
        else:
            phase = 'decline'

        g.sentiment_cache['phase'] = phase
        g.sentiment_cache['prev_limit_up'] = limit_up_today
        _strategy_log.info(f"当前情绪周期: {phase} (大盘涨幅:{pct_change:.2f}%, 涨停:{limit_up_today})")
        return phase
    except Exception as e:
        _strategy_log.error(f"更新情绪阶段失败: {str(e)}")
        return 'unknown'

def update_strategy_priority(context):
    """
    四模块优先级策略
    """
    phase = update_sentiment_phase(context)
    if phase == 'low_success_rate':
        g.priority_config = ["sb", "rzq"]
        _strategy_log.warning("【策略优先级】一进二失效，切换：首板 > 弱转强")
        return

    if phase in ['repair', 'main']:
        g.priority_config = ["yje", "sb", "rzq"]
        _strategy_log.info(f"情绪阶段[{phase}]，策略优先级：一进二 > 首板 > 弱转强")
    elif phase == 'divergence':
        g.priority_config = ["rzq", "sb", "yje"]
        _strategy_log.info(f"情绪阶段[{phase}]，策略优先级：弱转强 > 首板 > 一进二")
    elif phase == 'freeze':
        g.priority_config = ["rzq", "yje", "sb"]
        _strategy_log.info(f"情绪阶段[{phase}]，策略优先级：弱转强 > 一进二 > 首板")
    else:
        g.priority_config = ["rzq"]
        _strategy_log.warning(f"情绪阶段[{phase}]，仅弱转强模式")

    g.trade_stats['strategy_priority'] = {'phase': phase, 'priority': g.priority_config}

# ============================================================================
# 第八部分：买入相关（四模式分别处理）
# ============================================================================

def check_weibi_signal(context):
    """一进二专用委比监控"""
    time_now = context.current_dt
    hour = time_now.hour
    minute = time_now.minute
    if not ((hour == 9 and minute >= 25) or (hour == 10) or (hour == 11 and minute <= 30)):
        return
    if minute == g.last_weibi_check_minute:
        return
    g.last_weibi_check_minute = minute

    current_data = get_current_data()
    max_pos = max(1, int(g.max_positions * g.position_ratio))

    if not hasattr(g, 'yje_pending_list') or not g.yje_pending_list:
        return

    monitor_list = [str(s) for s in g.yje_pending_list
                    if str(s) not in context.portfolio.positions
                    and str(s) not in getattr(g, 'yje_weibi_bought', [])
                    and str(s) not in [str(x) for x in g.failed_buy_list]]
    if not monitor_list:
        return

    current_time = context.current_dt.strftime('%H:%M:%S')
    _strategy_log.debug(f"【一进二委比监控-{current_time}】监控{len(monitor_list)}只")

    for stock in monitor_list:
        try:
            stock = str(stock)
            if is_one_word_limit(stock, context):
                continue
            if len(context.portfolio.positions) >= max_pos:
                break

            weibi_value = None
            data_source = None
            current_price = current_data[stock].last_price

            try:
                tick = get_current_tick(stock)
                if tick and tick['b1_v'] is not None and tick['a1_v'] is not None:
                    buy_vol = sum([tick.get(f'b{i}_v', 0) for i in range(1, 6)
                                   if tick.get(f'b{i}_v') is not None])
                    sell_vol = sum([tick.get(f'a{i}_v', 0) for i in range(1, 6)
                                    if tick.get(f'a{i}_v') is not None])
                    if buy_vol + sell_vol > 0:
                        weibi_value = (buy_vol - sell_vol) / (buy_vol + sell_vol) * 100
                        data_source = "Tick(L2)"
            except:
                pass

            if weibi_value is None:
                try:
                    today_str = context.current_dt.strftime('%Y-%m-%d 09:30:00')
                    min_data = get_price(stock, start_date=today_str, end_date=context.current_dt, # <- 正确
                                     frequency='1m', fields=['weibi', 'close', 'volume'],
                                     skip_paused=True)
                    if not min_data.empty and 'weibi' in min_data.columns:
                        weibi_value = min_data['weibi'].iloc[-1]
                        data_source = "分钟级(weibi字段)"
                    else:
                        min_bar = get_price(stock, start_date=today_str, end_date=context.current_dt,
                                            frequency='1m', fields=['high', 'low', 'close'],
                                            skip_paused=True)
                        if not min_bar.empty:
                            h = min_bar['high'].iloc[-1]
                            l = min_bar['low'].iloc[-1]
                            c = min_bar['close'].iloc[-1]
                            if h > l:
                                weibi_value = (c - l) / (h - l) * 100
                                data_source = "模拟委比(价格位置)"
                except:
                    pass

            if weibi_value is not None and weibi_value >= 80:
                target_value = context.portfolio.total_value * g.position_ratio / g.yje_max_positions
                amount = int(target_value / current_price / 100) * 100
                actual_value = amount * current_price

                if context.portfolio.available_cash < actual_value or amount <= 0:
                    continue

                pre_close = _cached_previous_close(stock, context)
                if pre_close is None:
                    pre_close = current_price
                buy_pct = (current_price - pre_close) / pre_close * 100

                _strategy_log.info(f"【一进二委比买入】{current_time} | {stock} | "
                         f"委比:{weibi_value:.1f}% | 涨幅:{buy_pct:.2f}%")

                order_result = _order_target_value_logged(stock, target_value)
                if not order_result:
                    continue
                g.position_modes[stock] = 'yje'
                g.today_bought[stock] = current_price
                g.yje_weibi_bought.append(stock)

            elif weibi_value is not None and minute % 5 == 0:
                _strategy_log.debug(f"【一进二委比未触发】{stock} 委比:{weibi_value:.1f}%(<80%)")

        except Exception as e:
            _strategy_log.error(f"【一进二委比监控错误】{stock}: {str(e)}")

def check_compatibility_buy(context):
    """一进二 8%涨幅兜底买入"""
    time_now = context.current_dt
    hour = time_now.hour
    minute = time_now.minute
    if not ((hour == 9 and minute >= 25) or (hour == 10) or (hour == 11 and minute <= 30)):
        return
    if not hasattr(g, 'yje_pending_list') or not g.yje_pending_list:
        return

    current_data = get_current_data()
    max_pos = max(1, int(g.max_positions * g.position_ratio))

    if not hasattr(g, 'yje_weibi_bought'):
        g.yje_weibi_bought = []

    compat_list = [str(s) for s in g.yje_pending_list
                    if str(s) not in context.portfolio.positions
                    and str(s) not in g.yje_weibi_bought
                    and str(s) not in [str(x) for x in g.failed_buy_list]]
    if not compat_list:
        return

    current_time = context.current_dt.strftime('%H:%M:%S')
    for stock in compat_list:
        try:
            stock = str(stock)
            if is_one_word_limit(stock, context):
                continue
            if len(context.portfolio.positions) >= max_pos:
                return

            current_price = current_data[stock].last_price
            pre_close = _cached_previous_close(stock, context)
            if pre_close is None:
                continue
            rise_pct = (current_price - pre_close) / pre_close * 100

            if rise_pct >= 8.0:
                target_value = context.portfolio.total_value * g.position_ratio / g.yje_max_positions
                amount = int(target_value / current_price / 100) * 100
                actual_value = amount * current_price
                if context.portfolio.available_cash < actual_value or amount <= 0:
                    continue

                _strategy_log.info(f"【一进二8%兜底买入】{current_time} | {stock} | 涨幅:{rise_pct:.2f}%")
                order_result = _order_target_value_logged(stock, target_value)
                if not order_result:
                    continue
                g.position_modes[stock] = 'yje'
                g.today_bought[stock] = current_price
        except Exception as e:
            _strategy_log.error(f"【一进二兜底买入错误】{stock}: {str(e)}")

def buy_yje_mode(context):
    """一进二买入初始化"""
    current_time = context.current_dt.strftime('%H:%M:%S')
    if len(g.yje_stock_list) == 0:
        _strategy_log.info("【买入-一进二模式】股票池为空，跳过")
        return

    g.yje_pending_list = [str(s) for s in g.yje_stock_list]
    g.yje_weibi_bought = []

    one_word = sum(1 for s in g.yje_stock_list if is_one_word_limit(str(s), context))
    tradable = len(g.yje_stock_list) - one_word

    _strategy_log.info(f"【一进二买入初始化-{current_time}】候选{len(g.yje_stock_list)}只 | "
             f"可交易{tradable}只 | 一字板{one_word}只")

def handle_rzq_qs_sb_buy(context):
    """弱转强 + 趋势股 + 首板 买入处理"""
    current_data = get_current_data()
    current_time = context.current_dt.strftime('%H:%M:%S')
    hold_list = [str(s) for s in context.portfolio.positions.keys()]

    rzq_available = []
    for s in g.rzq_stock_list:
        s = str(s)
        if s in hold_list or s in [str(x) for x in g.failed_buy_list]:
            continue
        if is_one_word_limit(s, context):
            record_buy_attempt(s, 'rzq', '一字板')
        else:
            rzq_available.append(s)

    qs_available = []
    for s in g.qs_stock_list:
        s = str(s)
        if s in hold_list or s in [str(x) for x in g.failed_buy_list]:
            continue
        if is_one_word_limit(s, context):
            record_buy_attempt(s, 'qs', '一字板')
        else:
            qs_available.append(s)

    sb_count = len([s for s in g.sb_stock_list if str(s) not in hold_list and str(s) not in [str(x) for x in g.failed_buy_list]])

    _strategy_log.info(f"【买入-三模块统计】{current_time} "
             f"弱转强:{len(rzq_available)} 趋势股:{len(qs_available)} 首板:{sb_count}")

    actual_max_pos = max(1, int(g.max_positions * g.position_ratio))
    total_slots = actual_max_pos - len(context.portfolio.positions)
    if total_slots <= 0:
        return

    max_qs = min(max(1, int(g.max_positions * g.position_ratio * g.qs_max_ratio)),
                 g.qs_max_count)
    cur_qs = sum(1 for s in hold_list if g.position_modes.get(str(s)) == 'qs')
    qs_slots = min(total_slots // 2, max(0, max_qs - cur_qs))

    rzq_slots = total_slots - qs_slots

    base_value = (context.portfolio.total_value * g.position_ratio) / actual_max_pos
    success = {'rzq': 0, 'qs': 0}

    if rzq_slots > 0 and rzq_available:
        df = get_factor_filter_df(context, rzq_available, g.jqfactor, g.sort)
        for s in list(df.index)[:rzq_slots]:
            s = str(s)
            if check_and_buy_stock(context, s, 'rzq', base_value):
                success['rzq'] += 1

    if qs_slots > 0 and qs_available:
        for s in qs_available[:qs_slots]:
            if check_and_buy_stock(context, s, 'qs', base_value):
                success['qs'] += 1

    _strategy_log.info(f"【买入总结-非首板】弱转强:{success['rzq']} 趋势股:{success['qs']}")

def check_and_buy_stock(context, stock, mode, target_value):
    """通用买入检查与执行"""
    stock = str(stock)
    current_data = get_current_data()
    current_time = context.current_dt.strftime('%H:%M:%S')
    mode_name = {'yje': '一进二', 'rzq': '弱转强', 'qs': '趋势股', 'sb': '首板'}.get(mode, mode)

    if is_one_word_limit(stock, context):
        record_buy_attempt(stock, mode, '一字板')
        return False

    if current_data[stock].last_price >= current_data[stock].high_limit * 0.998:
        record_buy_attempt(stock, mode, '已涨停')
        return False

    if current_data[stock].paused:
        record_buy_attempt(stock, mode, '停牌')
        return False

    if 'ST' in current_data[stock].name or '退' in current_data[stock].name:
        record_buy_attempt(stock, mode, 'ST/退市')
        return False

    if context.portfolio.available_cash < current_data[stock].last_price * 100:
        record_buy_attempt(stock, mode, '资金不足')
        return False

    current_price = current_data[stock].last_price
    amount = int(target_value / current_price / 100) * 100
    if amount == 0:
        record_buy_attempt(stock, mode, '不足100股')
        return False

    actual_value = amount * current_price
    pre_close = _cached_previous_close(stock, context)
    if pre_close is None:
        pre_close = current_price
    buy_pct = (current_price - pre_close) / pre_close * 100 if pre_close else 0

    order_result = _order_target_value_logged(stock, target_value)
    if not order_result:
        record_buy_attempt(stock, mode, '委托未生成')
        return False

    g.today_bought[stock] = current_price
    g.position_modes[stock] = mode
    _strategy_log.info(f"【买入成功-{mode_name}】{current_time} | {stock}({current_data[stock].name}) | "
             f"价格:{current_price:.2f} | 数量:{amount}股 | 涨幅:{buy_pct:.2f}%")

    if mode == 'qs':
        record_qs_buy(context, stock, current_price, amount, actual_value)

    return True

def buy(context):
    """主买入函数（四模块）"""
    if not _selection_ready_for_today(context):
        _strategy_log.warning(
            "【买入跳过】当天候选池尚未完成竞价确认，避免使用过期候选"
        )
        return
    use_yje = 'yje' in g.priority_config and len(g.yje_stock_list) > 0
    use_sb = 'sb' in g.priority_config and len(g.sb_stock_list) > 0
    phase = g.sentiment_cache.get('phase', 'unknown')

    _strategy_log.info(f"【策略选择】情绪:{phase} | 优先级:{g.priority_config} | "
             f"一进二:{len(g.yje_stock_list)}只 | 首板:{len(g.sb_stock_list)}只")

    if use_yje:
        buy_yje_mode(context)

    if use_sb:
        buy_sb_mode(context)

    if not use_yje:
        handle_rzq_qs_sb_buy(context)
    else:
        if use_sb:
            _strategy_log.info("【首板并行】一进二优先模式下，首板仍独立运行30%仓位")

# ============================================================================
# 第九部分：卖出相关（四模式分别处理）
# ============================================================================

def initialize_sell_conditions(stock, open_price, pre_close, context):
    stock = str(stock)
    mode = g.position_modes.get(stock, 'unknown')
    g.opening_prices[stock] = open_price

    if mode == 'qs':
        position = context.portfolio.positions.get(stock)
        initial_profit = (open_price - position.avg_cost) / position.avg_cost * 100 \
            if position and position.avg_cost > 0 else 0
        hold_days, take_profit, stop = get_qs_hold_days_and_target(stock, context)
        condition = {
            'type': 'trend_stock_dynamic', 'stop_loss': stop,
            'highest_profit_pct': initial_profit, 'description': '趋势股动态止盈'
        }
        g.sell_conditions[stock] = condition
        combo_name = g.qs_combo_info.get(stock, {}).get('combo_name', '标准')
        _strategy_log.info(f"【卖出策略-趋势股】{stock}[{combo_name}]: 第{hold_days}天 止盈{take_profit}%")
        return condition

    if mode == 'sb':
        condition = {
            'type': 'sb_intraday', 'was_limit_up': False,
            'description': '首板尾盘止盈止损策略'
        }
        g.sell_conditions[stock] = condition
        if stock not in g.sb_sell_conditions:
            g.sb_sell_conditions[stock] = {}
        _strategy_log.info(f"【卖出策略-首板】{stock}: 开盘{(open_price/pre_close-1)*100:.2f}%")
        return condition

    open_pct = (open_price - pre_close) / pre_close * 100
    if open_pct > 5:
        condition = {'type': 'high_open', 'open_pct': open_pct, 'stop_loss': 0, 'time_exit': '14:30'}
    elif 0 <= open_pct <= 5:
        condition = {'type': 'flat_open', 'open_pct': open_pct, 'stop_loss': -2, 'time_exit': '10:00'}
    elif -2 <= open_pct < 0:
        condition = {'type': 'low_open', 'open_pct': open_pct, 'stop_loss': -3, 'time_exit': '10:00'}
    else:
        condition = {'type': 'extreme_low', 'open_pct': open_pct, 'immediate_sell': True}

    g.sell_conditions[stock] = condition
    mode_str = '一进二' if mode == 'yje' else '弱转强'
    _strategy_log.info(f"【卖出策略-{mode_str}】{stock}: 开盘{open_pct:.2f}% → {condition['type']}")
    return condition

def sell_check_open(context):
    current_data = get_current_data()
    for stock in list(context.portfolio.positions):
        stock = str(stock)
        if context.portfolio.positions[stock].closeable_amount == 0:
            continue

        open_price = current_data[stock].day_open
        hist = attribute_history(stock, 1, '1d', ['close'], skip_paused=False)
        if len(hist) == 0:
            continue
        pre_close = hist['close'][0]
        open_pct = (open_price - pre_close) / pre_close * 100
        mode = g.position_modes.get(stock, 'unknown')

        if mode == 'qs' and open_pct <= -5:
            if open_price > current_data[stock].low_limit:
                _strategy_log.info(f"【趋势股-开盘止损】{stock} 低开{open_pct:.2f}%")
                record_qs_sell(context, stock, '开盘止损-5%', open_price)
                continue

        if mode == 'sb' and open_pct <= -3:
            if open_price > current_data[stock].low_limit:
                _strategy_log.info(f"【首板-开盘止损】{stock} 低开{open_pct:.2f}%")
                record_sb_sell(context, stock, '开盘止损-3%', open_price)
                continue

        condition = initialize_sell_conditions(stock, open_price, pre_close, context)

        if condition.get('type') == 'extreme_low':
            if open_price > current_data[stock].low_limit:
                mode_str = '一进二' if mode == 'yje' else ('弱转强' if mode == 'rzq' else mode)
                _strategy_log.info(f"【大幅低开-立即卖出-{mode_str}】{stock} 低开{condition['open_pct']:.2f}%")
                profit_pct = None
                if mode == 'yje':
                    profit_pct = (open_price - context.portfolio.positions[stock].avg_cost) / \
                        context.portfolio.positions[stock].avg_cost * 100
                if _close_position_logged(context, stock, '开盘大幅低开卖出'):
                    if profit_pct is not None:
                        record_yje_trade_result(context, stock, profit_pct)
                    g.position_modes.pop(stock, None)
                    g.sell_conditions.pop(stock, None)
        elif open_price >= current_data[stock].high_limit * 0.995:
            _strategy_log.info(f"【竞价涨停-{mode}】{stock} 开盘涨停，继续持有")
            if stock not in g.sell_conditions:
                g.sell_conditions[stock] = {}
            g.sell_conditions[stock]['is_limit_up'] = True
            if mode == 'sb':
                g.sb_sell_conditions[stock] = {'was_limit_up': True}

def sell_realtime_monitor(context):
    current_data = get_current_data()
    for stock in list(context.portfolio.positions):
        stock = str(stock)
        if g.sell_conditions.get(stock, {}).get('is_limit_up'):
            continue

        condition = g.sell_conditions.get(stock, {})
        if not condition:
            continue

        curr_price = current_data[stock].last_price
        pre_close = _cached_previous_close(stock, context)
        if pre_close is None:
            continue
        curr_pct = (curr_price - pre_close) / pre_close * 100 if pre_close else 0

        position = context.portfolio.positions.get(stock)
        cost = position.avg_cost if position else pre_close
        profit_pct = (curr_price - cost) / cost * 100 if cost else 0
        mode = g.position_modes.get(stock, 'unknown')

        if mode == 'qs':
            # 【ATR14跟踪止损逻辑 - 替换原有阶梯止盈】

            # 1. 检查是否需要初始化（防止盘中买入未初始化）
            if stock not in g.qs_atr_stops:
                try:
                    atr = calculate_atr_for_qs(stock, context)
                    if atr:
                        initial_stop = cost - (atr * g.qs_atr_multiplier)
                        g.qs_atr_stops[stock] = initial_stop
                        g.qs_position_highs[stock] = cost
                except:
                    pass

            # 2. 更新持仓最高价（用于跟踪止损）
            if stock not in g.qs_position_highs:
                g.qs_position_highs[stock] = curr_price
            else:
                g.qs_position_highs[stock] = max(g.qs_position_highs[stock], curr_price)

            # 3. 计算新的跟踪止损价（只能上移，不能下移）
            try:
                atr = calculate_atr_for_qs(stock, context)
                if atr:
                    new_stop = g.qs_position_highs[stock] - (atr * g.qs_atr_multiplier)
                    if new_stop > g.qs_atr_stops.get(stock, 0):
                        old_stop = g.qs_atr_stops[stock]
                        g.qs_atr_stops[stock] = new_stop
                        _strategy_log.info(f"【趋势股-ATR止损上移】{stock} | {old_stop:.2f} -> {new_stop:.2f} | "
                                 f"最高价:{g.qs_position_highs[stock]:.2f} | ATR:{atr:.3f}")
            except:
                pass

            # 4. 检查是否触发止损
            current_stop = g.qs_atr_stops.get(stock, 0)
            if current_stop > 0 and curr_price <= current_stop:
                loss_pct = (curr_price - cost) / cost * 100
                _strategy_log.warn(f"【趋势股-ATR止损触发】{stock} | 当前:{curr_price:.2f} | "
                         f"止损价:{current_stop:.2f} | 亏损:{loss_pct:.2f}% | "
                         f"最高:{g.qs_position_highs.get(stock, cost):.2f}")
                record_qs_sell(context, stock,
                               f'ATR14止损(最高{g.qs_position_highs.get(stock, cost):.2f})',
                               curr_price)
                continue

            # 5. 保留强制时间止损（第10天强制卖出）
            hold_days, _, _ = get_qs_hold_days_and_target(stock, context)
            if hold_days >= g.qs_max_hold_days:
                record_qs_sell(context, stock,
                               f'持有满{g.qs_max_hold_days}天强制卖出',
                               curr_price)
                continue


        if mode == 'sb':
            if profit_pct <= -3.0:
                _strategy_log.info(f"【首板止损-跌破-3%】{stock} 盈亏:{profit_pct:.2f}%")
                record_sb_sell(context, stock, '盘中止损:跌破-3%', curr_price)
                continue

            if curr_price >= current_data[stock].high_limit * 0.998:
                if stock not in g.sb_sell_conditions:
                    g.sb_sell_conditions[stock] = {}
                g.sb_sell_conditions[stock]['was_limit_up'] = True
                if stock not in g.sell_conditions:
                    g.sell_conditions[stock] = {}
                g.sell_conditions[stock]['is_limit_up'] = True
                _strategy_log.info(f"【首板封板】{stock} 成功封板，持有")
                continue

        mode_str = '一进二' if mode == 'yje' else '弱转强'

        if profit_pct <= -3.0:
            _strategy_log.info(f"【盘中止损-{mode_str}】{stock} 盈亏:{profit_pct:.2f}% 跌破-3%")
            if _close_position_logged(context, stock, f'盘中止损-{mode_str}'):
                if mode == 'yje':
                    record_yje_trade_result(context, stock, profit_pct)
                g.position_modes.pop(stock, None)
                g.sell_conditions.pop(stock, None)
            continue

        if curr_price >= current_data[stock].high_limit * 0.995:
            _strategy_log.info(f"【盘中涨停-{mode_str}】{stock} 封板，持有")
            if stock not in g.sell_conditions:
                g.sell_conditions[stock] = {}
            g.sell_conditions[stock]['is_limit_up'] = True
            continue

def sell_30min_check(context):
    """
    30分钟检查改为空函数，逻辑移至尾盘检查
    """
    pass

def sell_check_afternoon(context):
    """
    尾盘检查：未涨停+跌破-3%（适用于一进二、首板、弱转强）
    """
    current_data = get_current_data()
    current_time = context.current_dt
    hour = current_time.hour
    minute = current_time.minute

    if not (hour == 14 and minute >= 30):
        return

    for stock in list(context.portfolio.positions):
        stock = str(stock)
        mode = g.position_modes.get(stock, 'unknown')

        if mode == 'qs':
            hold_days, _, _ = get_qs_hold_days_and_target(stock, context)
            if hold_days >= g.qs_max_hold_days:
                record_qs_sell(context, stock,
                                 f'持有满{g.qs_max_hold_days}天强制卖出',
                                 current_data[stock].last_price)
            elif current_data[stock].last_price <= current_data[stock].low_limit * 1.005:
                record_qs_sell(context, stock, '尾盘跌停', current_data[stock].last_price)
            continue

        if mode not in ['yje', 'rzq', 'sb']:
            continue

        position = context.portfolio.positions.get(stock)
        if not position:
            continue

        curr_price = current_data[stock].last_price
        high_limit = current_data[stock].high_limit
        cost = position.avg_cost
        profit_pct = (curr_price - cost) / cost * 100 if cost > 0 else 0

        if curr_price >= high_limit * 0.995:
            _strategy_log.info(f"【尾盘-{mode}】{stock} 封板，持有过夜")
            if stock not in g.sell_conditions:
                g.sell_conditions[stock] = {}
            g.sell_conditions[stock]['is_limit_up'] = True
            if mode == 'sb':
                g.sb_sell_conditions[stock] = {'was_limit_up': True}
            continue

        mode_str = '一进二' if mode == 'yje' else ('弱转强' if mode == 'rzq' else '首板')
        if profit_pct > 0:
            _strategy_log.info(f"【尾盘止盈-{mode_str}】{stock} 未涨停，盈亏:{profit_pct:.2f}%")
        else:
            _strategy_log.info(f"【尾盘止损-{mode_str}】{stock} 未涨停，盈亏:{profit_pct:.2f}%")

        if mode == 'yje':
            if _close_position_logged(context, stock, '尾盘未涨停'):
                record_yje_trade_result(context, stock, profit_pct)
                g.position_modes.pop(stock, None)
                g.sell_conditions.pop(stock, None)
        elif mode == 'sb':
            record_sb_sell(context, stock, f'尾盘未涨停{profit_pct:.2f}%', curr_price)
        else:
            if _close_position_logged(context, stock, '尾盘未涨停'):
                g.position_modes.pop(stock, None)
                g.sell_conditions.pop(stock, None)

def check_concept_implosion(context):
    try:
        current_data = get_current_data()
        holdings = [str(s) for s in context.portfolio.positions.keys()]
        if not holdings:
            return

        holding_concepts = {}
        for stock in holdings:
            try:
                stock = str(stock)
                # 【修复】增加None检查
                stock_info = get_security_info(stock)
                if stock_info is None or not hasattr(stock_info, 'concepts'):
                    continue
                concepts = stock_info.concepts
                holding_concepts[stock] = [c['name'] for c in concepts if c['name'] not in
                                           ['转融券标的', '融资融券', '深股通', '沪股通', '国企改革']]
            except:
                continue

        if not holding_concepts:
            return

        concept_implosion = {}
        intraday_high_map = _intraday_highs(context, holdings)

        for stock in holdings:
            if stock not in holding_concepts:
                continue

            high_limit = current_data[stock].high_limit
            if high_limit == 0:
                continue

            max_high = intraday_high_map.get(stock)
            if max_high is None:
                continue
            if (max_high >= high_limit * 0.998 and
                    current_data[stock].last_price < high_limit * 0.995):
                for concept in holding_concepts[stock]:
                    concept_implosion[concept] = concept_implosion.get(concept, 0) + 1

        submitted_stocks = set()
        for concept, count in concept_implosion.items():
            if count >= 3:
                event_key = f'{context.current_dt.date()}|{concept}'
                if event_key not in g.concept_implosion_log_keys:
                    g.concept_implosion_log_keys[event_key] = count
                    _strategy_log.warning(
                        f"【概念炸板风控】{concept} 炸板{count}只，触发批量卖出！"
                    )
                for stock in holdings:
                    if stock in submitted_stocks:
                        continue
                    if stock in holding_concepts and concept in holding_concepts[stock]:
                        if stock in context.portfolio.positions:
                            mode = g.position_modes.get(stock, 'unknown')
                            _strategy_log.info(f"【炸板卖出】{stock} ({concept}) [{mode}]")
                            if mode == 'yje':
                                profit_pct = (current_data[stock].last_price -
                                            context.portfolio.positions[stock].avg_cost) / \
                                            context.portfolio.positions[stock].avg_cost * 100
                                submitted = bool(_close_position_logged(
                                    context, stock, f'概念炸板({concept})'
                                ))
                                if submitted:
                                    record_yje_trade_result(context, stock, profit_pct)
                                    g.position_modes.pop(stock, None)
                                    g.sell_conditions.pop(stock, None)
                            elif mode == 'qs':
                                submitted = bool(record_qs_sell(
                                    context, stock, f'概念炸板({concept})',
                                    current_data[stock].last_price,
                                ))
                            elif mode == 'sb':
                                submitted = bool(record_sb_sell(
                                    context, stock, f'概念炸板({concept})',
                                    current_data[stock].last_price,
                                ))
                            else:
                                submitted = bool(_close_position_logged(
                                    context, stock, f'概念炸板({concept})'
                                ))
                                if submitted:
                                    g.position_modes.pop(stock, None)
                                    g.sell_conditions.pop(stock, None)
                            if submitted:
                                submitted_stocks.add(stock)
    except Exception as e:
        _strategy_log.error(f"【炸板监控错误】{str(e)}")

# ============================================================================
# 第十部分：持仓管理与日志
# ============================================================================

def print_position_info(context):
    all_value = context.portfolio.total_value
    current_data = get_current_data()
    _strategy_log.info("========== 持仓信息（四模块）==========")
    mode_label = {'yje': '一进二', 'rzq': '弱转强', 'qs': '趋势股', 'sb': '首板'}

    sb_positions = []
    other_positions = []

    for position in list(context.portfolio.positions.values()):
        securities = str(position.security)
        mode = g.position_modes.get(securities, 'unknown')
        if mode == 'sb':
            sb_positions.append(position)
        else:
            other_positions.append(position)

    if sb_positions:
        _strategy_log.info(f"【首板持仓-独立30%仓位】共{len(sb_positions)}只:")
        for position in sb_positions:
            securities = str(position.security)
            cost = position.avg_cost
            price = position.price
            ret = 100 * (price / cost - 1)
            value = position.value
            amount = position.total_amount

            was_hl = g.sb_sell_conditions.get(securities, {}).get('was_limit_up', False)
            status = "已封板" if was_hl else "未涨停"

            _strategy_log.info(f"  {securities}({get_security_info(securities).display_name}) [{status}]")
            _strategy_log.info(f"    成本:{cost:.2f} 现价:{price:.2f} 收益:{ret:.2f}% 市值:{value:.2f}元")

    if other_positions:
        _strategy_log.info(f"【其他持仓】共{len(other_positions)}只:")
        for position in other_positions:
            securities = str(position.security)
            cost = position.avg_cost
            price = position.price
            ret = 100 * (price / cost - 1)
            value = position.value
            amount = position.total_amount
            mode = g.position_modes.get(securities, 'unknown')
            mode_str = mode_label.get(mode, '未知')

            today_profit_str = ""
            if securities in g.today_bought:
                buy_price = g.today_bought[securities]
                today_profit = (price - buy_price) / buy_price * 100
                today_profit_str = f" | 当日盈亏:{today_profit:.2f}%"

            if mode == 'qs':
                hold_days, target, stop = get_qs_hold_days_and_target(securities, context)
                remaining = g.qs_max_hold_days - hold_days
                combo_name = g.qs_combo_info.get(securities, {}).get('combo_name', '未知')
                _strategy_log.info(f"  {securities}({get_security_info(securities).display_name}) "
                        f"模式:{mode_str}[{combo_name}] 第{hold_days}天"
                        f"(目标{target}%/止损{stop}%/余{remaining}天){today_profit_str}")
            else:
                _strategy_log.info(f"  {securities}({get_security_info(securities).display_name}) "
                        f"模式:{mode_str}{today_profit_str}")

            _strategy_log.info(f"    成本:{cost:.2f} 现价:{price:.2f} 收益:{ret:.2f}% "
                    f"持仓:{amount}股 市值:{value:.2f}元")

    _strategy_log.info(f"总资产: {all_value:.2f}")


def print_daily_audit(context):
    """收盘后只读汇总资产、持仓和四模式运行状态。"""
    positions = [
        position for position in context.portfolio.positions.values()
        if int(getattr(position, 'total_amount', 0) or 0) > 0
    ]
    total_value = float(getattr(context.portfolio, 'total_value', 0) or 0)
    cash = float(getattr(context.portfolio, 'available_cash', 0) or 0)
    market_value = max(0.0, total_value - cash)
    mode_counts = {'yje': 0, 'rzq': 0, 'qs': 0, 'sb': 0, 'unknown': 0}
    for position in positions:
        security = str(position.security)
        mode = g.position_modes.get(security, 'unknown')
        mode_counts[mode if mode in mode_counts else 'unknown'] += 1
    target_count = sum(len(getattr(g, name, []) or []) for name in (
        'yje_stock_list', 'rzq_stock_list', 'qs_stock_list', 'sb_stock_list'
    ))
    state = '紧急风控' if getattr(g, 'emergency_cut', False) else '正常运行'
    _strategy_log.event(
        '日终审计', '运行完成',
        f'总资产={total_value:.2f}元 | 持仓市值={market_value:.2f}元 | '
        f'现金={cash:.2f}元 | 持仓={len(positions)}只 | 状态={state} | '
        f'模式=一进二{mode_counts["yje"]}/弱转强{mode_counts["rzq"]}/'
        f'趋势股{mode_counts["qs"]}/首板{mode_counts["sb"]} | 候选={target_count}只',
        icon='💼',
    )

def daily_reset(context):
    _reset_runtime_data_cache()
    current_positions = set([str(s) for s in context.portfolio.positions.keys()])

    yje_f = sum(1 for s in g.failed_buy_list if str(s) in [str(x) for x in g.yje_stock_list])
    rzq_f = sum(1 for s in g.failed_buy_list if str(s) in [str(x) for x in g.rzq_stock_list])
    qs_f = sum(1 for s in g.failed_buy_list if str(s) in [str(x) for x in g.qs_stock_list])
    sb_f = sum(1 for s in g.failed_buy_list if str(s) in [str(x) for x in g.sb_stock_list])

    if g.failed_buy_list:
        _strategy_log.info(f"【当日统计】买入失败:{len(g.failed_buy_list)}只 "
                 f"(一进二:{yje_f} 弱转强:{rzq_f} 趋势股:{qs_f} 首板:{sb_f})")

    if g.yje_trade_stats:
        recent_3d = [t for t in g.yje_trade_stats
                     if (context.current_dt.date() - t['date']).days <= 3]
        if recent_3d:
            sc = sum(1 for t in recent_3d if t['success'])
            _strategy_log.info(f"【一进二近3日】{len(recent_3d)}笔 成功{sc}笔 胜率{sc/len(recent_3d):.1%}")

    print_sb_trade_summary(context)
    print_qs_trade_summary(context)

    g.position_modes = {str(k): v for k, v in g.position_modes.items() if str(k) in current_positions}
    g.sell_conditions = {str(k): v for k, v in g.sell_conditions.items() if str(k) in current_positions}
    g.sb_sell_conditions = {str(k): v for k, v in g.sb_sell_conditions.items()
                            if str(k) in current_positions}

    g.failed_buy_list = []
    g.opening_prices = {}
    g.last_check_minute = -1
    g.last_concept_check = -1
    g.last_weibi_check_minute = -1
    g.last_sb_weibi_minute = -1
    g.emergency_cut = False
    g.temp_position_ratio = 1.0
    g.today_bought = {}
    # 指标缓存仅服务当日选股，收盘后清理以控制模拟盘持久化体积。
    g._micro_data_cache = {}
    g._cache_date = None
    g.sell_skip_log_keys = {}
    g.buy_skip_log_keys = {}
    g.concept_implosion_log_keys = {}
    g.sb_limit_log_signature = None
    _clear_daily_candidate_lists()
    g.concept_pool = []
    g.preselection_state = {}
    g.preselection_ready_date = None
    g.selection_ready_date = None

    _strategy_log.info(f"【每日重置】完成，当前持仓: {len(current_positions)}只")

# ============================================================================
# 第十一部分：初始化（四模块）
# ============================================================================

def initialize(context):
    set_option('use_real_price', True)
    if getattr(getattr(context, 'run_params', None), 'type', None) == 'backtest':
        set_option('avoid_future_data', True)  # 仅回测环境支持
    set_option("match_by_signal", True)
    _apply_log_levels()
    _strategy_log.event('运行状态', '初始化开始',
                        f"交易通道={'实盘推送' if ENABLE_LIVE_TRADING else '仅模拟交易'} | 订单回执日志=开启",
                        icon='🔄')
    set_order_cost(OrderCost(
        open_tax=0, close_tax=0.001,
        open_commission=0.0003, close_commission=0.0003,
        close_today_commission=0, min_commission=5), type='stock')
    set_slippage(FixedSlippage(0.005))

    # ---- 配置区统一应用（配置定义位于文件顶部） ----
    _apply_config()

    # ---- 跨日运行状态：只补缺失字段，不覆盖已有状态 ----
    _ensure_runtime_state()
    _migrate_runtime_state()

    # ---- 定时任务 ----
    _register_tasks()

    _strategy_log.event('运行状态', '初始化完成',
                        f'总持仓上限={g.max_positions} | 首板监控池={g.sb_max_positions}只',
                        icon='✅')
    _strategy_log.debug("模块1：一进二（yje）新评分体系100分制 | 模块2：弱转强（rzq）6日涨幅<50% 竞价2-6%")
    _strategy_log.debug("模块3：趋势股（qs）市值300-1500亿+涨停前4概念 | 模块4：首板（sb）三重过滤+均线斜率过滤")
    _strategy_log.debug(f"总持仓上限:{g.max_positions} | 首板独立30%仓位（{g.sb_max_positions}只）")
    _strategy_log.debug("【首板新增】三重过滤：①总市值>50亿 ②0<PE<150 ③60日日均成交额>1亿")
    _strategy_log.debug("【首板新增】MA5斜率>0 | MA20斜率>=0 | VOL_MA5斜率>0")
    _strategy_log.debug("【卖出策略】尾盘未涨停 或 跌破-3%（一进二/首板/弱转强统一）")
    _strategy_log.debug("【关键修复】filter_by_concept增加get_security_info空值检查，防止NoneType错误")
    _strategy_log.debug("【关键修复】增加normalize_stock_code函数，标准化股票代码格式")
    _strategy_log.debug("【关键修复】增加filter_invalid_stocks函数，过滤005开头无效代码")
    _strategy_log.debug("【关键修复】增加ComboEvaluator.evaluate_combo_6方法，修复六维评估缺失")


def process_initialize(context):
    """模拟盘每日进程重启时补齐状态并重新注册任务。"""
    _reset_runtime_data_cache()
    _apply_log_levels()
    _apply_config()
    _ensure_runtime_state()
    _migrate_runtime_state()
    _register_tasks()
    _strategy_log.event(
        '运行状态', '进程初始化完成',
        f'交易通道={"实盘推送" if ENABLE_LIVE_TRADING else "仅模拟交易"}',
        icon='✅',
    )


def after_code_changed(context):
    """代码更新后重建任务，保留已有运行状态。"""
    _reset_runtime_data_cache()
    _apply_log_levels()
    _apply_config()
    _ensure_runtime_state()
    _migrate_runtime_state()
    _register_tasks()
    apply_configured_cash_events(context)
    _strategy_log.event(
        '运行状态', '代码更新完成',
        f'交易通道={"实盘推送" if ENABLE_LIVE_TRADING else "仅模拟交易"}',
        icon='🔄',
    )

def handle_data(context, data):
    current_minute = context.current_dt.hour * 60 + context.current_dt.minute
    if current_minute == g.last_check_minute:
        return
    g.last_check_minute = current_minute

    hour = context.current_dt.hour
    minute = context.current_dt.minute

    is_morning = (hour == 9 and minute >= 30) or (hour == 10) or (hour == 11 and minute <= 30)
    is_afternoon = (hour == 13) or (hour == 14 and minute <= 30)
    is_trade_time = is_morning or is_afternoon

    if is_trade_time and current_minute != g.last_concept_check:
        g.last_concept_check = current_minute
        check_concept_implosion(context)

    is_weibi_time = (hour == 9 and minute >= 25) or (hour == 10) or (hour == 11 and minute <= 30)
    if is_weibi_time and _selection_ready_for_today(context):
        if 'yje' in g.priority_config:
            check_weibi_signal(context)
            check_compatibility_buy(context)
        if 'sb' in g.priority_config:
            check_sb_weibi_signal(context)
            check_sb_compatibility_buy(context)

    if is_trade_time:
        sell_realtime_monitor(context)

    if is_trade_time and minute % 5 == 0:
        check_intraday_volatility(context)
