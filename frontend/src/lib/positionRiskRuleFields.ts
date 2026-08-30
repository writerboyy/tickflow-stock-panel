export type PositionRiskRuleField = {
  key: string
  label: string
  suffix: string
  min: number
  max?: number
  step: number
  percent?: boolean
  defaultValue?: number
  type?: 'number' | 'select'
  options?: Array<[string, string]>
}

export const LARGE_ORDER_FIELDS: PositionRiskRuleField[] = [
  { key: 'window_seconds', label: '观察窗口', suffix: '秒', min: 1, step: 1, defaultValue: 60 },
  { key: 'min_samples', label: '最少样本', suffix: '笔', min: 1, step: 1, defaultValue: 7 },
  { key: 'min_amount', label: '最低单笔金额', suffix: '元', min: 0, step: 100_000, defaultValue: 1_000_000 },
  { key: 'mad_multiplier', label: 'MAD 倍数', suffix: '倍', min: 0, step: 0.5, defaultValue: 3 },
  { key: 'min_z_score', label: '最低 Z 分数', suffix: '', min: 0, step: 0.5, defaultValue: 2.5 },
  { key: 'direction_ratio', label: '同向成交占比', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.65 },
]

export const POSITION_RISK_RULE_FIELDS: Record<string, PositionRiskRuleField[]> = {
  stop_loss: [
    { key: 'mode', label: '保护模式', suffix: '', min: 0, step: 1, type: 'select', options: [['fixed', '固定百分比'], ['atr', 'ATR 波动率'], ['max_fixed_atr', '固定与 ATR 取更严格']] },
    { key: 'threshold', label: '亏损阈值', suffix: '%', min: -100, max: 0, step: 1, percent: true, defaultValue: -0.10 },
    { key: 'atr_multiple', label: 'ATR14 倍数', suffix: '倍', min: 0.1, step: 0.1, defaultValue: 1.5 },
    { key: 'fees_buffer', label: '费用滑点缓冲', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  take_profit: [
    { key: 'threshold', label: '目标收益率', suffix: '%', min: 0, max: 500, step: 1, percent: true, defaultValue: 0.10 },
    { key: 'fees_buffer', label: '费用滑点缓冲', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  take_profit_ladder: [
    { key: 'first_r', label: '第一阶段 R', suffix: 'R', min: 0.1, step: 0.1, defaultValue: 1 },
    { key: 'first_action_pct', label: '第一阶段减仓', suffix: '%', min: 0, max: 100, step: 10, defaultValue: 30 },
    { key: 'second_r', label: '第二阶段 R', suffix: 'R', min: 0.2, step: 0.1, defaultValue: 1.5 },
    { key: 'second_action_pct', label: '第二阶段减仓', suffix: '%', min: 0, max: 100, step: 10, defaultValue: 30 },
    { key: 'runner_pct', label: '剩余仓位', suffix: '%', min: 0, max: 100, step: 10, defaultValue: 40 },
    { key: 'fees_buffer', label: '成本保护缓冲', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'break_even_r', label: '保本启动 R', suffix: 'R', min: 0.1, step: 0.1, defaultValue: 1 },
    { key: 'lock_profit_r', label: '锁定收益 R', suffix: 'R', min: 0, step: 0.1, defaultValue: 0.5 },
    { key: 'runner_atr_multiple', label: '剩余 ATR 倍数', suffix: '倍', min: 0.1, step: 0.1, defaultValue: 1.5 },
  ],
  trailing_drawdown: [
    { key: 'activation_gain', label: '启动盈利', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.05 },
    { key: 'threshold', label: '高点回撤', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  intraday_peak_pullback: [
    { key: 'activation_r', label: '启动盈利', suffix: 'R', min: 0.1, step: 0.1, defaultValue: 1 },
    { key: 'pullback_atr_multiple', label: '回撤 ATR 倍数', suffix: '倍', min: 0.1, step: 0.1, defaultValue: 1.5 },
    { key: 'confirm_bars', label: '闭合 5 分钟确认', suffix: '根', min: 1, max: 5, step: 1, defaultValue: 2 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  sector_leader_weakening: [
    { key: 'correlation_window_days', label: '相关性窗口', suffix: '交易日', min: 20, max: 60, step: 1, defaultValue: 20 },
    { key: 'min_correlation', label: '相关性阈值', suffix: '', min: -1, max: 1, step: 0.05, defaultValue: 0.5 },
    { key: 'decline_delta', label: '相关性下降', suffix: '', min: 0.05, max: 1, step: 0.05, defaultValue: 0.2 },
    { key: 'min_correlation_samples', label: '最少日线样本', suffix: '日', min: 20, max: 60, step: 1, defaultValue: 20 },
    { key: 'underperformance_gap', label: '5 分钟相对跑输', suffix: '%', min: -10, max: 0, step: 0.1, percent: true, defaultValue: -0.003 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  volume_price_divergence: [
    { key: 'lookback_bars', label: '观察 5 分钟 K', suffix: '根', min: 10, max: 60, step: 1, defaultValue: 24 },
    { key: 'min_peak_separation', label: '双峰最小间隔', suffix: '根', min: 2, max: 10, step: 1, defaultValue: 2 },
    { key: 'min_peak_prominence_atr', label: '创新高 ATR', suffix: '倍', min: 0.1, max: 3, step: 0.1, defaultValue: 0.5 },
    { key: 'max_peak_volume_ratio', label: '第二峰量比上限', suffix: '%', min: 1, max: 100, step: 5, percent: true, defaultValue: 0.8 },
    { key: 'confirm_bars', label: '闭合 K 确认', suffix: '根', min: 1, max: 5, step: 1, defaultValue: 2 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  opening_volume_selloff: [
    { key: 'baseline_sessions', label: '同窗历史基准', suffix: '交易日', min: 20, max: 60, step: 1, defaultValue: 20 },
    { key: 'volume_multiple', label: '早盘量比', suffix: '倍', min: 1, max: 10, step: 0.1, defaultValue: 2 },
    { key: 'price_confirmations', label: '价格确认', suffix: '项', min: 1, max: 3, step: 1, defaultValue: 2 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  next_day_gap_down: [
    { key: 'threshold', label: '跳空低开阈值', suffix: '%', min: -20, max: 0, step: 0.5, percent: true, defaultValue: -0.03 },
    { key: 'confirm_minutes', label: '确认分钟', suffix: '分', min: 1, max: 5, step: 1, defaultValue: 1 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  next_day_gap_up_take_profit: [
    { key: 'threshold', label: '目标收益', suffix: '%', min: 0, max: 100, step: 0.5, percent: true, defaultValue: 0.04 },
    { key: 'fees_buffer', label: '费用滑点缓冲', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'confirm_minutes', label: '确认分钟', suffix: '分', min: 1, max: 5, step: 1, defaultValue: 1 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  opening_range_failure: [
    { key: 'window_minutes', label: '开盘区间', suffix: '分', min: 5, max: 15, step: 5, defaultValue: 5 },
    { key: 'reference', label: '失败基准', suffix: '', min: 0, step: 1, type: 'select', options: [['opening_range_low', '开盘区间低点'], ['vwap', 'VWAP']] },
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'confirm_bars', label: '确认根数', suffix: '根', min: 1, max: 5, step: 1, defaultValue: 1 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  t_plus_one_exit: [
    { key: 'max_holding_days', label: '最长持仓', suffix: '交易日', min: 1, max: 20, step: 1, defaultValue: 1 },
    { key: 'close_before_minutes', label: '收盘前退出', suffix: '分', min: 0, max: 120, step: 5, defaultValue: 15 },
    { key: 'min_gain', label: '最低收益', suffix: '%', min: -100, max: 100, step: 0.5, percent: true, defaultValue: -1 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  ma5_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 },
  ],
  ma10_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  ma20_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  five_minute_drawdown: [
    { key: 'threshold', label: '回撤阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.03 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  vwap_breakdown: [
    { key: 'buffer', label: '负偏离阈值', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.01 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 30 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  structure_stop: [
    { key: 'reference', label: '结构基准', suffix: '', min: 0, step: 1, type: 'select', options: [['vwap', 'VWAP'], ['ema20', 'EMA20'], ['five_minute_low', '5 分钟前低'], ['opening_range_low', '开盘区间低点']] },
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'confirm_bars', label: '确认根数', suffix: '根', min: 1, max: 10, step: 1, defaultValue: 2 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  atr_protection: [
    { key: 'activation_gain', label: '启动盈利', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.02 },
    { key: 'atr_multiple', label: 'ATR 倍数', suffix: '倍', min: 0.1, step: 0.1, defaultValue: 2 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  time_stop: [
    { key: 'max_minutes', label: '最长持仓', suffix: '分', min: 1, step: 5, defaultValue: 120 },
    { key: 'min_gain', label: '最低收益', suffix: '%', min: -100, max: 100, step: 0.5, percent: true, defaultValue: 0 },
    { key: 'close_before_minutes', label: '收盘前提醒', suffix: '分', min: 0, max: 120, step: 5, defaultValue: 15 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  broken_limit_up: [{ key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 }],
  resealed_limit_up: [{ key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 }],
  sealed_order_shrink_50: [
    { key: 'threshold', label: '减少阈值', suffix: '%', min: 0, max: 100, step: 5, percent: true, defaultValue: 0.50 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  sealed_order_shrink_80: [
    { key: 'threshold', label: '减少阈值', suffix: '%', min: 0, max: 100, step: 5, percent: true, defaultValue: 0.80 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, percent: false, defaultValue: 50 },
  ],
  limit_down: [{ key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 }],
  continuous_outflow: [
    { key: 'direction_ratio', label: '卖出占比', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.65 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 10 },
  ],
  orderbook_imbalance: [
    { key: 'threshold', label: '失衡阈值', suffix: '', min: -1, max: 0, step: 0.05, defaultValue: -0.35 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 10 },
  ],
  fund_flow_pressure: [
    { key: 'min_evidence', label: '最少资金信号', suffix: '项', min: 2, max: 3, step: 1, defaultValue: 2 },
    { key: 'sustain_seconds', label: '确认持续时间', suffix: '秒', min: 1, step: 5, defaultValue: 30 },
    { key: 'recovery_seconds', label: '恢复持续时间', suffix: '秒', min: 1, step: 5, defaultValue: 60 },
    { key: 'cooldown_seconds', label: '同组冷却', suffix: '秒', min: 0, step: 60, defaultValue: 900 },
    { key: 'price_buffer', label: '价格确认幅度', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'strong_price_drop', label: '严重一分钟跌幅', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.01 },
    { key: 'recovery_sell_ratio', label: '卖压恢复占比', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.55 },
    { key: 'recovery_imbalance', label: '盘口恢复阈值', suffix: '', min: -1, max: 1, step: 0.05, defaultValue: -0.15 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
    { key: 'strong_action_pct', label: '强触发执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  daily_equity_loss: [
    { key: 'threshold', label: '亏损阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.03 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  equity_drawdown: [
    { key: 'threshold', label: '回撤阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  unrealized_loss: [
    { key: 'threshold', label: '浮亏阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  total_exposure: [
    { key: 'threshold', label: '仓位上限', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.95 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  symbol_concentration: [
    { key: 'threshold', label: '单票上限', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.30 },
    { key: 'target_pct', label: '降至', suffix: '%', min: 0, max: 100, step: 1, defaultValue: 30 },
  ],
  clustered_severe_events: [
    { key: 'count', label: '事件数量', suffix: '个', min: 1, step: 1, defaultValue: 3 },
    { key: 'window_seconds', label: '观察窗口', suffix: '秒', min: 1, step: 30, defaultValue: 300 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  quote_interruption: [
    { key: 'threshold_seconds', label: '中断阈值', suffix: '秒', min: 1, step: 1, defaultValue: 30 },
    { key: 'action_pct', label: '执行比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 },
  ],
  market_context: [
    { key: 'min_correlation', label: '最小相关性', suffix: '', min: -1, max: 1, step: 0.05, defaultValue: 0.5 },
    { key: 'sector_weakening', label: '板块弱化阈值', suffix: '%', min: -20, max: 20, step: 0.1, percent: true, defaultValue: -0.5 },
    { key: 'underperform_threshold', label: '个股跑输板块', suffix: '%', min: -20, max: 20, step: 0.1, percent: true, defaultValue: -1 },
    { key: 'min_flow_samples', label: '最少资金样本', suffix: '笔', min: 1, max: 100, step: 1, defaultValue: 3 },
    { key: 'normal_action_pct', label: '普通保护比例', suffix: '%', min: 0, max: 100, step: 5, defaultValue: 25 },
    { key: 'strong_action_pct', label: '强保护比例', suffix: '%', min: 0, max: 100, step: 5, defaultValue: 50 },
  ],
}
