import { useEffect, useState } from 'react'
import { Activity, ArrowRight, ChevronDown, Loader2, Save, Settings2, X } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Modal } from '@/components/Modal'
import { toast } from '@/components/Toast'
import { api, type PositionRiskOptions, type PositionRiskPortfolio } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

interface Props {
  open: boolean
  portfolio: PositionRiskPortfolio
  options: PositionRiskOptions | undefined
  onClose: () => void
}

const RULE_LABELS: Record<string, string> = {
  stop_loss: '成本止损', trailing_drawdown: '盈利后高点回撤',
  ma5_breakdown: '跌破 MA5', ma10_breakdown: '跌破 MA10', ma20_breakdown: '跌破 MA20',
  five_minute_drawdown: '5 分钟高点回撤', vwap_breakdown: '分时均价负偏离超限',
  broken_limit_up: '涨停炸板', resealed_limit_up: '涨停回封',
  sealed_order_shrink_50: '封单减少（一级）', sealed_order_shrink_80: '封单减少（二级）',
  limit_down: '跌停', large_buy: '大单买入', large_sell: '大单卖出',
  continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡',
  fund_flow_pressure: '资金卖压',
  daily_equity_loss: '当日权益亏损', equity_drawdown: '账户高点回撤',
  unrealized_loss: '持仓总浮亏', total_exposure: '总仓位上限',
  symbol_concentration: '单票仓位上限', clustered_severe_events: '严重事件聚集',
  quote_interruption: '行情中断',
}

const INDEPENDENT_RULE_GROUPS = [
  ['成本与趋势', ['stop_loss', 'trailing_drawdown', 'ma5_breakdown', 'ma10_breakdown', 'ma20_breakdown', 'five_minute_drawdown', 'vwap_breakdown']],
  ['涨跌停', ['broken_limit_up', 'resealed_limit_up', 'sealed_order_shrink_50', 'sealed_order_shrink_80', 'limit_down']],
  ['账户总控', ['daily_equity_loss', 'equity_drawdown', 'unrealized_loss', 'total_exposure', 'symbol_concentration', 'clustered_severe_events', 'quote_interruption']],
] as const

const FUND_EVIDENCE = [
  ['large_sell', '大单卖出', '异常卖出金额、方向占比和统计强度同时达标'],
  ['continuous_outflow', '连续净流出', '卖出方向成交额占比持续超过阈值'],
  ['orderbook_imbalance', '盘口失衡', '五档卖盘持续明显强于买盘'],
] as const

type DialogTab = 'independent' | 'combined' | 'builtin' | 'custom' | 'monitor'

export type PositionRiskRuleField = {
  key: string
  label: string
  suffix: string
  min: number
  max?: number
  step: number
  percent?: boolean
  defaultValue?: number
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
    { key: 'threshold', label: '亏损阈值', suffix: '%', min: -100, max: 0, step: 1, percent: true, defaultValue: -0.10 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 },
  ],
  trailing_drawdown: [
    { key: 'activation_gain', label: '启动盈利', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.05 },
    { key: 'threshold', label: '高点回撤', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  ma5_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 },
  ],
  ma10_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  ma20_breakdown: [
    { key: 'buffer', label: '跌破缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 5 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  five_minute_drawdown: [
    { key: 'threshold', label: '回撤阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.03 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  vwap_breakdown: [
    { key: 'buffer', label: '负偏离阈值', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.01 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 30 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  broken_limit_up: [{ key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 }],
  resealed_limit_up: [{ key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 }],
  sealed_order_shrink_50: [
    { key: 'threshold', label: '减少阈值', suffix: '%', min: 0, max: 100, step: 5, percent: true, defaultValue: 0.50 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  sealed_order_shrink_80: [
    { key: 'threshold', label: '减少阈值', suffix: '%', min: 0, max: 100, step: 5, percent: true, defaultValue: 0.80 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, percent: false, defaultValue: 50 },
  ],
  limit_down: [{ key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 100 }],
  continuous_outflow: [
    { key: 'direction_ratio', label: '卖出占比', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.65 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 10 },
  ],
  orderbook_imbalance: [
    { key: 'threshold', label: '失衡阈值', suffix: '', min: -1, max: 0, step: 0.05, defaultValue: -0.35 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 10 },
  ],
  fund_flow_pressure: [
    { key: 'min_evidence', label: '最少资金证据', suffix: '项', min: 2, max: 3, step: 1, defaultValue: 2 },
    { key: 'sustain_seconds', label: '确认持续时间', suffix: '秒', min: 1, step: 5, defaultValue: 30 },
    { key: 'recovery_seconds', label: '恢复持续时间', suffix: '秒', min: 1, step: 5, defaultValue: 60 },
    { key: 'cooldown_seconds', label: '同组冷却', suffix: '秒', min: 0, step: 60, defaultValue: 900 },
    { key: 'price_buffer', label: '价格确认幅度', suffix: '%', min: 0, max: 10, step: 0.1, percent: true, defaultValue: 0.002 },
    { key: 'strong_price_drop', label: '严重一分钟跌幅', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.01 },
    { key: 'recovery_sell_ratio', label: '卖压恢复占比', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.55 },
    { key: 'recovery_imbalance', label: '盘口恢复阈值', suffix: '', min: -1, max: 1, step: 0.05, defaultValue: -0.15 },
    { key: 'action_pct', label: '确认建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
    { key: 'strong_action_pct', label: '严重建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  daily_equity_loss: [
    { key: 'threshold', label: '亏损阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.03 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  equity_drawdown: [
    { key: 'threshold', label: '回撤阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  unrealized_loss: [
    { key: 'threshold', label: '浮亏阈值', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.08 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  total_exposure: [
    { key: 'threshold', label: '仓位上限', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.95 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  symbol_concentration: [
    { key: 'threshold', label: '单票上限', suffix: '%', min: 0, max: 100, step: 1, percent: true, defaultValue: 0.30 },
    { key: 'target_pct', label: '降至', suffix: '%', min: 0, max: 100, step: 1, defaultValue: 30 },
  ],
  clustered_severe_events: [
    { key: 'count', label: '事件数量', suffix: '个', min: 1, step: 1, defaultValue: 3 },
    { key: 'window_seconds', label: '观察窗口', suffix: '秒', min: 1, step: 30, defaultValue: 300 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 50 },
  ],
  quote_interruption: [
    { key: 'threshold_seconds', label: '中断阈值', suffix: '秒', min: 1, step: 1, defaultValue: 30 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 },
  ],
}

export function PositionRiskRulesDialog({ open, portfolio, options, onClose }: Props) {
  const queryClient = useQueryClient()
  const [template, setTemplate] = useState(portfolio.template)
  const [activeTab, setActiveTab] = useState<DialogTab>('independent')
  const [expandedRule, setExpandedRule] = useState<string | null>(null)
  const [showAdvancedPressure, setShowAdvancedPressure] = useState(false)

  useEffect(() => {
    if (open) {
      setTemplate(structuredClone(portfolio.template))
      setActiveTab('independent')
      setExpandedRule(null)
      setShowAdvancedPressure(false)
    }
  }, [open, portfolio.template])

  const mutation = useMutation({
    mutationFn: () => api.positionRiskUpdateTemplate(portfolio.revision, template),
    onSuccess: () => {
      toast('全局风控模板已更新', 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRisk })
      onClose()
    },
  })

  const toggleRule = (ruleId: string, enabled: boolean) => {
    setTemplate(previous => ({
      ...previous,
      rules: {
        ...previous.rules,
        [ruleId]: { ...(previous.rules[ruleId] ?? options?.rules[ruleId] ?? {}), enabled },
      },
    }))
  }

  const toggleRuleNotify = (ruleId: string, notify: boolean) => {
    setTemplate(previous => ({
      ...previous,
      rules: {
        ...previous.rules,
        [ruleId]: { ...(previous.rules[ruleId] ?? options?.rules[ruleId] ?? {}), notify },
      },
    }))
  }

  const updateRuleValue = (ruleId: string, key: string, value: number) => {
    setTemplate(previous => ({
      ...previous,
      rules: {
        ...previous.rules,
        [ruleId]: { ...(previous.rules[ruleId] ?? options?.rules[ruleId] ?? {}), [key]: value },
      },
    }))
  }

  const toggleSignal = (group: 'builtin' | 'custom', id: string, enabled: boolean, direction: string, label: string) => {
    setTemplate(previous => ({
      ...previous,
      signals: {
        ...previous.signals,
        [group]: {
          ...previous.signals[group],
          [id]: { ...(previous.signals[group][id] ?? {}), enabled, direction: previous.signals[group][id]?.direction ?? direction, label: previous.signals[group][id]?.label ?? label },
        },
      },
    }))
  }

  const updateSignal = (group: 'builtin' | 'custom' | 'monitor_rules', id: string, patch: Record<string, unknown>) => {
    setTemplate(previous => ({
      ...previous,
      signals: {
        ...previous.signals,
        [group]: {
          ...previous.signals[group],
          [id]: { ...(previous.signals[group][id] ?? {}), ...patch },
        },
      },
    }))
  }

  const ruleConfig = (ruleId: string) => template.rules[ruleId] ?? options?.rules[ruleId] ?? {}

  const displayValue = (value: unknown, percent = false) => {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) return 0
    return percent ? Number((numeric * 100).toFixed(6)) : numeric
  }

  const renderFields = (ruleId: string, fields: PositionRiskRuleField[], className = 'sm:grid-cols-2') => {
    const config = ruleConfig(ruleId)
    return (
      <div className={`grid gap-x-3 gap-y-2 ${className}`}>
        {fields.map(field => {
          const value = displayValue(config[field.key] ?? field.defaultValue ?? 0, field.percent)
          return (
            <label key={field.key} className="min-w-0 text-[10px] text-muted">
              <span>{field.label}</span>
              <span className="mt-1 flex h-8 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                <input
                  type="number"
                  min={field.min}
                  max={field.max}
                  step={field.step}
                  value={value}
                  disabled={config.enabled === false}
                  onChange={event => {
                    const next = Number(event.target.value)
                    if (Number.isFinite(next)) updateRuleValue(ruleId, field.key, field.percent ? next / 100 : next)
                  }}
                  className="min-w-0 flex-1 bg-transparent font-mono text-[11px] text-foreground outline-none disabled:opacity-50"
                />
                {field.suffix && <span className="ml-1 shrink-0">{field.suffix}</span>}
              </span>
            </label>
          )
        })}
      </div>
    )
  }

  const actionLabel = (ruleId: string) => {
    const config = ruleConfig(ruleId)
    if (ruleId === 'symbol_concentration') return `降至 ${Number(config.target_pct ?? 30)}%`
    return Number(config.action_pct ?? 0) > 0 ? `建议 ${config.action_pct}%` : '只提醒'
  }

  if (!open) return null
  const pressureConfig = ruleConfig('fund_flow_pressure')
  const minimumEvidence = Number(pressureConfig.min_evidence ?? 2)
  const pressureSustain = Number(pressureConfig.sustain_seconds ?? 30)
  const pressureAction = Number(pressureConfig.action_pct ?? 25)
  const strongPressureAction = Number(pressureConfig.strong_action_pct ?? 50)
  const strongDrop = displayValue(pressureConfig.strong_price_drop ?? 0.01, true)
  const pressurePrimaryFields = (POSITION_RISK_RULE_FIELDS.fund_flow_pressure ?? []).filter(field => [
    'min_evidence', 'sustain_seconds', 'price_buffer', 'strong_price_drop', 'action_pct', 'strong_action_pct',
  ].includes(field.key))
  const pressureAdvancedFields = (POSITION_RISK_RULE_FIELDS.fund_flow_pressure ?? []).filter(field => [
    'recovery_seconds', 'cooldown_seconds', 'recovery_sell_ratio', 'recovery_imbalance',
  ].includes(field.key))
  const tabs: Array<[DialogTab, string]> = [
    ['independent', '独立风险'],
    ['combined', '组合风险'],
    ['builtin', '系统信号'],
    ['custom', '自定义信号'],
    ['monitor', '监控中心规则'],
  ]
  return (
    <Modal
      onClose={onClose}
      labelledBy="position-risk-rules-title"
      panelClassName="flex max-h-[90vh] w-[96vw] max-w-6xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div className="min-w-0 pr-3">
          <h2 id="position-risk-rules-title" className="text-sm font-semibold">全局风控模板</h2>
          <p className="mt-0.5 truncate text-[11px] text-muted">仅影响持仓风控；公共信号和监控中心原规则保持不变</p>
        </div>
        <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭"><X className="h-4 w-4" /></button>
      </div>
      <nav className="flex shrink-0 gap-1 overflow-x-auto border-b border-border px-4" aria-label="风控模板分类">
        {tabs.map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => setActiveTab(id)}
            className={`h-10 shrink-0 border-b-2 px-3 text-xs transition-colors ${activeTab === id ? 'border-accent text-foreground' : 'border-transparent text-muted hover:text-foreground'}`}
          >
            {label}
          </button>
        ))}
      </nav>
      <div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-5">
        {activeTab === 'independent' && (
          <div>
            <div className="mb-5">
              <h3 className="text-sm font-semibold">独立风险</h3>
              <p className="mt-1 text-[11px] text-muted">每项规则单独形成风险结论；监控、通知和建议互不依赖。</p>
            </div>
            <div className="grid gap-x-8 gap-y-6 lg:grid-cols-3">
              {INDEPENDENT_RULE_GROUPS.map(([group, ruleIds]) => (
                <section key={group} className="min-w-0">
                  <h4 className="mb-2 text-[11px] font-semibold text-secondary">{group}</h4>
                  <div className="divide-y divide-border border-y border-border">
                    {ruleIds.filter(id => id in (options?.rules ?? template.rules)).map(id => {
                      const config = ruleConfig(id)
                      const expanded = expandedRule === id
                      return (
                        <div key={id} className="py-2.5">
                          <div className="flex min-h-8 items-center gap-2 text-xs">
                            <button type="button" onClick={() => setExpandedRule(expanded ? null : id)} className="flex min-w-0 flex-1 items-center gap-2 text-left" aria-expanded={expanded}>
                              <ChevronDown className={`h-3.5 w-3.5 shrink-0 text-muted transition-transform ${expanded ? '' : '-rotate-90'}`} />
                              <span className="truncate">{RULE_LABELS[id] ?? id}</span>
                            </button>
                            <span className="shrink-0 bg-elevated px-1.5 py-0.5 text-[10px] text-muted">{actionLabel(id)}</span>
                            <label className="flex shrink-0 cursor-pointer items-center gap-1 text-[10px] text-muted"><input type="checkbox" checked={config.enabled !== false} onChange={event => toggleRule(id, event.target.checked)} /><span>监控</span></label>
                            <label className="flex shrink-0 cursor-pointer items-center gap-1 text-[10px] text-muted"><input type="checkbox" checked={config.notify === true} onChange={event => toggleRuleNotify(id, event.target.checked)} /><span>通知</span></label>
                          </div>
                          {expanded && POSITION_RISK_RULE_FIELDS[id] && (
                            <div className="mt-3 pl-5">{renderFields(id, POSITION_RISK_RULE_FIELDS[id])}</div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </section>
              ))}
            </div>
          </div>
        )}

        {activeTab === 'combined' && (
          <div className="mx-auto max-w-5xl">
            <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-semibold">资金卖压</h3>
                  <span className="bg-warning/10 px-1.5 py-0.5 text-[10px] text-warning">组合风险</span>
                </div>
                <p className="mt-1 text-[11px] text-muted">原始证据不单独告警；同时满足证据、价格和持续时间后，只生成这一条结论。</p>
              </div>
              <div className="flex shrink-0 items-center gap-4 text-[11px] text-muted">
                <span className="bg-elevated px-2 py-1">确认后建议 {pressureAction}%</span>
                <label className="flex cursor-pointer items-center gap-1.5"><input type="checkbox" checked={pressureConfig.enabled !== false} onChange={event => toggleRule('fund_flow_pressure', event.target.checked)} /><span>监控</span></label>
                <label className="flex cursor-pointer items-center gap-1.5"><input type="checkbox" checked={pressureConfig.notify === true} onChange={event => toggleRuleNotify('fund_flow_pressure', event.target.checked)} /><span>通知</span></label>
              </div>
            </div>

            <div className="grid items-stretch gap-2 border-y border-border bg-elevated/30 p-3 sm:grid-cols-[1fr_auto_1fr_auto_1fr_auto_1.15fr] sm:items-center">
              <div className="px-2 py-2">
                <div className="text-[10px] text-muted">卖压证据</div>
                <div className="mt-1 text-sm font-semibold">至少 {minimumEvidence} / 3 项</div>
              </div>
              <ArrowRight className="hidden h-4 w-4 text-muted sm:block" />
              <div className="px-2 py-2">
                <div className="text-[10px] text-muted">价格确认</div>
                <div className="mt-1 text-sm font-semibold">走弱 1 / 3 项</div>
              </div>
              <ArrowRight className="hidden h-4 w-4 text-muted sm:block" />
              <div className="px-2 py-2">
                <div className="text-[10px] text-muted">稳定过滤</div>
                <div className="mt-1 text-sm font-semibold">持续 {pressureSustain} 秒</div>
              </div>
              <ArrowRight className="hidden h-4 w-4 text-muted sm:block" />
              <div className="border-l-2 border-warning px-3 py-2">
                <div className="text-[10px] text-muted">最终结论</div>
                <div className="mt-1 text-sm font-semibold text-warning">资金卖压</div>
              </div>
            </div>

            <section className="mt-6">
              <div className="mb-2 flex items-end justify-between gap-3">
                <div>
                  <h4 className="text-xs font-semibold">卖压证据</h4>
                  <p className="mt-1 text-[10px] text-muted">三项中达到设定数量才进入价格确认；关闭的证据不参与组合。</p>
                </div>
                <span className="shrink-0 text-[10px] text-muted">{minimumEvidence} 选 3</span>
              </div>
              <div className="divide-y divide-border border-y border-border">
                {FUND_EVIDENCE.map(([id, label, description]) => {
                  const config = ruleConfig(id)
                  const expanded = expandedRule === id
                  const fields = id === 'large_sell' ? LARGE_ORDER_FIELDS : POSITION_RISK_RULE_FIELDS[id] ?? []
                  return (
                    <div key={id} className="py-3">
                      <div className="flex items-center gap-3">
                        <Activity className="h-4 w-4 shrink-0 text-muted" />
                        <div className="min-w-0 flex-1">
                          <div className="text-xs font-medium">{label}</div>
                          <div className="mt-0.5 truncate text-[10px] text-muted">{description}</div>
                        </div>
                        <label className="flex shrink-0 cursor-pointer items-center gap-1.5 text-[10px] text-muted"><input type="checkbox" checked={config.enabled !== false} onChange={event => toggleRule(id, event.target.checked)} /><span>参与组合</span></label>
                        <button type="button" onClick={() => setExpandedRule(expanded ? null : id)} className="grid h-8 w-8 shrink-0 place-items-center hover:bg-elevated" aria-label={`设置${label}`} title={`设置${label}`}><Settings2 className="h-3.5 w-3.5" /></button>
                      </div>
                      {expanded && <div className="mt-3 pl-7">{renderFields(id, fields, 'sm:grid-cols-3')}</div>}
                    </div>
                  )
                })}
              </div>
            </section>

            <section className="mt-6 grid gap-6 lg:grid-cols-[1.5fr_1fr]">
              <div>
                <h4 className="mb-2 text-xs font-semibold">组合判断与建议</h4>
                <div className="border-y border-border py-3">
                  {renderFields('fund_flow_pressure', pressurePrimaryFields, 'sm:grid-cols-3')}
                </div>
                <button type="button" onClick={() => setShowAdvancedPressure(value => !value)} className="mt-2 flex h-8 items-center gap-1.5 text-[11px] text-muted hover:text-foreground" aria-expanded={showAdvancedPressure}>
                  <ChevronDown className={`h-3.5 w-3.5 transition-transform ${showAdvancedPressure ? '' : '-rotate-90'}`} />
                  恢复与冷却参数
                </button>
                {showAdvancedPressure && <div className="mt-2">{renderFields('fund_flow_pressure', pressureAdvancedFields, 'sm:grid-cols-2')}</div>}
              </div>
              <div>
                <h4 className="mb-2 text-xs font-semibold">结果分级</h4>
                <div className="divide-y divide-border border-y border-border text-[11px]">
                  <div className="flex items-center justify-between gap-3 py-3"><span className="text-muted">{minimumEvidence} 项证据 + 价格走弱</span><strong>观察</strong></div>
                  <div className="flex items-center justify-between gap-3 py-3"><span className="text-muted">3 项证据或已破 MA10 / MA20</span><strong>建议 {pressureAction}%</strong></div>
                  <div className="flex items-center justify-between gap-3 py-3"><span className="text-muted">3 项证据 + 1 分钟跌幅 ≥ {strongDrop}%</span><strong className="text-warning">建议 {strongPressureAction}%</strong></div>
                </div>
              </div>
            </section>

            <section className="mt-6 border-t border-border pt-4">
              <div className="flex items-center gap-3">
                <div className="min-w-0 flex-1">
                  <div className="text-xs font-medium">大单买入辅助采样</div>
                  <p className="mt-1 text-[10px] text-muted">仅保留买方观察数据，不计入“资金卖压”三项证据，也不会单独告警。</p>
                </div>
                <label className="flex shrink-0 cursor-pointer items-center gap-1.5 text-[10px] text-muted"><input type="checkbox" checked={ruleConfig('large_buy').enabled !== false} onChange={event => toggleRule('large_buy', event.target.checked)} /><span>采样</span></label>
                <button type="button" onClick={() => setExpandedRule(expandedRule === 'large_buy' ? null : 'large_buy')} className="grid h-8 w-8 shrink-0 place-items-center hover:bg-elevated" aria-label="设置大单买入" title="设置大单买入"><Settings2 className="h-3.5 w-3.5" /></button>
              </div>
              {expandedRule === 'large_buy' && <div className="mt-3">{renderFields('large_buy', LARGE_ORDER_FIELDS, 'sm:grid-cols-3')}</div>}
            </section>
          </div>
        )}

        {activeTab === 'builtin' && (
          <section className="mx-auto max-w-4xl">
              <div className="mb-2 flex items-center gap-2">
                <h3 className="text-xs font-semibold text-secondary">系统信号</h3>
                <span className="rounded bg-elevated px-1.5 py-0.5 text-[10px] text-muted">方向只读</span>
              </div>
              <div className="max-h-64 divide-y divide-border overflow-y-auto border-y border-border">
                {options?.builtin_signals.map(signal => {
                  const saved = template.signals.builtin[signal.id]
                  const direction = signal.direction
                  const actionPct = saved?.action_pct ?? (direction === 'exit' ? 25 : 0)
                  return (
                    <div key={signal.id} className="flex min-h-9 flex-col items-stretch gap-2 py-2 text-xs sm:flex-row sm:items-center sm:justify-between">
                      <span className="flex items-center gap-2 truncate">
                        <span>{signal.label}</span>
                        <span className="rounded bg-accent/10 px-1.5 py-0.5 text-[10px] text-accent">系统</span>
                      </span>
                      <span className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_auto] items-center gap-2 sm:flex sm:shrink-0">
                        <select value={direction} disabled className="h-7 cursor-not-allowed rounded border border-border bg-surface px-1 text-[10px] opacity-60" aria-label={`${signal.label}方向（只读）`} title="系统信号方向由公共信号定义，只读">
                          <option value="entry">入场</option><option value="exit">出场</option><option value="both">双向</option>
                        </select>
                        <select value={actionPct} onChange={event => updateSignal('builtin', signal.id, { action_pct: Number(event.target.value) })} className="h-7 rounded border border-border bg-surface px-1 text-[10px]" aria-label={`${signal.label}建议比例`}>
                          <option value={0}>提醒</option><option value={25}>减仓 25%</option><option value={50}>减仓 50%</option><option value={100}>清仓</option>
                        </select>
                        <label className="flex cursor-pointer items-center gap-1.5 text-[10px] text-muted"><input type="checkbox" checked={saved?.enabled !== false} onChange={event => toggleSignal('builtin', signal.id, event.target.checked, signal.direction, signal.label)} aria-label={`监控${signal.label}`} /><span>监控</span></label>
                        <label className="flex cursor-pointer items-center gap-1.5 text-[10px] text-muted"><input type="checkbox" checked={saved?.notify === true} onChange={event => updateSignal('builtin', signal.id, { notify: event.target.checked })} aria-label={`通知${signal.label}信号`} /><span>通知</span></label>
                      </span>
                    </div>
                  )
                })}
              </div>
          </section>
        )}

        {activeTab === 'custom' && (
          <section className="mx-auto max-w-4xl">
              <h3 className="mb-2 text-xs font-semibold text-secondary">自定义信号</h3>
              <div className="divide-y divide-border border-y border-border">
                {options?.custom_signals.length ? options.custom_signals.map(signal => {
                  const saved = template.signals.custom[signal.id]
                  const direction = saved?.direction ?? signal.direction
                  const actionPct = saved?.action_pct ?? (direction === 'exit' ? 25 : 0)
                  return (
                    <div key={signal.id} className={`flex min-h-9 flex-col items-stretch gap-2 py-2 text-xs sm:flex-row sm:items-center sm:justify-between ${signal.available ? '' : 'opacity-50'}`}>
                      <span className="truncate">{signal.label}{signal.available ? '' : ' · 信号不可用'}</span>
                      <span className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_auto] items-center gap-2 sm:flex sm:shrink-0">
                        <select disabled={!signal.available} value={direction} onChange={event => updateSignal('custom', signal.id, { direction: event.target.value })} className="h-7 rounded border border-border bg-surface px-1 text-[10px] disabled:cursor-not-allowed disabled:opacity-60" aria-label={`${signal.label}方向`}>
                          <option value="entry">入场</option><option value="exit">出场</option><option value="both">双向</option>
                        </select>
                        <select disabled={!signal.available} value={actionPct} onChange={event => updateSignal('custom', signal.id, { action_pct: Number(event.target.value) })} className="h-7 rounded border border-border bg-surface px-1 text-[10px] disabled:cursor-not-allowed disabled:opacity-60" aria-label={`${signal.label}建议比例`}>
                          <option value={0}>提醒</option><option value={25}>减仓 25%</option><option value={50}>减仓 50%</option><option value={100}>清仓</option>
                        </select>
                        <label className={`flex items-center gap-1.5 text-[10px] text-muted ${signal.available ? 'cursor-pointer' : 'cursor-not-allowed opacity-60'}`}><input type="checkbox" disabled={!signal.available} checked={signal.available && saved?.enabled !== false} onChange={event => toggleSignal('custom', signal.id, event.target.checked, signal.direction, signal.label)} aria-label={`监控${signal.label}`} /><span>监控</span></label>
                        <label className={`flex items-center gap-1.5 text-[10px] text-muted ${signal.available ? 'cursor-pointer' : 'cursor-not-allowed opacity-60'}`}><input type="checkbox" disabled={!signal.available} checked={signal.available && saved?.notify === true} onChange={event => updateSignal('custom', signal.id, { notify: event.target.checked })} aria-label={`通知${signal.label}信号`} /><span>通知</span></label>
                      </span>
                    </div>
                  )
                }) : <p className="py-3 text-xs text-muted">暂无已启用自定义信号</p>}
              </div>
          </section>
        )}

        {activeTab === 'monitor' && (
          <section className="mx-auto max-w-4xl">
              <h3 className="mb-2 text-xs font-semibold text-secondary">已有监控规则</h3>
              <div className="divide-y divide-border border-y border-border">
                {options?.monitor_rules.length ? options.monitor_rules.map(rule => {
                  const saved = template.signals.monitor_rules[rule.id]
                  return (
                    <div key={rule.id} className="flex min-h-10 items-center justify-between gap-3 py-2 text-xs">
                      <span className="min-w-0 truncate">{rule.name}</span>
                      <span className="flex shrink-0 items-center gap-3">
                        <label className="flex cursor-pointer items-center gap-1.5 text-[10px] text-muted"><input type="checkbox" checked={saved?.notify === true} onChange={event => updateSignal('monitor_rules', rule.id, { notify: event.target.checked })} aria-label={`通知${rule.name}信号`} /><span>通知</span></label>
                        <select
                          value={saved?.action_pct ?? 0}
                          onChange={event => updateSignal('monitor_rules', rule.id, { action_pct: Number(event.target.value) })}
                          className="h-7 rounded border border-border bg-surface px-2 text-[11px]"
                          aria-label={`${rule.name}建议比例`}
                        >
                          <option value={0}>只进时间线</option>
                          <option value={25}>建议减仓 25%</option>
                          <option value={50}>建议减仓 50%</option>
                          <option value={100}>建议清仓</option>
                        </select>
                      </span>
                    </div>
                  )
                }) : <p className="py-3 text-xs text-muted">暂无监控中心规则</p>}
              </div>
          </section>
        )}
      </div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
        <button type="button" onClick={onClose} className="h-8 rounded-btn px-3 text-xs hover:bg-elevated">取消</button>
        <button type="button" disabled={mutation.isPending} onClick={() => mutation.mutate()} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-50">
          {mutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}保存模板
        </button>
      </div>
    </Modal>
  )
}
