import { useEffect, useState } from 'react'
import { Loader2, Save, X } from 'lucide-react'
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
  five_minute_drawdown: '5 分钟高点回撤', vwap_breakdown: '低于 VWAP',
  broken_limit_up: '涨停炸板', resealed_limit_up: '涨停回封',
  sealed_order_shrink_50: '封单减少（一级）', sealed_order_shrink_80: '封单减少（二级）',
  limit_down: '跌停', large_buy: '大单买入', large_sell: '大单卖出',
  continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡',
  daily_equity_loss: '当日权益亏损', equity_drawdown: '账户高点回撤',
  unrealized_loss: '持仓总浮亏', total_exposure: '总仓位上限',
  symbol_concentration: '单票仓位上限', clustered_severe_events: '严重事件聚集',
  quote_interruption: '行情中断',
}

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
  { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 0 },
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
    { key: 'buffer', label: '低于缓冲', suffix: '%', min: 0, max: 20, step: 0.1, percent: true, defaultValue: 0.01 },
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
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
  ],
  orderbook_imbalance: [
    { key: 'threshold', label: '失衡阈值', suffix: '', min: -1, max: 0, step: 0.05, defaultValue: -0.35 },
    { key: 'sustain_seconds', label: '持续时间', suffix: '秒', min: 1, step: 1, defaultValue: 10 },
    { key: 'action_pct', label: '建议比例', suffix: '%', min: 0, max: 100, step: 25, defaultValue: 25 },
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

  useEffect(() => {
    if (open) setTemplate(structuredClone(portfolio.template))
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

  const updateSignal = (group: 'builtin' | 'custom', id: string, patch: Record<string, unknown>) => {
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

  if (!open) return null
  const rules = Object.entries(options?.rules ?? template.rules).filter(([id]) => id !== 'large_buy' && id !== 'large_sell')
  return (
    <Modal
      onClose={onClose}
      labelledBy="position-risk-rules-title"
      panelClassName="flex max-h-[88vh] w-[94vw] max-w-4xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 id="position-risk-rules-title" className="text-sm font-semibold">全局风控模板</h2>
          <p className="mt-0.5 text-[11px] text-muted">这里只修改持仓风控的阈值、方向和建议，不会改动公共信号或监控中心原规则；单股覆盖在持仓侧栏设置</p>
        </div>
        <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭"><X className="h-4 w-4" /></button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="grid gap-6 md:grid-cols-2">
          <section className="md:col-span-2">
            <div className="mb-2">
              <h3 className="text-xs font-semibold text-secondary">大单买入 / 卖出判定标准</h3>
              <p className="mt-1 break-words text-[11px] leading-5 text-muted">同一方向必须同时满足：窗口内样本数达标、单笔金额不低于阈值、金额异常度不低于“中位数 + MAD 倍数 × 1.4826 × MAD”、Z 分数达标，以及同向成交额占比达标。买卖方向按现价相对上一条报价的涨跌判断，同价成交不计入方向。</p>
            </div>
            <div className="grid divide-y divide-border border-y border-border md:grid-cols-2 md:divide-x md:divide-y-0">
              {(['large_buy', 'large_sell'] as const).map(ruleId => {
                const config = template.rules[ruleId] ?? options?.rules[ruleId] ?? {}
                return (
                  <section key={ruleId} className="p-3 first:md:pl-0 last:md:pr-0">
                    <div className="mb-3 flex items-center justify-between gap-3">
                      <div>
                        <h4 className="text-xs font-medium">{ruleId === 'large_buy' ? '大单买入' : '大单卖出'}</h4>
                        <p className="mt-0.5 text-[10px] text-muted">{Number(config.action_pct ?? 0) > 0 ? `命中后建议减仓 ${config.action_pct}%` : '命中后只提醒'}</p>
                      </div>
                      <input type="checkbox" checked={config.enabled !== false} onChange={event => toggleRule(ruleId, event.target.checked)} aria-label={`启用${ruleId === 'large_buy' ? '大单买入' : '大单卖出'}`} />
                    </div>
                    <div className="grid grid-cols-2 gap-x-3 gap-y-2">
                      {LARGE_ORDER_FIELDS.map(field => {
                        const rawValue = Number(config[field.key] ?? field.defaultValue ?? 0)
                        const value = field.percent ? rawValue * 100 : rawValue
                        return (
                          <label key={field.key} className="min-w-0 text-[10px] text-muted">
                            <span>{field.label}</span>
                            <span className="mt-1 flex h-7 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                              <input
                                type="number"
                                min={field.min}
                                max={field.max}
                                step={field.step}
                                value={Number.isFinite(value) ? value : ''}
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
                  </section>
                )
              })}
            </div>
          </section>
          <section>
            <h3 className="mb-2 text-xs font-semibold text-secondary">核心风控</h3>
            <div className="divide-y divide-border border-y border-border">
              {rules.map(([id, defaultConfig]) => {
                const config = template.rules[id] ?? defaultConfig
                const actionLabel = id === 'symbol_concentration'
                  ? `降至 ${Number(config.target_pct ?? 30)}%`
                  : Number(config.action_pct ?? 0) > 0 ? `建议 ${config.action_pct}%` : '只提醒'
                return (
                  <div key={id} className="border-b border-border py-2 last:border-b-0">
                    <div className="flex min-h-7 items-center justify-between gap-3 text-xs">
                      <span>{RULE_LABELS[id] ?? id}</span>
                      <span className="flex items-center gap-3">
                        <span className="text-[11px] text-muted">{actionLabel}</span>
                        <input type="checkbox" checked={config.enabled !== false} onChange={event => toggleRule(id, event.target.checked)} aria-label={`启用${RULE_LABELS[id] ?? id}`} />
                      </span>
                    </div>
                    {POSITION_RISK_RULE_FIELDS[id] && (
                      <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-2">
                        {POSITION_RISK_RULE_FIELDS[id].map(field => {
                          const rawValue = Number(config[field.key] ?? field.defaultValue ?? 0)
                          const value = field.percent ? rawValue * 100 : rawValue
                          return (
                            <label key={field.key} className="min-w-0 text-[10px] text-muted">
                              <span>{field.label}</span>
                              <span className="mt-1 flex h-7 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                                <input
                                  type="number"
                                  min={field.min}
                                  max={field.max}
                                  step={field.step}
                                  value={Number.isFinite(value) ? value : ''}
                                  disabled={config.enabled === false}
                                  onChange={event => {
                                    const next = Number(event.target.value)
                                    if (Number.isFinite(next)) updateRuleValue(id, field.key, field.percent ? next / 100 : next)
                                  }}
                                  className="min-w-0 flex-1 bg-transparent font-mono text-[11px] text-foreground outline-none disabled:opacity-50"
                                />
                                {field.suffix && <span className="ml-1 shrink-0">{field.suffix}</span>}
                              </span>
                            </label>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </section>
          <div className="space-y-6">
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">系统信号</h3>
              <div className="max-h-64 divide-y divide-border overflow-y-auto border-y border-border">
                {options?.builtin_signals.map(signal => {
                  const saved = template.signals.builtin[signal.id]
                  const direction = saved?.direction ?? signal.direction
                  const actionPct = saved?.action_pct ?? (direction === 'exit' ? 25 : 0)
                  return (
                    <div key={signal.id} className="flex min-h-9 flex-col items-stretch gap-2 py-2 text-xs sm:flex-row sm:items-center sm:justify-between">
                      <span className="truncate">{signal.label}</span>
                      <span className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_auto] items-center gap-2 sm:flex sm:shrink-0">
                        <select value={direction} onChange={event => updateSignal('builtin', signal.id, { direction: event.target.value })} className="h-7 rounded border border-border bg-surface px-1 text-[10px]" aria-label={`${signal.label}方向`}>
                          <option value="entry">入场</option><option value="exit">出场</option><option value="both">双向</option>
                        </select>
                        <select value={actionPct} onChange={event => updateSignal('builtin', signal.id, { action_pct: Number(event.target.value) })} className="h-7 rounded border border-border bg-surface px-1 text-[10px]" aria-label={`${signal.label}建议比例`}>
                          <option value={0}>提醒</option><option value={25}>减仓 25%</option><option value={50}>减仓 50%</option><option value={100}>清仓</option>
                        </select>
                        <input type="checkbox" checked={saved?.enabled !== false} onChange={event => toggleSignal('builtin', signal.id, event.target.checked, signal.direction, signal.label)} />
                      </span>
                    </div>
                  )
                })}
              </div>
            </section>
            <section>
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
                        <select disabled={!signal.available} value={direction} onChange={event => updateSignal('custom', signal.id, { direction: event.target.value })} className="h-7 rounded border border-border bg-surface px-1 text-[10px]" aria-label={`${signal.label}方向`}>
                          <option value="entry">入场</option><option value="exit">出场</option><option value="both">双向</option>
                        </select>
                        <select disabled={!signal.available} value={actionPct} onChange={event => updateSignal('custom', signal.id, { action_pct: Number(event.target.value) })} className="h-7 rounded border border-border bg-surface px-1 text-[10px]" aria-label={`${signal.label}建议比例`}>
                          <option value={0}>提醒</option><option value={25}>减仓 25%</option><option value={50}>减仓 50%</option><option value={100}>清仓</option>
                        </select>
                        <input type="checkbox" disabled={!signal.available} checked={signal.available && saved?.enabled !== false} onChange={event => toggleSignal('custom', signal.id, event.target.checked, signal.direction, signal.label)} />
                      </span>
                    </div>
                  )
                }) : <p className="py-3 text-xs text-muted">暂无已启用自定义信号</p>}
              </div>
            </section>
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">已有监控规则</h3>
              <div className="divide-y divide-border border-y border-border">
                {options?.monitor_rules.length ? options.monitor_rules.map(rule => {
                  const saved = template.signals.monitor_rules[rule.id]
                  return (
                    <div key={rule.id} className="flex min-h-10 items-center justify-between gap-3 py-2 text-xs">
                      <span className="min-w-0 truncate">{rule.name}</span>
                      <select
                        value={saved?.action_pct ?? 0}
                        onChange={event => setTemplate(previous => ({
                          ...previous,
                          signals: {
                            ...previous.signals,
                            monitor_rules: {
                              ...previous.signals.monitor_rules,
                              [rule.id]: { action_pct: Number(event.target.value) },
                            },
                          },
                        }))}
                        className="h-7 rounded border border-border bg-surface px-2 text-[11px]"
                      >
                        <option value={0}>只进时间线</option>
                        <option value={25}>建议减仓 25%</option>
                        <option value={50}>建议减仓 50%</option>
                        <option value={100}>建议清仓</option>
                      </select>
                    </div>
                  )
                }) : <p className="py-3 text-xs text-muted">暂无监控中心规则</p>}
              </div>
            </section>
          </div>
        </div>
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
