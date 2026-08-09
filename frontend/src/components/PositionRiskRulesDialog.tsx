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
  stop_loss: '成本止损 -10%', trailing_drawdown: '盈利后高点回撤 8%',
  ma5_breakdown: '跌破 MA5', ma10_breakdown: '跌破 MA10', ma20_breakdown: '跌破 MA20',
  five_minute_drawdown: '5 分钟高点回撤 3%', vwap_breakdown: '低于 VWAP 1%',
  broken_limit_up: '涨停炸板', resealed_limit_up: '涨停回封',
  sealed_order_shrink_50: '封单减少 50%', sealed_order_shrink_80: '封单减少 80%',
  limit_down: '跌停', large_buy: '大单买入', large_sell: '大单卖出',
  continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡',
  daily_equity_loss: '当日权益亏损 3%', equity_drawdown: '账户高点回撤 8%',
  unrealized_loss: '持仓总浮亏 8%', total_exposure: '总仓位超过 95%',
  symbol_concentration: '单票超过权益 30%', clustered_severe_events: '5 分钟 3 个严重事件',
  quote_interruption: '行情中断 30 秒',
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

  const toggleSignal = (group: 'builtin' | 'custom', id: string, enabled: boolean, direction: string, label: string) => {
    setTemplate(previous => ({
      ...previous,
      signals: {
        ...previous.signals,
        [group]: {
          ...previous.signals[group],
          [id]: { ...(previous.signals[group][id] ?? {}), enabled, direction, label },
        },
      },
    }))
  }

  if (!open) return null
  const rules = Object.entries(options?.rules ?? template.rules)
  return (
    <Modal
      onClose={onClose}
      labelledBy="position-risk-rules-title"
      panelClassName="flex max-h-[88vh] w-[94vw] max-w-4xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 id="position-risk-rules-title" className="text-sm font-semibold">全局风控模板</h2>
          <p className="mt-0.5 text-[11px] text-muted">所有规则和可用信号默认监听；单股覆盖在持仓侧栏设置</p>
        </div>
        <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭"><X className="h-4 w-4" /></button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="grid gap-6 md:grid-cols-2">
          <section>
            <h3 className="mb-2 text-xs font-semibold text-secondary">核心风控</h3>
            <div className="divide-y divide-border border-y border-border">
              {rules.map(([id, defaultConfig]) => {
                const config = template.rules[id] ?? defaultConfig
                const actionLabel = id === 'symbol_concentration'
                  ? `降至 ${Number(config.target_pct ?? 30)}%`
                  : Number(config.action_pct ?? 0) > 0 ? `建议 ${config.action_pct}%` : '只提醒'
                return (
                  <label key={id} className="flex min-h-10 items-center justify-between gap-3 py-2 text-xs">
                    <span>{RULE_LABELS[id] ?? id}</span>
                    <span className="flex items-center gap-3">
                      <span className="text-[11px] text-muted">{actionLabel}</span>
                      <input type="checkbox" checked={config.enabled !== false} onChange={event => toggleRule(id, event.target.checked)} />
                    </span>
                  </label>
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
                  return (
                    <label key={signal.id} className="flex min-h-9 items-center justify-between gap-3 py-1.5 text-xs">
                      <span className="truncate">{signal.label}</span>
                      <span className="flex shrink-0 items-center gap-2">
                        <span className="text-[10px] text-muted">{signal.direction === 'exit' ? '出场 · 25%' : '入场 · 提醒'}</span>
                        <input type="checkbox" checked={saved?.enabled !== false} onChange={event => toggleSignal('builtin', signal.id, event.target.checked, signal.direction, signal.label)} />
                      </span>
                    </label>
                  )
                })}
              </div>
            </section>
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">自定义信号</h3>
              <div className="divide-y divide-border border-y border-border">
                {options?.custom_signals.length ? options.custom_signals.map(signal => {
                  const saved = template.signals.custom[signal.id]
                  return (
                    <label key={signal.id} className={`flex min-h-9 items-center justify-between gap-3 py-1.5 text-xs ${signal.available ? '' : 'opacity-50'}`}>
                      <span className="truncate">{signal.label}{signal.available ? '' : ' · 信号不可用'}</span>
                      <input type="checkbox" disabled={!signal.available} checked={signal.available && saved?.enabled !== false} onChange={event => toggleSignal('custom', signal.id, event.target.checked, signal.direction, signal.label)} />
                    </label>
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
