import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  BellRing,
  Check,
  ChevronRight,
  FileClock,
  ImagePlus,
  Loader2,
  RefreshCw,
  Search,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  X,
} from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { PositionRiskImportDialog } from '@/components/PositionRiskImportDialog'
import { LARGE_ORDER_FIELDS, POSITION_RISK_RULE_FIELDS, PositionRiskRulesDialog } from '@/components/PositionRiskRulesDialog'
import { toast } from '@/components/Toast'
import {
  api,
  type PositionRiskOptions,
  type PositionRiskPosition,
  type PositionRiskRecommendation,
  type PositionRiskStatus,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { cn } from '@/lib/cn'
import { cnSignal, cnSignalText } from '@/lib/signals'

type Tab = 'positions' | 'pending' | 'events'

const STATUS_LABEL: Record<PositionRiskStatus, string> = {
  idle: '待导入',
  websocket: 'WS 实时',
  polling_degraded: '轮询降级',
  reconnecting: 'WS 重连',
  data_unavailable: '行情不可用',
}

const QMT_ORDER_STATUS: Record<string, string> = {
  submitting: '提交中',
  unknown: '状态待人工核对',
  rejected: '已拒绝',
  accepted_pending: '已受理待回查',
  confirmed: '委托已确认',
  '48': '未报',
  '49': '待报',
  '50': '已报',
  '51': '已报待撤',
  '52': '部成待撤',
  '53': '部撤',
  '54': '已撤',
  '55': '部成',
  '56': '已成',
  '57': '废单',
}

function qmtOrderStatus(value?: string) {
  return value ? QMT_ORDER_STATUS[value] ?? `状态 ${value}` : '状态未知'
}

const RULE_GROUPS = [
  ['成本趋势', ['stop_loss', 'trailing_drawdown', 'ma5_breakdown', 'ma10_breakdown', 'ma20_breakdown', 'five_minute_drawdown', 'vwap_breakdown']],
  ['涨跌停', ['broken_limit_up', 'resealed_limit_up', 'sealed_order_shrink_50', 'sealed_order_shrink_80', 'limit_down']],
  ['资金盘口', ['fund_flow_pressure', 'large_buy', 'large_sell', 'continuous_outflow', 'orderbook_imbalance']],
  ['仓位', ['symbol_concentration']],
] as const

const RULE_LABELS: Record<string, string> = {
  stop_loss: '成本止损', trailing_drawdown: '盈利回撤', ma5_breakdown: '破 MA5', ma10_breakdown: '破 MA10', ma20_breakdown: '破 MA20',
  five_minute_drawdown: '5 分钟回撤', vwap_breakdown: '分时均价负偏离超限', broken_limit_up: '炸板', resealed_limit_up: '回封',
  sealed_order_shrink_50: '封单减少 50%', sealed_order_shrink_80: '封单减少 80%', limit_down: '跌停', large_buy: '大单买入',
  large_sell: '大单卖出', continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡', daily_equity_loss: '当日权益亏损',
  fund_flow_pressure: '资金卖压',
  equity_drawdown: '账户高点回撤', unrealized_loss: '持仓总浮亏', total_exposure: '总仓位', symbol_concentration: '单票集中度',
  clustered_severe_events: '严重事件聚集', quote_interruption: '行情中断',
}

function money(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  if (Math.abs(value) >= 100_000_000) return `${(value / 100_000_000).toFixed(2)}亿`
  if (Math.abs(value) >= 10_000) return `${(value / 10_000).toFixed(1)}万`
  return value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

function price(value: number | null | undefined) {
  return value == null ? '—' : value.toFixed(value < 10 ? 3 : 2)
}

function pct(value: number | null | undefined) {
  return value == null ? '—' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(2)}%`
}

function riskTone(score: number) {
  if (score >= 70) return 'text-danger'
  if (score >= 40) return 'text-warning'
  return 'text-bull'
}

function StatusDot({ status }: { status: PositionRiskStatus }) {
  const active = status === 'websocket'
  const warning = status === 'polling_degraded' || status === 'reconnecting'
  return <span className={cn('h-2 w-2 rounded-full', active ? 'bg-bull' : warning ? 'bg-warning' : 'bg-muted')} />
}

function PositionInspector({ row, options, onClose }: { row: PositionRiskPosition; options: PositionRiskOptions | undefined; onClose: () => void }) {
  const portfolioQuery = useQuery({ queryKey: QK.positionRisk, queryFn: api.positionRiskPortfolio })
  const qmt = useQuery({ queryKey: QK.positionRiskQmt, queryFn: api.qmtStatus, refetchInterval: 30_000 })
  const orders = useQuery({ queryKey: QK.positionRiskQmtOrders, queryFn: api.qmtOrders, enabled: Boolean(qmt.data?.configured), refetchInterval: 15_000 })
  const queryClient = useQueryClient()
  const [tradeAction, setTradeAction] = useState<'BUY' | 'SELL'>('SELL')
  const [tradePrice, setTradePrice] = useState(String(row.price ?? row.cost_price ?? ''))
  const [tradePriceType, setTradePriceType] = useState<'LIMIT' | 'LATEST'>('LIMIT')
  const [tradeVolume, setTradeVolume] = useState(100)
  const tradeMutation = useMutation({
    mutationFn: () => api.qmtSubmitOrder({
      action: tradeAction,
      symbol: row.symbol,
      volume: tradeVolume,
      price: tradePriceType === 'LIMIT' ? Number(tradePrice) : null,
      price_type: tradePriceType,
      idempotency_key: `position-risk-${row.symbol}-${tradeAction}-${Date.now()}`,
    }),
    onSuccess: result => {
      toast(`委托结果：${qmtOrderStatus(result.order.status)}`, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmt })
    },
  })
  const cancelMutation = useMutation({
    mutationFn: api.qmtCancelOrder,
    onSuccess: () => {
      toast('已请求撤单', 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
    },
  })
  const portfolio = portfolioQuery.data
  const override = portfolio?.overrides[row.symbol] ?? {}
  const mutation = useMutation({
    mutationFn: (next: Record<string, any>) => api.positionRiskUpdateOverride(row.symbol, portfolio!.revision, next),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: QK.positionRisk }),
  })
  const setRule = (ruleId: string, enabled: boolean) => {
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), enabled } },
    })
  }
  const setRuleValue = (ruleId: string, key: string, value: number) => {
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), [key]: value } },
    })
  }
  const setRuleNotify = (ruleId: string, notify: boolean) => {
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), notify } },
    })
  }
  const setSignal = (
    group: 'builtin' | 'custom',
    signal: { id: string; label: string; direction: string },
    enabled: boolean,
  ) => {
    mutation.mutate({
      ...override,
      signals: {
        ...(override.signals ?? {}),
        [group]: {
          ...(override.signals?.[group] ?? {}),
          [signal.id]: {
            ...(override.signals?.[group]?.[signal.id] ?? {}),
            enabled,
            direction: override.signals?.[group]?.[signal.id]?.direction ?? signal.direction,
            label: override.signals?.[group]?.[signal.id]?.label ?? signal.label,
          },
        },
      },
    })
  }
  const setSignalValue = (group: 'builtin' | 'custom' | 'monitor_rules', signalId: string, key: string, value: string | number | boolean | null) => {
    const groupValues = { ...(override.signals?.[group] ?? {}) }
    const signalValues = { ...(groupValues[signalId] ?? {}) }
    if (value == null) delete signalValues[key]
    else signalValues[key] = value
    if (Object.keys(signalValues).length) groupValues[signalId] = signalValues
    else delete groupValues[signalId]
    mutation.mutate({
      ...override,
      signals: { ...(override.signals ?? {}), [group]: groupValues },
    })
  }
  const setMonitorAction = (ruleId: string, actionPct: number | null) => {
    const monitorRules = { ...(override.signals?.monitor_rules ?? {}) }
    if (actionPct == null) {
      const existing = monitorRules[ruleId]
      if (existing && 'notify' in existing) monitorRules[ruleId] = { notify: existing.notify }
      else delete monitorRules[ruleId]
    }
    else monitorRules[ruleId] = { ...(monitorRules[ruleId] ?? {}), action_pct: actionPct }
    mutation.mutate({
      ...override,
      signals: {
        ...(override.signals ?? {}),
        monitor_rules: monitorRules,
      },
    })
  }
  const signalGroups = [
    ['入场信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'entry'), 'builtin'],
    ['出场信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'exit'), 'builtin'],
    ['双向信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'both'), 'builtin'],
    ['分时信号', (options?.builtin_signals ?? []).filter(signal => signal.group === 'intraday'), 'builtin'],
    ['自定义信号', options?.custom_signals ?? [], 'custom'],
  ] as const
  return (
    <div className="fixed inset-0 z-40 bg-black/35" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <aside className="absolute inset-y-0 right-0 flex w-full max-w-md flex-col border-l border-border bg-surface shadow-xl">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold">{row.name}</div>
            <div className="font-mono text-[11px] text-muted">{row.symbol}</div>
          </div>
          <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭"><X className="h-4 w-4" /></button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto p-4">
          <div className="grid grid-cols-3 gap-px border-y border-border bg-border text-xs">
            {[
              ['风险分', String(row.risk_score)], ['盈亏', pct(row.profit_loss_pct)], ['仓位', pct(row.weight)],
              ['MA5', price(row.ma5)], ['MA10', price(row.ma10)], ['MA20', price(row.ma20)],
            ].map(([label, value]) => <div key={label} className="bg-surface px-3 py-2"><div className="text-[10px] text-muted">{label}</div><div className="mt-1 font-mono">{value}</div></div>)}
          </div>

          <section className="mt-4 border-y border-border py-3">
            <div className="flex items-center justify-between">
              <h3 className="text-xs font-semibold text-secondary">QMT交易</h3>
              <span className={cn('text-[10px]', qmt.data?.trade_enabled ? 'text-warning' : 'text-muted')}>{qmt.data?.trade_enabled ? '真实交易已开启' : '交易开关未开启'}</span>
            </div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-muted">
              <label>方向<select value={tradeAction} onChange={event => setTradeAction(event.target.value as 'BUY' | 'SELL')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="SELL">卖出</option><option value="BUY">买入</option></select></label>
              <label>数量<select value={tradeVolume} onChange={event => setTradeVolume(Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value={100}>1 手（100）</option></select></label>
              <label>价格方式<select value={tradePriceType} onChange={event => setTradePriceType(event.target.value as 'LIMIT' | 'LATEST')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="LIMIT">限价</option><option value="LATEST">最新价</option></select></label>
              <label className={tradePriceType === 'LATEST' ? 'opacity-50' : ''}>限价<input type="number" min="0.001" step="0.001" value={tradePrice} disabled={tradePriceType === 'LATEST'} onChange={event => setTradePrice(event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px] disabled:cursor-not-allowed" /></label>
            </div>
            <button type="button" disabled={!qmt.data?.trade_enabled || qmt.data.state !== 'ready' || tradeMutation.isPending} onClick={() => {
              if (!window.confirm(`确认${tradeAction === 'BUY' ? '买入' : '卖出'} ${row.name} ${tradeVolume} 股？每笔最多 1 手。`)) return
              tradeMutation.mutate()
            }} className={cn('mt-2 h-8 w-full rounded-btn text-xs text-white disabled:cursor-not-allowed disabled:opacity-40', tradeAction === 'BUY' ? 'bg-danger' : 'bg-bull')}>
              {tradeMutation.isPending ? '提交中…' : `发送${tradeAction === 'BUY' ? '买入' : '卖出'}委托`}
            </button>
            {tradeMutation.isError && <p className="mt-2 text-[10px] text-danger">委托失败，请检查 QMT 状态和交易开关。</p>}
            <p className="mt-2 text-[10px] leading-4 text-muted">确认风险建议不会下单；此处委托会进入真实 QMT 账户，成交结果以云端回报为准。</p>
          </section>

          {orders.data?.orders?.some(order => order.symbol === row.symbol && order.order_sys_id) && <section className="border-b border-border pb-3">
            <h3 className="mb-2 text-xs font-semibold text-secondary">当前委托</h3>
            <div className="space-y-1">
              {orders.data.orders.filter(order => order.symbol === row.symbol && order.order_sys_id).slice(0, 5).map(order => <div key={order.order_sys_id} className="flex items-center justify-between gap-2 text-[10px] text-muted"><span>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '—'} · {qmtOrderStatus(order.status)}</span><button type="button" disabled={cancelMutation.isPending} onClick={() => cancelMutation.mutate(order.order_sys_id!)} className="h-6 rounded border border-border px-2 hover:bg-elevated">撤单</button></div>)}
            </div>
          </section>}

          <div className="mt-5 space-y-5">
            {RULE_GROUPS.map(([group, rules]) => (
              <section key={group}>
                <h3 className="mb-2 text-xs font-semibold text-secondary">{group}</h3>
                <div className="divide-y divide-border border-y border-border">
                  {rules.map(ruleId => {
                    const evidenceOnly = ['large_buy', 'large_sell', 'continuous_outflow', 'orderbook_imbalance'].includes(ruleId)
                    const inherited = portfolio?.template.rules[ruleId]?.enabled !== false
                    const explicit = override.rules?.[ruleId]?.enabled
                    const enabled = explicit ?? inherited
                    const hasOverride = Object.keys(override.rules?.[ruleId] ?? {}).length > 0
                    const fields = ruleId === 'large_buy' || ruleId === 'large_sell'
                      ? LARGE_ORDER_FIELDS
                      : POSITION_RISK_RULE_FIELDS[ruleId] ?? []
                    return (
                      <div key={ruleId} className="py-2 text-xs">
                        <div className="flex min-h-7 items-center justify-between gap-3">
                          <span>{RULE_LABELS[ruleId] ?? ruleId}</span>
                          <span className="flex items-center gap-2">
                            <span className="text-[10px] text-muted">{hasOverride ? '单股覆盖' : '继承模板'}</span>
                            <label className="flex items-center gap-1 text-[10px] text-muted"><span>监控</span><input type="checkbox" checked={enabled} disabled={mutation.isPending} onChange={event => setRule(ruleId, event.target.checked)} aria-label={`监控${RULE_LABELS[ruleId] ?? ruleId}`} /></label>
                            {!evidenceOnly && <label className="flex items-center gap-1 text-[10px] text-muted"><span>通知</span><input type="checkbox" checked={override.rules?.[ruleId]?.notify ?? (portfolio?.template.rules[ruleId]?.notify === true)} disabled={mutation.isPending} onChange={event => setRuleNotify(ruleId, event.target.checked)} aria-label={`通知${RULE_LABELS[ruleId] ?? ruleId}信号`} /></label>}
                          </span>
                        </div>
                        {fields.length > 0 && (
                          <div className="mt-2 grid grid-cols-2 gap-2">
                            {fields.map(field => {
                              const inheritedValue = portfolio?.template.rules[ruleId]?.[field.key] ?? field.defaultValue ?? 0
                              const storedValue = override.rules?.[ruleId]?.[field.key] ?? inheritedValue
                              const displayValue = field.percent ? Number(storedValue) * 100 : Number(storedValue)
                              return (
                                <label key={field.key} className="min-w-0 text-[10px] text-muted">
                                  <span>{field.label}</span>
                                  <span className="mt-1 flex h-7 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                                    <input
                                      key={`${ruleId}-${field.key}-${displayValue}`}
                                      type="number"
                                      min={field.min}
                                      max={field.max}
                                      step={field.step}
                                      defaultValue={displayValue}
                                      disabled={mutation.isPending || !enabled}
                                      onBlur={event => {
                                        const next = Number(event.target.value)
                                        const stored = field.percent ? next / 100 : next
                                        if (Number.isFinite(next) && stored !== Number(storedValue)) setRuleValue(ruleId, field.key, stored)
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
            ))}
            {signalGroups.map(([title, signals, storageGroup]) => (
              <section key={title}>
                <h3 className="mb-2 text-xs font-semibold text-secondary">{title}</h3>
                <div className="divide-y divide-border border-y border-border">
                  {signals.length ? signals.map(signal => {
                    const explicit = override.signals?.[storageGroup]?.[signal.id]?.enabled
                    const inherited = portfolio?.template.signals[storageGroup]?.[signal.id]?.enabled !== false
                    const available = !('available' in signal) || signal.available
                    const directionReadonly = storageGroup === 'builtin'
                    const hasOverride = Object.keys(override.signals?.[storageGroup]?.[signal.id] ?? {}).length > 0
                    const inheritedDirection = directionReadonly ? signal.direction : portfolio?.template.signals[storageGroup]?.[signal.id]?.direction ?? signal.direction
                    const inheritedAction = portfolio?.template.signals[storageGroup]?.[signal.id]?.action_pct ?? (inheritedDirection === 'exit' ? 25 : 0)
                    const explicitDirection = directionReadonly ? undefined : override.signals?.[storageGroup]?.[signal.id]?.direction
                    const explicitAction = override.signals?.[storageGroup]?.[signal.id]?.action_pct
                    const explicitNotify = override.signals?.[storageGroup]?.[signal.id]?.notify
                    const inheritedNotify = portfolio?.template.signals[storageGroup]?.[signal.id]?.notify === true
                    return (
                      <div key={signal.id} className={cn('py-2 text-xs', available ? '' : 'opacity-50')}>
                        <div className="flex min-h-7 items-center justify-between gap-3">
                        <span className="min-w-0 truncate">{signal.label}{available ? '' : ' · 信号不可用'}</span>
                        <span className="flex shrink-0 items-center gap-2">
                          <span className="text-[10px] text-muted">{hasOverride ? '单股覆盖' : '继承模板'}</span>
                          <input
                            type="checkbox"
                            checked={available && (explicit ?? inherited)}
                            disabled={mutation.isPending || !available}
                            onChange={event => setSignal(storageGroup, signal, event.target.checked)}
                          />
                          <label className="flex items-center gap-1 text-[10px] text-muted"><span>通知</span><input type="checkbox" checked={explicitNotify ?? inheritedNotify} disabled={mutation.isPending || !available} onChange={event => setSignalValue(storageGroup, signal.id, 'notify', event.target.checked)} aria-label={`通知${signal.label}信号`} /></label>
                        </span>
                        </div>
                        <div className="mt-2 grid grid-cols-2 gap-2">
                          <label className="text-[10px] text-muted">信号方向{directionReadonly ? '（只读）' : ''}
                            <select value={explicitDirection ?? 'inherit'} disabled={mutation.isPending || !available || directionReadonly} onChange={event => setSignalValue(storageGroup, signal.id, 'direction', event.target.value === 'inherit' ? null : event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[10px] disabled:cursor-not-allowed disabled:opacity-60" title={directionReadonly ? '系统信号方向由公共信号定义，只读' : undefined}>
                              <option value="inherit">继承（{inheritedDirection === 'exit' ? '出场' : inheritedDirection === 'entry' ? '入场' : '双向'}）</option>
                              <option value="entry">入场</option><option value="exit">出场</option><option value="both">双向</option>
                            </select>
                          </label>
                          <label className="text-[10px] text-muted">建议比例
                            <select value={explicitAction == null ? 'inherit' : String(explicitAction)} disabled={mutation.isPending || !available} onChange={event => setSignalValue(storageGroup, signal.id, 'action_pct', event.target.value === 'inherit' ? null : Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[10px]">
                              <option value="inherit">继承（{inheritedAction ? `${inheritedAction}%` : '提醒'}）</option>
                              <option value="0">提醒</option><option value="25">减仓 25%</option><option value="50">减仓 50%</option><option value="100">清仓</option>
                            </select>
                          </label>
                        </div>
                      </div>
                    )
                  }) : <p className="py-2 text-xs text-muted">暂无可用信号</p>}
                </div>
              </section>
            ))}
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">已有监控规则</h3>
              <div className="divide-y divide-border border-y border-border">
                {options?.monitor_rules.length ? options.monitor_rules.map(rule => {
                  const explicit = override.signals?.monitor_rules?.[rule.id]?.action_pct
                  const inherited = portfolio?.template.signals.monitor_rules[rule.id]?.action_pct ?? 0
                  const explicitNotify = override.signals?.monitor_rules?.[rule.id]?.notify
                  const inheritedNotify = portfolio?.template.signals.monitor_rules[rule.id]?.notify === true
                  return (
                    <div key={rule.id} className="flex min-h-10 items-center justify-between gap-3 py-2 text-xs">
                      <span className="min-w-0 truncate">{rule.name}</span>
                      <span className="flex shrink-0 items-center gap-2">
                        <label className="flex items-center gap-1 text-[10px] text-muted"><span>通知</span><input type="checkbox" checked={explicitNotify ?? inheritedNotify} disabled={mutation.isPending} onChange={event => setSignalValue('monitor_rules', rule.id, 'notify', event.target.checked)} aria-label={`通知${rule.name}信号`} /></label>
                        <select
                          value={explicit == null ? 'inherit' : String(explicit)}
                          disabled={mutation.isPending}
                          onChange={event => setMonitorAction(rule.id, event.target.value === 'inherit' ? null : Number(event.target.value))}
                          className="h-7 rounded border border-border bg-surface px-2 text-[11px]"
                          aria-label={`${rule.name}建议比例`}
                        >
                          <option value="inherit">继承模板（{inherited ? `${inherited}%` : '时间线'}）</option>
                          <option value="0">只进时间线</option>
                          <option value="25">建议减仓 25%</option>
                          <option value="50">建议减仓 50%</option>
                          <option value="100">建议清仓</option>
                        </select>
                      </span>
                    </div>
                  )
                }) : <p className="py-2 text-xs text-muted">暂无监控中心规则</p>}
              </div>
            </section>
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">证据状态</h3>
              <div className="border-y border-border py-2 text-xs">
                <div className="flex justify-between"><span className="text-muted">最新命中</span><span>{row.latest_signal ? cnSignal(row.latest_signal) : '暂无'}</span></div>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {Object.entries(row.evidence).map(([key, ready]) => (
                    <span key={key} className={cn('rounded px-1.5 py-0.5 text-[10px]', ready ? 'bg-bull/10 text-bull' : 'bg-elevated text-muted')}>
                      {({ cost: '成本', history: '历史', quote: '实时价', depth: '五档', flow: '资金' } as Record<string, string>)[key]}
                    </span>
                  ))}
                </div>
              </div>
            </section>
          </div>
        </div>
        <div className="flex items-center justify-between border-t border-border px-4 py-3">
          <span className="text-[11px] text-muted">{Object.keys(override).length ? '存在单股覆盖' : '全部继承全局模板'}</span>
          <button type="button" disabled={mutation.isPending || !Object.keys(override).length} onClick={() => mutation.mutate({})} className="h-8 rounded-btn border border-border px-3 text-xs disabled:opacity-40">恢复继承</button>
        </div>
      </aside>
    </div>
  )
}

function PendingRow({ item, revision, name, signalNames }: { item: PositionRiskRecommendation; revision: number; name?: string; signalNames: Record<string, string> }) {
  const queryClient = useQueryClient()
  const ruleLabel = RULE_LABELS[item.rule_id] ?? cnSignal(item.rule_id)
  const mutation = useMutation({
    mutationFn: (action: 'confirm' | 'dismiss') => api.positionRiskRecommendationAction(item.id, action, revision),
    onSuccess: data => {
      toast(data.message, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRisk })
    },
  })
  return (
    <div className="grid gap-3 border-b border-border px-3 py-3 md:grid-cols-[minmax(0,1fr)_110px_190px] md:items-center">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2 text-sm font-medium">
          <span>{item.symbol ? <>{name || item.symbol}<span className="ml-1.5 font-mono text-xs text-muted">{item.symbol}</span></> : '组合'}</span>
          <span className="rounded bg-elevated px-1.5 py-0.5 text-[11px] text-secondary">{ruleLabel}</span>
          <span className={cn('font-mono text-xs', riskTone(item.risk_score))}>{item.risk_score} 分</span>
          <span className="rounded bg-warning/10 px-1.5 py-0.5 text-xs font-semibold text-warning">结论：{item.action} {item.reduction_pct}%</span>
        </div>
        <div className="mt-2 break-words text-xs leading-5 text-muted">{item.reasons.map(reason => cnSignalText(reason, signalNames)).join('；')}</div>
      </div>
      <time className="text-[11px] text-muted">{new Date(item.created_at).toLocaleString('zh-CN')}</time>
      <div className="flex justify-end gap-2">
        <button type="button" disabled={mutation.isPending} onClick={() => mutation.mutate('dismiss')} className="h-8 rounded-btn border border-border px-3 text-xs hover:bg-elevated">忽略</button>
        <button type="button" disabled={mutation.isPending} onClick={() => mutation.mutate('confirm')} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white">
          {mutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}确认建议
        </button>
      </div>
    </div>
  )
}

export function LargeOrders() {
  const [tab, setTab] = useState<Tab>('positions')
  const [search, setSearch] = useState('')
  const [risk, setRisk] = useState<'all' | 'medium' | 'high'>('all')
  const [importOpen, setImportOpen] = useState(false)
  const [rulesOpen, setRulesOpen] = useState(false)
  const [selected, setSelected] = useState<PositionRiskPosition | null>(null)
  const portfolio = useQuery({ queryKey: QK.positionRisk, queryFn: api.positionRiskPortfolio, refetchInterval: 30_000 })
  const qmt = useQuery({ queryKey: QK.positionRiskQmt, queryFn: api.qmtStatus, refetchInterval: 30_000 })
  const qmtOrders = useQuery({ queryKey: QK.positionRiskQmtOrders, queryFn: api.qmtOrders, enabled: Boolean(qmt.data?.configured), refetchInterval: 15_000 })
  const options = useQuery({ queryKey: QK.positionRiskOptions, queryFn: api.positionRiskOptions })
  const pending = useQuery({ queryKey: QK.positionRiskRecommendations('pending'), queryFn: () => api.positionRiskRecommendations('pending') })
  const events = useQuery({ queryKey: QK.positionRiskEvents, queryFn: api.positionRiskEvents })
  const queryClient = useQueryClient()
  const qmtProbe = useMutation({ mutationFn: api.qmtProbe, onSuccess: () => queryClient.invalidateQueries({ queryKey: QK.positionRiskQmt }) })
  const qmtSync = useMutation({
    mutationFn: api.qmtSync,
    onSuccess: result => {
      queryClient.setQueryData(QK.positionRisk, result.portfolio)
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmt })
      toast(result.message, 'success')
    },
  })
  const qmtToggle = useMutation({
    mutationFn: api.qmtTradingToggle,
    onSuccess: result => queryClient.setQueryData(QK.positionRiskQmt, result.status),
  })
  const signalNames = useMemo(
    () => Object.fromEntries((options.data?.custom_signals ?? []).map(signal => [signal.id, signal.label])),
    [options.data?.custom_signals],
  )
  const rows = useMemo(() => (portfolio.data?.positions ?? []).filter(row => {
    const matchesSearch = !search || row.symbol.toLowerCase().includes(search.toLowerCase()) || row.name.includes(search)
    const matchesRisk = risk === 'all' || row.risk_level === risk
    return matchesSearch && matchesRisk
  }), [portfolio.data?.positions, risk, search])
  const evidenceCoverage = portfolio.data?.positions.length
    ? portfolio.data.positions.reduce((sum, row) => sum + row.evidence_coverage, 0) / portfolio.data.positions.length
    : 0

  if (portfolio.isLoading) return <div className="grid h-full place-items-center"><Loader2 className="h-6 w-6 animate-spin text-accent" /></div>
  if (portfolio.isError || !portfolio.data) return <EmptyState icon={AlertTriangle} title="持仓风控加载失败" hint="请检查后端服务后重试" />
  const data = portfolio.data
  const namesBySymbol = new Map(data.positions.map(row => [row.symbol, row.name]))

  return (
    <div className="min-h-full bg-background">
      <PageHeader
        title="持仓风控"
        subtitle={data.imported_at ? `账户快照 ${new Date(data.imported_at).toLocaleString('zh-CN')}` : '尚未导入账户快照'}
        right={<div className="flex min-w-max items-center gap-2">
          <button type="button" onClick={() => qmtSync.mutate()} disabled={!qmt.data?.configured || qmtSync.isPending} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:cursor-not-allowed disabled:opacity-50"><RefreshCw className={cn('h-3.5 w-3.5', qmtSync.isPending && 'animate-spin')} />同步QMT</button>
          <button type="button" onClick={() => setImportOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-3 text-xs hover:bg-elevated"><ImagePlus className="h-3.5 w-3.5" />图片导入</button>
          <button type="button" onClick={() => setRulesOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-3 text-xs hover:bg-elevated"><Settings2 className="h-3.5 w-3.5" />规则模板</button>
          <button type="button" onClick={() => portfolio.refetch()} className="grid h-8 w-8 place-items-center rounded-btn border border-border hover:bg-elevated" title="刷新"><RefreshCw className="h-3.5 w-3.5" /></button>
        </div>}
      />

      <div className="border-b border-border px-4 py-2 sm:px-5">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
          <span className="font-medium">{data.account.name}</span>
          <span className="text-muted">持仓 <b className="font-mono text-foreground">{data.positions.length}</b></span>
          <span className="inline-flex items-center gap-1.5"><StatusDot status={data.runtime.status} />{STATUS_LABEL[data.runtime.status]}</span>
          <span className="text-muted">证据覆盖 <b className="font-mono text-foreground">{Math.round(evidenceCoverage * 100)}%</b></span>
          <span className="text-muted">总资产 <b className="font-mono text-foreground">{money(data.account.total_asset)}</b></span>
          <span className={cn('inline-flex items-center gap-1.5', qmt.data?.state === 'ready' ? 'text-bull' : 'text-warning')}><StatusDot status={qmt.data?.state === 'ready' ? 'websocket' : 'data_unavailable'} />QMT {qmt.data?.state === 'ready' ? '已连接' : qmt.data?.configured ? '待检查' : '未配置'}</span>
          <span className={qmt.data?.auto_sync_running ? 'text-bull' : 'text-muted'}>{qmt.data?.auto_sync_running ? `自动同步 ${qmt.data.auto_sync_interval_seconds}秒` : '自动同步未运行'}</span>
          <label className="inline-flex items-center gap-1.5 text-muted" title={!qmt.data?.trade_authorized ? '后端未授权真实交易' : '本次后端运行有效，重启后自动关闭'}><input type="checkbox" checked={qmt.data?.trade_enabled === true} disabled={!qmt.data?.configured || !qmt.data?.trade_authorized || qmtToggle.isPending} onChange={event => qmtToggle.mutate(event.target.checked)} />允许真实交易</label>
          <button type="button" onClick={() => qmtProbe.mutate()} disabled={qmtProbe.isPending} className="h-7 rounded-btn border border-border px-2 text-[11px] hover:bg-elevated disabled:opacity-50">检查连接</button>
          <span className="text-warning">每笔最多 {qmt.data?.max_order_lots ?? 1} 手</span>
          <span className="ml-auto max-w-full truncate text-[11px] text-muted" title={data.runtime.reason}>{data.runtime.reason}</span>
        </div>
      </div>

      <div className="flex flex-col gap-2 border-b border-border px-4 py-2 sm:flex-row sm:items-center sm:justify-between sm:px-5">
        <div className="inline-flex w-fit max-w-full rounded-btn bg-elevated p-0.5">
          {([
            ['positions', '持仓监控', data.positions.length, ShieldCheck],
            ['pending', '待确认', pending.data?.count ?? data.runtime.pending_count, BellRing],
            ['events', '触发记录', events.data?.count ?? 0, FileClock],
          ] as const).map(([id, label, count, Icon]) => (
            <button key={id} type="button" onClick={() => setTab(id)} className={cn('inline-flex h-7 shrink-0 items-center gap-1.5 whitespace-nowrap rounded px-2.5 text-xs', tab === id ? 'bg-surface text-foreground shadow-sm' : 'text-muted')}>
              <Icon className="h-3.5 w-3.5" />{label}<span className="font-mono text-[10px]">{count}</span>
            </button>
          ))}
        </div>
        {tab === 'positions' && <div className="flex items-center gap-2 self-end sm:self-auto">
          <label className="relative hidden sm:block"><Search className="absolute left-2 top-2 h-3.5 w-3.5 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="代码 / 名称" className="h-8 w-44 rounded-btn border border-border bg-transparent pl-7 pr-2 text-xs" /></label>
          <div className="relative"><SlidersHorizontal className="pointer-events-none absolute left-2 top-2 h-3.5 w-3.5 text-muted" /><select value={risk} onChange={event => setRisk(event.target.value as typeof risk)} className="h-8 rounded-btn border border-border bg-surface pl-7 pr-6 text-xs"><option value="all">全部风险</option><option value="high">高风险</option><option value="medium">中风险</option></select></div>
        </div>}
      </div>

      {tab === 'positions' && (rows.length ? <>
        <div className="hidden overflow-x-auto md:block">
          <table className="w-full min-w-[1120px] text-xs">
            <thead className="sticky top-0 bg-background text-muted"><tr className="border-b border-border">
              {['证券', '数量 / 可用', '成本 / 现价', '盈亏', '仓位', 'MA5 / MA10 / MA20', '证据', '信号', '风险', '建议', '交易'].map(label => <th key={label} className="px-3 py-2 text-left font-medium">{label}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-border/70">
              {rows.map(row => <tr key={row.symbol} className="hover:bg-elevated/35">
                <td className="px-3 py-2"><button type="button" onClick={() => setSelected(row)} className="text-left"><div className="font-medium">{row.name}</div><div className="font-mono text-[10px] text-muted">{row.symbol}</div></button></td>
                <td className="px-3 py-2 font-mono">{row.quantity.toLocaleString()}<div className="text-[10px] text-muted">可用 {row.available.toLocaleString()}</div></td>
                <td className="px-3 py-2 font-mono">{price(row.cost_price)}<div className="text-[10px] text-muted">{price(row.price)}</div></td>
                <td className={cn('px-3 py-2 font-mono', (row.profit_loss ?? 0) >= 0 ? 'text-danger' : 'text-bull')}>{money(row.profit_loss)}<div className="text-[10px]">{pct(row.profit_loss_pct)}</div></td>
                <td className="px-3 py-2 font-mono">{pct(row.weight)}</td>
                <td className="px-3 py-2 font-mono text-muted">{price(row.ma5)} / {price(row.ma10)} / {price(row.ma20)}</td>
                <td className="px-3 py-2"><div className="h-1.5 w-20 overflow-hidden rounded-full bg-elevated"><div className="h-full bg-accent" style={{ width: `${row.evidence_coverage * 100}%` }} /></div><span className="mt-1 block font-mono text-[10px] text-muted">{Math.round(row.evidence_coverage * 100)}%</span></td>
                <td className="max-w-36 truncate px-3 py-2 text-muted" title={row.latest_signal ? cnSignal(row.latest_signal) : ''}>{row.latest_signal ? cnSignal(row.latest_signal) : '—'}</td>
                <td className={cn('px-3 py-2 font-mono text-sm font-semibold', riskTone(row.risk_score))}>{row.risk_score}</td>
                <td className="px-3 py-2">{row.suggestion ? <span className="text-warning">{row.suggestion.action} {row.suggestion.reduction_pct}%</span> : <span className="text-muted">观察</span>}</td>
                <td className="px-3 py-2"><button type="button" onClick={() => setSelected(row)} className="grid h-7 w-7 place-items-center rounded hover:bg-elevated" title="单股规则与交易"><ChevronRight className="h-4 w-4" /></button></td>
              </tr>)}
            </tbody>
          </table>
        </div>
        <div className="divide-y divide-border md:hidden">
          {rows.map(row => <button key={row.symbol} type="button" onClick={() => setSelected(row)} className="grid w-full grid-cols-[1fr_auto] gap-3 px-4 py-3 text-left">
            <div><div className="font-medium">{row.name}<span className="ml-2 font-mono text-[10px] text-muted">{row.symbol}</span></div><div className="mt-1 text-xs text-muted">{row.quantity.toLocaleString()} 股 · 成本 {price(row.cost_price)} · 现价 {price(row.price)}</div><div className="mt-1 text-[11px] text-muted">{row.suggestion ? `${row.suggestion.action} ${row.suggestion.reduction_pct}%` : row.latest_signal ? cnSignal(row.latest_signal) : '观察'}</div></div>
            <div className="text-right"><div className={cn('font-mono text-base font-semibold', riskTone(row.risk_score))}>{row.risk_score}</div><div className={(row.profit_loss ?? 0) >= 0 ? 'text-danger' : 'text-bull'}>{pct(row.profit_loss_pct)}</div></div>
          </button>)}
        </div>
      </> : <EmptyState icon={ShieldCheck} title={data.positions.length ? '没有符合筛选的持仓' : '尚未导入持仓'} hint={data.positions.length ? '调整搜索或风险筛选' : '使用顶部“图片导入”上传同花顺手机持仓截图'} />)}

      {tab === 'pending' && <div>
        <div className="border-b border-border bg-warning/5 px-4 py-2 text-[11px] text-warning sm:px-5">确认建议仅记录人工判断，不修改持仓、模拟盘或通知券商委托。</div>
        {pending.data?.recommendations.length ? pending.data.recommendations.map(item => <PendingRow key={item.id} item={item} revision={data.revision} name={item.symbol ? namesBySymbol.get(item.symbol) : undefined} signalNames={signalNames} />) : <EmptyState icon={ShieldCheck} title="没有待确认建议" hint="风险事件触发后会在这里汇总" />}
      </div>}

      {tab === 'events' && <div className="divide-y divide-border">
        {events.data?.events.length ? events.data.events.map((event, index) => <div key={`${event.ts}-${index}`} className="grid gap-2 px-4 py-3 text-xs sm:grid-cols-[150px_120px_1fr_80px] sm:px-5">
          <time className="font-mono text-muted">{new Date(event.ts).toLocaleString('zh-CN')}</time><span>{event.symbol || '组合'} {event.name}</span><span className="min-w-0"><span className="rounded bg-elevated px-1.5 py-0.5 text-[11px] text-secondary">{event.rule_id === 'vwap_breakdown' ? RULE_LABELS.vwap_breakdown : event.rule_name || RULE_LABELS[event.rule_id || ''] || cnSignalText(event.message, signalNames)}</span>{event.reasons?.length ? <span className="mt-1 block break-words text-[11px] leading-5 text-muted">{event.reasons.map(reason => cnSignalText(reason, signalNames)).join('；')}</span> : null}</span><span className={event.severity === 'critical' ? 'text-danger' : event.severity === 'warn' ? 'text-warning' : 'text-muted'}>{event.source === 'position_risk' ? '持仓风控' : '监控中心'}</span>
        </div>) : <EmptyState icon={FileClock} title="暂无触发记录" hint="持仓规则和监控中心命中会进入同一时间线" />}
      </div>}

      {tab === 'positions' && qmtOrders.data?.orders?.length ? <section className="border-t border-border px-4 py-3 sm:px-5">
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold"><span>QMT委托</span><span className="text-[10px] font-normal text-muted">成交状态以云端 QMT 查询为准</span></div>
        <div className="grid gap-1 text-[11px] text-muted sm:grid-cols-[130px_100px_90px_120px_1fr]">{qmtOrders.data.orders.slice(0, 20).map((order, index) => <div key={`${order.order_sys_id ?? order.idempotency_key ?? index}`} className="contents"><span>{order.symbol ?? order.stock_code ?? '—'}</span><span className={order.action === 'SELL' ? 'text-bull' : 'text-danger'}>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '—'}</span><span>{order.price ?? order.price_type ?? '—'}</span><span>{qmtOrderStatus(order.status)}</span><span className="truncate">{order.order_sys_id ? `委托号 ${order.order_sys_id}` : order.status === 'unknown' ? '请在QMT核对，禁止原单重发' : '等待云端委托号'}</span></div>)}</div>
      </section> : null}

      <PositionRiskImportDialog open={importOpen} portfolio={data} onClose={() => setImportOpen(false)} />
      <PositionRiskRulesDialog open={rulesOpen} portfolio={data} options={options.data} onClose={() => setRulesOpen(false)} />
      {selected && <PositionInspector row={selected} options={options.data} onClose={() => setSelected(null)} />}
    </div>
  )
}
