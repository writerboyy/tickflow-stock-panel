import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  BellRing,
  Check,
  ChevronDown,
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
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { toast } from '@/components/Toast'
import {
  api,
  type PositionRiskOptions,
  type PositionRiskPosition,
  type PositionRiskPortfolio,
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

const RUNTIME_REASON_LABELS: Record<string, string> = {
  '行情已恢复，正在重新建立连续性基线': '基线重建中',
  '正在建立持仓行情连续性基线': '建立行情基线',
  '持仓池已整体接入共享 TickFlow WS': 'WS 已接入',
  '持仓池行情连续性已恢复': '行情已恢复',
  'WS 能力不可用，全部持仓已转行情轮询': '已转行情轮询',
}

function compactRuntimeReason(reason: unknown): string {
  const text = String(reason ?? '').trim()
  if (!text) return ''
  const known = RUNTIME_REASON_LABELS[text]
  if (known) return known
  const interrupted = text.match(/^仍有 (\d+) 只持仓行情中断/)
  if (interrupted) return `${interrupted[1]} 只行情中断`
  return text.length > 12 ? `${text.slice(0, 12)}…` : text
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

function qmtOrderPrice(value: unknown, priceType?: string) {
  const numericValue = Number(value)
  if (value != null && Number.isFinite(numericValue)) return price(numericValue)
  return priceType === 'LATEST' ? '最新价' : priceType || '—'
}

const TAKE_PROFIT_RULES = ['take_profit', 'trailing_drawdown', 'take_profit_ladder'] as const
const STOP_LOSS_RULE_GROUPS = [
  ['硬止损与分时结构', ['stop_loss', 'structure_stop', 'ma5_breakdown', 'ma10_breakdown', 'ma20_breakdown', 'five_minute_drawdown', 'vwap_breakdown']],
  ['波动与时间保护', ['atr_protection', 'time_stop']],
  ['涨跌停退出', ['broken_limit_up', 'resealed_limit_up', 'sealed_order_shrink_50', 'sealed_order_shrink_80', 'limit_down']],
] as const
const ADVANCED_RULES = ['fund_flow_pressure', 'large_buy', 'large_sell', 'continuous_outflow', 'orderbook_imbalance', 'daily_equity_loss', 'equity_drawdown', 'unrealized_loss', 'total_exposure', 'symbol_concentration', 'clustered_severe_events', 'quote_interruption'] as const
const INTRADAY_RULE_TAB: Array<['take_profit' | 'stop_loss' | 't_trading', string]> = [
  ['take_profit', '止盈'],
  ['stop_loss', '止损'],
  ['t_trading', '做 T'],
]
const SHORT_TERM_RULES = new Set(['take_profit_ladder', 'structure_stop', 'atr_protection', 'time_stop'])

const RULE_LABELS: Record<string, string> = {
  stop_loss: '成本止损', take_profit: '固定止盈', trailing_drawdown: '盈利回撤', ma5_breakdown: '破 MA5', ma10_breakdown: '破 MA10', ma20_breakdown: '破 MA20',
  t_trading: '做 T',
  five_minute_drawdown: '5 分钟回撤', vwap_breakdown: '分时均价负偏离超限', broken_limit_up: '炸板', resealed_limit_up: '回封',
  sealed_order_shrink_50: '封单减少 50%', sealed_order_shrink_80: '封单减少 80%', limit_down: '跌停', large_buy: '大单买入',
  large_sell: '大单卖出', continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡', daily_equity_loss: '当日权益亏损',
  fund_flow_pressure: '资金卖压',
  take_profit_ladder: 'R 倍数分批止盈', structure_stop: '分时结构止损', atr_protection: 'ATR 移动保护', time_stop: '时间止损',
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

function percentage(value: unknown, sign = false) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '—'
  return `${sign && numeric >= 0 ? '+' : ''}${(numeric * 100).toFixed(0)}%`
}

function actionLabel(value: unknown) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) return '提醒'
  if (numeric >= 100) return '清仓'
  return `减仓${numeric}%`
}

function effectiveRule(portfolio: PositionRiskPortfolio, symbol: string, ruleId: string) {
  return {
    ...(portfolio.template.rules[ruleId] ?? {}),
    ...(portfolio.overrides[symbol]?.rules?.[ruleId] ?? {}),
  }
}

function effectiveRuleEnabled(ruleId: string, config: Record<string, any>) {
  return SHORT_TERM_RULES.has(ruleId) ? config.active === true : config.enabled !== false
}

function hasRuleOverride(portfolio: PositionRiskPortfolio, symbol: string, ruleIds: readonly string[]) {
  const rules = portfolio.overrides[symbol]?.rules ?? {}
  return ruleIds.some(ruleId => Object.keys(rules[ruleId] ?? {}).length > 0)
}

function hasSignalOverride(portfolio: PositionRiskPortfolio, symbol: string, group: string, signalIds?: readonly string[]) {
  const values = portfolio.overrides[symbol]?.signals?.[group] ?? {}
  if (!signalIds) return Object.keys(values).length > 0
  return signalIds.some(signalId => Object.keys(values[signalId] ?? {}).length > 0)
}

function moduleSource(portfolio: PositionRiskPortfolio, symbol: string, ruleIds: readonly string[], signalGroups: readonly string[] = []) {
  const covered = hasRuleOverride(portfolio, symbol, ruleIds)
    || signalGroups.some(group => hasSignalOverride(portfolio, symbol, group))
  return covered ? '已覆盖' : '默认'
}

function enabledRuleLabels(portfolio: PositionRiskPortfolio, symbol: string, ruleIds: readonly string[]) {
  return ruleIds.filter(ruleId => effectiveRuleEnabled(ruleId, effectiveRule(portfolio, symbol, ruleId))).map(ruleId => RULE_LABELS[ruleId] ?? ruleId)
}

function RiskSettingsSummary({ portfolio, symbol, options }: { portfolio: PositionRiskPortfolio; symbol: string; options?: PositionRiskOptions }) {
  const takeProfit = TAKE_PROFIT_RULES.map(ruleId => effectiveRule(portfolio, symbol, ruleId))
  const fixedTakeProfit = takeProfit[0]
  const trailingTakeProfit = takeProfit[1]
  const ladderTakeProfit = takeProfit[2]
  const takeProfitEnabled = TAKE_PROFIT_RULES.some((ruleId, index) => effectiveRuleEnabled(ruleId, takeProfit[index]))
  const takeProfitText = takeProfitEnabled
    ? [
        effectiveRuleEnabled('take_profit', fixedTakeProfit) ? `固定 ${percentage(fixedTakeProfit.threshold, true)} · ${actionLabel(fixedTakeProfit.action_pct)}` : null,
        effectiveRuleEnabled('trailing_drawdown', trailingTakeProfit) ? `回撤 ${percentage(trailingTakeProfit.threshold)} · ${actionLabel(trailingTakeProfit.action_pct)}` : null,
        effectiveRuleEnabled('take_profit_ladder', ladderTakeProfit) ? `分批 ${Number(ladderTakeProfit.first_r ?? 1).toFixed(1)}R/${Number(ladderTakeProfit.second_r ?? 2).toFixed(1)}R` : null,
      ].filter(Boolean).join(' / ')
    : '未启用'

  const stopLoss = effectiveRule(portfolio, symbol, 'stop_loss')
  const stopLossLabels = enabledRuleLabels(portfolio, symbol, STOP_LOSS_RULE_GROUPS.flatMap(([, rules]) => rules)).filter(label => label !== RULE_LABELS.stop_loss)
  const stopLossText = [
    stopLoss.enabled !== false ? `成本 ${percentage(stopLoss.threshold)} · ${actionLabel(stopLoss.action_pct)}` : null,
    stopLossLabels.length ? `${stopLossLabels.slice(0, 3).join(' / ')}${stopLossLabels.length > 3 ? ` +${stopLossLabels.length - 3}` : ''}` : null,
  ].filter(Boolean).join(' · ') || '未启用'

  const tTrading = effectiveRule(portfolio, symbol, 't_trading')
  const tTradingText = tTrading.enabled !== false ? `买 ${percentage(Number(tTrading.buy_pct) / 100)} / 卖 ${percentage(Number(tTrading.sell_pct) / 100)}` : '未启用'
  const intradaySignalIds = options?.builtin_signals.filter(signal => signal.group === 'intraday').map(signal => signal.id) ?? []
  const tTradingSource = hasRuleOverride(portfolio, symbol, ['t_trading']) || hasSignalOverride(portfolio, symbol, 'builtin', intradaySignalIds) ? '已覆盖' : '默认'
  return (
    <div className="min-w-[225px] space-y-1 text-[10px] leading-4">
      {[
        ['止盈', moduleSource(portfolio, symbol, TAKE_PROFIT_RULES), takeProfitText],
        ['止损', moduleSource(portfolio, symbol, STOP_LOSS_RULE_GROUPS.flatMap(([, rules]) => rules), ['builtin', 'custom', 'monitor_rules']), stopLossText],
        ['做 T', tTradingSource, tTradingText],
      ].map(([label, source, value]) => (
        <div key={label} className="grid grid-cols-[32px_38px_minmax(0,1fr)] items-baseline gap-1">
          <span className="text-secondary">{label}</span>
          <span className={cn('text-[9px]', source === '已覆盖' ? 'text-accent' : 'text-muted')}>{source}</span>
          <span className={cn('min-w-0 break-words', value === '未启用' ? 'text-muted' : 'text-foreground')} title={value}>{value}</span>
        </div>
      ))}
    </div>
  )
}

function riskTone(score: number) {
  if (score >= 70) return 'text-danger'
  if (score >= 40) return 'text-warning'
  return 'text-bull'
}

function StatusDot({ status }: { status: PositionRiskStatus }) {
  const active = status === 'websocket'
  const warning = status === 'polling_degraded' || status === 'reconnecting'
  return <span className={cn('h-2 w-2 rounded-full', active ? 'bg-bear' : warning ? 'bg-warning' : 'bg-muted')} />
}

type TradePreset = {
  recommendationId?: string
  action: 'BUY' | 'SELL'
  price?: number | null
  volume?: number | null
}

function TradeOrderPanel({ row, tradePreset, onClose, onTradeSubmitted }: { row: PositionRiskPosition; tradePreset?: TradePreset | null; onClose: () => void; onTradeSubmitted?: (recommendationId: string) => void }) {
  const qmt = useQuery({ queryKey: QK.positionRiskQmt, queryFn: api.qmtStatus, refetchInterval: 30_000 })
  const orders = useQuery({ queryKey: QK.positionRiskQmtOrders, queryFn: api.qmtOrders, enabled: Boolean(qmt.data?.configured), refetchInterval: 15_000 })
  const queryClient = useQueryClient()
  const [tradeAction, setTradeAction] = useState<'BUY' | 'SELL'>(tradePreset?.action ?? 'SELL')
  const [tradePrice, setTradePrice] = useState(String(tradePreset?.price ?? row.price ?? row.cost_price ?? ''))
  const [tradePriceType, setTradePriceType] = useState<'LIMIT' | 'LATEST'>('LIMIT')
  const [tradeVolume, setTradeVolume] = useState(tradePreset?.volume ?? 100)
  useEffect(() => {
    setTradeAction(tradePreset?.action ?? 'SELL')
    setTradePrice(String(tradePreset?.price ?? row.price ?? row.cost_price ?? ''))
    setTradeVolume(tradePreset?.volume ?? 100)
    setTradePriceType('LIMIT')
  }, [row.symbol, row.price, row.cost_price, tradePreset?.action, tradePreset?.price, tradePreset?.volume])
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
      if (tradePreset?.recommendationId && result.order.status !== 'rejected' && result.order.status !== 'unknown') onTradeSubmitted?.(tradePreset.recommendationId)
    },
  })
  const cancelMutation = useMutation({
    mutationFn: api.qmtCancelOrder,
    onSuccess: () => {
      toast('已请求撤单', 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
    },
  })
  return (
    <div className="fixed inset-0 z-40 bg-black/35" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <aside className="absolute inset-y-0 right-0 flex w-full max-w-md flex-col border-l border-border bg-surface shadow-xl">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="min-w-0"><div className="truncate text-sm font-semibold">QMT交易</div><div className="font-mono text-[11px] text-muted">{row.name} · {row.symbol}</div></div>
          <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭交易面板"><X className="h-4 w-4" /></button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto p-4">
          <section className="border-y border-border py-3">
            <div className="flex items-center justify-between"><h3 className="text-xs font-semibold text-secondary">委托参数</h3><span className={cn('text-[10px]', qmt.data?.trade_enabled ? 'text-warning' : 'text-muted')}>{qmt.data?.trade_enabled ? '真实交易已开启' : '交易开关未开启'}</span></div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-muted">
              <label>方向<select value={tradeAction} onChange={event => setTradeAction(event.target.value as 'BUY' | 'SELL')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="SELL">卖出</option><option value="BUY">买入</option></select></label>
              <label>数量<input type="number" min="100" step="100" value={tradeVolume} onChange={event => setTradeVolume(Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px]" /></label>
              <label>价格方式<select value={tradePriceType} onChange={event => setTradePriceType(event.target.value as 'LIMIT' | 'LATEST')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="LIMIT">限价</option><option value="LATEST">最新价</option></select></label>
              <label className={tradePriceType === 'LATEST' ? 'opacity-50' : ''}>限价<input type="number" min="0.001" step="0.001" value={tradePrice} disabled={tradePriceType === 'LATEST'} onChange={event => setTradePrice(event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px] disabled:cursor-not-allowed" /></label>
            </div>
            <button type="button" disabled={!qmt.data?.trade_enabled || qmt.data.state !== 'ready' || tradeMutation.isPending} onClick={() => {
              if (!window.confirm(`确认${tradeAction === 'BUY' ? '买入' : '卖出'} ${row.name} ${tradeVolume} 股？`)) return
              tradeMutation.mutate()
            }} className={cn('mt-2 h-8 w-full rounded-btn text-xs text-white disabled:cursor-not-allowed disabled:opacity-40', tradeAction === 'BUY' ? 'bg-bull' : 'bg-bear')}>
              {tradeMutation.isPending ? '提交中…' : `发送${tradeAction === 'BUY' ? '买入' : '卖出'}委托`}
            </button>
            {tradeMutation.isError && <p className="mt-2 text-[10px] text-danger">委托失败，请检查 QMT 状态和交易开关。</p>}
            <p className="mt-2 text-[10px] leading-4 text-muted">风险配置面板不包含交易；此处委托会进入真实 QMT 账户，成交结果以云端回报为准。</p>
          </section>

          {orders.data?.orders?.some(order => order.symbol === row.symbol && order.order_sys_id) && <section className="border-b border-border py-3">
            <h3 className="mb-2 text-xs font-semibold text-secondary">当前委托</h3>
            <div className="space-y-1">
              {orders.data.orders.filter(order => order.symbol === row.symbol && order.order_sys_id).slice(0, 5).map(order => <div key={order.order_sys_id} className="flex items-center justify-between gap-2 text-[10px] text-muted"><span><span className={order.action === 'SELL' ? 'text-bear' : 'text-bull'}>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '—'}</span> · {qmtOrderStatus(order.status)}</span><button type="button" disabled={cancelMutation.isPending} onClick={() => cancelMutation.mutate(order.order_sys_id!)} className="h-6 rounded border border-border px-2 hover:bg-elevated">撤单</button></div>)}
            </div>
          </section>}
        </div>
      </aside>
    </div>
  )
}

function PositionInspector({ row, options, onClose }: { row: PositionRiskPosition; options: PositionRiskOptions | undefined; onClose: () => void }) {
  const portfolioQuery = useQuery({ queryKey: QK.positionRisk, queryFn: api.positionRiskPortfolio })
  const featuresQuery = useQuery({ queryKey: QK.positionRiskFeatures(row.symbol), queryFn: () => api.positionRiskFeatures([row.symbol]), refetchInterval: 15_000 })
  const queryClient = useQueryClient()
  const [activeRuleTab, setActiveRuleTab] = useState<'take_profit' | 'stop_loss' | 't_trading'>('stop_loss')
  const [expandedRule, setExpandedRule] = useState<string | null>(null)
  const [showAdvancedRules, setShowAdvancedRules] = useState(false)
  const portfolio = portfolioQuery.data
  const override = portfolio?.overrides[row.symbol] ?? {}
  const mutation = useMutation({
    mutationFn: (next: Record<string, any>) => api.positionRiskUpdateOverride(row.symbol, portfolio!.revision, next),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: QK.positionRisk }),
  })
  const setRule = (ruleId: string, enabled: boolean) => {
    const key = SHORT_TERM_RULES.has(ruleId) ? 'active' : 'enabled'
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), [key]: enabled } },
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
  const signalGroups: Array<[string, Array<{ id: string; label: string; direction: string; available?: boolean }>, 'builtin' | 'custom']> = [
    ['入场信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'entry'), 'builtin'],
    ['出场信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'exit'), 'builtin'],
    ['双向信号', (options?.builtin_signals ?? []).filter(signal => signal.group !== 'intraday' && signal.direction === 'both'), 'builtin'],
    ['分时信号', (options?.builtin_signals ?? []).filter(signal => signal.group === 'intraday'), 'builtin'],
    ['自定义信号', options?.custom_signals ?? [], 'custom'],
  ]
  const renderRuleRows = (rules: readonly string[]) => rules.map(ruleId => {
    const evidenceOnly = ['large_buy', 'large_sell', 'continuous_outflow', 'orderbook_imbalance'].includes(ruleId)
    const inherited = SHORT_TERM_RULES.has(ruleId) ? portfolio?.template.rules[ruleId]?.active === true : portfolio?.template.rules[ruleId]?.enabled !== false
    const explicit = SHORT_TERM_RULES.has(ruleId) ? override.rules?.[ruleId]?.active : override.rules?.[ruleId]?.enabled
    const enabled = explicit ?? inherited
    const hasOverride = Object.keys(override.rules?.[ruleId] ?? {}).length > 0
    const fields = ruleId === 'large_buy' || ruleId === 'large_sell'
      ? LARGE_ORDER_FIELDS
      : POSITION_RISK_RULE_FIELDS[ruleId] ?? []
    const expanded = expandedRule === ruleId
    return (
      <div key={ruleId} className="py-2 text-xs">
        <div className="flex min-h-8 items-center gap-2">
          <button type="button" onClick={() => setExpandedRule(expanded ? null : ruleId)} className="flex min-w-0 flex-1 items-center gap-2 text-left" aria-expanded={expanded}>
            <ChevronDown className={cn('h-3.5 w-3.5 shrink-0 text-muted transition-transform', expanded ? '' : '-rotate-90')} />
            <span className="truncate">{RULE_LABELS[ruleId] ?? ruleId}</span>
          </button>
          <span className="shrink-0 bg-elevated px-1.5 py-0.5 text-[10px] text-muted">{hasOverride ? '已覆盖' : '默认'}</span>
          <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted"><span>监控</span><input type="checkbox" checked={enabled} disabled={mutation.isPending} onChange={event => setRule(ruleId, event.target.checked)} aria-label={`监控${RULE_LABELS[ruleId] ?? ruleId}`} /></label>
          {!evidenceOnly && <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted"><span>通知</span><input type="checkbox" checked={override.rules?.[ruleId]?.notify ?? (portfolio?.template.rules[ruleId]?.notify === true)} disabled={mutation.isPending} onChange={event => setRuleNotify(ruleId, event.target.checked)} aria-label={`通知${RULE_LABELS[ruleId] ?? ruleId}信号`} /></label>}
        </div>
        {expanded && fields.length > 0 && (
          <div className="mt-2 grid grid-cols-2 gap-2 pl-5">
            {fields.map(field => {
              const inheritedValue = portfolio?.template.rules[ruleId]?.[field.key] ?? field.defaultValue ?? 0
              const storedValue = override.rules?.[ruleId]?.[field.key] ?? inheritedValue
              const displayValue = field.percent ? Number(storedValue) * 100 : Number(storedValue)
              return (
                <label key={field.key} className="min-w-0 text-[10px] text-muted">
                  <span>{field.label}</span>
                  <span className="mt-1 flex h-7 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                    {field.type === 'select' ? (
                      <select
                        value={String(override.rules?.[ruleId]?.[field.key] ?? portfolio?.template.rules[ruleId]?.[field.key] ?? field.options?.[0]?.[0] ?? '')}
                        disabled={mutation.isPending || !enabled}
                        onChange={event => mutation.mutate({
                          ...override,
                          rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), [field.key]: event.target.value } },
                        })}
                        className="min-w-0 flex-1 bg-transparent text-[11px] text-foreground outline-none disabled:opacity-50"
                      >
                        {(field.options ?? []).map(([optionValue, optionLabel]) => <option key={optionValue} value={optionValue}>{optionLabel}</option>)}
                      </select>
                    ) : (
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
                    )}
                    {field.suffix && <span className="ml-1 shrink-0">{field.suffix}</span>}
                  </span>
                </label>
              )
            })}
          </div>
        )}
      </div>
    )
  })
  const renderSignalGroups = (groups: Array<[string, Array<{ id: string; label: string; direction: string; available?: boolean }>, 'builtin' | 'custom']>) => groups.map(([title, signals, storageGroup]) => (
    <section key={title}>
      <h3 className="mb-2 text-xs font-semibold text-secondary">{title}</h3>
      <div className="divide-y divide-border border-y border-border">
        {signals.length ? signals.map(signal => {
          const explicit = override.signals?.[storageGroup]?.[signal.id]?.enabled
          const inherited = portfolio?.template.signals[storageGroup]?.[signal.id]?.enabled !== false
          const available = signal.available !== false
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
                  <span className="text-[10px] text-muted">{hasOverride ? '已覆盖' : '默认'}</span>
                  <input type="checkbox" checked={available && (explicit ?? inherited)} disabled={mutation.isPending || !available} onChange={event => setSignal(storageGroup, signal, event.target.checked)} aria-label={`监控${signal.label}`} />
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
                {title === '分时信号' ? (
                  <div className="text-[10px] text-muted"><span>建议比例</span><span className="mt-1 flex h-7 items-center border border-border bg-elevated px-2">由做T模块控制</span></div>
                ) : (
                  <label className="text-[10px] text-muted">建议比例
                    <select value={explicitAction == null ? 'inherit' : String(explicitAction)} disabled={mutation.isPending || !available} onChange={event => setSignalValue(storageGroup, signal.id, 'action_pct', event.target.value === 'inherit' ? null : Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[10px]">
                      <option value="inherit">继承（{inheritedAction ? `${inheritedAction}%` : '提醒'}）</option>
                      <option value="0">提醒</option><option value="25">减仓 25%</option><option value="50">减仓 50%</option><option value="100">清仓</option>
                    </select>
                  </label>
                )}
              </div>
            </div>
          )
        }) : <p className="py-2 text-xs text-muted">暂无可用信号</p>}
      </div>
    </section>
  ))
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
          <div className="grid grid-cols-5 gap-px border-y border-border bg-border text-xs">
            {[
              ['风险分', String(row.risk_score)], ['仓位', pct(row.weight)],
              ['MA5', price(row.ma5)], ['MA10', price(row.ma10)], ['MA20', price(row.ma20)],
            ].map(([label, value]) => <div key={label} className="bg-surface px-3 py-2"><div className="text-[10px] text-muted">{label}</div><div className="mt-1 font-mono">{value}</div></div>)}
          </div>

          {(() => {
            const feature = featuresQuery.data?.features[row.symbol]
            const featureState = feature?.fresh ? '数据新鲜' : feature?.reason || '等待闭合分钟数据'
            return (
              <section className="mt-3 border-y border-border bg-elevated/30 px-3 py-2 text-[10px]">
                <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                  <span className={cn('font-medium', feature?.fresh ? 'text-bull' : 'text-warning')}>{featureState}</span>
                  <span className="text-muted">阶段 <b className="text-foreground">{feature?.stage || 'initial'}</b></span>
                  <span className="text-muted">R <b className="font-mono text-foreground">{feature?.r_multiple == null ? '—' : feature.r_multiple.toFixed(2)}</b></span>
                  <span className="text-muted">有效保护价 <b className="font-mono text-foreground">{price(feature?.effective_stop_price)}</b></span>
                  <span className="text-muted">1m/5m <b className="font-mono text-foreground">{feature?.bars_1m ?? 0}/{feature?.bars_5m ?? 0}</b></span>
                  <span className="text-muted">今日做T <b className="font-mono text-foreground">{feature?.t_trade_count ?? 0} 次</b></span>
                </div>
                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-muted">
                  <span>{feature?.as_of ? `最后闭合 ${new Date(feature.as_of).toLocaleTimeString('zh-CN')}` : '分时能力不可用时不会生成新的短线建议'}</span>
                  {feature?.session_vwap != null && <span>VWAP <b className="font-mono text-foreground">{price(feature.session_vwap)}</b></span>}
                  {feature?.ema9_1m != null && feature?.ema20_1m != null && <span>EMA9/20 <b className="font-mono text-foreground">{price(feature.ema9_1m)}/{price(feature.ema20_1m)}</b></span>}
                  {feature?.atr14_5m != null && <span>ATR5m <b className="font-mono text-foreground">{price(feature.atr14_5m)}</b></span>}
                  {(feature?.previous_day_high != null || feature?.previous_day_low != null) && <span>昨高/昨低 <b className="font-mono text-foreground">{price(feature.previous_day_high)}/{price(feature.previous_day_low)}</b></span>}
                </div>
              </section>
            )
          })()}

          <nav className="mt-5 flex border-b border-border" aria-label="单股风控模块">
            {INTRADAY_RULE_TAB.map(([id, label]) => (
              <button key={id} type="button" onClick={() => { setActiveRuleTab(id); setExpandedRule(null) }} className={cn('h-9 flex-1 border-b-2 text-xs', activeRuleTab === id ? 'border-accent text-foreground' : 'border-transparent text-muted hover:text-foreground')}>
                {label}
              </button>
            ))}
          </nav>

          {activeRuleTab === 'take_profit' && (
            <section className="mt-4">
              <div className="mb-2 flex items-end justify-between gap-3">
                <div><h3 className="text-xs font-semibold text-secondary">止盈规则</h3><p className="mt-1 text-[10px] text-muted">固定目标和盈利后高点回撤分别判断。</p></div>
                <span className="text-[10px] text-muted">{portfolio && hasRuleOverride(portfolio, row.symbol, TAKE_PROFIT_RULES) ? '本股覆盖' : '继承模板'}</span>
              </div>
              <div className="divide-y divide-border border-y border-border">{renderRuleRows(TAKE_PROFIT_RULES)}</div>
            </section>
          )}

          {activeRuleTab === 'stop_loss' && (
            <div className="mt-4 space-y-5">
              {STOP_LOSS_RULE_GROUPS.map(([group, rules]) => (
                <section key={group}>
                  <h3 className="mb-2 text-xs font-semibold text-secondary">{group}</h3>
                  <div className="divide-y divide-border border-y border-border">{renderRuleRows(rules)}</div>
                </section>
              ))}
              <button type="button" onClick={() => setShowAdvancedRules(value => !value)} className="flex h-9 w-full items-center justify-between border-y border-border px-1 text-left text-xs text-secondary hover:text-foreground" aria-expanded={showAdvancedRules}>
                <span className="flex items-center gap-2"><ChevronDown className={cn('h-3.5 w-3.5 transition-transform', showAdvancedRules ? '' : '-rotate-90')} />高级风控与信号</span>
                <span className="text-[10px] text-muted">账户、资金、系统信号和监控中心</span>
              </button>
              {showAdvancedRules && (
                <div className="space-y-5">
                  <section>
                    <h3 className="mb-2 text-xs font-semibold text-secondary">账户与资金</h3>
                    <div className="divide-y divide-border border-y border-border">{renderRuleRows(ADVANCED_RULES)}</div>
                  </section>
                  {renderSignalGroups(signalGroups.filter(([title]) => title !== '分时信号'))}
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
                              <select value={explicit == null ? 'inherit' : String(explicit)} disabled={mutation.isPending} onChange={event => setMonitorAction(rule.id, event.target.value === 'inherit' ? null : Number(event.target.value))} className="h-7 rounded border border-border bg-surface px-2 text-[11px]" aria-label={`${rule.name}建议比例`}>
                                <option value="inherit">继承模板（{inherited ? `${inherited}%` : '时间线'}）</option><option value="0">只进时间线</option><option value="25">建议减仓 25%</option><option value="50">建议减仓 50%</option><option value="100">建议清仓</option>
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
                        {Object.entries(row.evidence).map(([key, ready]) => <span key={key} className={cn('rounded px-1.5 py-0.5 text-[10px]', ready ? 'bg-bull/10 text-bull' : 'bg-elevated text-muted')}>{({ cost: '成本', history: '历史', quote: '实时价', depth: '五档', flow: '资金' } as Record<string, string>)[key]}</span>)}
                      </div>
                    </div>
                  </section>
                </div>
              )}
            </div>
          )}

          {activeRuleTab === 't_trading' && (
            <section className="mt-4 space-y-5">
              <div className="flex flex-col gap-2 border-y border-border py-3 sm:flex-row sm:items-start sm:justify-between">
                <div><h3 className="text-xs font-semibold text-secondary">做 T 参数</h3><p className="mt-1 text-[10px] text-muted">入场信号生成买入建议，出场信号生成卖出建议；不会自动下单。</p></div>
                <span className={cn('text-[10px]', options?.capabilities.intraday.available ? 'text-bull' : 'text-warning')}>{options?.capabilities.intraday.available ? `分时可用 · 最多 ${options.capabilities.intraday.max_symbols} 只` : options?.capabilities.intraday.reason || '分时不可用'}</span>
              </div>
              <div className="divide-y divide-border border-y border-border">{renderRuleRows(['t_trading'])}</div>
              <div>{renderSignalGroups(signalGroups.filter(([title]) => title === '分时信号'))}</div>
            </section>
          )}
        </div>
        <div className="flex items-center justify-between border-t border-border px-4 py-3">
          <span className="text-[11px] text-muted">{Object.keys(override).length ? '存在单股覆盖' : '全部继承全局模板'}</span>
          <button type="button" disabled={mutation.isPending || !Object.keys(override).length} onClick={() => mutation.mutate({})} className="h-8 rounded-btn border border-border px-3 text-xs disabled:opacity-40">恢复继承</button>
        </div>
      </aside>
    </div>
  )
}

function PendingRow({ item, revision, name, signalNames, onOpenTrade }: { item: PositionRiskRecommendation; revision: number; name?: string; signalNames: Record<string, string>; onOpenTrade: (item: PositionRiskRecommendation) => void }) {
  const queryClient = useQueryClient()
  const ruleLabel = item.trade_action ? '做T' : RULE_LABELS[item.rule_id] ?? cnSignal(item.rule_id)
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
        <button type="button" disabled={mutation.isPending} onClick={() => item.trade_action ? onOpenTrade(item) : mutation.mutate('confirm')} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white">
          {mutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : item.trade_action ? <ChevronRight className="h-3.5 w-3.5" /> : <Check className="h-3.5 w-3.5" />}{item.trade_action ? '打开下单面板' : '确认建议'}
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
  const [tradeRow, setTradeRow] = useState<PositionRiskPosition | null>(null)
  const [tradePreset, setTradePreset] = useState<TradePreset | null>(null)
  const [preview, setPreview] = useState<{ symbol: string; name: string } | null>(null)
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
  const tradeRecommendationMutation = useMutation({
    mutationFn: (recommendationId: string) => api.positionRiskRecommendationAction(recommendationId, 'confirm', data.revision),
    onSuccess: result => {
      toast(result.message, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskRecommendations('pending') })
      queryClient.invalidateQueries({ queryKey: QK.positionRisk })
    },
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
  if (portfolio.isLoading) return <div className="grid h-full place-items-center"><Loader2 className="h-6 w-6 animate-spin text-accent" /></div>
  if (portfolio.isError || !portfolio.data) return <EmptyState icon={AlertTriangle} title="持仓风控加载失败" hint="请检查后端服务后重试" />
  const data = portfolio.data
  const namesBySymbol = new Map(data.positions.map(row => [row.symbol, row.name]))
  const runtimeReason = compactRuntimeReason(data.runtime.reason)
  const openTradeRecommendation = (item: PositionRiskRecommendation) => {
    const row = data.positions.find(position => position.symbol === item.symbol)
    if (!row || !item.trade_action) return
    setSelected(null)
    setTradeRow(row)
    setTradePreset({
      recommendationId: item.id,
      action: item.trade_action,
      price: item.suggested_price ?? row.price,
      volume: item.suggested_volume ?? 100,
    })
  }
  const openTradeForRow = (row: PositionRiskPosition) => {
    setSelected(null)
    setTradeRow(row)
    setTradePreset(null)
  }

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
          <span className="text-muted">总资产 <b className="font-mono text-foreground">{money(data.account.total_asset)}</b></span>
          <span className={cn('inline-flex items-center gap-1.5', data.runtime.status === 'websocket' ? 'text-bear' : data.runtime.status === 'polling_degraded' || data.runtime.status === 'reconnecting' ? 'text-warning' : 'text-muted')}><StatusDot status={data.runtime.status} />{STATUS_LABEL[data.runtime.status]}</span>
          <span className={cn('inline-flex items-center gap-1.5', qmt.data?.state === 'ready' ? 'text-bear' : qmt.data?.configured ? 'text-warning' : 'text-muted')}><StatusDot status={qmt.data?.state === 'ready' ? 'websocket' : 'data_unavailable'} />QMT {qmt.data?.state === 'ready' ? '已连接' : qmt.data?.configured ? '待检查' : '未配置'}</span>
          <span className={qmt.data?.auto_sync_running ? 'text-bear' : 'text-muted'}>{qmt.data?.auto_sync_running ? `自动同步 ${qmt.data.auto_sync_interval_seconds}秒` : '自动同步未运行'}</span>
          <label className="inline-flex items-center gap-1.5 text-muted" title={!qmt.data?.trade_authorized ? '后端未授权实盘交易' : '取消勾选可暂停本次运行的实盘下单'}><input type="checkbox" checked={qmt.data?.trade_enabled === true} disabled={!qmt.data?.configured || !qmt.data?.trade_authorized || qmtToggle.isPending} onChange={event => qmtToggle.mutate(event.target.checked)} />实盘模式</label>
          <button type="button" onClick={() => qmtProbe.mutate()} disabled={qmtProbe.isPending} className="h-7 rounded-btn border border-border px-2 text-[11px] hover:bg-elevated disabled:opacity-50">{qmtProbe.isPending ? '检查中…' : '检查 QMT 连接'}</button>
          {runtimeReason && <span className="ml-auto max-w-[10rem] truncate text-[11px] text-muted" title={data.runtime.reason}>{runtimeReason}</span>}
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
              {['证券', '数量 / 可用', '成本 / 现价', '仓位', '风控设置', '证据', '信号', '风险', '建议', '交易'].map(label => <th key={label} className="px-3 py-2 text-left font-medium">{label}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-border/70">
              {rows.map(row => <tr key={row.symbol} className="hover:bg-elevated/35">
                <td className="px-3 py-2"><button type="button" onClick={() => setPreview({ symbol: row.symbol, name: row.name })} className="text-left hover:text-accent" title="查看 K 线与分时"><div className="font-medium">{row.name}</div><div className="font-mono text-[10px] text-muted">{row.symbol}</div></button></td>
                <td className="px-3 py-2 font-mono">{row.quantity.toLocaleString()}<div className="text-[10px] text-muted">可用 {row.available.toLocaleString()}</div></td>
                <td className="px-3 py-2 font-mono">{price(row.cost_price)}<div className="text-[10px] text-muted">{price(row.price)}</div></td>
                <td className="px-3 py-2 font-mono">{pct(row.weight)}</td>
                <td className="px-3 py-2"><div className="flex items-start gap-2"><RiskSettingsSummary portfolio={data} symbol={row.symbol} options={options.data} /><button type="button" onClick={() => setSelected(row)} className="grid h-7 w-7 shrink-0 place-items-center rounded hover:bg-elevated" title="编辑单股风控" aria-label={`编辑${row.name}风控设置`}><Settings2 className="h-3.5 w-3.5" /></button></div></td>
                <td className="px-3 py-2"><div className="h-1.5 w-20 overflow-hidden rounded-full bg-elevated"><div className="h-full bg-accent" style={{ width: `${row.evidence_coverage * 100}%` }} /></div><span className="mt-1 block font-mono text-[10px] text-muted">{Math.round(row.evidence_coverage * 100)}%</span></td>
                <td className="max-w-36 truncate px-3 py-2 text-muted" title={row.latest_signal ? cnSignal(row.latest_signal) : ''}>{row.latest_signal ? cnSignal(row.latest_signal) : '—'}</td>
                <td className={cn('px-3 py-2 font-mono text-sm font-semibold', riskTone(row.risk_score))}>{row.risk_score}</td>
                <td className="px-3 py-2">{row.suggestion ? <span className="text-warning">{row.suggestion.action} {row.suggestion.reduction_pct}%</span> : <span className="text-muted">观察</span>}</td>
                <td className="px-3 py-2"><button type="button" onClick={() => openTradeForRow(row)} className="grid h-7 w-7 place-items-center rounded hover:bg-elevated" title="打开交易面板" aria-label={`打开${row.name}交易面板`}><ChevronRight className="h-4 w-4" /></button></td>
              </tr>)}
            </tbody>
          </table>
        </div>
        <div className="divide-y divide-border md:hidden">
          {rows.map(row => <div key={row.symbol} className="grid w-full grid-cols-[1fr_auto] gap-3 px-4 py-3 text-left">
            <button type="button" onClick={() => setPreview({ symbol: row.symbol, name: row.name })} className="min-w-0 text-left" title="查看 K 线与分时"><div className="font-medium hover:text-accent">{row.name}<span className="ml-2 font-mono text-[10px] text-muted">{row.symbol}</span></div><div className="mt-1 text-xs text-muted">{row.quantity.toLocaleString()} 股 · 成本 {price(row.cost_price)} · 现价 {price(row.price)}</div><div className="mt-1 text-[11px] text-muted">{row.suggestion ? `${row.suggestion.action} ${row.suggestion.reduction_pct}%` : row.latest_signal ? cnSignal(row.latest_signal) : '观察'}</div><div className="mt-2 border-t border-border/70 pt-2"><RiskSettingsSummary portfolio={data} symbol={row.symbol} options={options.data} /></div></button>
            <div className="flex items-center gap-2"><div className="text-right"><div className="text-[10px] text-muted">风险</div><div className={cn('font-mono text-base font-semibold', riskTone(row.risk_score))}>{row.risk_score}</div></div><button type="button" onClick={() => openTradeForRow(row)} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" title="打开交易面板" aria-label={`打开${row.name}交易面板`}><ChevronRight className="h-4 w-4" /></button><button type="button" onClick={() => setSelected(row)} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" title="编辑单股风控" aria-label={`编辑${row.name}风控设置`}><Settings2 className="h-4 w-4" /></button></div>
          </div>)}
        </div>
      </> : <EmptyState icon={ShieldCheck} title={data.positions.length ? '没有符合筛选的持仓' : '尚未导入持仓'} hint={data.positions.length ? '调整搜索或风险筛选' : '使用顶部“图片导入”上传同花顺手机持仓截图'} />)}

      {tab === 'pending' && <div>
        <div className="border-b border-border bg-warning/5 px-4 py-2 text-[11px] text-warning sm:px-5">确认建议仅记录人工判断，不修改持仓、模拟盘或通知券商委托。</div>
        {pending.data?.recommendations.length ? pending.data.recommendations.map(item => <PendingRow key={item.id} item={item} revision={data.revision} name={item.symbol ? namesBySymbol.get(item.symbol) : undefined} signalNames={signalNames} onOpenTrade={openTradeRecommendation} />) : <EmptyState icon={ShieldCheck} title="没有待确认建议" hint="风险事件触发后会在这里汇总" />}
      </div>}

      {tab === 'events' && <div className="divide-y divide-border">
        {events.data?.events.length ? events.data.events.map((event, index) => <div key={`${event.fingerprint ?? event.ts}-${index}`} className="grid gap-2 px-4 py-3 text-xs sm:grid-cols-[150px_120px_1fr_80px] sm:px-5">
          <time className="font-mono text-muted"><span className="block">{new Date(event.ts).toLocaleString('zh-CN')}</span>{(event.occurrence_count ?? 1) > 1 && event.first_ts ? <span className="mt-0.5 block text-[10px]">首次 {new Date(event.first_ts).toLocaleTimeString('zh-CN')}</span> : null}</time>{event.symbol ? <button type="button" onClick={() => setPreview({ symbol: event.symbol!, name: event.name || event.symbol! })} className="text-left hover:text-accent" title="查看 K 线与分时">{event.symbol} {event.name}</button> : <span>组合</span>}<span className="min-w-0"><span className="inline-flex flex-wrap items-center gap-1.5"><span className="rounded bg-elevated px-1.5 py-0.5 text-[11px] text-secondary">{event.rule_id?.startsWith('t:') ? '做T' : event.rule_id === 'vwap_breakdown' ? RULE_LABELS.vwap_breakdown : event.rule_name || RULE_LABELS[event.rule_id || ''] || cnSignalText(event.message, signalNames)}</span>{(event.occurrence_count ?? 1) > 1 ? <span className="rounded bg-warning/10 px-1.5 py-0.5 font-mono text-[10px] text-warning">共 {event.occurrence_count} 次</span> : null}</span>{event.reasons?.length ? <span className="mt-1 block break-words text-[11px] leading-5 text-muted">{event.reasons.map(reason => cnSignalText(reason, signalNames)).join('；')}</span> : null}</span><span className={event.severity === 'critical' ? 'text-danger' : event.severity === 'warn' ? 'text-warning' : 'text-muted'}>{event.source === 'position_risk' ? '持仓风控' : '监控中心'}</span>
        </div>) : <EmptyState icon={FileClock} title="暂无触发记录" hint="持仓规则和监控中心命中会进入同一时间线" />}
      </div>}

      {tab === 'positions' && qmtOrders.data?.orders?.length ? <section className="border-t border-border px-4 py-3 sm:px-5">
        <div className="mb-2 flex flex-wrap items-baseline gap-x-2 gap-y-1"><h2 className="text-xs font-semibold">QMT 委托</h2><span className="text-[10px] text-muted">成交状态以云端 QMT 查询为准</span></div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[720px] table-fixed text-left text-[11px]">
            <thead className="border-y border-border bg-elevated/45 text-muted">
              <tr>
                <th className="w-[18%] px-3 py-2 font-medium">证券代码</th>
                <th className="w-[16%] px-3 py-2 font-medium">方向 / 数量</th>
                <th className="w-[14%] px-3 py-2 font-medium">委托价格</th>
                <th className="w-[18%] px-3 py-2 font-medium">委托状态</th>
                <th className="w-[34%] px-3 py-2 font-medium">委托编号 / 说明</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/70 text-muted">
              {qmtOrders.data.orders.slice(0, 20).map((order, index) => <tr key={`${order.order_sys_id ?? order.idempotency_key ?? index}`} className="hover:bg-elevated/30">
                <td className="px-3 py-2 font-mono text-secondary">{order.symbol ?? order.stock_code ?? '—'}</td>
                <td className={cn('px-3 py-2 font-mono font-medium', order.action === 'SELL' ? 'text-bear' : 'text-bull')}>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '—'}</td>
                <td className="px-3 py-2 font-mono">{qmtOrderPrice(order.price, order.price_type)}</td>
                <td className="px-3 py-2">{qmtOrderStatus(order.status)}</td>
                <td className="px-3 py-2"><span className="block truncate" title={order.order_sys_id ? `委托号 ${order.order_sys_id}` : undefined}>{order.order_sys_id ? `委托号 ${order.order_sys_id}` : order.status === 'unknown' ? '请在 QMT 核对，禁止原单重发' : '等待云端委托号'}</span></td>
              </tr>)}
            </tbody>
          </table>
        </div>
      </section> : null}

      <PositionRiskImportDialog open={importOpen} portfolio={data} onClose={() => setImportOpen(false)} />
      <PositionRiskRulesDialog open={rulesOpen} portfolio={data} options={options.data} onClose={() => setRulesOpen(false)} />
      {selected && <PositionInspector row={selected} options={options.data} onClose={() => setSelected(null)} />}
      {tradeRow && <TradeOrderPanel row={tradeRow} tradePreset={tradePreset} onTradeSubmitted={recommendationId => tradeRecommendationMutation.mutate(recommendationId)} onClose={() => { setTradeRow(null); setTradePreset(null) }} />}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
