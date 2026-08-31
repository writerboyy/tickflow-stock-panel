import { lazy, Suspense, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  ChevronDown,
  FileClock,
  ImagePlus,
  LineChart,
  Loader2,
  RefreshCw,
  Search,
  ShieldCheck,
  X,
} from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { PositionRiskImportDialog } from '@/components/PositionRiskImportDialog'
import { Modal } from '@/components/Modal'
import { LARGE_ORDER_FIELDS, POSITION_RISK_RULE_FIELDS } from '@/lib/positionRiskRuleFields'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { toast } from '@/components/Toast'
import { QmtTradePanel, type QmtRiskTradeContext, type QmtTradePreset } from '@/components/QmtTradePanel'
import {
  api,
  type AiStockReport,
  type PositionRiskOptions,
  type PositionRiskEvent,
  type PositionRiskFeatureSnapshot,
  type PositionRiskPosition,
  type PositionRiskPortfolio,
  type PositionRiskStatus,
  type QmtOrder,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { cn } from '@/lib/cn'
import { cnSignalText } from '@/lib/signals'

const StockAnalysis = lazy(() => import('./StockAnalysis').then(module => ({ default: module.StockAnalysis })))

type Tab = 'positions' | 'events'

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
  blocked: '未提交（门禁）',
  error: '提交异常',
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

function qmtOrderReason(order: QmtOrder): string | null {
  const value = [order.error, order.status_msg, order.status_message, order.message, order.reason]
    .find(item => typeof item === 'string' && item.trim())
  return typeof value === 'string' ? value.trim() : null
}

function qmtOrderPrice(value: unknown, priceType?: string) {
  const numericValue = Number(value)
  if (value != null && Number.isFinite(numericValue)) return price(numericValue)
  return priceType === 'LATEST' ? '最新价' : priceType || '—'
}

const STOP_LOSS_RULE_GROUPS = [
  ['动态行为退出', ['intraday_peak_pullback', 'sector_leader_weakening', 'volume_price_divergence', 'opening_volume_selloff', 'broken_limit_up']],
] as const
const SHORT_TERM_RULES = new Set([
  'take_profit_ladder', 'structure_stop', 'atr_protection', 'time_stop',
  'next_day_gap_down', 'next_day_gap_up_take_profit',
  'opening_range_failure', 't_plus_one_exit',
])
const ACTIVE_RULES = new Set([...SHORT_TERM_RULES, 'intraday_peak_pullback'])
const DYNAMIC_EXIT_RULES = ['intraday_peak_pullback', 'sector_leader_weakening', 'volume_price_divergence', 'opening_volume_selloff'] as const
type RiskModuleTab = 'all' | 'stop_loss'

const RULE_LABELS: Record<string, string> = {
  market_context: '市场上下文门控',
  stop_loss: '成本保护', take_profit: '固定收益卖出', trailing_drawdown: '盈利回撤卖出', ma5_breakdown: '破 MA5', ma10_breakdown: '破 MA10', ma20_breakdown: '破 MA20',
  intraday_peak_pullback: '盘中冲高回落', sector_leader_weakening: '板块/龙头相关性走弱', volume_price_divergence: '双峰量价背离', opening_volume_selloff: '早盘放量杀跌', next_day_gap_down: '次日跳空低开', next_day_gap_up_take_profit: '次日高开卖出', opening_range_failure: '开盘区间失败', t_plus_one_exit: 'T+1 强制退出',
  five_minute_drawdown: '5 分钟回撤', vwap_breakdown: '分时均价负偏离超限', broken_limit_up: '炸板', resealed_limit_up: '回封',
  sealed_order_shrink_50: '封单减少 50%', sealed_order_shrink_80: '封单减少 80%', limit_down: '跌停', large_buy: '大单买入',
  large_sell: '大单卖出', continuous_outflow: '连续净流出', orderbook_imbalance: '盘口失衡', daily_equity_loss: '当日权益亏损',
  fund_flow_pressure: '资金卖压',
  take_profit_ladder: 'R 倍数分批卖出', structure_stop: '分时结构保护', atr_protection: 'ATR 移动保护', time_stop: '时间保护退出',
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

function holdingPnl(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${value >= 0 ? '+' : ''}${value.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function todayPnl(totalAsset: number | null | undefined, previousCloseTotalAsset: number | null | undefined, available = true) {
  if (!available || totalAsset == null || previousCloseTotalAsset == null || !Number.isFinite(totalAsset) || !Number.isFinite(previousCloseTotalAsset)) return null
  return totalAsset - previousCloseTotalAsset
}

function pct(value: number | null | undefined) {
  return value == null ? '—' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(2)}%`
}

const CONTEXT_STATE_LABELS: Record<string, string> = {
  supportive: '支持', neutral: '中性', weakening: '走弱', divergent: '背离', unavailable: '等待数据',
}

function contextStateLabel(value?: string | null) {
  return value ? CONTEXT_STATE_LABELS[value] ?? value : '等待数据'
}

function contextStateClass(value?: string | null) {
  return value === 'supportive' ? 'text-bull' : value === 'weakening' || value === 'divergent' ? 'text-warning' : 'text-muted'
}

function effectiveRule(portfolio: PositionRiskPortfolio, symbol: string, ruleId: string) {
  return portfolio.overrides[symbol]?.rules?.[ruleId] ?? {}
}

function isRuleEnabled(portfolio: PositionRiskPortfolio, symbol: string, ruleId: string) {
  const config = effectiveRule(portfolio, symbol, ruleId)
  const dynamicDefault = ['intraday_peak_pullback', 'sector_leader_weakening', 'volume_price_divergence', 'opening_volume_selloff'].includes(ruleId)
  const enabled = config.enabled ?? dynamicDefault
  return enabled === true && (!ACTIVE_RULES.has(ruleId) || config.active !== false)
}

function RiskSettingsSummary({ portfolio, symbol, onOpen }: { portfolio: PositionRiskPortfolio; symbol: string; onOpen: (tab: RiskModuleTab) => void }) {
  const sellRuleLines = (['intraday_peak_pullback', 'sector_leader_weakening', 'volume_price_divergence', 'opening_volume_selloff', 'broken_limit_up'] as const)
    .filter(ruleId => isRuleEnabled(portfolio, symbol, ruleId) || !portfolio.overrides[symbol]?.rules?.[ruleId])
    .map(ruleId => RULE_LABELS[ruleId] ?? ruleId)
  return (
    <button type="button" onClick={() => onOpen('all')} className="min-h-[82px] w-full min-w-0 rounded border border-border px-1.5 py-1 text-left text-[10px] leading-4 hover:border-accent/50 hover:bg-elevated" title="卖出规则参数">
      <span className="block truncate text-secondary">卖出规则</span>
      {sellRuleLines.map((line, index) => <span key={`sell-rule-${index}`} className="block truncate text-[9px] text-secondary">{line}</span>)}
    </button>
  )
}

function reportTime(report: AiStockReport) {
  const value = new Date(report.created_at)
  if (Number.isNaN(value.getTime())) return ''
  return value.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
}

function StockAnalysisStatus({ row, report, loading, onOpen }: { row: PositionRiskPosition; report?: AiStockReport; loading: boolean; onOpen: () => void }) {
  if (loading) return <span className="text-[10px] text-muted">分析状态加载中…</span>
  return (
    <button type="button" onClick={onOpen} className="group flex min-w-[112px] items-center gap-1.5 text-left hover:text-accent" title="在侧边栏打开完整个股分析页面" aria-label={`打开${row.name}完整个股分析`}>
      <LineChart className="h-3.5 w-3.5 shrink-0 text-accent/80 group-hover:text-accent" />
      <span className="min-w-0">
        <span className={cn('block text-[11px]', report ? 'text-secondary' : 'text-warning')}>{report ? '打开完整分析' : '暂无分析 · 查看'}</span>
        {report && <span className="block text-[10px] text-muted">{reportTime(report)}</span>}
      </span>
    </button>
  )
}

function StockAnalysisDrawer({ symbol, name, onClose }: { symbol: string; name: string; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/35" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <aside role="dialog" aria-label={`${name}完整个股分析`} className="h-full w-full max-w-[1180px] overflow-y-auto border-l border-border bg-base shadow-2xl">
        <Suspense fallback={<div className="grid h-full place-items-center"><Loader2 className="h-6 w-6 animate-spin text-accent" /></div>}>
          <StockAnalysis embedded initialSymbol={symbol} initialName={name} onClose={onClose} />
        </Suspense>
      </aside>
    </div>
  )
}

function StatusDot({ status }: { status: PositionRiskStatus }) {
  const active = status === 'websocket'
  const warning = status === 'polling_degraded' || status === 'reconnecting'
  return <span className={cn('h-2 w-2 rounded-full', active ? 'bg-bear' : warning ? 'bg-warning' : 'bg-muted')} />
}

type BatchDynamicDraft = Record<string, Record<string, number | boolean>>

function createBatchDynamicDraft(options?: PositionRiskOptions): BatchDynamicDraft {
  return Object.fromEntries(DYNAMIC_EXIT_RULES.map(ruleId => {
    const defaults = options?.rules[ruleId] ?? {}
    return [ruleId, {
      enabled: defaults.enabled !== false,
      active: ruleId === 'intraday_peak_pullback' ? true : undefined,
      notify: defaults.notify !== false,
      action_pct: Number(defaults.action_pct ?? 100),
      ...Object.fromEntries((POSITION_RISK_RULE_FIELDS[ruleId] ?? []).filter(field => field.key !== 'action_pct').map(field => [
        field.key,
        Number(defaults[field.key] ?? field.defaultValue ?? 0),
      ])),
    }]
  })) as BatchDynamicDraft
}

function BatchDynamicDialog({
  scope,
  count,
  options,
  pending,
  onClose,
  onSubmit,
}: {
  scope: 'selected' | 'all'
  count: number
  options?: PositionRiskOptions
  pending: boolean
  onClose: () => void
  onSubmit: (rules: Record<string, Record<string, any>>, clearRuleIds: string[]) => void
}) {
  const [draft, setDraft] = useState<BatchDynamicDraft>(() => createBatchDynamicDraft(options))
  const [cleared, setCleared] = useState<Set<string>>(new Set())
  const update = (ruleId: string, key: string, value: number | boolean) => {
    setDraft(current => ({ ...current, [ruleId]: { ...current[ruleId], [key]: value } }))
  }
  const submit = () => {
    const rules: Record<string, Record<string, any>> = {}
    for (const ruleId of DYNAMIC_EXIT_RULES) {
      if (!cleared.has(ruleId)) {
        const config = { ...draft[ruleId] }
        if (ruleId === 'intraday_peak_pullback') config.active = config.enabled === true
        rules[ruleId] = config
      }
    }
    onSubmit(rules, [...cleared])
  }
  return (
    <Modal onClose={onClose} labelledBy="batch-dynamic-risk-title" closeOnBackdrop={!pending} panelClassName="max-h-[90vh] w-[min(760px,94vw)] overflow-hidden bg-surface border border-border rounded-card shadow-xl">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div><h2 id="batch-dynamic-risk-title" className="text-sm font-semibold">批量配置动态退出</h2><p className="mt-1 text-[10px] text-muted">应用到{scope === 'selected' ? `选中 ${count} 只持仓` : '全部当前持仓'}，未填写字段保留逐票覆盖。</p></div>
        <button type="button" onClick={onClose} disabled={pending} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated disabled:opacity-50" aria-label="关闭"><X className="h-4 w-4" /></button>
      </div>
      <div className="max-h-[68vh] overflow-y-auto p-4">
        <div className="space-y-4">
          {DYNAMIC_EXIT_RULES.map(ruleId => {
            const config = draft[ruleId]
            const isCleared = cleared.has(ruleId)
            return (
              <section key={ruleId} className="border-y border-border py-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="text-xs font-semibold text-secondary">{RULE_LABELS[ruleId]}</span>
                  <div className="flex flex-wrap items-center gap-3 text-[10px] text-muted">
                    <label className="inline-flex items-center gap-1"><input type="checkbox" checked={config.enabled === true} disabled={pending || isCleared} onChange={event => update(ruleId, 'enabled', event.target.checked)} />启用</label>
                    <label className="inline-flex items-center gap-1"><input type="checkbox" checked={config.notify === true} disabled={pending || isCleared} onChange={event => update(ruleId, 'notify', event.target.checked)} />通知</label>
                    <label className="inline-flex items-center gap-1"><input type="checkbox" checked={isCleared} disabled={pending} onChange={event => setCleared(current => { const next = new Set(current); if (event.target.checked) next.add(ruleId); else next.delete(ruleId); return next })} />清除逐票覆盖</label>
                  </div>
                </div>
                <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3">
                  {(POSITION_RISK_RULE_FIELDS[ruleId] ?? []).map(field => {
                    const stored = Number(config[field.key] ?? field.defaultValue ?? 0)
                    const display = field.percent ? stored * 100 : stored
                    return <label key={field.key} className="min-w-0 text-[10px] text-muted"><span>{field.label}</span><span className="mt-1 flex h-7 items-center border border-border bg-background px-2"><input type="number" min={field.min} max={field.max} step={field.step} value={Number.isFinite(display) ? display : ''} disabled={pending || isCleared || config.enabled !== true} onChange={event => { const next = Number(event.target.value); if (Number.isFinite(next)) update(ruleId, field.key, field.percent ? next / 100 : next) }} className="min-w-0 flex-1 bg-transparent font-mono text-[11px] text-foreground outline-none disabled:opacity-50" />{field.suffix && <span className="ml-1 shrink-0">{field.suffix}</span>}</span></label>
                  })}
                </div>
              </section>
            )
          })}
        </div>
      </div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" onClick={onClose} disabled={pending} className="h-8 rounded-btn border border-border px-3 text-xs text-secondary disabled:opacity-50">取消</button><button type="button" onClick={submit} disabled={pending} className="h-8 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-50">{pending ? '应用中…' : '应用配置'}</button></div>
    </Modal>
  )
}

function DynamicExitSummary({ feature }: { feature?: PositionRiskFeatureSnapshot }) {
  if (!feature || feature.data_status === 'unavailable' || feature.available !== true || feature.fresh !== true) {
    return <div className="text-[10px] text-muted">动态退出 · 数据不足</div>
  }
  const rules = feature.dynamic_exit_rules ?? []
  const labels = rules.map(ruleId => RULE_LABELS[ruleId] ?? ruleId)
  if (!rules.length) return <div className="text-[10px] text-muted">动态退出 · 已就绪，未触发</div>
  return <div className="min-w-0"><div className="text-[10px] text-danger">建议清仓 100% · 待人工确认</div><div className="truncate text-[10px] text-secondary" title={labels.join('、')}>触发 {labels.join('、')}</div></div>
}

function PositionInspector({ row, options, feature, events, initialTab, onClose }: { row: PositionRiskPosition; options: PositionRiskOptions | undefined; feature?: PositionRiskFeatureSnapshot; events?: PositionRiskEvent[]; initialTab: RiskModuleTab; onClose: () => void }) {
  const portfolioQuery = useQuery({ queryKey: QK.positionRisk, queryFn: api.positionRiskPortfolio })
  const queryClient = useQueryClient()
  const activeRuleTab = initialTab
  const [expandedRule, setExpandedRule] = useState<string | null>(null)
  const portfolio = portfolioQuery.data
  const override = portfolio?.overrides[row.symbol] ?? {}
  const mutation = useMutation({
    mutationFn: (next: Record<string, any>) => api.positionRiskUpdateOverride(row.symbol, portfolio!.revision, next),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: QK.positionRisk }),
  })
  const ruleConfig = (ruleId: string) => override.rules?.[ruleId] ?? options?.rules[ruleId] ?? {}
  const ruleEnabled = (ruleId: string) => {
    const config = ruleConfig(ruleId)
    return config.enabled === true && (!ACTIVE_RULES.has(ruleId) || config.active !== false)
  }
  const setRuleEnabled = (ruleId: string, enabled: boolean) => {
    // Keep a legacy peak-pullback override in percentage mode when toggling it.
    const config = { ...(override.rules?.[ruleId] ?? {}), enabled }
    if (ACTIVE_RULES.has(ruleId)) config.active = enabled
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: config },
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
  const setRuleAutoExecute = (ruleId: string, auto_execute: boolean) => {
    mutation.mutate({
      ...override,
      rules: { ...(override.rules ?? {}), [ruleId]: { ...(override.rules?.[ruleId] ?? {}), auto_execute } },
    })
  }
  const renderRuleRows = (rules: readonly string[]) => rules.map(ruleId => {
    const evidenceOnly = ['large_buy', 'large_sell', 'continuous_outflow', 'orderbook_imbalance'].includes(ruleId)
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
          <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted"><span>{evidenceOnly ? '采样' : '启用'}</span><input type="checkbox" checked={ruleEnabled(ruleId)} disabled={mutation.isPending} onChange={event => setRuleEnabled(ruleId, event.target.checked)} aria-label={`${ruleEnabled(ruleId) ? '停用' : '启用'}${RULE_LABELS[ruleId] ?? ruleId}`} /></label>
          {!evidenceOnly && <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted"><span>通知</span><input type="checkbox" checked={(override.rules?.[ruleId]?.notify ?? options?.rules[ruleId]?.notify) === true} disabled={mutation.isPending} onChange={event => setRuleNotify(ruleId, event.target.checked)} aria-label={`通知${RULE_LABELS[ruleId] ?? ruleId}信号`} /></label>}
          {(override.rules?.[ruleId]?.auto_execute ?? options?.rules[ruleId]?.auto_execute) !== undefined && <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted"><span>自动委托</span><input type="checkbox" checked={override.rules?.[ruleId]?.auto_execute === true} disabled={mutation.isPending} onChange={event => setRuleAutoExecute(ruleId, event.target.checked)} /></label>}
        </div>
        {expanded && fields.length > 0 && (
          <div className="mt-2 grid grid-cols-2 gap-2 pl-5">
            {fields.map(field => {
              const defaultValue = options?.rules[ruleId]?.[field.key] ?? field.defaultValue ?? 0
              const storedValue = override.rules?.[ruleId]?.[field.key] ?? defaultValue
              const displayValue = field.percent ? Number(storedValue) * 100 : Number(storedValue)
              return (
                <label key={field.key} className="min-w-0 text-[10px] text-muted">
                  <span>{field.label}</span>
                  <span className="mt-1 flex h-7 items-center border border-border bg-surface px-2 focus-within:border-accent/50">
                    {field.type === 'select' ? (
                      <select
                        value={String(override.rules?.[ruleId]?.[field.key] ?? options?.rules[ruleId]?.[field.key] ?? field.options?.[0]?.[0] ?? '')}
                        disabled={mutation.isPending || !ruleEnabled(ruleId)}
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
                        disabled={mutation.isPending || !ruleEnabled(ruleId)}
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
  return (
    <div className="fixed inset-0 z-40 bg-black/35" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
      <aside className="absolute inset-y-0 right-0 flex w-full max-w-md flex-col border-l border-border bg-surface shadow-xl">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold">{row.name}</div>
            <div className="font-mono text-[11px] text-muted">{row.symbol}</div>
            <div className="mt-2"><DynamicExitSummary feature={feature} /></div>
          </div>
          <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭"><X className="h-4 w-4" /></button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto p-4">
          {events?.filter(event => event.symbol === row.symbol && event.triggered_rules?.length).slice(0, 3).map(event => (
            <div key={`${event.fingerprint ?? event.ts}-evidence`} className="mb-3 border-y border-border py-2 text-[10px] text-muted">
              <div className="text-secondary">最近动态证据 · {event.rule_name ?? event.rule_id}</div>
              {event.triggered_rules?.length ? <div className="mt-1">合并规则：{event.triggered_rules.map(ruleId => RULE_LABELS[ruleId] ?? ruleId).join('、')}</div> : null}
              {event.exit_evidence?.length ? <div className="mt-1">{event.exit_evidence.join('；')}</div> : null}
              <div className="mt-1">建议清仓 {event.action_pct ?? 0}% · {event.action_eligible ? '待人工确认' : event.blocked_reason ?? '动作已门控'}</div>
            </div>
          ))}
          <div className="mt-4">

          {(activeRuleTab === 'stop_loss' || activeRuleTab === 'all') && (
            <div className="mt-4 space-y-5">
              {STOP_LOSS_RULE_GROUPS.map(([group, rules]) => (
                <section key={group}>
                  <h3 className="mb-2 text-xs font-semibold text-secondary">{group}</h3>
                  <div className="divide-y divide-border border-y border-border">{renderRuleRows(rules)}</div>
                </section>
              ))}
            </div>
          )}

          </div>
        </div>
        <div className="flex justify-end border-t border-border px-4 py-3">
          <button type="button" disabled={mutation.isPending || !Object.keys(override).length} onClick={() => mutation.mutate({})} className="h-8 rounded-btn border border-border px-3 text-xs disabled:opacity-40">清除本股覆盖</button>
        </div>
      </aside>
    </div>
  )
}

export function LargeOrders() {
  const [tab, setTab] = useState<Tab>('positions')
  const [search, setSearch] = useState('')
  const [positionSort, setPositionSort] = useState<'asc' | 'desc' | null>(null)
  const [importOpen, setImportOpen] = useState(false)
  const [selected, setSelected] = useState<{ row: PositionRiskPosition; tab: RiskModuleTab } | null>(null)
  const [tradeRow, setTradeRow] = useState<PositionRiskPosition | null>(null)
  const [tradePreset, setTradePreset] = useState<QmtTradePreset | null>(null)
  const [tradeRiskContext, setTradeRiskContext] = useState<QmtRiskTradeContext | null>(null)
  const [preview, setPreview] = useState<{ symbol: string; name: string } | null>(null)
  const [analysisPanel, setAnalysisPanel] = useState<{ symbol: string; name: string } | null>(null)
  const [analysisEnabled, setAnalysisEnabled] = useState(false)
  const [bulkSymbols, setBulkSymbols] = useState<Set<string>>(new Set())
  const [batchScope, setBatchScope] = useState<'selected' | 'all' | null>(null)
  const openRiskSettings = (row: PositionRiskPosition, tab: RiskModuleTab) => setSelected({ row, tab })
  const portfolio = useQuery({ queryKey: QK.positionRisk, queryFn: api.positionRiskPortfolio, refetchInterval: 30_000 })
  const qmt = useQuery({ queryKey: QK.positionRiskQmt, queryFn: api.qmtStatus, refetchInterval: 30_000 })
  const qmtProbeQuery = useQuery({
    queryKey: QK.positionRiskQmtProbe,
    queryFn: () => api.qmtProbe(true),
    enabled: Boolean(qmt.data?.configured),
    refetchInterval: 30_000,
    refetchIntervalInBackground: true,
    retry: false,
  })
  const qmtOrders = useQuery({ queryKey: QK.positionRiskQmtOrders, queryFn: api.qmtOrders, enabled: Boolean(qmt.data?.configured), refetchInterval: 15_000 })
  const options = useQuery({ queryKey: QK.positionRiskOptions, queryFn: api.positionRiskOptions })
  const events = useQuery({ queryKey: QK.positionRiskEvents, queryFn: api.positionRiskEvents })
  const queryClient = useQueryClient()
  const qmtProbe = useMutation({
    mutationFn: () => api.qmtProbe(),
    onSuccess: result => {
      queryClient.setQueryData(QK.positionRiskQmt, result)
      queryClient.setQueryData(QK.positionRiskQmtProbe, result)
    },
  })
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
  const qmtConnection = useMutation({
    mutationFn: api.qmtConnectionMode,
    onSuccess: result => {
      queryClient.setQueryData(QK.positionRiskQmt, result.status)
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
      toast(`已切换到${result.status.connection_mode === 'local' ? '本地' : '远程'} QMT`, 'success')
    },
  })
  const batchRiskRules = useMutation({
    mutationFn: (payload: { scope: 'selected' | 'all'; rules: Record<string, Record<string, any>>; clear_rule_ids: string[] }) => api.positionRiskBatchUpdateOverrides({
      revision: portfolio.data!.revision,
      scope: payload.scope,
      symbols: payload.scope === 'selected' ? [...bulkSymbols] : [],
      rules: payload.rules,
      clear_rule_ids: payload.clear_rule_ids,
    }),
    onSuccess: result => {
      queryClient.setQueryData(QK.positionRisk, result.portfolio)
      queryClient.invalidateQueries({ queryKey: QK.positionRiskFeatures() })
      setBulkSymbols(new Set())
      setBatchScope(null)
      toast(`已应用动态退出规则到 ${result.affected_symbols.length} 只持仓`, 'success')
    },
  })
  const probeLatency = qmtProbeQuery.data?.latency_ms ?? qmtProbe.data?.latency_ms ?? qmt.data?.latency_ms
  const probeButtonLabel = qmtProbe.isPending || qmtProbeQuery.isFetching
    ? '检查 QMT 连接（检查中…）'
    : qmtProbe.isError || qmtProbeQuery.isError
      ? '检查 QMT 连接（失败）'
      : probeLatency != null
        ? `检查 QMT 连接（${probeLatency}ms）`
        : '检查 QMT 连接'
  const signalNames = useMemo(
    () => Object.fromEntries((options.data?.custom_signals ?? []).map(signal => [signal.id, signal.label])),
    [options.data?.custom_signals],
  )
  const positionSymbols = useMemo(
    () => (portfolio.data?.positions ?? []).map(row => row.symbol),
    [portfolio.data?.positions],
  )
  const positionSymbolsKey = positionSymbols.join(',')
  const riskFeatures = useQuery({
    queryKey: QK.positionRiskFeatures(positionSymbolsKey),
    queryFn: () => api.positionRiskFeatures(positionSymbols),
    enabled: positionSymbols.length > 0,
    refetchInterval: 30_000,
  })
  const analysisReports = useQuery({
    queryKey: QK.stockAnalysisLatest(positionSymbols),
    queryFn: () => api.stockAnalysisReportsLatest(positionSymbols),
    enabled: analysisEnabled && positionSymbols.length > 0,
    staleTime: 30_000,
  })
  const rows = useMemo(() => {
    const filtered = (portfolio.data?.positions ?? []).filter(row => {
      const matchesSearch = !search || row.symbol.toLowerCase().includes(search.toLowerCase()) || row.name.includes(search)
      return matchesSearch
    })
    if (!positionSort) return filtered
    return [...filtered].sort((left, right) => {
      if (left.weight == null) return right.weight == null ? 0 : 1
      if (right.weight == null) return -1
      return positionSort === 'asc' ? left.weight - right.weight : right.weight - left.weight
    })
  }, [portfolio.data?.positions, positionSort, search])
  if (portfolio.isLoading) return <div className="grid h-full place-items-center"><Loader2 className="h-6 w-6 animate-spin text-accent" /></div>
  if (portfolio.isError || !portfolio.data) return <EmptyState icon={AlertTriangle} title="持仓风控加载失败" hint="请检查后端服务后重试" />
  const data = portfolio.data
  const qmtReady = qmt.data?.state === 'ready'
  const qmtTotalAsset = qmtReady ? qmt.data.account?.total_asset : null
  const currentTotalAsset = qmtTotalAsset ?? data.account.total_asset
  const accountTodayPnl = data.account.today_profit_loss ?? (qmtReady ? null : todayPnl(currentTotalAsset, data.account.previous_close_total_asset, data.runtime.status !== 'data_unavailable'))
  const accountTodayPnlTitle = data.account.today_profit_loss != null
    ? 'QMT 持仓统计 float_profit 合计（毛收益，未扣佣金等费用）'
    : qmtReady
      ? 'QMT 今日盈亏尚未同步'
      : accountTodayPnl == null
        ? '行情不可用或缺少上日收盘总资产，暂无法计算'
        : undefined
  const qmtConnectionMode = qmt.data?.connection_mode ?? 'remote'
  const runtimeReason = compactRuntimeReason(data.runtime.reason)
  const openTradeForRow = (row: PositionRiskPosition) => {
    setSelected(null)
    setTradeRow(row)
    setTradePreset(null)
    setTradeRiskContext(null)
  }
  const openTradeForEvent = (event: PositionRiskEvent, enforceRisk = false) => {
    if (!event.symbol || (event.trade_action !== 'BUY' && event.trade_action !== 'SELL')) return
    const row = data.positions.find(item => item.symbol === event.symbol)
    if (!row) return
    const action = event.trade_action
    const price = event.price ?? row.price
    setSelected(null)
    setTradeRow(row)
    setTradePreset({
      action,
      price,
      allocationMode: 'fixed',
      allocationValue: price != null && Number.isFinite(price) ? price * 100 : 10_000,
    })
    setTradeRiskContext(
      enforceRisk && event.fingerprint
        ? {
            fingerprint: event.fingerprint,
          }
        : null,
    )
  }

  return (
    <div className="min-h-full bg-background">
      <PageHeader
        title="持仓风控"
        subtitle={data.imported_at ? `账户快照 ${new Date(data.imported_at).toLocaleString('zh-CN')}` : '尚未导入账户快照'}
        right={<div className="flex min-w-max items-center gap-2">
          <button type="button" onClick={() => qmtSync.mutate()} disabled={!qmt.data?.configured || qmtSync.isPending} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:cursor-not-allowed disabled:opacity-50"><RefreshCw className={cn('h-3.5 w-3.5', qmtSync.isPending && 'animate-spin')} />同步QMT</button>
          <button type="button" onClick={() => setImportOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-3 text-xs hover:bg-elevated"><ImagePlus className="h-3.5 w-3.5" />图片导入</button>
          <button type="button" onClick={() => portfolio.refetch()} className="grid h-8 w-8 place-items-center rounded-btn border border-border hover:bg-elevated" title="刷新"><RefreshCw className="h-3.5 w-3.5" /></button>
        </div>}
      />

      <div className="border-b border-border px-4 py-2 sm:px-5">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
          <span className="font-medium">{data.account.name}</span>
          <span className="text-muted">持仓 <b className="font-mono text-foreground">{data.positions.length}</b></span>
          <span className="text-muted">总资产 <b className="font-mono text-foreground">{money(currentTotalAsset)}</b></span>
          <span className={cn('text-muted', accountTodayPnl != null && (accountTodayPnl >= 0 ? 'text-bull' : 'text-bear'))} title={accountTodayPnlTitle}>今日盈亏 <b className="font-mono">{holdingPnl(accountTodayPnl)}</b></span>
          <span className={cn('inline-flex items-center gap-1.5', data.runtime.status === 'websocket' ? 'text-bear' : data.runtime.status === 'polling_degraded' || data.runtime.status === 'reconnecting' ? 'text-warning' : 'text-muted')}><StatusDot status={data.runtime.status} />{STATUS_LABEL[data.runtime.status]}</span>
          <span className={cn('inline-flex items-center gap-1.5', qmt.data?.state === 'ready' ? 'text-bear' : qmt.data?.configured ? 'text-warning' : 'text-muted')}><StatusDot status={qmt.data?.state === 'ready' ? 'websocket' : 'data_unavailable'} />QMT {qmt.data?.state === 'ready' ? '已连接' : qmt.data?.configured ? '待检查' : '未配置'}</span>
          <span className="inline-flex items-center gap-1.5 text-muted" title="切换 QMT 连接位置">
            <span className="whitespace-nowrap text-[10px]">当前连接：{qmtConnectionMode === 'local' ? '本地' : '远程'}</span>
            <span className="inline-flex items-center rounded-btn border border-border bg-elevated p-0.5">
            {(['remote', 'local'] as const).map(mode => {
              const active = qmtConnectionMode === mode
              const available = Boolean(qmt.data) && (mode === 'local' ? qmt.data?.local_configured === true : qmt.data?.remote_configured === true)
              return <button key={mode} type="button" onClick={() => qmtConnection.mutate(mode)} disabled={active || !available || qmtConnection.isPending} className={cn('h-6 rounded px-2 text-[10px] font-medium transition-colors disabled:cursor-not-allowed', active ? 'bg-accent/20 text-accent ring-1 ring-inset ring-accent/50 disabled:opacity-100' : 'text-muted hover:text-foreground disabled:opacity-50')}>
                {mode === 'local' ? '本地' : '远程'}
              </button>
            })}
            </span>
          </span>
          <span className={qmt.data?.auto_sync_running ? 'text-bear' : 'text-muted'}>{qmt.data?.auto_sync_running ? `自动同步 ${qmt.data.auto_sync_interval_seconds}秒` : '自动同步未运行'}</span>
          <label className="inline-flex items-center gap-1.5 text-muted" title={!qmt.data?.trade_authorized ? '后端未授权实盘交易' : '取消勾选可暂停本次运行的实盘下单'}><input type="checkbox" checked={qmt.data?.trade_enabled === true} disabled={!qmt.data?.configured || !qmt.data?.trade_authorized || qmtToggle.isPending} onChange={event => qmtToggle.mutate(event.target.checked)} />实盘模式</label>
          <button type="button" onClick={() => qmtProbe.mutate()} disabled={qmtProbe.isPending || qmtProbeQuery.isFetching} className="h-7 rounded-btn border border-border px-2 text-[11px] hover:bg-elevated disabled:opacity-50" title="点击立即检查；页面每 30 秒自动更新一次延迟">{probeButtonLabel}</button>
          {runtimeReason && <span className="ml-auto max-w-[10rem] truncate text-[11px] text-muted" title={data.runtime.reason}>{runtimeReason}</span>}
        </div>
      </div>

      <div className="flex flex-col gap-2 border-b border-border px-4 py-2 sm:flex-row sm:items-center sm:justify-between sm:px-5">
        <div className="inline-flex w-fit max-w-full rounded-btn bg-elevated p-0.5">
          {([
            ['positions', '持仓监控', data.positions.length, ShieldCheck],
            ['events', '触发记录', events.data?.count ?? 0, FileClock],
          ] as const).map(([id, label, count, Icon]) => (
            <button key={id} type="button" onClick={() => setTab(id)} className={cn('inline-flex h-7 shrink-0 items-center gap-1.5 whitespace-nowrap rounded px-2.5 text-xs', tab === id ? 'bg-surface text-foreground shadow-sm' : 'text-muted')}>
              <Icon className="h-3.5 w-3.5" />{label}<span className="font-mono text-[10px]">{count}</span>
            </button>
          ))}
        </div>
        {tab === 'positions' && <div className="flex items-center gap-2 self-end sm:self-auto">
          <label className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2 text-[11px] text-secondary" title="开启后加载并显示持仓个股分析"><input type="checkbox" checked={analysisEnabled} onChange={event => { const enabled = event.target.checked; setAnalysisEnabled(enabled); if (!enabled) setAnalysisPanel(null) }} />个股分析</label>
          <label className="relative hidden sm:block"><Search className="absolute left-2 top-2 h-3.5 w-3.5 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="代码 / 名称" className="h-8 w-44 rounded-btn border border-border bg-transparent pl-7 pr-2 text-xs" /></label>
          <button type="button" onClick={() => setBatchScope('selected')} disabled={!bulkSymbols.size || batchRiskRules.isPending} className="h-8 rounded-btn border border-border px-2 text-[11px] hover:bg-elevated disabled:opacity-50">应用到选中 {bulkSymbols.size || ''}</button>
          <button type="button" onClick={() => setBatchScope('all')} disabled={!data.positions.length || batchRiskRules.isPending} className="h-8 rounded-btn border border-border px-2 text-[11px] hover:bg-elevated disabled:opacity-50">应用到全部</button>
        </div>}
      </div>

      {tab === 'positions' && (rows.length ? <>
          <div className="hidden overflow-x-auto md:block">
          <table className="w-full min-w-[1300px] text-xs">
            <thead className="sticky top-0 bg-background text-muted"><tr className="border-b border-border">
              <th className="w-9 px-2 py-2"><input type="checkbox" checked={rows.length > 0 && rows.every(row => bulkSymbols.has(row.symbol))} onChange={event => setBulkSymbols(event.target.checked ? new Set(rows.map(row => row.symbol)) : new Set())} aria-label="全选当前持仓" /></th>
              {['证券', '数量 / 可用', '成本', '现价', '持仓盈亏', '仓位', ...(analysisEnabled ? ['最新分析'] : []), '动态退出', '风控设置', '操作'].map(label => <th key={label} className={cn('px-3 py-2 font-medium', ['成本', '现价', '持仓盈亏', '仓位'].includes(label) ? 'text-right' : 'text-left')}>{label === '仓位' ? <button type="button" onClick={() => setPositionSort(current => current === 'desc' ? 'asc' : 'desc')} className="inline-flex items-center gap-1 font-medium hover:text-foreground" title="按仓位排序" aria-label={`按仓位${positionSort === 'asc' ? '升序' : '降序'}排序`}>仓位{positionSort ? <span className="font-mono text-[10px] text-accent">{positionSort === 'asc' ? '↑' : '↓'}</span> : null}</button> : label}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-border/70">
              {rows.map(row => <tr key={row.symbol} className="hover:bg-elevated/35">
                <td className="px-2 py-2"><input type="checkbox" checked={bulkSymbols.has(row.symbol)} onChange={event => setBulkSymbols(current => { const next = new Set(current); if (event.target.checked) next.add(row.symbol); else next.delete(row.symbol); return next })} aria-label={`选择${row.name}`} /></td>
                <td className="px-3 py-2"><button type="button" onClick={() => setPreview({ symbol: row.symbol, name: row.name })} className="text-left hover:text-accent" title="查看 K 线与分时"><div className="font-medium">{row.name}</div><div className="font-mono text-[10px] text-muted">{row.symbol}</div></button></td>
                <td className="px-3 py-2 font-mono">{row.quantity.toLocaleString()}<div className="text-[10px] text-muted">可用 {row.available.toLocaleString()}</div></td>
                <td className="px-3 py-2 text-right font-mono">{price(row.cost_price)}</td>
                <td className="px-3 py-2 text-right font-mono">{price(row.price)}</td>
                <td className={cn('px-3 py-2 text-right font-mono', row.profit_loss != null && row.profit_loss >= 0 ? 'text-bull' : 'text-bear')}>{holdingPnl(row.profit_loss)}</td>
                <td className="px-3 py-2 text-right font-mono">{pct(row.weight)}</td>
                {analysisEnabled && <td className="px-3 py-2"><StockAnalysisStatus row={row} report={analysisReports.data?.reports[row.symbol]} loading={analysisReports.isLoading} onOpen={() => setAnalysisPanel({ symbol: row.symbol, name: row.name })} /></td>}
                <td className="w-[210px] max-w-[210px] px-3 py-2"><DynamicExitSummary feature={riskFeatures.data?.features[row.symbol]} /></td>
                <td className="w-[330px] max-w-[330px] px-3 py-2"><RiskSettingsSummary portfolio={data} symbol={row.symbol} onOpen={tab => openRiskSettings(row, tab)} /></td>
                <td className="px-3 py-2"><button type="button" onClick={() => openTradeForRow(row)} className="h-7 rounded px-2 text-[11px] text-secondary hover:bg-elevated hover:text-foreground" title="打开交易面板" aria-label={`打开${row.name}交易面板`}>交易</button></td>
              </tr>)}
            </tbody>
          </table>
        </div>
        <div className="divide-y divide-border md:hidden">
          {rows.map(row => <div key={row.symbol} className="grid w-full grid-cols-[auto_1fr_auto] gap-3 px-4 py-3 text-left">
            <input type="checkbox" checked={bulkSymbols.has(row.symbol)} onChange={event => setBulkSymbols(current => { const next = new Set(current); if (event.target.checked) next.add(row.symbol); else next.delete(row.symbol); return next })} aria-label={`选择${row.name}`} />
            <div className="min-w-0">
              <button type="button" onClick={() => setPreview({ symbol: row.symbol, name: row.name })} className="min-w-0 text-left" title="查看 K 线与分时"><div className="font-medium hover:text-accent">{row.name}<span className="ml-2 font-mono text-[10px] text-muted">{row.symbol}</span></div><div className="mt-1 text-xs text-muted">{row.quantity.toLocaleString()} 股</div><div className="mt-2 grid grid-cols-3 gap-2 text-[10px]"><div><div className="text-muted">成本</div><div className="mt-0.5 font-mono text-xs text-foreground">{price(row.cost_price)}</div></div><div><div className="text-muted">现价</div><div className="mt-0.5 font-mono text-xs text-foreground">{price(row.price)}</div></div><div><div className="text-muted">持仓盈亏</div><div className={cn('mt-0.5 font-mono text-xs', row.profit_loss != null && row.profit_loss >= 0 ? 'text-bull' : 'text-bear')}>{holdingPnl(row.profit_loss)}</div></div></div></button>
              <div className="mt-2 border-t border-border/70 pt-2">{analysisEnabled && <StockAnalysisStatus row={row} report={analysisReports.data?.reports[row.symbol]} loading={analysisReports.isLoading} onOpen={() => setAnalysisPanel({ symbol: row.symbol, name: row.name })} />}<div className="mt-2"><DynamicExitSummary feature={riskFeatures.data?.features[row.symbol]} /></div><div className="mt-2"><RiskSettingsSummary portfolio={data} symbol={row.symbol} onOpen={tab => openRiskSettings(row, tab)} /></div></div>
            </div>
            <div className="flex items-center gap-2"><button type="button" onClick={() => openTradeForRow(row)} className="h-8 rounded-btn px-2 text-[11px] text-secondary hover:bg-elevated hover:text-foreground" title="打开交易面板" aria-label={`打开${row.name}交易面板`}>交易</button></div>
          </div>)}
        </div>
      </> : <EmptyState icon={ShieldCheck} title={data.positions.length ? '没有符合搜索的持仓' : '尚未导入持仓'} hint={data.positions.length ? '调整搜索条件' : '使用顶部“图片导入”上传同花顺手机持仓截图'} />)}

      {tab === 'events' && <div className="divide-y divide-border">
        {events.data?.events.length ? events.data.events.map((event, index) => <div key={`${event.fingerprint ?? event.ts}-${index}`} className="grid gap-2 px-4 py-3 text-xs sm:grid-cols-[150px_120px_1fr_220px] sm:px-5">
          <time className="font-mono text-muted"><span className="block">{new Date(event.ts).toLocaleString('zh-CN')}</span>{(event.occurrence_count ?? 1) > 1 && event.first_ts ? <span className="mt-0.5 block text-[10px]">首次 {new Date(event.first_ts).toLocaleTimeString('zh-CN')}</span> : null}</time>
          {event.symbol ? <button type="button" onClick={() => setPreview({ symbol: event.symbol!, name: event.name || event.symbol! })} className="text-left hover:text-accent" title="查看 K 线与分时">{event.symbol} {event.name}</button> : <span>组合</span>}
          <span className="min-w-0"><span className="inline-flex flex-wrap items-center gap-1.5"><span className="rounded bg-elevated px-1.5 py-0.5 text-[11px] text-secondary">{event.rule_id?.startsWith('t:') ? '历史分时规则（已停用）' : event.rule_id === 'vwap_breakdown' ? RULE_LABELS.vwap_breakdown : event.rule_name || RULE_LABELS[event.rule_id || ''] || cnSignalText(event.message, signalNames)}</span>{event.triggered_rules?.length ? <span className="text-[10px] text-muted">合并 {event.triggered_rules.map(ruleId => RULE_LABELS[ruleId] ?? ruleId).join('、')}</span> : null}{(event.occurrence_count ?? 1) > 1 ? <span className="rounded bg-warning/10 px-1.5 py-0.5 font-mono text-[10px] text-warning">共 {event.occurrence_count} 次</span> : null}</span>{event.exit_evidence?.length ? <span className="mt-1 block truncate text-[10px] text-muted" title={event.exit_evidence.join('；')}>{event.exit_evidence.join('；')}</span> : null}</span>
          <span className="flex flex-wrap items-center justify-end gap-2"><span className={event.severity === 'critical' ? 'text-danger' : event.severity === 'warn' ? 'text-warning' : 'text-muted'}>{event.severity === 'critical' ? '严重' : event.severity === 'warn' ? '警告' : '提示'} · 执行 {event.action_pct ?? 0}%</span>{event.blocked_reason && <span className="text-[10px] text-warning">{event.blocked_reason}</span>}{event.auto_order_status && event.auto_order_status !== 'disabled' && <span className="text-[10px] text-muted">自动委托 {qmtOrderStatus(event.auto_order_status)}</span>}{event.context_state && <span className={cn('text-[10px]', contextStateClass(event.context_state))}>{contextStateLabel(event.context_state)}</span>}{(event.trade_action === 'BUY' || event.trade_action === 'SELL') && event.symbol ? (() => { const row = data.positions.find(item => item.symbol === event.symbol); if (!row) return null; return <button type="button" onClick={() => openTradeForEvent(event, event.action_eligible === true)} className="h-7 rounded border border-border px-2 text-[10px] text-secondary hover:bg-elevated" title={event.action_eligible ? '打开统一交易面板并保留风控确认' : '打开手动下单面板'}>{event.action_eligible ? '确认委托' : '手动下单'}</button> })() : null}</span>
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
                <td className="px-3 py-2"><span className="block truncate" title={qmtOrderReason(order) ?? (order.order_sys_id ? `委托号 ${order.order_sys_id}` : undefined)}>{qmtOrderReason(order) ?? (order.order_sys_id ? `委托号 ${order.order_sys_id}` : order.status === 'unknown' ? '请在 QMT 核对，禁止原单重发' : '等待云端委托号')}</span></td>
              </tr>)}
            </tbody>
          </table>
        </div>
      </section> : null}

      <PositionRiskImportDialog open={importOpen} portfolio={data} onClose={() => setImportOpen(false)} />
      {batchScope && <BatchDynamicDialog key={`${batchScope}-${options.data ? 'ready' : 'loading'}`} scope={batchScope} count={batchScope === 'selected' ? bulkSymbols.size : data.positions.length} options={options.data} pending={batchRiskRules.isPending} onClose={() => { if (!batchRiskRules.isPending) setBatchScope(null) }} onSubmit={(rules, clearRuleIds) => batchRiskRules.mutate({ scope: batchScope, rules, clear_rule_ids: clearRuleIds })} />}
      {selected && <PositionInspector key={`${selected.row.symbol}-${selected.tab}`} row={selected.row} options={options.data} feature={riskFeatures.data?.features[selected.row.symbol]} events={events.data?.events} initialTab={selected.tab} onClose={() => setSelected(null)} />}
      {tradeRow && <QmtTradePanel instrument={{ symbol: tradeRow.symbol, name: tradeRow.name, price: tradeRow.price ?? tradeRow.cost_price }} preset={tradePreset} riskContext={tradeRiskContext} onClose={() => { setTradeRow(null); setTradePreset(null); setTradeRiskContext(null) }} />}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} onClose={() => setPreview(null)} />
      {analysisPanel && <StockAnalysisDrawer symbol={analysisPanel.symbol} name={analysisPanel.name} onClose={() => setAnalysisPanel(null)} />}
    </div>
  )
}
