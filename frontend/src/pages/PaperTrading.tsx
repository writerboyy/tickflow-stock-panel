import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  Bell,
  ArrowDownRight,
  ArrowUpRight,
  CirclePause,
  CirclePlay,
  FileText,
  Gauge,
  ListOrdered,
  Minus,
  Pencil,
  Plus,
  RefreshCw,
  ShieldAlert,
  Square,
  Trash2,
  WalletCards,
  Wifi,
} from 'lucide-react'
import * as echarts from 'echarts'
import type { EChartsOption } from 'echarts'
import { api, type CreatePaperAccount, type KlineRow, type PaperAccount, type PaperEvent, type PaperFill, type PaperMarketMode, type PaperOrder } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { DatePicker } from '@/components/DatePicker'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { PageHeader } from '@/components/PageHeader'
import { toast } from '@/components/Toast'
import { pushAlertToast } from '@/components/AlertToast'
import { useECharts } from './backtest/charts/useECharts'
import { useChartTheme } from '@/lib/theme'
import { formatInstrumentLabel } from '@/lib/format'

const INPUT = 'w-full rounded-input border border-border bg-surface px-2.5 py-1.5 text-xs text-foreground focus:border-accent focus:outline-none'
const MONEY = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const BENCHMARK_COLOR = '#64748b'

const MODE_LABEL: Record<string, string> = {
  bar_1m: '1分钟K线',
  bar_1d: '日K',
  poll_3s: '3秒行情',
  websocket: 'WebSocket',
  bar_5m: '5分钟K线（旧账户）',
  bar_30m: '30分钟K线（旧账户）',
}

const DECISION_REASON_LABEL: Record<string, string> = {
  exit_without_target: '已卖出原持仓，暂无符合条件的买入标的',
  hold_without_target: '暂无合格买入标的，原持仓继续持有',
  hold_top_rank: '当前持仓仍是排名最高的合格标的',
  no_eligible_target: '暂无符合条件的买入标的',
  ranked_target: '调仓至排名最高的合格标的',
  weekly_rebalance: '例行更新候选池',
  afternoon_replacement: '盘中替换涨停失效标的',
}

type DetailTab = 'positions' | 'decisions' | 'trades' | 'logs'
type EquityPoint = NonNullable<PaperAccount['account']>['equity_curve'][number]

const DEFAULT_RISK = {
  max_symbol_exposure_pct: 1,
  daily_loss_pct: 0.1,
  max_drawdown_pct: 0.3,
  max_orders_per_minute: 60,
}

function statusLabel(value: PaperAccount['status']) {
  return value === 'running' ? '运行中' : value === 'paused' ? '已暂停' : '已停止'
}

function statusClass(value: PaperAccount['status']) {
  return value === 'running' ? 'text-success' : value === 'paused' ? 'text-warning' : 'text-muted'
}

function syncLabel(account: PaperAccount) {
  const phase = account.sync?.phase
  if (phase === 'catching_up') {
    const done = account.sync?.processed_days ?? 0
    const total = account.sync?.total_days ?? 0
    return total > 0 ? `补齐中 ${done}/${total}` : '补齐中'
  }
  if (phase === 'error') return '同步失败'
  return ''
}

function syncClass(account: PaperAccount) {
  if (account.sync?.phase === 'catching_up') return 'text-warning'
  if (account.sync?.phase === 'error') return 'text-danger'
  return 'text-muted'
}

function returnClass(value?: number) {
  const normalized = Number(value ?? 0)
  return normalized > 0 ? 'text-bull' : normalized < 0 ? 'text-bear' : 'text-muted'
}

function signedPercent(value: number) {
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function signedMoney(value: number) {
  return `${value > 0 ? '+' : ''}${MONEY.format(value)}`
}

function dailyPerformance(rows: EquityPoint[], tradingDate: string, equity?: number, initialCapital?: number) {
  if (equity == null || !Number.isFinite(equity)) return null
  const previousClose = rows.filter(row => !tradingDate || row.timestamp.slice(0, 10) < tradingDate).at(-1)?.equity
  const baseline = previousClose ?? initialCapital
  if (baseline == null || !Number.isFinite(baseline) || baseline <= 0) return null
  const amount = equity - baseline
  return {
    amount,
    pct: amount / baseline * 100,
    reference: previousClose == null ? 'initial' : 'previous-close',
  } as const
}

function decisionReasonText(event: PaperEvent) {
  if (event.decision === 'rebalance' && event.trigger_reason) return event.trigger_reason
  if (event.reason_code === 'low_correlation_switch') {
    return '当前持仓未进入当日候选范围，切换至排名更高的候选标的'
  }
  const reason = event.reason_code ?? event.reason
  if (event.decision === 'hold' && reason === 'ranked_target') {
    return '当前持仓仍为排名最高的合格标的'
  }
  return reason ? (DECISION_REASON_LABEL[reason] ?? reason) : '已完成当日决策'
}

function decisionLabel(event: PaperEvent) {
  if (event.decision === 'rebalance') return '调仓'
  if (event.decision === 'hold') return '继续持有'
  if (!(event.target_symbols ?? []).length && (event.holding_symbols ?? []).length) return '计划清仓'
  return '保持空仓'
}

function eventText(event: PaperEvent, symbolNames: Record<string, string> = {}) {
  const symbol = event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : ''
  if (event.type === 'fill') return `${event.side === 'buy' ? '买入' : '卖出'} ${symbol} ${Number(event.quantity ?? 0).toLocaleString()} 股 @ ${Number(event.price ?? 0).toFixed(3)}`
  if (event.type === 'rejected') return `${symbol || '委托'}：${event.reason ?? '已拒绝'}`
  if (event.type === 'risk') return String(event.reason ?? '风控锁定')
  if (event.type === 'signal') {
    const label = decisionLabel(event)
    const targets = (event.target_symbols ?? []).map(item => formatInstrumentLabel(item, symbolNames[item])).join('、') || '空仓'
    const targetLabel = event.strategy === 'small_cap_limitup' ? '候选池' : '目标'
    return `${label} · ${targetLabel} ${targets} · ${decisionReasonText(event)}`
  }
  return String(event.message ?? event.reason ?? event.type)
}

function formatTime(value?: string) {
  if (!value) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString('zh-CN', { hour12: false })
}

function eventTradingDate(event: PaperEvent) {
  return String(event.trading_date || event.timestamp || '').slice(0, 10)
}

function orderDirection(order: PaperOrder, fills: PaperFill[]) {
  const side = order.executed_side ?? fills.find(fill => fill.order_id === order.id)?.side ?? order.side
  return side === 'buy' || side === 'sell' ? side : 'target'
}

function orderInstruction(order: PaperOrder) {
  if (order.target_quantity != null) {
    return Number(order.target_quantity) === 0 ? '清仓' : `目标 ${Number(order.target_quantity).toLocaleString()} 股`
  }
  if (order.target_percent != null) {
    return Number(order.target_percent) === 0 ? '清仓' : `目标仓位 ${(Number(order.target_percent) * 100).toFixed(0)}%`
  }
  if (order.target_value != null) return `目标市值 ${MONEY.format(Number(order.target_value))}`
  const action = order.side === 'buy' ? '买入' : order.side === 'sell' ? '卖出' : '调整'
  if (order.quantity != null) return `${action} ${Number(order.quantity).toLocaleString()} 股`
  if (order.value != null) return `${action} ${MONEY.format(Number(order.value))}`
  return action
}

function orderStatusLabel(status: string) {
  return ({ filled: '已成交', rejected: '已拒绝', skipped: '已跳过', cancelled: '已取消', pending: '待成交' } as Record<string, string>)[status] ?? status
}

function orderStatusClass(status: string) {
  if (status === 'filled') return 'text-success'
  if (status === 'rejected') return 'text-danger'
  if (status === 'pending') return 'text-warning'
  return 'text-muted'
}

type ReturnZoomRange = { start: number; end: number }

function buildReturnChartData(rows: EquityPoint[], benchmarkRows: KlineRow[], zoomRange: ReturnZoomRange) {
  const lastRowByDate = new Map<string, EquityPoint>()
  rows.forEach(row => lastRowByDate.set(row.timestamp.slice(0, 10), row))
  const dailyRows = [...lastRowByDate.values()]
  if (!dailyRows.length) return { dailyRows, returns: [], benchmarkReturns: [], strategyReturn: null, benchmarkReturn: null }

  const zoomStart = Math.max(0, Math.min(100, zoomRange.start))
  const zoomEnd = Math.max(zoomStart, Math.min(100, zoomRange.end))
  const startIndex = Math.round((dailyRows.length - 1) * zoomStart / 100)
  const endIndex = Math.round((dailyRows.length - 1) * zoomEnd / 100)
  const returnBase = Number(dailyRows[startIndex]?.nav)
  const returns = dailyRows.map(row => Number.isFinite(returnBase) && returnBase > 0 ? (Number(row.nav) / returnBase - 1) * 100 : null)
  const benchmarkByDate = new Map<string, number>()
  benchmarkRows.forEach(row => {
    const close = Number(row.close)
    if (Number.isFinite(close) && close > 0) benchmarkByDate.set(String(row.date).slice(0, 10), close)
  })
  const benchmarkBase = dailyRows.slice(startIndex, endIndex + 1)
    .map(row => benchmarkByDate.get(row.timestamp.slice(0, 10)))
    .find(value => value != null)
  const benchmarkReturns = dailyRows.map(row => {
    const close = benchmarkByDate.get(row.timestamp.slice(0, 10))
    return benchmarkBase != null && close != null ? (close / benchmarkBase - 1) * 100 : null
  })
  const benchmarkReturn = benchmarkReturns.slice(startIndex, endIndex + 1).findLast(value => value != null) ?? null
  return { dailyRows, returns, benchmarkReturns, strategyReturn: returns[endIndex] ?? null, benchmarkReturn }
}

function ReturnChart({ data, benchmarkLabel, zoomRange, onZoomRangeChange }: { data: ReturnType<typeof buildReturnChartData>; benchmarkLabel: string; zoomRange: ReturnZoomRange; onZoomRangeChange: (range: ReturnZoomRange) => void }) {
  const theme = useChartTheme()
  const containerRef = useRef<HTMLDivElement>(null)
  const { dailyRows, returns, benchmarkReturns } = data
  const zoomed = zoomRange.start > 0.01 || zoomRange.end < 99.99
  const option = useMemo<EChartsOption | null>(() => {
    if (!dailyRows.length) return null
    const zoomStart = Math.max(0, Math.min(100, zoomRange.start))
    const zoomEnd = Math.max(zoomStart, Math.min(100, zoomRange.end))
    const benchmarkVisible = benchmarkReturns.some(value => value != null)
    const timestamps = dailyRows.map(row => row.timestamp)
    const indexByTimestamp = new Map(timestamps.map((timestamp, index) => [timestamp, index]))
    const xAxis = {
      type: 'category' as const,
      data: timestamps,
      boundaryGap: false,
      axisLabel: {
        color: theme.text,
        fontSize: 10,
        hideOverlap: true,
        showMinLabel: true,
        showMaxLabel: true,
        formatter: (value: string) => value.slice(0, 10),
      },
      axisLine: { lineStyle: { color: theme.border } },
    }
    return {
      animation: false,
      legend: { top: 0, right: 18, itemWidth: 14, itemHeight: 2, textStyle: { color: theme.text, fontSize: 10 } },
      grid: { left: 62, right: 24, top: 24, bottom: 58 },
      dataZoom: dailyRows.length > 1 ? [
        { type: 'inside', xAxisIndex: [0], filterMode: 'filter', start: zoomStart, end: zoomEnd },
        {
          type: 'slider',
          xAxisIndex: [0],
          height: 14,
          bottom: 4,
          borderColor: theme.border,
          fillerColor: theme.zoomFill,
          textStyle: { color: theme.text },
          start: zoomStart,
          end: zoomEnd,
        },
      ] : undefined,
      tooltip: {
        trigger: 'axis',
        backgroundColor: theme.tooltipBg,
        borderColor: theme.tooltipBorder,
        textStyle: { color: theme.tooltipText, fontSize: 11 },
        formatter: (params: any) => {
          const items = (Array.isArray(params) ? params : [params]).filter(item => item?.value != null && Number.isFinite(Number(item.value)))
          const timestamp = String(items[0]?.axisValue ?? '')
          const index = indexByTimestamp.get(timestamp)
          if (index == null) return ''
          const lines = [
            ['模拟收益', returns[index]],
            ...(benchmarkVisible ? [[benchmarkLabel, benchmarkReturns[index]]] : []),
          ].filter((item): item is [string, number] => item[1] != null && Number.isFinite(Number(item[1])))
            .map(([name, rawValue]) => {
              const value = Number(rawValue)
              return `<div>${name} ${value >= 0 ? '+' : ''}${value.toFixed(2)}%</div>`
            }).join('')
          return `<div>${formatTime(timestamp)}${zoomed ? ' · 区间收益' : ''}</div>${lines}`
        },
      },
      xAxis,
      yAxis: {
        type: 'value', scale: true, min: 'dataMin',
        axisLabel: { color: theme.text, fontSize: 10, formatter: (value: number) => `${value.toFixed(1)}%` },
        splitLine: { lineStyle: { color: theme.grid } },
      },
      series: [
        { type: 'line', name: '模拟收益', data: returns, showSymbol: false, smooth: 0.2, lineStyle: { width: 1.6, color: theme.accent }, itemStyle: { color: theme.accent }, areaStyle: { color: theme.accent, opacity: 0.1 } },
        ...(benchmarkVisible ? [{ type: 'line' as const, name: benchmarkLabel, data: benchmarkReturns, showSymbol: false, smooth: 0.25, connectNulls: true, lineStyle: { width: 1.8, color: BENCHMARK_COLOR }, itemStyle: { color: BENCHMARK_COLOR } }] : []),
      ],
    }
  }, [benchmarkLabel, benchmarkReturns, dailyRows, returns, theme, zoomRange, zoomed])
  const ref = useECharts(option, [option], containerRef)
  useEffect(() => {
    const chart = containerRef.current ? echarts.getInstanceByDom(containerRef.current) : null
    if (!chart) return
    const handleZoom = () => {
      const zoom = (chart.getOption() as any)?.dataZoom?.[0]
      if (typeof zoom?.start !== 'number' || typeof zoom?.end !== 'number') return
      onZoomRangeChange({ start: zoom.start, end: zoom.end })
    }
    chart.on('dataZoom', handleZoom)
    return () => {
      if (!chart.isDisposed()) chart.off('dataZoom', handleZoom)
    }
  }, [dailyRows.length, onZoomRangeChange])
  return <div className="relative h-56 w-full">
    <div ref={ref} className="h-full w-full" />
    {zoomed ? <span className="pointer-events-none absolute left-2 top-1 text-[10px] text-muted">区间收益</span> : null}
    {!dailyRows.length ? <div className="absolute inset-0 grid place-items-center text-xs text-muted">等待首个收益采样</div> : null}
  </div>
}

function LatestDecision({ event, instrumentLabel, actualHoldingSymbols, orders, fills }: { event?: PaperEvent; instrumentLabel: (symbol: unknown) => string; actualHoldingSymbols: string[]; orders: PaperOrder[]; fills: PaperFill[] }) {
  if (!event) return <div className="grid min-h-52 place-items-center px-5 text-center text-xs text-muted">当日暂无策略决策</div>
  const targets = event.target_symbols ?? []
  const holdings = event.holding_symbols ?? []
  const isCandidatePool = event.strategy === 'small_cap_limitup'
  const clearing = !targets.length && holdings.length > 0
  const actualHoldings = new Set(actualHoldingSymbols)
  const remaining = clearing ? holdings.filter(symbol => actualHoldings.has(symbol)) : []
  const clearingIncomplete = clearing && remaining.length > 0
  const holdingsRemainCandidates = isCandidatePool && targets.length > 0 && actualHoldingSymbols.length > 0 && actualHoldingSymbols.every(symbol => targets.includes(symbol))
  const unchangedCandidateHoldings = event.decision === 'rebalance' && holdingsRemainCandidates && orders.length === 0
  const executionText = event.decision === 'rebalance' && !clearing
    ? orders.length === 0
      ? isCandidatePool && holdingsRemainCandidates ? '未产生委托，当前持仓仍在候选池内' : '未产生委托'
      : fills.length === 0 ? `当日委托 ${orders.length} 笔，暂无成交` : `当日委托 ${orders.length} 笔，成交 ${fills.length} 笔`
    : ''
  const label = clearingIncomplete ? '清仓未完成' : unchangedCandidateHoldings ? '持仓未变' : decisionLabel(event)
  const decisionTone = clearingIncomplete ? 'text-warning' : unchangedCandidateHoldings ? 'text-muted' : event.decision === 'rebalance' ? 'text-bull' : event.decision === 'empty' ? 'text-muted' : 'text-foreground'
  return <div className="space-y-3 px-4 py-3 text-xs">
    <div className="flex items-center justify-between gap-3"><span className={`font-semibold ${decisionTone}`}>{label}</span><span className="font-mono text-[10px] text-muted">{event.trading_date ?? formatTime(event.timestamp)}</span></div>
    <div className="grid grid-cols-[72px_minmax(0,1fr)] gap-x-3 gap-y-2">
      <span className="text-muted">{isCandidatePool ? '候选池' : '目标标的'}</span><span className="break-words">{targets.length ? targets.map(instrumentLabel).join('、') : '空仓'}</span>
      <span className="text-muted">{isCandidatePool ? '当前持仓' : '决策前持仓'}</span><span className="break-words">{holdings.length ? holdings.map(instrumentLabel).join('、') : '空仓'}</span>
      {executionText ? <><span className="text-muted">执行结果</span><span className={orders.length === 0 ? 'text-muted' : fills.length === 0 ? 'text-warning' : 'text-success'}>{executionText}</span></> : null}
      {clearing ? <><span className="text-muted">执行结果</span><span className={clearingIncomplete ? 'text-warning' : 'text-success'}>{clearingIncomplete ? `未完成，仍持有 ${remaining.length} 只` : '已完成'}</span></> : null}
      <span className="text-muted">决策原因</span><span className="leading-5">{decisionReasonText(event)}</span>
    </div>
  </div>
}

function RenameAccountDialog({ account, onClose, onSaved }: { account: PaperAccount; onClose: () => void; onSaved: () => Promise<unknown> }) {
  const [name, setName] = useState(account.name)
  const [pending, setPending] = useState(false)
  const submit = async () => {
    const normalized = name.trim()
    if (!normalized || normalized.length > 40) return
    setPending(true)
    try {
      await api.renamePaperAccount(account.id, normalized)
      await onSaved()
      toast('模拟名称已修改', 'success')
      onClose()
    } catch {
      return
    } finally {
      setPending(false)
    }
  }
  return <Modal labelledBy="rename-paper-title" onClose={onClose} closeOnBackdrop={!pending} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl">
    <form onSubmit={event => { event.preventDefault(); void submit() }}>
      <div className="border-b border-border px-4 py-3"><h2 id="rename-paper-title" className="text-sm font-semibold">修改模拟名称</h2></div>
      <div className="p-4"><label className="text-xs text-muted">名称<input autoFocus className={`${INPUT} mt-1.5 text-foreground`} maxLength={40} value={name} onChange={event => setName(event.target.value)} /></label><div className="mt-1 text-right text-[10px] text-muted">{name.trim().length}/40</div></div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" disabled={pending} onClick={onClose} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted">取消</button><button type="submit" disabled={pending || !name.trim() || name.trim().length > 40} className="rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40">{pending ? '保存中...' : '保存'}</button></div>
    </form>
  </Modal>
}

function CreateAccountDialog({ strategyId, backtestJobId, onClose, onCreated }: { strategyId: string; backtestJobId: string; onClose: () => void; onCreated: (account: PaperAccount) => void }) {
  const strategies = useQuery({ queryKey: ['free-strategies'], queryFn: api.freeStrategies })
  const paperStatus = useQuery({ queryKey: ['free-paper-status'], queryFn: api.paperStatus })
  const list = strategies.data?.strategies ?? []
  const initialStrategy = strategyId || list[0]?.id || ''
  const [form, setForm] = useState<CreatePaperAccount>({
    name: '量化策略 · 模拟',
    strategy_id: initialStrategy,
    symbols: [],
    timeframe: '1d',
    asset_type: 'etf',
    initial_capital: 1_000_000,
    fees_pct: 0.0002,
    commission_pct: null,
    min_commission: 0,
    stamp_tax_pct: 0.001,
    slippage_bps: 5,
    price_tick: null,
    lot_size: 100,
    max_exposure_pct: 1,
    settlement: 't1',
    fill_policy: 'next_open',
    benchmark_symbol: '510300.SH',
    market_mode: 'bar_1d',
    continuation_job_id: backtestJobId || null,
    risk_config: DEFAULT_RISK,
  })
  const [pending, setPending] = useState(false)

  useEffect(() => {
    if (!form.strategy_id && initialStrategy) setForm(current => ({ ...current, strategy_id: initialStrategy }))
  }, [form.strategy_id, initialStrategy])

  useEffect(() => {
    const selected = list.find(item => item.id === form.strategy_id)
    if (!selected) return
    const saved = selected.config ?? {}
    setForm(current => ({
      ...current,
      name: current.name === '量化策略 · 模拟' ? `${selected.name} · 模拟` : current.name,
      asset_type: saved.asset_type === 'stock' ? 'stock' : 'etf',
      market_mode: saved.timeframe === '1d' ? 'bar_1d' : 'bar_1m',
      initial_capital: Number(saved.paper_initial_capital ?? saved.initial_capital ?? current.initial_capital),
      fees_pct: Number(saved.fees_pct ?? current.fees_pct),
      commission_pct: saved.commission_pct == null ? null : Number(saved.commission_pct),
      sell_commission_pct: saved.sell_commission_pct == null ? null : Number(saved.sell_commission_pct),
      min_commission: Number(saved.min_commission ?? current.min_commission),
      reserve_buy_fees: typeof saved.reserve_buy_fees === 'boolean' ? saved.reserve_buy_fees : current.reserve_buy_fees,
      stamp_tax_pct: Number(saved.stamp_tax_pct ?? current.stamp_tax_pct),
      slippage_bps: Number(saved.slippage_bps ?? current.slippage_bps),
      price_tick: saved.price_tick == null ? null : Number(saved.price_tick),
      lot_size: Number(saved.lot_size ?? current.lot_size),
      max_exposure_pct: Number(saved.max_exposure_pct ?? current.max_exposure_pct),
      benchmark_symbol: String(saved.benchmark_symbol ?? current.benchmark_symbol),
      settlement: saved.settlement === 't0' ? 't0' : 't1',
      t0_symbols: Array.isArray(saved.t0_symbols) ? saved.t0_symbols.map(String) : [],
      allow_stale_fills: saved.allow_stale_fills === true,
      fill_policy: saved.fill_policy === 'close' ? 'close' : 'next_open',
      continuation_job_id: backtestJobId || null,
    }))
  }, [backtestJobId, form.strategy_id, list])

  const setRisk = (key: keyof typeof DEFAULT_RISK, value: number) => setForm(current => ({ ...current, risk_config: { ...current.risk_config, [key]: value } }))
  const submit = async () => {
    setPending(true)
    try {
      const timeframe = form.market_mode === 'bar_1d' ? '1d' : '1m'
      const created = await api.createPaperAccount({ ...form, timeframe })
      toast('模拟账户已创建', 'success')
      onCreated(created)
    } catch {
      return
    } finally {
      setPending(false)
    }
  }

  return <Modal labelledBy="create-paper-title" onClose={onClose} closeOnBackdrop={!pending} panelClassName="w-[94vw] max-w-2xl rounded-card border border-border bg-surface shadow-xl">
    <form onSubmit={event => { event.preventDefault(); void submit() }}>
      <div className="border-b border-border px-4 py-3"><h2 id="create-paper-title" className="text-sm font-semibold">创建模拟账户</h2></div>
      <div className="grid max-h-[72vh] grid-cols-2 gap-3 overflow-y-auto p-4 text-xs max-sm:grid-cols-1">
        <label className="col-span-2 max-sm:col-span-1">账户名称<input className={`${INPUT} mt-1`} maxLength={40} value={form.name} onChange={event => setForm({ ...form, name: event.target.value })} /></label>
        <label>策略<select className={`${INPUT} mt-1`} value={form.strategy_id} onChange={event => setForm({ ...form, strategy_id: event.target.value })}>{list.map(item => <option key={item.id} value={item.id}>{item.name} · r{item.revision}</option>)}</select></label>
        <label>行情模式<select className={`${INPUT} mt-1`} value={form.market_mode} onChange={event => setForm({ ...form, market_mode: event.target.value as PaperMarketMode })}><option value="bar_1m">1分钟K线</option><option value="bar_1d">日K</option><option value="poll_3s" disabled={paperStatus.data?.poll_3s.available === false}>3秒行情{paperStatus.data?.poll_3s.available === false ? `（套餐最低 ${paperStatus.data.poll_3s.min_interval_s} 秒）` : ''}</option><option value="websocket">WebSocket</option></select></label>
        <label>资产<select className={`${INPUT} mt-1`} value={form.asset_type} onChange={event => setForm({ ...form, asset_type: event.target.value as 'stock' | 'etf' })}><option value="etf">ETF</option><option value="stock">股票</option></select></label>
        <label>初始资金<input type="number" min="1" className={`${INPUT} mt-1`} value={form.initial_capital} onChange={event => setForm({ ...form, initial_capital: Number(event.target.value) })} /></label>
        <label>基准<input className={`${INPUT} mt-1 font-mono`} value={form.benchmark_symbol} onChange={event => setForm({ ...form, benchmark_symbol: event.target.value.toUpperCase() })} /></label>
        <label>成交时点<select className={`${INPUT} mt-1`} value={form.fill_policy} onChange={event => setForm({ ...form, fill_policy: event.target.value as 'next_open' | 'close' })}><option value="next_open">{form.market_mode.startsWith('bar_') ? '下一根开盘' : '下一次报价'}</option><option value="close">{form.market_mode.startsWith('bar_') ? '当前收盘' : '当前报价'}</option></select></label>
        <div className="col-span-2 mt-1 border-t border-border pt-3 text-[11px] font-medium max-sm:col-span-1">统一风控</div>
        <label>单标的仓位上限<input type="number" min="1" max="100" step="1" className={`${INPUT} mt-1`} value={form.risk_config.max_symbol_exposure_pct * 100} onChange={event => setRisk('max_symbol_exposure_pct', Number(event.target.value) / 100)} /></label>
        <label>日亏损上限<input type="number" min="0.1" max="10" step="0.1" className={`${INPUT} mt-1`} value={form.risk_config.daily_loss_pct * 100} onChange={event => setRisk('daily_loss_pct', Number(event.target.value) / 100)} /></label>
        <label>最大回撤上限<input type="number" min="0.1" max="30" step="0.1" className={`${INPUT} mt-1`} value={form.risk_config.max_drawdown_pct * 100} onChange={event => setRisk('max_drawdown_pct', Number(event.target.value) / 100)} /></label>
        <label>每分钟委托上限<input type="number" min="1" max="60" className={`${INPUT} mt-1`} value={form.risk_config.max_orders_per_minute} onChange={event => setRisk('max_orders_per_minute', Number(event.target.value))} /></label>
      </div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" disabled={pending} onClick={onClose} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted">取消</button><button type="submit" disabled={pending || !form.strategy_id || !form.name.trim()} className="rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40">{pending ? '创建中...' : '创建'}</button></div>
    </form>
  </Modal>
}

export function PaperTrading() {
  const qc = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const [selectedId, setSelectedId] = useState('')
  const [tab, setTab] = useState<DetailTab>('positions')
  const [selectedDate, setSelectedDate] = useState('')
  const [returnZoomRange, setReturnZoomRange] = useState<ReturnZoomRange>({ start: 0, end: 100 })
  const updateReturnZoomRange = useCallback((range: ReturnZoomRange) => {
    setReturnZoomRange(previous => Math.abs(previous.start - range.start) < 0.01 && Math.abs(previous.end - range.end) < 0.01 ? previous : range)
  }, [])
  const [showCreate, setShowCreate] = useState(searchParams.get('create') === '1')
  const [pendingAction, setPendingAction] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<PaperAccount | null>(null)
  const [renameTarget, setRenameTarget] = useState<PaperAccount | null>(null)
  const notifiedSequence = useRef(0)
  const accountsQuery = useQuery({ queryKey: ['free-paper-accounts'], queryFn: api.paperAccounts, refetchInterval: 10_000 })
  const statusQuery = useQuery({ queryKey: ['free-paper-status'], queryFn: api.paperStatus, refetchInterval: 3_000 })
  const detailQuery = useQuery({ queryKey: ['free-paper-account', selectedId], queryFn: () => api.paperAccount(selectedId), enabled: Boolean(selectedId), refetchInterval: selectedId ? 5_000 : false })
  const eventsQuery = useQuery({ queryKey: ['free-paper-events', selectedId], queryFn: () => api.paperEvents(selectedId), enabled: Boolean(selectedId) })
  const signalsQuery = useQuery({ queryKey: ['free-paper-signals', selectedId], queryFn: () => api.paperSignals(selectedId), enabled: Boolean(selectedId) })
  const accounts = accountsQuery.data?.accounts ?? []
  const account = detailQuery.data ?? accounts.find(item => item.id === selectedId)
  const events = useMemo(() => eventsQuery.data?.events ?? [], [eventsQuery.data?.events])
  const instrumentSymbols = useMemo(() => {
    const symbols = new Set<string>(account?.universe ?? [])
    const snapshot = account?.account
    Object.keys(snapshot?.positions ?? account?.positions ?? {}).forEach(symbol => symbols.add(symbol))
    snapshot?.orders?.forEach(order => symbols.add(order.symbol))
    snapshot?.fills?.forEach(fill => symbols.add(fill.symbol))
    events.forEach(event => { if (event.symbol) symbols.add(event.symbol) })
    signalsQuery.data?.signals.forEach(event => {
      event.target_symbols?.forEach(symbol => symbols.add(symbol))
      event.holding_symbols?.forEach(symbol => symbols.add(symbol))
      event.candidates?.forEach(candidate => symbols.add(candidate.symbol))
    })
    return [...symbols].sort()
  }, [account, events, signalsQuery.data?.signals])
  const namesQuery = useQuery({
    queryKey: ['instrument-names', instrumentSymbols.join(',')],
    queryFn: () => api.instrumentNames(instrumentSymbols),
    enabled: instrumentSymbols.length > 0,
    staleTime: 300_000,
  })
  const symbolNames = namesQuery.data?.names ?? {}
  const instrumentLabel = (symbol: unknown) => formatInstrumentLabel(symbol, symbolNames[String(symbol ?? '')])

  useEffect(() => {
    if (!selectedId && accounts[0]) setSelectedId(accounts[0].id)
    if (selectedId && accounts.length && !accounts.some(item => item.id === selectedId)) setSelectedId(accounts[0].id)
  }, [accounts, selectedId])

  useEffect(() => {
    setSelectedDate('')
  }, [selectedId])

  useEffect(() => {
    notifiedSequence.current = Math.max(0, ...(detailQuery.data?.events ?? []).map(event => event.sequence))
  }, [detailQuery.data?.events, selectedId])

  useEffect(() => {
    if (!selectedId || account?.status !== 'running' || !detailQuery.data) return
    const stream = new EventSource(`/api/free-strategies/paper/accounts/${encodeURIComponent(selectedId)}/stream?after=${notifiedSequence.current}`)
    stream.addEventListener('paper', raw => {
      const event = JSON.parse((raw as MessageEvent).data) as PaperEvent
      notifiedSequence.current = Math.max(notifiedSequence.current, event.sequence)
      void qc.invalidateQueries({ queryKey: ['free-paper-account', selectedId] })
      void qc.invalidateQueries({ queryKey: ['free-paper-events', selectedId] })
      if (event.type === 'signal') void qc.invalidateQueries({ queryKey: ['free-paper-signals', selectedId] })
      if (['fill', 'rejected', 'risk'].includes(event.type)) {
        pushAlertToast({ ts: Date.now(), source: 'strategy', type: event.type, symbol: event.symbol, message: eventText(event), severity: event.type === 'risk' ? 'critical' : event.type === 'rejected' ? 'warn' : 'info' })
      }
    })
    return () => stream.close()
  }, [account?.status, qc, selectedId])

  const action = async (value: 'start' | 'pause' | 'resume' | 'stop' | 'unlock-risk') => {
    if (!account) return
    setPendingAction(value)
    try {
      await api.paperAction(account.id, value)
      await Promise.all([accountsQuery.refetch(), detailQuery.refetch(), statusQuery.refetch(), eventsQuery.refetch(), signalsQuery.refetch()])
    } catch {
      return
    } finally {
      setPendingAction('')
    }
  }

  const toggleSystemNotify = async () => {
    if (!account) return
    try {
      await api.updatePaperSystemNotify(account.id, !account.system_notify_enabled)
      await Promise.all([accountsQuery.refetch(), detailQuery.refetch()])
      toast(account.system_notify_enabled ? '该模拟策略已关闭系统通知' : '该模拟策略已开启系统通知', 'success')
    } catch (e) {
      toast(`通知设置失败 · ${String((e as Error)?.message || e)}`, 'error')
    }
  }

  const remove = async () => {
    if (!deleteTarget) return
    setPendingAction('delete')
    try {
      await api.deletePaperAccount(deleteTarget.id)
      setDeleteTarget(null)
      setSelectedId('')
      await accountsQuery.refetch()
      toast('模拟账户已删除', 'success')
    } catch {
      return
    } finally {
      setPendingAction('')
    }
  }

  const accountState = account?.account
  const equityRows = accountState?.equity_curve ?? []
  const benchmarkSymbol = account?.config?.benchmark_symbol?.trim() ?? ''
  const benchmarkStart = equityRows[0]?.timestamp.slice(0, 10) ?? ''
  const benchmarkEnd = equityRows.at(-1)?.timestamp.slice(0, 10) ?? ''
  const benchmarkQuery = useQuery({
    queryKey: QK.kline(benchmarkSymbol, benchmarkStart, benchmarkEnd),
    queryFn: () => api.klineDaily(benchmarkSymbol, 120, { start: benchmarkStart, end: benchmarkEnd }),
    enabled: Boolean(benchmarkSymbol && benchmarkStart && benchmarkEnd),
    staleTime: 300_000,
  })
  const benchmarkRows = benchmarkQuery.data?.rows ?? []
  const benchmarkLabel = formatInstrumentLabel(benchmarkSymbol, benchmarkQuery.data?.name)
  const decisionEvents = signalsQuery.data?.signals ?? []
  const allFills = accountState?.fills ?? []
  const allOrders = accountState?.orders ?? []
  const allLogEvents = events.filter(event => event.type === 'log' && event.source === 'strategy')
  const availableDates = [...new Set([
    ...equityRows.map(row => row.timestamp.slice(0, 10)),
    account?.valuation?.date ?? '',
    ...allFills.map(fill => fill.timestamp.slice(0, 10)),
    ...allOrders.map(order => order.submitted_at.slice(0, 10)),
    ...decisionEvents.map(eventTradingDate),
    ...allLogEvents.map(eventTradingDate),
  ].filter(Boolean))].sort()
  const latestDate = availableDates.at(-1) ?? ''
  const activeDate = availableDates.includes(selectedDate) ? selectedDate : latestDate
  const visibleEquityRows = activeDate ? equityRows.filter(row => row.timestamp.slice(0, 10) <= activeDate) : equityRows
  const selectedSnapshot = visibleEquityRows.at(-1)
  const latestSnapshotDate = equityRows.at(-1)?.timestamp.slice(0, 10) ?? ''
  const useCurrentState = !activeDate || !latestSnapshotDate || activeDate >= latestSnapshotDate
  const positions = Object.entries((useCurrentState ? account?.positions : selectedSnapshot?.positions) ?? {}).filter(([, quantity]) => quantity > 0)
  const fills = allFills.filter(fill => fill.timestamp.slice(0, 10) === activeDate)
  const orders = allOrders.filter(order => order.submitted_at.slice(0, 10) === activeDate)
  const visibleDecisionEvents = decisionEvents.filter(event => eventTradingDate(event) === activeDate)
  const latestDecision = visibleDecisionEvents.find(event => event.signal_type === 'daily_decision')
  const logEvents = allLogEvents.filter(event => eventTradingDate(event) === activeDate)
  const selectedEquity = useCurrentState ? account?.equity : selectedSnapshot?.equity
  const selectedCash = useCurrentState ? account?.cash : selectedSnapshot?.cash
  const selectedReturn = useCurrentState
    ? Number(account?.return_pct ?? 0)
    : selectedSnapshot ? (Number(selectedSnapshot.nav) - 1) * 100 : null
  const returnChartData = useMemo(
    () => buildReturnChartData(visibleEquityRows, benchmarkRows, returnZoomRange),
    [benchmarkRows, returnZoomRange, visibleEquityRows],
  )
  const returnZoomed = returnZoomRange.start > 0.01 || returnZoomRange.end < 99.99
  const comparisonReturn = returnZoomed ? returnChartData.strategyReturn : selectedReturn
  const selectedBenchmarkReturn = returnChartData.benchmarkReturn
  const selectedExcessReturn = comparisonReturn != null && selectedBenchmarkReturn != null
    ? comparisonReturn - selectedBenchmarkReturn
    : null
  const drawdownValues = visibleEquityRows
    .map(row => Number(row.drawdown_pct))
    .filter(Number.isFinite)
  if (useCurrentState && account?.drawdown_pct != null && Number.isFinite(Number(account.drawdown_pct))) {
    drawdownValues.push(Number(account.drawdown_pct))
  }
  if (useCurrentState && account?.max_drawdown_pct != null && Number.isFinite(Number(account.max_drawdown_pct))) {
    drawdownValues.push(Number(account.max_drawdown_pct))
  }
  const selectedMaxDrawdown = drawdownValues.reduce<number | null>(
    (maximum, value) => maximum == null ? value : Math.max(maximum, value),
    null,
  )
  const initialCapital = account?.config?.initial_capital
  const viewingLatest = !selectedDate
  const liveValuationPending = viewingLatest && account?.status === 'running' && account.valuation?.live === false
  const selectedDailyPerformance = liveValuationPending
    ? null
    : dailyPerformance(equityRows, activeDate, selectedEquity, initialCapital)
  const DailyReturnIcon = selectedDailyPerformance == null || selectedDailyPerformance.amount === 0
    ? Minus
    : selectedDailyPerformance.amount > 0 ? ArrowUpRight : ArrowDownRight
  const dailyMetricLabel = selectedDate ? '当日收益' : '今日收益'
  const selectTradingDate = (value: string) => {
    const resolved = availableDates.includes(value)
      ? value
      : availableDates.filter(date => date <= value).at(-1) ?? availableDates[0] ?? ''
    setSelectedDate(resolved)
  }
  const status = statusQuery.data

  return <div className="flex h-full min-h-0 flex-col max-md:fixed max-md:inset-0 max-md:z-[10000] max-md:bg-base">
    <PageHeader title="模拟" subtitle={`${status?.running_accounts ?? 0} 个账户运行中`} right={<button type="button" onClick={() => setShowCreate(true)} className="inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white"><Plus className="h-3.5 w-3.5" />创建账户</button>} />
    <div className="grid min-h-0 flex-1 grid-cols-[228px_minmax(0,1fr)] max-xl:grid-cols-1">
      <aside className={`min-h-0 overflow-y-auto border-r border-border p-2 max-xl:max-h-36 max-xl:border-b max-xl:border-r-0 ${account ? 'max-md:max-h-40' : ''}`}>
        <div className="space-y-1 max-xl:grid max-xl:grid-cols-2 max-xl:gap-1 max-xl:space-y-0 max-sm:grid-cols-1">
          {accounts.map(item => <button key={item.id} type="button" onClick={() => setSelectedId(item.id)} className={`w-full border-l-2 px-2.5 py-2 text-left transition-colors ${selectedId === item.id ? 'border-l-accent bg-accent/8' : 'border-l-transparent hover:bg-elevated'}`}>
            <div className="flex items-center justify-between gap-2"><span className="truncate text-xs font-medium">{item.name}</span><span className={`shrink-0 text-[10px] ${syncLabel(item) ? syncClass(item) : statusClass(item.status)}`}>{syncLabel(item) || statusLabel(item.status)}</span></div>
            <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted"><span className="truncate">{MODE_LABEL[item.market_mode] ?? item.market_mode} · {formatTime(item.sync?.through ?? item.last_bar ?? item.updated_at)}</span><span className={`shrink-0 tabular-nums ${returnClass(item.return_pct)}`}>累计 {signedPercent(Number(item.return_pct ?? 0))}</span></div>
            <div className="mt-0.5 flex items-center justify-between gap-2 text-[10px]"><span className="text-muted">今日收益率</span><span className={`shrink-0 tabular-nums ${returnClass(item.today_return_pct ?? undefined)}`}>{item.today_return_pct == null ? '—' : signedPercent(Number(item.today_return_pct))}</span></div>
          </button>)}
          {!accounts.length && !accountsQuery.isLoading ? <div className="py-10 text-center text-xs text-muted">暂无模拟账户</div> : null}
        </div>
      </aside>
      {!account ? <EmptyState icon={WalletCards} title="选择或创建模拟账户" /> : <main className="min-h-0 min-w-0 overflow-y-auto">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-4 py-2.5">
          <div className="min-w-0"><div className="flex items-center gap-1.5"><h2 className="truncate text-sm font-semibold">{account.name}</h2><button type="button" title="修改模拟名称" onClick={() => setRenameTarget(account)} className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded text-muted hover:bg-elevated hover:text-foreground"><Pencil className="h-3.5 w-3.5" /></button><span className={`text-[11px] ${statusClass(account.status)}`}>{statusLabel(account.status)}</span>{syncLabel(account) ? <span className={`text-[10px] ${syncClass(account)}`}>{syncLabel(account)}</span> : null}</div><div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-muted"><span>{MODE_LABEL[account.market_mode]}</span><span>策略 r{account.source_revision}</span><span className="font-mono">{account.source_hash?.slice(0, 8)}</span><span>{account.execution_mode === 'scheduled' ? '定时执行' : account.execution_mode === 'quote' ? '报价驱动' : '闭合1分钟K线'}</span></div></div>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <DatePicker value={activeDate} onChange={selectTradingDate} min={availableDates[0]} max={latestDate} align="right" />
            <div className="flex items-center gap-1">
              <button type="button" title={account.system_notify_enabled ? '关闭策略通知（系统与 Webhook）' : '开启策略通知（系统与 Webhook）'} onClick={() => void toggleSystemNotify()} className={`inline-flex h-8 w-8 items-center justify-center rounded border ${account.system_notify_enabled ? 'border-warning bg-warning text-base shadow-[0_0_12px_rgba(245,158,11,0.6)] hover:bg-warning/85' : 'border-border text-muted hover:border-accent hover:text-accent'}`}><Bell className="h-4 w-4" /></button>
              <button type="button" title={account.status === 'paused' ? '恢复' : '启动'} disabled={Boolean(pendingAction) || account.status === 'running'} onClick={() => void action(account.status === 'paused' ? 'resume' : 'start')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-success/40 text-success hover:border-success hover:bg-success/10 disabled:opacity-35"><CirclePlay className="h-4 w-4" /></button>
              <button type="button" title="暂停" disabled={Boolean(pendingAction) || account.status !== 'running'} onClick={() => void action('pause')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-warning/40 text-warning hover:border-warning hover:bg-warning/10 disabled:opacity-35"><CirclePause className="h-4 w-4" /></button>
              <button type="button" title="停止" disabled={Boolean(pendingAction) || account.status === 'stopped'} onClick={() => void action('stop')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-danger/40 text-danger hover:border-danger hover:bg-danger/10 disabled:opacity-35"><Square className="h-4 w-4" /></button>
              <button type="button" title="刷新" onClick={() => void Promise.all([detailQuery.refetch(), eventsQuery.refetch(), signalsQuery.refetch()])} className="inline-flex h-8 w-8 items-center justify-center rounded border border-accent/40 text-accent hover:border-accent hover:bg-accent/10"><RefreshCw className="h-4 w-4" /></button>
              <button type="button" title={account.status === 'stopped' ? '删除' : '请先停止账户'} disabled={Boolean(pendingAction) || account.status !== 'stopped'} onClick={() => setDeleteTarget(account)} className="inline-flex h-8 w-8 items-center justify-center rounded border border-danger/40 text-danger hover:border-danger hover:bg-danger/10 disabled:opacity-35"><Trash2 className="h-4 w-4" /></button>
            </div>
          </div>
        </div>

        {account.last_error ? <div className="mx-4 mt-3 flex gap-2 rounded border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger"><AlertTriangle className="h-4 w-4 shrink-0" />{account.last_error}</div> : null}
        {(account.risk_status?.daily_loss_locked || account.risk_status?.drawdown_locked) ? <div className="mx-4 mt-3 flex items-center gap-2 rounded border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning"><ShieldAlert className="h-4 w-4" /><span className="flex-1">{account.risk_status.reason ?? '风控锁定'}</span>{account.risk_status.drawdown_locked ? <button type="button" onClick={() => void action('unlock-risk')} className="rounded border border-warning/50 px-2 py-1 text-[11px]">确认恢复</button> : null}</div> : null}

        <section className="grid grid-cols-[1.15fr_1fr_1fr_1fr] border-b border-border max-lg:grid-cols-2">
          <div className={`relative min-w-0 overflow-hidden border-r border-border px-4 py-3.5 max-lg:border-b ${selectedDailyPerformance == null || selectedDailyPerformance.amount === 0 ? '' : selectedDailyPerformance.amount > 0 ? 'bg-bull/[0.045]' : 'bg-bear/[0.045]'}`}>
            {selectedDailyPerformance != null && selectedDailyPerformance.amount !== 0 ? <span className={`absolute inset-y-0 left-0 w-0.5 ${selectedDailyPerformance.amount > 0 ? 'bg-bull' : 'bg-bear'}`} /> : null}
            <div className={`flex items-center gap-1.5 text-[10px] ${selectedDailyPerformance == null ? 'text-muted' : returnClass(selectedDailyPerformance.amount)}`}><DailyReturnIcon className="h-3.5 w-3.5" />{dailyMetricLabel}{viewingLatest && account.valuation?.live ? <span className="inline-flex items-center gap-1 text-success"><Wifi className="h-3 w-3" />实时</span> : null}</div>
            <div className={`mt-1 whitespace-nowrap font-mono text-xl font-semibold tabular-nums max-sm:text-lg ${returnClass(selectedDailyPerformance?.amount)}`}>
              {selectedDailyPerformance == null ? '—' : signedMoney(selectedDailyPerformance.amount)}
            </div>
            <div className="mt-1 flex min-h-4 items-center gap-1.5 text-[10px] text-muted">
              {liveValuationPending ? '等待实时行情' : selectedDailyPerformance == null ? '等待收益基准' : <><span className={`font-mono ${returnClass(selectedDailyPerformance.pct)}`}>{signedPercent(selectedDailyPerformance.pct)}</span><span>{selectedDailyPerformance.reference === 'previous-close' ? '较前一交易日' : '较初始资金'}</span></>}
            </div>
          </div>
          <div className="min-w-0 border-r border-border px-4 py-3.5 max-lg:border-b max-lg:border-r-0">
            <div className="flex items-center gap-1.5 text-[10px] text-muted"><Activity className="h-3.5 w-3.5" />总资产</div>
            <div className="mt-1 font-mono text-lg tabular-nums">{selectedEquity == null ? '—' : MONEY.format(selectedEquity)}</div>
            <div className="mt-1 min-h-4 truncate text-[10px] text-muted">可用现金 <span className="font-mono text-secondary">{selectedCash == null ? '—' : MONEY.format(selectedCash)}</span></div>
          </div>
          <div className="min-w-0 border-r border-border px-4 py-3.5">
            <div className="flex items-center gap-1.5 text-[10px] text-muted"><Gauge className="h-3.5 w-3.5" />累计收益</div>
            <div className={`mt-1 font-mono text-lg tabular-nums ${selectedReturn == null ? '' : returnClass(selectedReturn)}`}>{selectedReturn == null ? '—' : signedPercent(selectedReturn)}</div>
            <div className="mt-1 min-h-4 truncate text-[10px] text-muted">初始资金 <span className="font-mono text-secondary">{initialCapital == null ? '—' : MONEY.format(initialCapital)}</span></div>
          </div>
          <div className="min-w-0 px-4 py-3.5">
            <div className="flex items-center gap-1.5 text-[10px] text-muted"><ShieldAlert className="h-3.5 w-3.5" />最大回撤</div>
            <div className={`mt-1 font-mono text-lg tabular-nums ${selectedMaxDrawdown != null && selectedMaxDrawdown > 0 ? 'text-bear' : 'text-muted'}`}>{selectedMaxDrawdown == null ? '—' : selectedMaxDrawdown > 0 ? `-${selectedMaxDrawdown.toFixed(2)}%` : '0.00%'}</div>
            <div className="mt-1 min-h-4 truncate text-[10px] text-muted">风控阈值 <span className="font-mono text-secondary">{account.risk_config?.max_drawdown_pct == null ? '—' : `${(account.risk_config.max_drawdown_pct * 100).toFixed(0)}%`}</span></div>
          </div>
        </section>

        <section className="grid grid-cols-[minmax(0,1.7fr)_minmax(280px,1fr)] border-b border-border max-lg:grid-cols-1">
          <div className="min-w-0 border-r border-border px-3 py-2 max-lg:border-b max-lg:border-r-0"><div className="flex min-h-5 flex-wrap items-center justify-between gap-x-3 gap-y-1 text-[11px] font-medium"><span>收益曲线</span><div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px] font-normal text-muted">{benchmarkQuery.isError ? <span className="text-danger">{benchmarkLabel || '基准'}暂不可用</span> : <><span>基准 <span className={`font-mono ${returnClass(selectedBenchmarkReturn ?? undefined)}`}>{selectedBenchmarkReturn == null ? '—' : signedPercent(selectedBenchmarkReturn)}</span></span><span>超额 <span className={`font-mono ${returnClass(selectedExcessReturn ?? undefined)}`}>{selectedExcessReturn == null ? '—' : signedPercent(selectedExcessReturn)}</span></span></>}</div></div><ReturnChart data={returnChartData} benchmarkLabel={benchmarkLabel || benchmarkSymbol || '基准'} zoomRange={returnZoomRange} onZoomRangeChange={updateReturnZoomRange} /></div>
          <div className="min-w-0"><div className="border-b border-border px-4 py-2 text-[11px] font-medium">最新决策</div><LatestDecision event={latestDecision} instrumentLabel={instrumentLabel} actualHoldingSymbols={positions.map(([symbol]) => symbol)} orders={orders} fills={fills} /></div>
        </section>

        {syncLabel(account) || account.market_mode === 'poll_3s' || account.market_mode === 'websocket' ? <section className="flex min-h-10 flex-wrap items-center gap-x-5 gap-y-2 border-b border-border px-4 py-2 text-[10px] text-muted">
          {syncLabel(account) ? <span className={`inline-flex items-center gap-1.5 ${syncClass(account)}`}><Activity className="h-3.5 w-3.5" />{syncLabel(account)}</span> : null}
          {account.sync?.phase === 'catching_up' ? <span className="font-mono text-warning">目标 {formatTime(account.sync.target ?? undefined)}</span> : null}
          {(account.sync?.queue_delay_seconds ?? 0) > 0.05 ? <span className="inline-flex items-center gap-1.5"><Gauge className="h-3.5 w-3.5" />队列等待 {Number(account.sync?.queue_delay_seconds).toFixed(1)} 秒</span> : null}
          {account.market_mode === 'poll_3s' ? <span className="inline-flex items-center gap-1.5"><Gauge className="h-3.5 w-3.5" />{status?.poll_3s.actual_fetch_ms != null ? `${status.poll_3s.actual_fetch_ms} ms` : '—'}</span> : null}
          {account.market_mode === 'websocket' ? <span className="inline-flex items-center gap-1.5"><Wifi className="h-3.5 w-3.5" />{status?.websocket.status ?? 'disconnected'} · {status?.websocket.symbols ?? 0}/{status?.websocket.capacity ?? 100}</span> : null}
        </section> : null}

        <div className="flex border-b border-border px-3 pt-2">
          {([['positions', '持仓', WalletCards, positions.length], ['decisions', '决策', Activity, visibleDecisionEvents.length], ['trades', '交易', ListOrdered, orders.length], ['logs', '日志', FileText, logEvents.length]] as const).map(([value, label, Icon, count]) => <button key={value} type="button" onClick={() => setTab(value)} className={`inline-flex h-8 items-center gap-1.5 border-b-2 px-3 text-xs ${tab === value ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><Icon className="h-3.5 w-3.5" />{label}<span className="font-mono text-[9px] opacity-70">{count}</span></button>)}
        </div>
        <section className="min-h-56 overflow-x-auto p-3">
          {tab === 'positions' ? <table className="w-full min-w-[560px] text-left text-xs"><thead className="text-[10px] text-muted"><tr><th className="px-2 py-2 font-medium">标的</th><th className="px-2 py-2 text-right font-medium">数量</th><th className="px-2 py-2 text-right font-medium">成本</th><th className="px-2 py-2 text-right font-medium">成本市值</th></tr></thead><tbody>{positions.map(([symbol, quantity]) => {
            const averageCost = (useCurrentState ? accountState?.avg_cost : selectedSnapshot?.avg_cost)?.[symbol]
            return <tr key={symbol} className="border-t border-border"><td className="px-2 py-2.5 font-mono">{instrumentLabel(symbol)}</td><td className="px-2 py-2.5 text-right font-mono">{quantity.toLocaleString()}</td><td className="px-2 py-2.5 text-right font-mono">{averageCost == null ? '—' : Number(averageCost).toFixed(3)}</td><td className="px-2 py-2.5 text-right font-mono">{averageCost == null ? '—' : MONEY.format(quantity * Number(averageCost))}</td></tr>
          })}</tbody></table> : null}
          {tab === 'decisions' ? <EventRows rows={visibleDecisionEvents} symbolNames={symbolNames} /> : null}
          {tab === 'trades' ? <TradeTable orders={orders} fills={fills} instrumentLabel={instrumentLabel} /> : null}
          {tab === 'logs' ? <EventRows rows={logEvents} symbolNames={symbolNames} /> : null}
          {((tab === 'positions' && !positions.length) || (tab === 'decisions' && !visibleDecisionEvents.length) || (tab === 'trades' && !orders.length) || (tab === 'logs' && !logEvents.length)) ? <div className="py-12 text-center text-xs text-muted">暂无记录</div> : null}
        </section>
      </main>}
    </div>
    {showCreate ? <CreateAccountDialog strategyId={searchParams.get('strategy_id') ?? ''} backtestJobId={searchParams.get('backtest_job_id') ?? ''} onClose={() => { setShowCreate(false); setSearchParams({}) }} onCreated={created => { setShowCreate(false); setSearchParams({}); setSelectedId(created.id); void accountsQuery.refetch() }} /> : null}
    {renameTarget ? <RenameAccountDialog account={renameTarget} onClose={() => setRenameTarget(null)} onSaved={() => Promise.all([accountsQuery.refetch(), detailQuery.refetch()])} /> : null}
    {deleteTarget ? <Modal labelledBy="delete-paper-title" onClose={() => setDeleteTarget(null)} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl"><div className="p-4"><h2 id="delete-paper-title" className="text-sm font-semibold">删除「{deleteTarget.name}」？</h2><div className="mt-2 text-xs text-muted">账户 checkpoint 与事件流水将被删除。</div><div className="mt-5 flex justify-end gap-2"><button type="button" onClick={() => setDeleteTarget(null)} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted">取消</button><button type="button" disabled={pendingAction === 'delete'} onClick={() => void remove()} className="rounded-btn bg-danger px-3 py-1.5 text-xs font-medium text-white">确认删除</button></div></div></Modal> : null}
  </div>
}

function TradeTable({ orders, fills, instrumentLabel }: { orders: PaperOrder[]; fills: PaperFill[]; instrumentLabel: (symbol: unknown) => string }) {
  return <table className="w-full min-w-[980px] text-left text-xs">
    <thead className="text-[10px] text-muted"><tr><th className="px-2 py-2 font-medium">时间</th><th className="px-2 py-2 font-medium">标的</th><th className="px-2 py-2 font-medium">指令</th><th className="px-2 py-2 font-medium">成交</th><th className="px-2 py-2 font-medium">状态</th><th className="px-2 py-2 text-right font-medium">费用</th><th className="px-2 py-2 font-medium">说明</th></tr></thead>
    <tbody>{orders.slice().reverse().map(order => {
      const matchedFills = fills.filter(fill => fill.order_id === order.id)
      const filledQuantity = matchedFills.reduce((sum, fill) => sum + Number(fill.quantity), 0)
      const filledValue = matchedFills.reduce((sum, fill) => sum + Number(fill.value), 0)
      const fee = matchedFills.reduce((sum, fill) => sum + Number(fill.fee), 0)
      const averagePrice = filledQuantity > 0 ? filledValue / filledQuantity : null
      const side = orderDirection(order, matchedFills)
      const fillText = averagePrice == null ? '未成交' : `${side === 'buy' ? '买入' : '卖出'} ${filledQuantity.toLocaleString()} 股 @ ${averagePrice.toFixed(3)}`
      const explanation = order.reason || (order.status === 'filled' ? '正常成交' : order.status === 'pending' ? '等待可成交行情' : '未生成成交')
      return <tr key={order.id} className="border-t border-border">
        <td className="whitespace-nowrap px-2 py-2.5 text-muted">{formatTime(order.submitted_at)}</td>
        <td className="whitespace-nowrap px-2 py-2.5 font-mono">{instrumentLabel(order.symbol)}</td>
        <td className="whitespace-nowrap px-2 py-2.5">{orderInstruction(order)}</td>
        <td className={`whitespace-nowrap px-2 py-2.5 font-mono ${side === 'buy' ? 'text-bull' : side === 'sell' ? 'text-bear' : 'text-muted'}`}>{fillText}</td>
        <td className={`whitespace-nowrap px-2 py-2.5 ${orderStatusClass(order.status)}`}>{orderStatusLabel(order.status)}</td>
        <td className="whitespace-nowrap px-2 py-2.5 text-right font-mono">{fee > 0 ? MONEY.format(fee) : '—'}</td>
        <td className="px-2 py-2.5 text-muted">{explanation}</td>
      </tr>
    })}</tbody>
  </table>
}

function EventRows({ rows, symbolNames }: { rows: PaperEvent[]; symbolNames: Record<string, string> }) {
  return <div className="space-y-1">{rows.map(event => <div key={event.id} className="grid grid-cols-[150px_180px_minmax(0,1fr)] gap-2 border-b border-border px-2 py-2 text-xs max-sm:grid-cols-[110px_minmax(0,1fr)]"><span className="text-[10px] text-muted">{formatTime(event.timestamp)}</span><span className="truncate font-mono text-[10px] text-muted max-sm:hidden" title={event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : event.type}>{event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : event.type === 'log' ? event.level ?? 'INFO' : event.type}</span><span className={`whitespace-pre-wrap break-words ${event.type === 'error' || event.type === 'risk' || event.level === 'ERROR' ? 'text-danger' : event.type === 'rejected' || event.level === 'WARNING' ? 'text-warning' : ''}`}>{eventText(event, symbolNames)}</span></div>)}</div>
}
