import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  CirclePause,
  CirclePlay,
  Clock3,
  FileText,
  Gauge,
  ListOrdered,
  Plus,
  Radio,
  RefreshCw,
  ShieldAlert,
  Square,
  Trash2,
  WalletCards,
  Wifi,
} from 'lucide-react'
import type { EChartsOption } from 'echarts'
import { api, type CreatePaperAccount, type PaperAccount, type PaperEvent, type PaperMarketMode } from '@/lib/api'
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

const MODE_LABEL: Record<string, string> = {
  bar_1m: '1分钟K线',
  bar_1d: '日K',
  poll_3s: '3秒行情',
  websocket: 'WebSocket',
  bar_5m: '5分钟K线（旧账户）',
  bar_30m: '30分钟K线（旧账户）',
}

type DetailTab = 'positions' | 'signals' | 'orders' | 'fills' | 'logs'

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

function eventText(event: PaperEvent, symbolNames: Record<string, string> = {}) {
  const symbol = event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : ''
  if (event.type === 'fill') return `${event.side === 'buy' ? '买入' : '卖出'} ${symbol} ${Number(event.quantity ?? 0).toLocaleString()} 股 @ ${Number(event.price ?? 0).toFixed(3)}`
  if (event.type === 'rejected') return `${symbol || '委托'}：${event.reason ?? '已拒绝'}`
  if (event.type === 'risk') return String(event.reason ?? '风控锁定')
  return String(event.message ?? event.reason ?? event.type)
}

function formatTime(value?: string) {
  if (!value) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString('zh-CN', { hour12: false })
}

function ReturnChart({ account }: { account: PaperAccount }) {
  const theme = useChartTheme()
  const rows = useMemo(() => account.account?.equity_curve ?? [], [account.account?.equity_curve])
  const option = useMemo<EChartsOption | null>(() => {
    if (!rows.length) return null
    const returns = rows.map(row => (Number(row.nav) - 1) * 100)
    const isPositive = (returns.at(-1) ?? 0) >= 0
    const lineColor = isPositive ? '#f04438' : '#12b76a'
    const areaColor = isPositive ? 'rgba(240,68,56,0.08)' : 'rgba(18,183,106,0.08)'
    return {
      animation: false,
      grid: { left: 62, right: 18, top: 20, bottom: 34 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: theme.tooltipBg,
        borderColor: theme.tooltipBorder,
        textStyle: { color: theme.tooltipText, fontSize: 11 },
        formatter: (params: any) => {
          const item = Array.isArray(params) ? params[0] : params
          const value = Number(item?.value ?? 0)
          return `<div>${String(item?.axisValue ?? '')}</div><div>累计收益 ${value >= 0 ? '+' : ''}${value.toFixed(2)}%</div>`
        },
      },
      xAxis: {
        type: 'category',
        data: rows.map(row => formatTime(row.timestamp)),
        boundaryGap: false,
        axisLabel: { color: theme.text, fontSize: 10, hideOverlap: true },
        axisLine: { lineStyle: { color: theme.border } },
      },
      yAxis: {
        type: 'value', scale: true,
        min: (range: { min: number }) => Math.min(0, range.min),
        axisLabel: { color: theme.text, fontSize: 10, formatter: (value: number) => `${value.toFixed(1)}%` },
        splitLine: { lineStyle: { color: theme.grid } },
      },
      series: [{ type: 'line', name: '累计收益', data: returns, showSymbol: false, lineStyle: { width: 1.5, color: lineColor }, itemStyle: { color: lineColor }, areaStyle: { color: areaColor } }],
    }
  }, [rows, theme])
  const ref = useECharts(option, [option])
  return <div className="relative h-52 w-full">
    <div ref={ref} className="h-full w-full" />
    {!rows.length ? <div className="absolute inset-0 grid place-items-center text-xs text-muted">等待首个收益采样</div> : null}
  </div>
}

function CreateAccountDialog({ strategyId, onClose, onCreated }: { strategyId: string; onClose: () => void; onCreated: (account: PaperAccount) => void }) {
  const strategies = useQuery({ queryKey: ['free-strategies'], queryFn: api.freeStrategies })
  const paperStatus = useQuery({ queryKey: ['free-paper-status'], queryFn: api.paperStatus })
  const list = strategies.data?.strategies ?? []
  const initialStrategy = strategyId || list[0]?.id || ''
  const [form, setForm] = useState<CreatePaperAccount>({
    name: '量化策略模拟账户',
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
      name: current.name === '量化策略模拟账户' ? `${selected.name} · 模拟盘` : current.name,
      asset_type: saved.asset_type === 'stock' ? 'stock' : 'etf',
      initial_capital: Number(saved.initial_capital ?? current.initial_capital),
      benchmark_symbol: String(saved.benchmark_symbol ?? current.benchmark_symbol),
      settlement: saved.settlement === 't0' ? 't0' : 't1',
      fill_policy: saved.fill_policy === 'close' ? 'close' : 'next_open',
    }))
  }, [form.strategy_id, list])

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
        <label className="col-span-2 max-sm:col-span-1">账户名称<input className={`${INPUT} mt-1`} maxLength={120} value={form.name} onChange={event => setForm({ ...form, name: event.target.value })} /></label>
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
  const [showCreate, setShowCreate] = useState(searchParams.get('create') === '1')
  const [pendingAction, setPendingAction] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<PaperAccount | null>(null)
  const notifiedSequence = useRef(0)
  const accountsQuery = useQuery({ queryKey: ['free-paper-accounts'], queryFn: api.paperAccounts, refetchInterval: 10_000 })
  const statusQuery = useQuery({ queryKey: ['free-paper-status'], queryFn: api.paperStatus, refetchInterval: 3_000 })
  const detailQuery = useQuery({ queryKey: ['free-paper-account', selectedId], queryFn: () => api.paperAccount(selectedId), enabled: Boolean(selectedId), refetchInterval: selectedId ? 5_000 : false })
  const eventsQuery = useQuery({ queryKey: ['free-paper-events', selectedId], queryFn: () => api.paperEvents(selectedId), enabled: Boolean(selectedId) })
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
    return [...symbols].sort()
  }, [account, events])
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
      await Promise.all([accountsQuery.refetch(), detailQuery.refetch(), statusQuery.refetch(), eventsQuery.refetch()])
    } catch {
      return
    } finally {
      setPendingAction('')
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

  const snapshot = account?.account
  const positions = Object.entries(snapshot?.positions ?? account?.positions ?? {}).filter(([, quantity]) => quantity > 0)
  const orders = snapshot?.orders ?? []
  const fills = snapshot?.fills ?? []
  const signalEvents = events.filter(event => ['order', 'rejected', 'risk'].includes(event.type))
  const logEvents = events.filter(event => ['log', 'error', 'market_gap', 'start', 'pause', 'stop'].includes(event.type))
  const status = statusQuery.data

  return <div className="flex h-full min-h-0 flex-col max-md:fixed max-md:inset-0 max-md:z-[10000] max-md:bg-base">
    <PageHeader title="模拟盘" subtitle={`${status?.running_accounts ?? 0} 个账户运行中`} right={<button type="button" onClick={() => setShowCreate(true)} className="inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white"><Plus className="h-3.5 w-3.5" />创建账户</button>} />
    <div className="grid min-h-0 flex-1 grid-cols-[260px_minmax(0,1fr)] max-md:grid-cols-1">
      <aside className={`min-h-0 overflow-y-auto border-r border-border p-2.5 max-md:border-b max-md:border-r-0 ${account ? 'max-md:max-h-44' : ''}`}>
        <div className="space-y-1.5">
          {accounts.map(item => <button key={item.id} type="button" onClick={() => setSelectedId(item.id)} className={`w-full rounded border px-2.5 py-2.5 text-left ${selectedId === item.id ? 'border-accent bg-accent/10' : 'border-border hover:border-accent/50 hover:bg-elevated'}`}>
            <div className="flex items-center justify-between gap-2"><span className="truncate text-xs font-medium">{item.name}</span><span className={`shrink-0 text-[10px] ${statusClass(item.status)}`}>{statusLabel(item.status)}</span></div>
            <div className="mt-1.5 flex items-center justify-between gap-2 text-[10px] text-muted"><span>{MODE_LABEL[item.market_mode] ?? item.market_mode}</span><span className="tabular-nums">{Number(item.return_pct ?? 0) >= 0 ? '+' : ''}{Number(item.return_pct ?? 0).toFixed(2)}%</span></div>
          </button>)}
          {!accounts.length && !accountsQuery.isLoading ? <div className="py-10 text-center text-xs text-muted">暂无模拟账户</div> : null}
        </div>
      </aside>
      {!account ? <EmptyState icon={WalletCards} title="选择或创建模拟账户" /> : <main className="min-h-0 overflow-y-auto">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-4 py-2.5">
          <div className="min-w-0"><div className="flex items-center gap-2"><h2 className="truncate text-sm font-semibold">{account.name}</h2><span className={`text-[11px] ${statusClass(account.status)}`}>{statusLabel(account.status)}</span></div><div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-muted"><span>{MODE_LABEL[account.market_mode]}</span><span>策略 r{account.source_revision}</span><span className="font-mono">{account.source_hash?.slice(0, 8)}</span><span>{account.execution_mode === 'scheduled' ? '定时执行' : account.execution_mode === 'quote' ? '报价驱动' : 'K线驱动'}</span></div></div>
          <div className="flex items-center gap-1">
            <button type="button" title={account.status === 'paused' ? '恢复' : '启动'} disabled={Boolean(pendingAction) || account.status === 'running'} onClick={() => void action(account.status === 'paused' ? 'resume' : 'start')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-border text-muted hover:border-accent hover:text-accent disabled:opacity-35"><CirclePlay className="h-4 w-4" /></button>
            <button type="button" title="暂停" disabled={Boolean(pendingAction) || account.status !== 'running'} onClick={() => void action('pause')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-border text-muted hover:border-warning hover:text-warning disabled:opacity-35"><CirclePause className="h-4 w-4" /></button>
            <button type="button" title="停止" disabled={Boolean(pendingAction) || account.status === 'stopped'} onClick={() => void action('stop')} className="inline-flex h-8 w-8 items-center justify-center rounded border border-border text-muted hover:border-danger hover:text-danger disabled:opacity-35"><Square className="h-4 w-4" /></button>
            <button type="button" title="刷新" onClick={() => void Promise.all([detailQuery.refetch(), eventsQuery.refetch()])} className="inline-flex h-8 w-8 items-center justify-center rounded border border-border text-muted hover:text-foreground"><RefreshCw className="h-4 w-4" /></button>
            <button type="button" title={account.status === 'stopped' ? '删除' : '请先停止账户'} disabled={Boolean(pendingAction) || account.status !== 'stopped'} onClick={() => setDeleteTarget(account)} className="inline-flex h-8 w-8 items-center justify-center rounded border border-border text-muted hover:border-danger hover:text-danger disabled:opacity-35"><Trash2 className="h-4 w-4" /></button>
          </div>
        </div>

        {account.last_error ? <div className="mx-4 mt-3 flex gap-2 rounded border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger"><AlertTriangle className="h-4 w-4 shrink-0" />{account.last_error}</div> : null}
        {(account.risk_status?.daily_loss_locked || account.risk_status?.drawdown_locked) ? <div className="mx-4 mt-3 flex items-center gap-2 rounded border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning"><ShieldAlert className="h-4 w-4" /><span className="flex-1">{account.risk_status.reason ?? '风控锁定'}</span>{account.risk_status.drawdown_locked ? <button type="button" onClick={() => void action('unlock-risk')} className="rounded border border-warning/50 px-2 py-1 text-[11px]">确认恢复</button> : null}</div> : null}

        <section className="grid grid-cols-4 border-b border-border max-lg:grid-cols-2">
          {[['总资产', MONEY.format(account.equity ?? snapshot?.cash ?? account.cash), Activity], ['可用现金', MONEY.format(snapshot?.cash ?? account.cash), WalletCards], ['累计收益', `${Number(account.return_pct ?? 0) >= 0 ? '+' : ''}${Number(account.return_pct ?? 0).toFixed(2)}%`, Gauge], ['当前回撤', `${Number(account.drawdown_pct ?? 0).toFixed(2)}%`, ShieldAlert]].map(([label, value, Icon], index) => <div key={String(label)} className={`px-4 py-3 ${index < 3 ? 'border-r border-border max-lg:odd:border-r' : ''} max-lg:border-b`}><div className="flex items-center gap-1.5 text-[10px] text-muted"><Icon className="h-3.5 w-3.5" />{label as string}</div><div className="mt-1.5 font-mono text-[16px] tabular-nums">{value as string}</div></div>)}
        </section>

        <section className="grid grid-cols-[minmax(0,1fr)_260px] border-b border-border max-lg:grid-cols-1">
          <div className="min-w-0 border-r border-border px-3 py-2 max-lg:border-b max-lg:border-r-0"><div className="text-[11px] font-medium">收益曲线</div><ReturnChart account={account} /></div>
          <div className="p-3 text-[11px]">
            <div className="mb-3 font-medium">行情状态</div>
            <div className="space-y-2 text-muted">
              <div className="flex items-center justify-between gap-2"><span className="inline-flex items-center gap-1.5"><Clock3 className="h-3.5 w-3.5" />最后行情</span><span className="truncate font-mono text-[10px] text-foreground">{formatTime(account.last_quote ?? account.last_bar ?? status?.last_quote_at ?? undefined)}</span></div>
              <div className="flex items-center justify-between"><span className="inline-flex items-center gap-1.5"><Gauge className="h-3.5 w-3.5" />轮询耗时</span><span className="font-mono text-foreground">{status?.poll_3s.actual_fetch_ms != null ? `${status.poll_3s.actual_fetch_ms} ms` : '—'}</span></div>
              <div className="flex items-center justify-between"><span className="inline-flex items-center gap-1.5"><Wifi className="h-3.5 w-3.5" />WebSocket</span><span className="text-foreground">{status?.websocket.status ?? 'disconnected'} · {status?.websocket.symbols ?? 0}/{status?.websocket.capacity ?? 100}</span></div>
              <div className="flex items-center justify-between" title="策略当前监听的标的数量，由每个策略独立维护"><span className="inline-flex items-center gap-1.5"><Radio className="h-3.5 w-3.5" />当前订阅池</span><span className="font-mono text-foreground">{account.universe?.length ?? 0}</span></div>
            </div>
          </div>
        </section>

        <div className="flex border-b border-border px-3 pt-2">
          {([['positions', '持仓', WalletCards], ['signals', '信号', Activity], ['orders', '委托', ListOrdered], ['fills', '成交', Gauge], ['logs', '日志', FileText]] as const).map(([value, label, Icon]) => <button key={value} type="button" onClick={() => setTab(value)} className={`inline-flex h-8 items-center gap-1.5 border-b-2 px-3 text-xs ${tab === value ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><Icon className="h-3.5 w-3.5" />{label}</button>)}
        </div>
        <section className="min-h-56 overflow-x-auto p-3">
          {tab === 'positions' ? <table className="w-full min-w-[560px] text-left text-xs"><thead className="text-[10px] text-muted"><tr><th className="px-2 py-2 font-medium">标的</th><th className="px-2 py-2 text-right font-medium">数量</th><th className="px-2 py-2 text-right font-medium">成本</th><th className="px-2 py-2 text-right font-medium">成本市值</th></tr></thead><tbody>{positions.map(([symbol, quantity]) => <tr key={symbol} className="border-t border-border"><td className="px-2 py-2.5 font-mono">{instrumentLabel(symbol)}</td><td className="px-2 py-2.5 text-right font-mono">{quantity.toLocaleString()}</td><td className="px-2 py-2.5 text-right font-mono">{Number(snapshot?.avg_cost?.[symbol] ?? 0).toFixed(3)}</td><td className="px-2 py-2.5 text-right font-mono">{MONEY.format(quantity * Number(snapshot?.avg_cost?.[symbol] ?? 0))}</td></tr>)}</tbody></table> : null}
          {tab === 'signals' ? <EventRows rows={signalEvents} symbolNames={symbolNames} /> : null}
          {tab === 'orders' ? <table className="w-full min-w-[680px] text-left text-xs"><thead className="text-[10px] text-muted"><tr><th className="px-2 py-2 font-medium">时间</th><th className="px-2 py-2 font-medium">标的</th><th className="px-2 py-2 font-medium">方向</th><th className="px-2 py-2 text-right font-medium">数量</th><th className="px-2 py-2 font-medium">状态</th><th className="px-2 py-2 font-medium">原因</th></tr></thead><tbody>{orders.slice().reverse().map(order => <tr key={order.id} className="border-t border-border"><td className="px-2 py-2.5 text-muted">{formatTime(order.submitted_at)}</td><td className="px-2 py-2.5 font-mono">{instrumentLabel(order.symbol)}</td><td className="px-2 py-2.5">{order.side}</td><td className="px-2 py-2.5 text-right font-mono">{order.quantity ?? '—'}</td><td className="px-2 py-2.5">{order.status}</td><td className="px-2 py-2.5 text-muted">{order.reason || '—'}</td></tr>)}</tbody></table> : null}
          {tab === 'fills' ? <table className="w-full min-w-[680px] text-left text-xs"><thead className="text-[10px] text-muted"><tr><th className="px-2 py-2 font-medium">时间</th><th className="px-2 py-2 font-medium">标的</th><th className="px-2 py-2 font-medium">方向</th><th className="px-2 py-2 text-right font-medium">数量</th><th className="px-2 py-2 text-right font-medium">价格</th><th className="px-2 py-2 text-right font-medium">费用</th></tr></thead><tbody>{fills.slice().reverse().map((fill, index) => <tr key={`${fill.order_id}-${index}`} className="border-t border-border"><td className="px-2 py-2.5 text-muted">{formatTime(fill.timestamp)}</td><td className="px-2 py-2.5 font-mono">{instrumentLabel(fill.symbol)}</td><td className="px-2 py-2.5">{fill.side}</td><td className="px-2 py-2.5 text-right font-mono">{fill.quantity.toLocaleString()}</td><td className="px-2 py-2.5 text-right font-mono">{fill.price.toFixed(3)}</td><td className="px-2 py-2.5 text-right font-mono">{fill.fee.toFixed(2)}</td></tr>)}</tbody></table> : null}
          {tab === 'logs' ? <EventRows rows={logEvents} symbolNames={symbolNames} /> : null}
          {((tab === 'positions' && !positions.length) || (tab === 'signals' && !signalEvents.length) || (tab === 'orders' && !orders.length) || (tab === 'fills' && !fills.length) || (tab === 'logs' && !logEvents.length)) ? <div className="py-12 text-center text-xs text-muted">暂无记录</div> : null}
        </section>
      </main>}
    </div>
    {showCreate ? <CreateAccountDialog strategyId={searchParams.get('strategy_id') ?? ''} onClose={() => { setShowCreate(false); setSearchParams({}) }} onCreated={created => { setShowCreate(false); setSearchParams({}); setSelectedId(created.id); void accountsQuery.refetch() }} /> : null}
    {deleteTarget ? <Modal labelledBy="delete-paper-title" onClose={() => setDeleteTarget(null)} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl"><div className="p-4"><h2 id="delete-paper-title" className="text-sm font-semibold">删除「{deleteTarget.name}」？</h2><div className="mt-2 text-xs text-muted">账户 checkpoint 与事件流水将被删除。</div><div className="mt-5 flex justify-end gap-2"><button type="button" onClick={() => setDeleteTarget(null)} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted">取消</button><button type="button" disabled={pendingAction === 'delete'} onClick={() => void remove()} className="rounded-btn bg-danger px-3 py-1.5 text-xs font-medium text-white">确认删除</button></div></div></Modal> : null}
  </div>
}

function EventRows({ rows, symbolNames }: { rows: PaperEvent[]; symbolNames: Record<string, string> }) {
  return <div className="space-y-1">{rows.map(event => <div key={event.id} className="grid grid-cols-[150px_180px_minmax(0,1fr)] gap-2 border-b border-border px-2 py-2 text-xs max-sm:grid-cols-[110px_minmax(0,1fr)]"><span className="text-[10px] text-muted">{formatTime(event.timestamp)}</span><span className="truncate font-mono text-[10px] text-muted max-sm:hidden" title={event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : event.type}>{event.symbol ? formatInstrumentLabel(event.symbol, symbolNames[event.symbol]) : event.type}</span><span className={event.type === 'error' || event.type === 'risk' ? 'text-danger' : event.type === 'rejected' ? 'text-warning' : ''}>{eventText(event, symbolNames)}</span></div>)}</div>
}
