import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, AlertTriangle, BarChart3, ChartCandlestick, Check, ChevronDown, Clock3, Database, RefreshCw, Settings2, Zap } from 'lucide-react'
import { DatePicker } from '@/components/DatePicker'
import { EmptyState } from '@/components/EmptyState'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { api, type LargeOrderEvidenceMode, type LargeOrderMarketSegment, type LargeOrderRow, type Preferences } from '@/lib/api'
import { cn } from '@/lib/cn'
import { QK } from '@/lib/queryKeys'

const WINDOWS = [15, 60, 300] as const
const MODES: Array<{ value: LargeOrderEvidenceMode; label: string }> = [
  { value: 'combined', label: '综合证据' },
  { value: 'execution', label: '主动成交' },
  { value: 'intent', label: '委托意图' },
]
const MARKET_SEGMENTS: Array<{ value: LargeOrderMarketSegment; label: string }> = [
  { value: 'main', label: '沪深主板' },
  { value: 'star', label: '科创板' },
  { value: 'chinext', label: '创业板' },
  { value: 'bse', label: '北交所' },
  { value: 'st', label: 'ST' },
]
const DEFAULT_MARKET_SEGMENTS: LargeOrderMarketSegment[] = ['main', 'star', 'chinext']

const money = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(Number(value))) return '--'
  const n = Number(value)
  if (Math.abs(n) >= 100_000_000) return `${(n / 100_000_000).toFixed(2)} 亿`
  if (Math.abs(n) >= 10_000) return `${(n / 10_000).toFixed(1)} 万`
  return n.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}
const pct = (value: number | null | undefined) => value == null ? '--' : `${(Number(value) * 100).toFixed(2)}%`
const age = (value: number | null | undefined) => value == null ? '--' : value < 1000 ? '刚刚' : `${Math.round(value / 1000)} 秒前`
const clock = (value: number | null | undefined) => value == null ? '--' : new Date(value * 1000).toLocaleTimeString('zh-CN', { hour12: false })

function Metric({ label, value, tone = 'text-foreground' }: { label: string; value: string; tone?: string }) {
  return <div><div className="text-[11px] text-muted">{label}</div><div className={cn('mt-1 truncate font-mono text-sm font-medium', tone)}>{value}</div></div>
}

function Evidence({ label, active, detail }: { label: string; active: boolean; detail: string }) {
  return <div className={cn('border-l-2 px-3 py-2', active ? 'border-accent bg-accent/5' : 'border-border bg-surface/30')}><div className="flex items-center gap-2 text-xs font-medium text-foreground"><span className={cn('h-1.5 w-1.5 rounded-full', active ? 'bg-accent' : 'bg-muted')} />{label}</div><div className="mt-1 text-[11px] leading-4 text-muted">{active ? detail : '暂无可用证据'}</div></div>
}

function OrderBook({ snapshot }: { snapshot: any }) {
  if (!snapshot) return <div className="grid h-56 place-items-center border border-border bg-surface/30 text-xs text-muted">暂无五档数据。需要批量五档能力且标的进入监控池。</div>
  const rows = Array.from({ length: 5 }, (_, index) => ({
    side: '卖', price: snapshot.ask_prices?.[4 - index], volume: snapshot.ask_volumes?.[4 - index], tone: 'text-bull',
  })).concat(Array.from({ length: 5 }, (_, index) => ({ side: '买', price: snapshot.bid_prices?.[index], volume: snapshot.bid_volumes?.[index], tone: 'text-danger' })))
  const max = Math.max(...rows.map((row) => Number(row.volume) || 0), 1)
  return <div className="border border-border bg-surface/30">
    <div className="flex items-center justify-between border-b border-border px-3 py-2 text-xs"><span>五档盘口</span><span className="text-muted">{age(snapshot.freshness_ms)} · 失衡 {pct(snapshot.book_imbalance)}</span></div>
    <div className="space-y-1 p-3">{rows.map((row, index) => <div key={`${row.side}-${index}`} className="relative grid grid-cols-[28px_1fr_80px] items-center gap-2 text-xs"><div className={cn('font-medium', row.tone)}>{row.side}{row.side === '卖' ? 5 - index : index + 1}</div><div className="relative h-6 overflow-hidden bg-elevated"><div className={cn('absolute inset-y-0 right-0 opacity-20', row.side === '卖' ? 'bg-bull' : 'bg-danger')} style={{ width: `${Math.min(100, ((Number(row.volume) || 0) / max) * 100)}%` }} /><span className="relative z-10 px-2 font-mono leading-6">{row.price == null ? '--' : Number(row.price).toFixed(2)}</span></div><div className="text-right font-mono text-muted">{money(row.volume)}</div></div>)}</div>
  </div>
}

function MarketScopeSelect({ selected, pending, error, onChange }: {
  selected: LargeOrderMarketSegment[]
  pending: boolean
  error: boolean
  onChange: (segments: LargeOrderMarketSegment[]) => void
}) {
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false)
    }
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', closeOnOutsideClick)
    document.addEventListener('keydown', closeOnEscape)
    return () => {
      document.removeEventListener('mousedown', closeOnOutsideClick)
      document.removeEventListener('keydown', closeOnEscape)
    }
  }, [open])

  const toggle = (segment: LargeOrderMarketSegment) => {
    const next = selected.includes(segment)
      ? selected.filter((item) => item !== segment)
      : MARKET_SEGMENTS.map((item) => item.value).filter((item) => item === segment || selected.includes(item))
    onChange(next)
  }

  return <div ref={rootRef} className="relative">
    <button
      type="button"
      aria-haspopup="menu"
      aria-expanded={open}
      onClick={() => setOpen((value) => !value)}
      className="inline-flex h-8 items-center gap-1.5 border border-border bg-surface/30 px-2.5 text-xs text-muted hover:text-foreground"
    >
      <span>市场范围 {selected.length}/{MARKET_SEGMENTS.length}</span>
      <ChevronDown className={cn('h-3.5 w-3.5 transition-transform', open && 'rotate-180')} />
    </button>
    {open && <div role="menu" className="absolute left-0 z-40 mt-1 w-40 border border-border bg-surface py-1 shadow-lg">
      {MARKET_SEGMENTS.map((item) => {
        const checked = selected.includes(item.value)
        return <button
          key={item.value}
          type="button"
          role="menuitemcheckbox"
          aria-checked={checked}
          disabled={pending}
          onClick={() => toggle(item.value)}
          className="flex h-8 w-full items-center gap-2 px-2.5 text-left text-xs text-secondary hover:bg-elevated hover:text-foreground disabled:cursor-wait disabled:opacity-60"
        >
          <span className={cn('grid h-3.5 w-3.5 shrink-0 place-items-center border', checked ? 'border-accent bg-accent text-white' : 'border-border')}>
            {checked && <Check className="h-3 w-3" strokeWidth={3} />}
          </span>
          {item.label}
        </button>
      })}
      {error && <div className="border-t border-border px-2.5 py-1.5 text-[11px] text-danger">保存失败，请重试</div>}
    </div>}
  </div>
}

export function LargeOrders() {
  const qc = useQueryClient()
  const [window, setWindow] = useState<number>(60)
  const [scope, setScope] = useState<'all' | 'watchlist'>('all')
  const [mode, setMode] = useState<LargeOrderEvidenceMode>('combined')
  const [selected, setSelected] = useState<LargeOrderRow | null>(null)
  const [preview, setPreview] = useState<{ symbol: string; name?: string } | null>(null)
  const [auditDate, setAuditDate] = useState('')
  const preferences = useQuery({ queryKey: QK.preferences, queryFn: api.preferences, staleTime: 60000 })
  const saveFilters = useMutation({
    mutationFn: (market_segments: LargeOrderMarketSegment[]) => api.updateLargeOrdersPreferences({ market_segments }),
    onSuccess: (response) => {
      qc.setQueryData<Preferences>(QK.preferences, (current) => current ? { ...current, large_orders: response.large_orders } : current)
      qc.invalidateQueries({ queryKey: QK.preferences })
      qc.invalidateQueries({ queryKey: QK.largeOrders })
    },
  })
  const status = useQuery({ queryKey: [...QK.largeOrders, 'status'], queryFn: api.largeOrdersStatus, refetchInterval: 15000, placeholderData: (p) => p })
  const ranking = useQuery({ queryKey: [...QK.largeOrders, 'ranking', window, scope, mode], queryFn: () => api.largeOrdersRanking(window, scope, mode), refetchInterval: 15000, placeholderData: (p) => p })
  const dates = useQuery({ queryKey: [...QK.largeOrders, 'dates'], queryFn: () => api.largeOrdersDates(30), staleTime: 60000 })
  const rows = ranking.data?.rows ?? []
  useEffect(() => {
    if (!rows.length) {
      if (selected) setSelected(null)
      return
    }
    if (!selected || !rows.some((row) => row.symbol === selected.symbol)) setSelected(rows[0])
  }, [rows, selected])
  useEffect(() => { if (!auditDate && dates.data?.dates?.[0]) setAuditDate(dates.data.dates[0]) }, [auditDate, dates.data?.dates])
  const analysis = useQuery({ queryKey: QK.largeOrdersAnalysis(selected?.symbol ?? ''), queryFn: () => api.largeOrdersAnalysis(selected!.symbol), enabled: Boolean(selected?.symbol), refetchInterval: 15000, placeholderData: (p) => p })
  const history = useQuery({ queryKey: [...QK.largeOrders, 'history', auditDate, selected?.symbol, 'combined'], queryFn: () => api.largeOrdersHistory({ date: auditDate, symbol: selected?.symbol, mode: 'combined', limit: 300, order: 'desc' }), enabled: Boolean(auditDate), placeholderData: (p) => p })
  const detail = analysis.data
  const selectedRow = detail?.ranking ?? selected
  const flowBars = useMemo(() => detail?.tape.timeline?.slice(-24) ?? [], [detail?.tape.timeline])
  const marketSegments = preferences.data?.large_orders?.market_segments ?? DEFAULT_MARKET_SEGMENTS

  return <div className="space-y-4">
    <PageHeader title="实时大单" subtitle="发现盘中资金异动，按成交、意图和盘口证据研判，不将代理数据当作真实资金流。" right={<button type="button" title="刷新实时大单" onClick={() => ranking.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn border border-border text-muted hover:bg-elevated hover:text-foreground"><RefreshCw className="h-4 w-4" /></button>} />
    <div className="grid gap-2 border-y border-border py-2 text-xs text-muted md:grid-cols-4">
      <div className="flex items-center gap-2"><Activity className="h-3.5 w-3.5 text-accent" />{status.data?.market_phase ?? '市场状态未知'} <span className={cn(status.data?.stale ? 'text-warning' : 'text-bull')}>{status.data?.stale ? '数据滞后' : '实时'}</span></div>
      <div><span className="text-foreground">监控池</span> {status.data?.coverage_count ?? 0} 只 · 候选 {status.data?.candidate_count ?? 0} 只</div>
      <div><span className="text-foreground">精确证据</span> {status.data?.precise_count ?? 0} 只 · 五档按能力采样</div>
      <div className="flex items-center gap-1 md:justify-end"><Database className="h-3.5 w-3.5" />记录 {status.data?.storage?.written_rows ?? 0} 条</div>
    </div>
    <div className="flex flex-wrap items-center gap-2">
      <div className="flex border border-border bg-surface/30">{WINDOWS.map((item) => <button key={item} type="button" onClick={() => setWindow(item)} className={cn('px-3 py-1.5 text-xs', window === item ? 'bg-accent text-white' : 'text-muted hover:text-foreground')}>{item < 60 ? `${item}秒` : item === 60 ? '1分钟' : '5分钟'}</button>)}</div>
      <div className="flex border border-border bg-surface/30">{(['all', 'watchlist'] as const).map((item) => <button key={item} type="button" onClick={() => setScope(item)} className={cn('px-3 py-1.5 text-xs', scope === item ? 'bg-elevated text-foreground' : 'text-muted hover:text-foreground')}>{item === 'all' ? '异动候选' : '自选股'}</button>)}</div>
      <select value={mode} onChange={(event) => setMode(event.target.value as LargeOrderEvidenceMode)} className="h-8 border border-border bg-surface/30 px-2 text-xs text-foreground outline-none">{MODES.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select>
      <MarketScopeSelect selected={marketSegments} pending={saveFilters.isPending} error={saveFilters.isError} onChange={(segments) => saveFilters.mutate(segments)} />
      <span className="ml-auto text-[11px] text-muted">更新时间 {status.data?.last_updated_ms ? new Date(status.data.last_updated_ms).toLocaleTimeString('zh-CN', { hour12: false }) : '--'}</span>
    </div>
    <div className="grid min-h-[560px] gap-3 xl:grid-cols-[minmax(420px,1.15fr)_minmax(360px,1fr)_300px]">
      <section className="min-w-0 overflow-hidden border border-border bg-surface/30"><div className="flex items-center justify-between border-b border-border px-3 py-2.5 text-sm font-medium"><span className="flex items-center gap-2"><Zap className="h-4 w-4 text-warning" />异动扫描</span><span className="text-xs text-muted">{rows.length} 只</span></div>{ranking.isLoading && !ranking.data ? <div className="grid h-64 place-items-center text-xs text-muted">加载候选中…</div> : rows.length === 0 ? <EmptyState icon={BarChart3} title="暂无大单候选" hint="等待实时行情增量，或检查行情与数据源授权。" /> : <div className="max-h-[520px] overflow-y-auto">{rows.map((row) => <button key={row.symbol} type="button" onClick={() => setSelected(row)} className={cn('grid w-full grid-cols-[1fr_76px_90px] gap-2 border-b border-border/70 px-3 py-3 text-left transition-colors hover:bg-elevated/60', selected?.symbol === row.symbol && 'bg-elevated')}><div className="min-w-0"><div className="truncate font-medium text-foreground">{row.name}</div><div className="mt-1 font-mono text-[11px] text-muted">{row.symbol} · {row.data_quality === 'precise' ? '成交证据' : '快照推断'}</div><div className="mt-2 h-1 overflow-hidden bg-border"><div className={cn('h-full', row.net_buy_amount >= 0 ? 'bg-danger' : 'bg-bull')} style={{ width: `${Math.min(100, Math.max(4, row.score))}%` }} /></div></div><div className="text-right"><div className={cn('font-mono text-lg font-semibold', row.score >= 75 ? 'text-warning' : 'text-foreground')}>{row.score.toFixed(0)}</div><div className="text-[11px] text-muted">评分</div></div><div className="text-right"><div className={cn('font-mono font-medium', row.net_buy_amount >= 0 ? 'text-danger' : 'text-bull')}>{money(row.net_buy_amount)}</div><div className="mt-1 text-[11px] text-muted">净买额</div><div className="mt-1 text-[11px] text-muted">{age(row.freshness_ms)}</div></div></button>)}</div>}</section>
      <section className="min-w-0 space-y-3 border border-border bg-surface/30 p-3"><div className="flex items-start justify-between border-b border-border pb-3"><div><div className="font-mono text-xs text-muted">{selectedRow?.symbol ?? '--'}</div><div className="mt-1 text-lg font-semibold text-foreground">{selectedRow?.name ?? '选择候选查看盘口'}</div></div>{selectedRow && <button type="button" title="打开个股分析" onClick={() => setPreview({ symbol: selectedRow.symbol, name: selectedRow.name })} className="inline-flex h-8 w-8 items-center justify-center rounded-btn border border-border text-muted hover:bg-elevated hover:text-foreground"><ChartCandlestick className="h-4 w-4" /></button>}</div>{selectedRow ? <><div className="grid grid-cols-2 gap-3"><Metric label={`${window < 60 ? `${window}秒` : window === 60 ? '1分钟' : '5分钟'}净买额`} value={money(selectedRow.net_buy_amount)} tone={selectedRow.net_buy_amount >= 0 ? 'text-danger' : 'text-bull'} /><Metric label="主动买入占比" value={pct(selectedRow.buy_ratio)} /><Metric label="成交额突增" value={selectedRow.zscore.toFixed(2)} /><Metric label="盘口失衡" value={pct(selectedRow.book_imbalance)} /><Metric label="撤单率" value={pct(selectedRow.cancel_rate)} /><Metric label="距涨停" value={pct(selectedRow.limit_up_gap_pct)} /></div><div className="border-t border-border pt-3"><div className="mb-2 flex items-center gap-2 text-xs font-medium"><Clock3 className="h-3.5 w-3.5 text-accent" />资金节奏</div>{flowBars.length ? <div className="flex h-24 items-end gap-1 border-b border-border px-1">{flowBars.map((point, index) => <div key={`${point.ts}-${index}`} className="group relative flex-1" title={`${clock(point.ts)} ${money(point.amount)}`}><div className={cn('min-h-1 w-full', point.buy >= point.sell ? 'bg-danger/70' : 'bg-bull/70')} style={{ height: `${Math.min(100, Math.max(5, (point.amount / Math.max(selectedRow.large_threshold, 1)) * 25))}%` }} /></div>)}</div> : <div className="grid h-24 place-items-center text-xs text-muted">暂无资金时间线</div>}</div><OrderBook snapshot={detail?.orderbook} /></> : <div className="grid h-[440px] place-items-center text-xs text-muted">从左侧选择股票</div>}</section>
      <aside className="min-w-0 space-y-3 border border-border bg-surface/30 p-3"><div className="flex items-center gap-2 border-b border-border pb-2 text-sm font-medium"><Settings2 className="h-4 w-4 text-accent" />证据与结论</div>{selectedRow ? <><div className="space-y-2"><Evidence label="推断资金流" active={Boolean(detail?.evidence.proxy)} detail="由累计成交额增量与价格方向推断，仅作候选筛选。" /><Evidence label="主动成交" active={Boolean(detail?.evidence.execution)} detail="开盘啦逐笔主动买卖证据，优先于快照推断。" /><Evidence label="委托意图" active={Boolean(detail?.evidence.intent)} detail={`委托/撤单 ${selectedRow.intent_count ?? 0} 笔，撤单率 ${pct(selectedRow.cancel_rate)}。`} /><Evidence label="五档盘口" active={Boolean(detail?.evidence.orderbook)} detail={detail?.orderbook ? `盘口失衡 ${pct(detail.orderbook.book_imbalance)} · OFI ${money(detail.orderbook.ofi)}` : detail?.degraded_reason ?? '等待采样'} /></div><div className="border-t border-border pt-3 text-xs leading-5 text-secondary">{selectedRow.explanation}<br /><span className="text-muted">高置信度需要成交强度、价格确认、盘口共振且数据不过期。</span></div>{detail?.degraded_reason && <div className="flex gap-2 border border-warning/25 bg-warning/10 px-2.5 py-2 text-[11px] text-warning"><AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />{detail.degraded_reason}</div>}</> : <div className="py-10 text-center text-xs text-muted">暂无结论</div>}</aside>
    </div>
    <section className="border border-border bg-surface/30"><div className="flex flex-wrap items-center gap-2 border-b border-border px-3 py-2.5"><div className="flex items-center gap-2 text-sm font-medium"><Clock3 className="h-4 w-4 text-accent" />盘中回放</div><DatePicker value={auditDate} onChange={setAuditDate} min={dates.data?.dates?.at(-1)} max={dates.data?.dates?.[0]} buttonClassName="font-mono" /><span className="text-xs text-muted">{selected?.symbol ?? '全部标的'} · {history.data?.count ?? 0} 条</span></div>{history.data?.rows?.length ? <div className="max-h-64 overflow-y-auto">{history.data.rows.map((event) => <div key={`${event.event_kind}:${event.event_id}`} className="grid grid-cols-[72px_92px_1fr_100px] gap-2 border-b border-border/60 px-3 py-2 text-xs"><span className="font-mono text-muted">{clock(event.event_ts_ms / 1000)}</span><span className="text-muted">{event.event_kind === 'orderbook_snapshot' ? '五档快照' : event.event_kind === 'kaipanla_trade' ? '主动成交' : event.event_kind === 'kaipanla_intent' ? '委托意图' : '资金推断'}</span><span className="truncate text-secondary">{event.name || event.symbol} · {event.amount != null ? money(event.amount) : `失衡 ${pct(event.book_imbalance)}`}</span><span className="text-right font-mono text-muted">{event.symbol}</span></div>)}</div> : <div className="px-3 py-10 text-center text-xs text-muted">当前日期暂无可回放事件</div>}</section>
    {preview && <StockPreviewDialog symbol={preview.symbol} name={preview.name} onClose={() => setPreview(null)} />}
  </div>
}
