import { useEffect, useMemo, useRef, useState } from 'react'
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useVirtualizer } from '@tanstack/react-virtual'
import {
  AlertTriangle,
  BarChart3,
  ChartCandlestick,
  CheckCircle2,
  Clock3,
  Database,
  Loader2,
  RefreshCw,
  Settings2,
  XCircle,
  Zap,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { DatePicker } from '@/components/DatePicker'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import {
  api,
  type LargeOrderEvidenceMode,
  type LargeOrderHistoryEvent,
  type LargeOrderReconciliationRow,
  type LargeOrderRow,
} from '@/lib/api'
import { cn } from '@/lib/cn'
import { QK } from '@/lib/queryKeys'

const WINDOWS = [
  { value: 15, label: '15 秒' },
  { value: 60, label: '1 分钟' },
  { value: 300, label: '5 分钟' },
] as const

const MODES: Array<{ value: LargeOrderEvidenceMode; label: string }> = [
  { value: 'combined', label: '综合' },
  { value: 'execution', label: '执行成交' },
  { value: 'intent', label: '委托意图' },
]

function money(value: number | null | undefined) {
  if (value == null) return '--'
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  if (Math.abs(n) >= 100_000_000) return `${(n / 100_000_000).toFixed(2)} 亿`
  if (Math.abs(n) >= 10_000) return `${(n / 10_000).toFixed(1)} 万`
  return n.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function pct(value: number | null | undefined) {
  if (value == null) return '--'
  const n = Number(value)
  return Number.isFinite(n) ? `${(n * 100).toFixed(2)}%` : '--'
}

function freshness(value: number | null | undefined) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  if (n < 1000) return '刚刚'
  return `${Math.round(n / 1000)} 秒前`
}

function clockTime(value: number | null | undefined) {
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  return new Date(n * 1000).toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

function bucketTime(value: number) {
  return new Date(value).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false })
}

function dateTimeMs(date: string, time: string | undefined, endOfMinute = false) {
  if (!date || !time) return undefined
  const [hour, minute] = time.split(':').map(Number)
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return undefined
  return (
    new Date(`${date}T${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`).getTime() +
    (endOfMinute ? 59_999 : 0)
  )
}

function todayDate() {
  const now = new Date()
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
}

function confidenceMeta(value: string) {
  if (value === 'high') return { label: '高置信度', cls: 'text-bull bg-bull/10 border-bull/25' }
  if (value === 'medium') return { label: '中置信度', cls: 'text-warning bg-warning/10 border-warning/25' }
  return { label: '低置信度', cls: 'text-muted bg-elevated border-border' }
}

function statusMeta(value: LargeOrderReconciliationRow['status']) {
  if (value === 'matched') return { label: '已匹配', cls: 'text-bull bg-bull/10 border-bull/25', icon: CheckCircle2 }
  if (value === 'proxy_only')
    return { label: '仅代理', cls: 'text-warning bg-warning/10 border-warning/25', icon: Database }
  if (value === 'precise_only')
    return { label: '仅精确', cls: 'text-accent bg-accent/10 border-accent/25', icon: Database }
  if (value === 'intent_only') return { label: '仅委托', cls: 'text-muted bg-elevated border-border', icon: Database }
  return { label: '缺日终参考', cls: 'text-danger bg-danger/10 border-danger/25', icon: XCircle }
}

function eventKindLabel(kind: LargeOrderHistoryEvent['event_kind']) {
  if (kind === 'kaipanla_trade') return '精确成交'
  if (kind === 'kaipanla_intent') return '委托撤单'
  return '快照代理'
}

function eventDetail(event: LargeOrderHistoryEvent) {
  if (event.event_kind === 'kaipanla_trade')
    return `${event.direction === 'active_buy' ? '主动买' : '主动卖'} ${money(event.amount)}`
  if (event.event_kind === 'kaipanla_intent')
    return `${event.cancel_flag ? '撤单' : '委托'} ${event.side === 'buy' ? '买' : '卖'} ${money(event.amount)}`
  return `净额 ${money((event.buy_amount ?? 0) - (event.sell_amount ?? 0))}`
}

function Score({ value }: { value: number }) {
  const tone = value >= 75 ? 'text-danger' : value >= 55 ? 'text-warning' : 'text-muted'
  return <span className={cn('font-mono text-lg font-semibold', tone)}>{value.toFixed(0)}</span>
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] text-muted">{label}</div>
      <div className={cn('mt-1 truncate font-mono text-sm font-medium text-foreground', tone)}>{value}</div>
    </div>
  )
}

export function LargeOrders() {
  const qc = useQueryClient()
  const [scope, setScope] = useState<'all' | 'watchlist'>('all')
  const [window, setWindow] = useState(60)
  const [mode, setMode] = useState<LargeOrderEvidenceMode>('combined')
  const [selected, setSelected] = useState<LargeOrderRow | null>(null)
  const [previewStock, setPreviewStock] = useState<{ symbol: string; name?: string } | null>(null)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [threshold, setThreshold] = useState(75)
  const [cooldown, setCooldown] = useState(120)
  const [minLimitGapPercent, setMinLimitGapPercent] = useState(2)
  const [auditDate, setAuditDate] = useState('')
  const [historySymbol, setHistorySymbol] = useState('')
  const [fromTime, setFromTime] = useState('')
  const [toTime, setToTime] = useState('')
  const [newEvents, setNewEvents] = useState(false)
  const historyRef = useRef<HTMLDivElement>(null)
  const wasNearTopRef = useRef(true)
  const newestEventIdRef = useRef<string | null>(null)
  const openStockPreview = (symbol: string, name?: string | null) => {
    setPreviewStock({ symbol, name: name || undefined })
  }

  const preferences = useQuery({ queryKey: QK.preferences, queryFn: api.preferences })
  const status = useQuery({
    queryKey: [...QK.largeOrders, 'status'],
    queryFn: api.largeOrdersStatus,
    refetchInterval: 15000,
    refetchIntervalInBackground: true,
  })
  const ranking = useQuery({
    queryKey: [...QK.largeOrders, 'ranking', window, scope, mode],
    queryFn: () => api.largeOrdersRanking(window, scope, mode),
    refetchInterval: 15000,
    refetchIntervalInBackground: true,
    placeholderData: (previous) => previous,
  })
  const dates = useQuery({
    queryKey: [...QK.largeOrders, 'dates'],
    queryFn: () => api.largeOrdersDates(30),
    staleTime: 60_000,
  })
  useEffect(() => {
    if (!auditDate && dates.isSuccess) setAuditDate(dates.data.dates[0] ?? todayDate())
  }, [auditDate, dates.data?.dates, dates.isSuccess])

  const fromMs = dateTimeMs(auditDate, fromTime)
  const toMs = dateTimeMs(auditDate, toTime, true)
  const history = useInfiniteQuery({
    queryKey: [...QK.largeOrders, 'history', auditDate, historySymbol, mode, fromMs, toMs],
    queryFn: ({ pageParam }) =>
      api.largeOrdersHistory({
        date: auditDate,
        mode,
        symbol: historySymbol.trim() || undefined,
        from_ms: fromMs,
        to_ms: toMs,
        cursor: pageParam,
        limit: 240,
        order: 'desc',
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => (last.has_more ? (last.next_cursor ?? undefined) : undefined),
    enabled: !!auditDate,
    refetchInterval: auditDate === todayDate() ? 15000 : false,
    refetchIntervalInBackground: true,
  })
  const reconciliation = useQuery({
    queryKey: [...QK.largeOrders, 'reconciliation', auditDate, historySymbol, fromMs, toMs],
    queryFn: () =>
      api.largeOrdersReconciliation({
        date: auditDate,
        symbol: historySymbol.trim() || undefined,
        from_ms: fromMs,
        to_ms: toMs,
        limit: 1200,
        order: 'desc',
      }),
    enabled: !!auditDate,
    refetchInterval: auditDate === todayDate() ? 30000 : false,
    refetchIntervalInBackground: true,
  })
  const tape = useQuery({
    queryKey: [...QK.largeOrders, 'tape', selected?.symbol ?? ''],
    queryFn: () => api.largeOrdersTape(selected!.symbol),
    enabled: !!selected,
    staleTime: 5000,
  })
  const savePreferences = useMutation({
    mutationFn: () =>
      api.updateLargeOrdersPreferences({
        score_threshold: threshold,
        cooldown_seconds: cooldown,
        min_limit_up_gap_pct: minLimitGapPercent / 100,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.preferences })
      qc.invalidateQueries({ queryKey: QK.largeOrders })
      setSettingsOpen(false)
    },
  })

  useEffect(() => {
    const current = preferences.data?.large_orders
    if (!current) return
    setThreshold(current.score_threshold)
    setCooldown(current.cooldown_seconds)
    setMinLimitGapPercent(current.min_limit_up_gap_pct * 100)
  }, [preferences.data?.large_orders])

  const rows = useMemo(() => ranking.data?.rows ?? [], [ranking.data?.rows])
  const events = useMemo(() => history.data?.pages.flatMap((page) => page.rows) ?? [], [history.data?.pages])
  const eventVirtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => historyRef.current,
    estimateSize: () => 48,
    getItemKey: (index) => (events[index] ? `${events[index].event_kind}:${events[index].event_id}` : index),
    overscan: 8,
  })
  useEffect(() => {
    setNewEvents(false)
    wasNearTopRef.current = true
    newestEventIdRef.current = null
    historyRef.current?.scrollTo({ top: 0 })
  }, [auditDate, historySymbol, mode, fromMs, toMs])
  useEffect(() => {
    const newestId = events[0] ? `${events[0].event_kind}:${events[0].event_id}` : undefined
    if (!newestId) return
    const previousId = newestEventIdRef.current
    newestEventIdRef.current = newestId
    if (!previousId || previousId === newestId) return
    if (wasNearTopRef.current) {
      historyRef.current?.scrollTo({ top: 0 })
      setNewEvents(false)
    } else {
      setNewEvents(true)
    }
  }, [events])
  const onHistoryScroll = () => {
    const el = historyRef.current
    if (!el) return
    wasNearTopRef.current = el.scrollTop < 70
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 160 && history.hasNextPage && !history.isFetchingNextPage)
      history.fetchNextPage()
  }

  const selectedRow = useMemo(() => rows.find((row) => row.symbol === selected?.symbol) ?? null, [rows, selected])
  const preciseCount = rows.filter((row) => row.data_quality === 'precise').length
  const sourceLabel = preciseCount > 0 ? '开盘啦主动成交 + TickFlow' : 'TickFlow 快照方向代理'
  const phaseLabel = status.data?.market_phase === 'continuous' ? '连续竞价' : status.data?.market_phase || '非交易时段'
  const lastUpdatedMs = ranking.data?.last_updated_ms ?? status.data?.last_updated_ms
  const latestUpdated = lastUpdatedMs ? new Date(lastUpdatedMs).toLocaleTimeString('zh-CN') : '--'
  const windowLabel = WINDOWS.find((item) => item.value === window)?.label ?? `${window} 秒`
  const degraded =
    status.data?.data_source === 'kaipanla' &&
    (preciseCount === 0 || !!status.data?.last_error || status.data?.deep_dive_calls_remaining === 0)
  const reconSummary = reconciliation.data?.summary

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader title="实时大单" subtitle="主力买入候选 · 主动成交优先" />
      <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
        <div className="mx-auto max-w-[1500px] space-y-4">
          <section className="grid gap-3 border-b border-border pb-4 sm:grid-cols-2 xl:grid-cols-5">
            <Metric label="数据源" value={sourceLabel} tone={status.data?.stale ? 'text-warning' : 'text-bull'} />
            <Metric label="覆盖标的" value={`${status.data?.coverage_count ?? 0} 只`} />
            <Metric label="候选 / 精确" value={`${ranking.data?.count ?? 0} / ${preciseCount}`} />
            <Metric label="阶段" value={phaseLabel} />
            <Metric label="最后更新" value={latestUpdated} />
          </section>

          <div className="flex flex-wrap items-center gap-2">
            <div className="flex overflow-hidden rounded-btn border border-border bg-surface">
              {(['all', 'watchlist'] as const).map((item) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => setScope(item)}
                  className={cn(
                    'px-3 py-1.5 text-xs transition-colors',
                    scope === item ? 'bg-elevated text-foreground' : 'text-muted hover:text-foreground',
                  )}
                >
                  {item === 'all' ? '全市场' : '自选'}
                </button>
              ))}
            </div>
            <div className="flex overflow-hidden rounded-btn border border-border bg-surface">
              {WINDOWS.map((item) => (
                <button
                  key={item.value}
                  type="button"
                  onClick={() => setWindow(item.value)}
                  className={cn(
                    'px-3 py-1.5 text-xs transition-colors',
                    window === item.value ? 'bg-elevated text-foreground' : 'text-muted hover:text-foreground',
                  )}
                >
                  {item.label}
                </button>
              ))}
            </div>
            <label className="flex items-center gap-2 rounded-btn border border-border bg-surface px-3 py-1.5 text-xs text-muted">
              <span>证据</span>
              <select
                value={mode}
                onChange={(event) => setMode(event.target.value as LargeOrderEvidenceMode)}
                className="bg-transparent text-foreground outline-none"
              >
                {MODES.map((item) => (
                  <option key={item.value} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              onClick={() => qc.invalidateQueries({ queryKey: QK.largeOrders })}
              className="ml-auto inline-flex items-center gap-1.5 rounded-btn border border-border px-3 py-1.5 text-xs text-muted hover:bg-elevated hover:text-foreground"
              title="刷新榜单"
            >
              <RefreshCw className={cn('h-3.5 w-3.5', (ranking.isFetching || status.isFetching) && 'animate-spin')} />{' '}
              刷新
            </button>
            <button
              type="button"
              onClick={() => setSettingsOpen((value) => !value)}
              className="inline-flex items-center justify-center rounded-btn border border-border p-1.5 text-muted hover:bg-elevated hover:text-foreground"
              title="大单评分设置"
              aria-label="大单评分设置"
            >
              <Settings2 className="h-4 w-4" />
            </button>
          </div>

          {settingsOpen && (
            <section className="flex flex-wrap items-end gap-4 border border-border bg-surface px-4 py-3 text-xs">
              <label className="space-y-1 text-muted">
                告警评分
                <input
                  type="number"
                  min={50}
                  max={100}
                  value={threshold}
                  onChange={(event) => setThreshold(Number(event.target.value))}
                  className="mt-1 block w-24 rounded border border-border bg-base px-2 py-1 text-foreground"
                />
              </label>
              <label className="space-y-1 text-muted">
                冷却秒数
                <input
                  type="number"
                  min={30}
                  max={3600}
                  value={cooldown}
                  onChange={(event) => setCooldown(Number(event.target.value))}
                  className="mt-1 block w-24 rounded border border-border bg-base px-2 py-1 text-foreground"
                />
              </label>
              <label className="space-y-1 text-muted">
                最小涨停空间%
                <input
                  type="number"
                  min={0}
                  max={10}
                  step={0.1}
                  value={minLimitGapPercent}
                  onChange={(event) => setMinLimitGapPercent(Number(event.target.value))}
                  className="mt-1 block w-28 rounded border border-border bg-base px-2 py-1 text-foreground"
                />
              </label>
              <button
                type="button"
                onClick={() => savePreferences.mutate()}
                disabled={savePreferences.isPending}
                className="inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50"
              >
                {savePreferences.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />} 保存
              </button>
              <span className="text-muted">距涨停不超过该空间的标的不进入候选、深挖和告警。</span>
            </section>
          )}
          {status.data?.stale && (
            <div className="flex items-center gap-2 border border-warning/25 bg-warning/10 px-3 py-2 text-xs text-warning">
              <AlertTriangle className="h-4 w-4 shrink-0" /> 行情快照已过期，已停止新的告警；当前仅保留最近有效榜单。
            </div>
          )}
          {!status.data?.stale && degraded && (
            <div className="flex items-center gap-2 border border-warning/25 bg-warning/10 px-3 py-2 text-xs text-warning">
              <AlertTriangle className="h-4 w-4 shrink-0" /> 开盘啦精确成交暂不可用，当前金额为 TickFlow 快照方向估算。
            </div>
          )}
          {((status.data?.filtered_near_limit_count ?? 0) > 0 || (status.data?.unassessable_count ?? 0) > 0) && (
            <div className="text-xs text-muted">
              {(status.data?.filtered_near_limit_count ?? 0) > 0
                ? `已过滤 ${status.data?.filtered_near_limit_count} 只接近涨停标的`
                : ''}
              {(status.data?.unassessable_count ?? 0) > 0
                ? `${(status.data?.filtered_near_limit_count ?? 0) > 0 ? '，另有' : '有'} ${status.data?.unassessable_count} 只无法可靠计算涨停空间`
                : ''}
              。
            </div>
          )}

          <div className="grid min-h-[480px] gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
            <section className="min-w-0 overflow-hidden rounded-panel border border-border bg-surface/40">
              <div className="flex items-center justify-between border-b border-border px-4 py-3">
                <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                  <Zap className="h-4 w-4 text-warning" /> 大单候选
                </div>
                <span className="text-xs text-muted">
                  {ranking.data?.count ?? 0} 只 · {mode === 'intent' ? '委托意图辅助' : '主动成交优先'}
                </span>
              </div>
              {ranking.isLoading ? (
                <div className="grid h-64 place-items-center text-sm text-muted">
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  加载候选中
                </div>
              ) : rows.length === 0 ? (
                <EmptyState
                  icon={BarChart3}
                  title="暂无大单候选"
                  hint="等待实时行情增量，或检查实时行情与开盘啦授权状态。"
                />
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[1120px] text-left text-xs">
                    <thead className="border-b border-border bg-surface text-muted">
                      <tr>
                        {[
                          '标的',
                          '时间',
                          '评分',
                          '置信度',
                          '主动买额',
                          '主动卖额',
                          '净买额',
                          '买入占比',
                          '最大单笔',
                          '撤单率',
                          '涨跌幅',
                          '距涨停',
                          '新鲜度',
                        ].map((title) => (
                          <th key={title} className="whitespace-nowrap px-3 py-2.5 font-medium">
                            {title}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row) => {
                        const meta = confidenceMeta(row.confidence)
                        const active = selected?.symbol === row.symbol
                        return (
                          <tr
                            key={row.symbol}
                            className={cn(
                              'border-b border-border/70 transition-colors hover:bg-elevated/60',
                              active && 'bg-elevated',
                            )}
                          >
                            <td className="px-3 py-3">
                              <div className="flex min-w-[138px] items-center gap-1">
                                <button type="button" className="min-w-[104px] text-left" onClick={() => setSelected(row)}>
                                  <div className="font-medium text-foreground">{row.name || '--'}</div>
                                  <div className="mt-0.5 font-mono text-[11px] text-muted">{row.symbol}</div>
                                </button>
                                <button
                                  type="button"
                                  onClick={() => openStockPreview(row.symbol, row.name)}
                                  className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded text-muted hover:bg-elevated hover:text-accent"
                                  title="查看 K 线和分时"
                                  aria-label={`查看 ${row.name || row.symbol} K 线和分时`}
                                >
                                  <ChartCandlestick className="h-4 w-4" />
                                </button>
                              </div>
                            </td>
                            <td className="whitespace-nowrap px-3 py-3 font-mono text-muted">
                              {clockTime(row.last_seen_ts)}
                            </td>
                            <td className="px-3 py-3">
                              <Score value={row.score} />
                            </td>
                            <td className="px-3 py-3">
                              <span
                                className={cn('whitespace-nowrap rounded border px-1.5 py-0.5 text-[11px]', meta.cls)}
                              >
                                {meta.label}
                              </span>
                            </td>
                            <td className="px-3 py-3 font-mono text-bull">{money(row.active_buy_amount)}</td>
                            <td className="px-3 py-3 font-mono text-danger">{money(row.active_sell_amount)}</td>
                            <td
                              className={cn(
                                'px-3 py-3 font-mono font-medium',
                                row.net_buy_amount >= 0 ? 'text-bull' : 'text-danger',
                              )}
                            >
                              {money(row.net_buy_amount)}
                            </td>
                            <td className="px-3 py-3 font-mono">{pct(row.buy_ratio)}</td>
                            <td className="px-3 py-3 font-mono">{money(row.max_order_amount)}</td>
                            <td className="px-3 py-3 font-mono">{pct(row.cancel_rate)}</td>
                            <td
                              className={cn(
                                'px-3 py-3 font-mono',
                                row.change_pct == null
                                  ? 'text-muted'
                                  : row.change_pct >= 0
                                    ? 'text-danger'
                                    : 'text-bull',
                              )}
                            >
                              {pct(row.change_pct)}
                            </td>
                            <td className="px-3 py-3 font-mono text-warning">{pct(row.limit_up_gap_pct)}</td>
                            <td className="whitespace-nowrap px-3 py-3 text-muted">{freshness(row.freshness_ms)}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </section>
            <aside className="min-w-0 rounded-panel border border-border bg-surface/40">
              <div className="border-b border-border px-4 py-3 text-sm font-medium text-foreground">
                {selectedRow ? `${selectedRow.name} · 信号解释` : '信号详情'}
              </div>
              {!selectedRow ? (
                <div className="grid h-[420px] place-items-center px-6 text-center text-xs text-muted">
                  <div>
                    <BarChart3 className="mx-auto mb-2 h-6 w-6 opacity-50" />
                    点击榜单中的标的查看成交时间线与委托意图
                  </div>
                </div>
              ) : (
                <div className="space-y-4 p-4 text-xs">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="font-mono text-muted">{selectedRow.symbol}</div>
                      <div className="mt-1 text-2xl font-semibold text-foreground">
                        <Score value={selectedRow.score} />
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => openStockPreview(selectedRow.symbol, selectedRow.name)}
                      className="rounded-btn border border-border px-2.5 py-1.5 text-muted hover:bg-elevated hover:text-foreground"
                    >
                      打开个股
                    </button>
                  </div>
                  <div className="border-l-2 border-accent pl-3 leading-5 text-secondary">
                    {selectedRow.explanation}
                    <br />
                    数据来源：{selectedRow.data_quality === 'precise' ? '开盘啦 /13 主动成交' : 'TickFlow 快照方向代理'}
                    。委托撤单只作意图证据。
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <Metric
                      label={`${windowLabel}净买额`}
                      value={money(selectedRow.net_buy_amount)}
                      tone={selectedRow.net_buy_amount >= 0 ? 'text-bull' : 'text-danger'}
                    />
                    <Metric label="主动买入占比" value={pct(selectedRow.buy_ratio)} />
                    <Metric label="成交额 z-score" value={selectedRow.zscore.toFixed(2)} />
                    <Metric label="盘口不平衡" value={pct(selectedRow.book_imbalance)} />
                    <Metric label="距涨停" value={pct(selectedRow.limit_up_gap_pct)} />
                    <Metric label="涨停价" value={money(selectedRow.limit_up_price)} />
                    <Metric label="撤单率" value={pct(selectedRow.cancel_rate)} />
                  </div>
                  <div className="border-t border-border pt-3">
                    <div className="mb-2 flex items-center gap-1.5 font-medium text-foreground">
                      <Clock3 className="h-3.5 w-3.5" />
                      成交时间线
                    </div>
                    {tape.data?.timeline?.length ? (
                      <div className="space-y-1.5">
                        {tape.data.timeline
                          .slice(-8)
                          .reverse()
                          .map((point, index) => (
                            <div key={`${point.ts}-${index}`} className="flex items-center gap-2">
                              <span className="w-14 text-muted">
                                {new Date(point.ts * 1000).toLocaleTimeString('zh-CN', {
                                  minute: '2-digit',
                                  second: '2-digit',
                                })}
                              </span>
                              <div className="h-1.5 flex-1 overflow-hidden rounded bg-elevated">
                                <div
                                  className={cn('h-full', point.buy >= point.sell ? 'bg-danger' : 'bg-bull')}
                                  style={{
                                    width: `${Math.min(100, Math.max(4, (point.amount / Math.max(selectedRow.large_threshold, 1)) * 25))}%`,
                                  }}
                                />
                              </div>
                              <span className="w-14 text-right font-mono text-muted">{money(point.amount)}</span>
                            </div>
                          ))}
                      </div>
                    ) : (
                      <div className="text-muted">暂无逐笔成交，当前为快照代理。</div>
                    )}
                  </div>
                  {tape.data?.error && <div className="text-warning">开盘啦深挖降级：{tape.data.error}</div>}
                </div>
              )}
            </aside>
          </div>

          <section className="border-t border-border pt-4">
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <div className="mr-2 flex items-center gap-2 text-sm font-medium text-foreground">
                <Clock3 className="h-4 w-4 text-accent" />
                历史事件流水
              </div>
              <DatePicker
                value={auditDate}
                onChange={setAuditDate}
                min={dates.data?.dates?.at(-1)}
                max={dates.data?.dates?.[0]}
                buttonClassName="font-mono"
              />
              <input
                value={historySymbol}
                onChange={(event) => setHistorySymbol(event.target.value)}
                placeholder="标的代码"
                className="h-7 w-28 rounded-input border border-border bg-elevated px-2 text-xs text-foreground outline-none focus:border-accent/60"
              />
              <label className="flex h-7 items-center gap-1.5 rounded-input border border-border bg-elevated px-2 text-xs text-muted">
                时段{' '}
                <input
                  type="time"
                  value={fromTime}
                  onChange={(event) => setFromTime(event.target.value)}
                  className="bg-transparent font-mono text-foreground outline-none"
                />{' '}
                -{' '}
                <input
                  type="time"
                  value={toTime}
                  onChange={(event) => setToTime(event.target.value)}
                  className="bg-transparent font-mono text-foreground outline-none"
                />
              </label>
              <span className="ml-auto text-[11px] text-muted">
                {events.length} 条已载入{history.hasNextPage ? ' · 下滚加载更早记录' : ''}
              </span>
            </div>
            {history.isLoadingError ? (
              <div className="flex items-center gap-2 border border-danger/25 bg-danger/10 px-3 py-2 text-xs text-danger">
                <AlertTriangle className="h-4 w-4" />
                历史事件加载失败，请稍后重试。
              </div>
            ) : history.isLoading ? (
              <div className="grid h-48 place-items-center text-xs text-muted">
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                加载事件流水
              </div>
            ) : events.length === 0 ? (
              <div className="border border-border bg-surface/30 px-4 py-12 text-center text-xs text-muted">
                当前日期与筛选条件下没有可核对事件。
              </div>
            ) : (
              <div className="relative overflow-hidden border border-border bg-surface/30">
                <div
                  ref={historyRef}
                  onScroll={onHistoryScroll}
                  className="h-[360px] overflow-y-auto"
                  role="log"
                  aria-label="历史事件流水"
                >
                  <div className="relative w-full" style={{ height: eventVirtualizer.getTotalSize() }}>
                    {eventVirtualizer.getVirtualItems().map((item) => {
                      const event = events[item.index]
                      return (
                        <div
                          key={`${event.event_kind}:${event.event_id}`}
                          className="absolute left-0 top-0 flex w-full items-center gap-3 border-b border-border/60 px-3 text-xs"
                          style={{ height: item.size, transform: `translateY(${item.start}px)` }}
                        >
                          <span className="w-16 shrink-0 font-mono text-muted">
                            {clockTime(event.event_ts_ms / 1000)}
                          </span>
                          <button
                            type="button"
                            onClick={() => openStockPreview(event.symbol, event.name)}
                            className="w-20 shrink-0 text-left font-mono text-foreground hover:text-accent"
                            title="查看 K 线和分时"
                          >
                            {event.symbol}
                          </button>
                          <span
                            className={cn(
                              'w-16 shrink-0 rounded border px-1.5 py-0.5 text-center text-[10px]',
                              event.event_kind === 'kaipanla_trade'
                                ? 'border-bull/25 bg-bull/10 text-bull'
                                : event.event_kind === 'kaipanla_intent'
                                  ? 'border-warning/25 bg-warning/10 text-warning'
                                  : 'border-border bg-elevated text-muted',
                            )}
                          >
                            {eventKindLabel(event.event_kind)}
                          </span>
                          <span className="min-w-0 flex-1 truncate text-secondary">
                            <span className="hidden sm:inline">{event.name || '--'} · </span>
                            {eventDetail(event)}
                          </span>
                          <span className="hidden w-24 shrink-0 text-right font-mono text-muted md:block">
                            {money(event.price)}
                          </span>
                          <span className="hidden w-28 shrink-0 text-right text-[10px] text-muted xl:block">
                            {event.event_id.slice(-12)}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
                {newEvents && (
                  <button
                    type="button"
                    onClick={() => {
                      historyRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
                      setNewEvents(false)
                    }}
                    className="absolute left-1/2 top-2 -translate-x-1/2 rounded-full border border-accent/30 bg-surface px-3 py-1 text-[11px] text-accent shadow-sm"
                  >
                    有新数据 · 回到最新
                  </button>
                )}
                {history.isFetchNextPageError && (
                  <button
                    type="button"
                    onClick={() => history.fetchNextPage()}
                    className="w-full border-t border-danger/25 bg-danger/10 px-3 py-2 text-center text-[11px] text-danger"
                  >
                    更早记录加载失败，点击重试
                  </button>
                )}
                {history.isFetchingNextPage && (
                  <div className="border-t border-border px-3 py-2 text-center text-[11px] text-muted">
                    <Loader2 className="mr-1 inline h-3 w-3 animate-spin" />
                    加载更早事件
                  </div>
                )}
              </div>
            )}
          </section>

          <section className="border-t border-border pt-4">
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <div className="mr-2 flex items-center gap-2 text-sm font-medium text-foreground">
                <Database className="h-4 w-4 text-accent" />
                盘后核对
              </div>
              <span className="text-[11px] text-muted">
                按标的 + 北京时间 60 秒桶，逐桶对比代理、精确成交、委托撤单和日终参考
              </span>
              <button
                type="button"
                onClick={() => reconciliation.refetch()}
                className="ml-auto inline-flex items-center gap-1 rounded-btn border border-border px-2.5 py-1.5 text-[11px] text-muted hover:bg-elevated hover:text-foreground"
              >
                <RefreshCw className={cn('h-3 w-3', reconciliation.isFetching && 'animate-spin')} />
                刷新
              </button>
            </div>
            {reconciliation.isError ? (
              <div className="flex items-center gap-2 border border-danger/25 bg-danger/10 px-3 py-2 text-xs text-danger">
                <AlertTriangle className="h-4 w-4" />
                盘后核对数据源不可用。
              </div>
            ) : reconciliation.isLoading ? (
              <div className="grid h-36 place-items-center text-xs text-muted">
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                加载盘后核对
              </div>
            ) : (
              <>
                <div className="mb-3 grid gap-3 border border-border bg-surface/30 px-3 py-3 sm:grid-cols-2 lg:grid-cols-6">
                  <Metric label="代理净额" value={money(reconSummary?.proxy_net_amount)} />
                  <Metric label="精确净额" value={money(reconSummary?.precise_net_amount)} />
                  <Metric
                    label="净额差异"
                    value={money(reconSummary?.net_difference)}
                    tone={
                      reconSummary?.net_difference && reconSummary.net_difference >= 0 ? 'text-bull' : 'text-danger'
                    }
                  />
                  <Metric label="精确覆盖率" value={pct(reconSummary?.precise_coverage)} />
                  <Metric label="已匹配桶" value={`${reconSummary?.matched_buckets ?? 0}`} />
                  <Metric
                    label="日终参考"
                    value={reconSummary?.daily_reference_net == null ? '缺失' : money(reconSummary.daily_reference_net)}
                    tone={reconSummary?.daily_reference_net == null ? 'text-warning' : 'text-foreground'}
                  />
                </div>
                {(reconciliation.data?.rows?.length ?? 0) === 0 ? (
                  <div className="border border-border bg-surface/30 px-4 py-10 text-center text-xs text-muted">
                    当前筛选条件没有可核对的分钟桶。
                    {reconSummary?.reference_status === 'reference_missing' ? ' 日终参考数据缺失。' : ''}
                  </div>
                ) : (
                  <div className="overflow-x-auto border border-border">
                    <table className="w-full min-w-[1180px] text-left text-xs">
                      <thead className="bg-surface text-muted">
                        <tr>
                          {[
                            '时间桶',
                            '标的',
                            '代理买额',
                            '精确买额',
                            '代理净额',
                            '精确净额',
                            '净额差异',
                            '精确覆盖',
                            '委托数',
                            '撤单率',
                            '日终参考',
                            '状态',
                          ].map((title) => (
                            <th key={title} className="whitespace-nowrap px-3 py-2.5 font-medium">
                              {title}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {(reconciliation.data?.rows ?? []).map((row) => {
                          const meta = statusMeta(row.status)
                          const Icon = meta.icon
                          return (
                            <tr key={`${row.bucket_start_ms}-${row.symbol}`} className="border-t border-border/70">
                              <td className="whitespace-nowrap px-3 py-2.5 font-mono text-muted">
                                {bucketTime(row.bucket_start_ms)}
                              </td>
                              <td className="px-3 py-2.5">
                                <button
                                  type="button"
                                  onClick={() => openStockPreview(row.symbol, row.name)}
                                  className="text-left hover:text-accent"
                                  title="查看 K 线和分时"
                                >
                                  <div className="font-medium text-foreground">{row.name || '--'}</div>
                                  <div className="font-mono text-[10px] text-muted">{row.symbol}</div>
                                </button>
                              </td>
                              <td className="px-3 py-2.5 font-mono text-muted">{money(row.proxy_buy_amount)}</td>
                              <td className="px-3 py-2.5 font-mono text-bull">{money(row.precise_buy_amount)}</td>
                              <td
                                className={cn(
                                  'px-3 py-2.5 font-mono',
                                  row.proxy_net_amount >= 0 ? 'text-bull' : 'text-danger',
                                )}
                              >
                                {money(row.proxy_net_amount)}
                              </td>
                              <td
                                className={cn(
                                  'px-3 py-2.5 font-mono',
                                  row.precise_net_amount >= 0 ? 'text-bull' : 'text-danger',
                                )}
                              >
                                {money(row.precise_net_amount)}
                              </td>
                              <td
                                className={cn(
                                  'px-3 py-2.5 font-mono',
                                  row.net_difference >= 0 ? 'text-bull' : 'text-danger',
                                )}
                              >
                                {money(row.net_difference)}
                              </td>
                              <td className="px-3 py-2.5 font-mono">{pct(row.precise_coverage)}</td>
                              <td className="px-3 py-2.5 font-mono">{row.intent_count}</td>
                              <td className="px-3 py-2.5 font-mono">{pct(row.cancel_rate)}</td>
                              <td className="px-3 py-2.5 font-mono text-muted">
                                {money(row.main_net_amount_over_300k)}
                              </td>
                              <td className="px-3 py-2.5">
                                <span
                                  className={cn(
                                    'inline-flex items-center gap-1 whitespace-nowrap rounded border px-1.5 py-0.5 text-[10px]',
                                    meta.cls,
                                  )}
                                >
                                  <Icon className="h-3 w-3" />
                                  {meta.label}
                                </span>
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
                {reconciliation.data?.truncated && (
                  <div className="mt-2 text-[11px] text-warning">结果已截断，请缩小时间范围或指定标的。</div>
                )}
              </>
            )}
          </section>
        </div>
      </div>
      <StockPreviewDialog
        symbol={previewStock?.symbol ?? null}
        name={previewStock?.name}
        onClose={() => setPreviewStock(null)}
      />
    </div>
  )
}
