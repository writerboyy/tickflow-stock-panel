import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Ban,
  Bell,
  Check,
  CheckCircle2,
  CircleDot,
  Crosshair,
  Flame,
  Plus,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  Trash2,
  Wifi,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { api, type LimitBoardRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

type Tab = 'first' | 'selected' | 'pool' | 'events'
type TableMode = Exclude<Tab, 'events'>

const STATUS: Record<string, { label: string; tone: string }> = {
  watching: { label: '观察中', tone: 'text-muted' },
  near_limit: { label: '临板', tone: 'text-warning' },
  touched: { label: '触板', tone: 'text-accent' },
  sealed: { label: '封板', tone: 'text-bull' },
  broken: { label: '炸板', tone: 'text-danger' },
  resealed: { label: '回封', tone: 'text-bull' },
  blacklisted: { label: '今日黑名单', tone: 'text-danger' },
}

const ORDER_STATUS: Record<string, { label: string; tone: string }> = {
  submitting: { label: '提交中', tone: 'text-accent' },
  accepted_pending: { label: '已受理', tone: 'text-warning' },
  filled: { label: '已成交', tone: 'text-bear' },
  rejected: { label: '已拒绝', tone: 'text-danger' },
  unknown: { label: '待人工核对', tone: 'text-danger' },
  blocked: { label: '交易未就绪', tone: 'text-muted' },
}

interface RowProps {
  row: LimitBoardRow
  mode: TableMode
  inPool: boolean
  busy: boolean
  onOpen: () => void
  onAddPool: () => void
  onRemoveSelected: () => void
  onToggleAuto: (enabled: boolean) => void
  onRemovePool: () => void
}

function Row({
  row,
  mode,
  inPool,
  busy,
  onOpen,
  onAddPool,
  onRemoveSelected,
  onToggleAuto,
  onRemovePool,
}: RowProps) {
  const status = STATUS[row.status || 'watching'] || STATUS.watching
  const gap = row.limit_gap_pct == null ? '--' : `${(row.limit_gap_pct * 100).toFixed(2)}%`
  const orderStatus = !row.auto_trade && !row.auto_order_key
    ? { label: '未开启', tone: 'text-muted' }
    : row.auto_order_status
    ? ORDER_STATUS[row.auto_order_status] || { label: row.auto_order_status, tone: 'text-muted' }
    : { label: '等待触板', tone: 'text-muted' }

  return (
    <tr className="border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="py-2.5 pl-3 pr-2">
        <button type="button" onClick={onOpen} className="text-left hover:text-accent" title="查看 K 线与分时">
          <div className="font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
        </button>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-accent">{row.limit_up?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-warning">{gap}</td>
      <td className="px-2">
        <span className={`inline-flex items-center gap-1 font-medium ${status.tone}`}>
          <CircleDot className="h-3 w-3" />{status.label}
        </span>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.break_count ? `${row.break_count} 次` : '0 次'}</td>
      <td className="px-2 font-mono tabular-nums text-secondary">{row.bid1_volume ? row.bid1_volume.toLocaleString('zh-CN') : '--'}</td>
      <td className="px-2">
        <span className={row.ws_active ? 'text-bear' : 'text-muted'}>{row.ws_active ? 'WS' : '快照'}</span>
      </td>
      {mode === 'pool' ? (
        <>
          <td className="px-2">
            <label className="inline-flex items-center gap-1.5 whitespace-nowrap text-secondary">
              <input
                type="checkbox"
                checked={row.auto_trade === true}
                disabled={busy}
                onChange={event => onToggleAuto(event.target.checked)}
              />
              {row.auto_trade ? '已开启' : '已关闭'}
            </label>
          </td>
          <td className={`px-2 font-medium ${orderStatus.tone}`} title={row.auto_order_error || undefined}>
            {orderStatus.label}
          </td>
          <td className="pr-3 text-right">
            <button type="button" title="移出打板池" disabled={busy} onClick={onRemovePool} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger disabled:opacity-40">
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </td>
        </>
      ) : (
        <>
          <td className="px-2">
            <button
              type="button"
              title={inPool ? '已在打板池' : '加入打板池'}
              disabled={inPool || busy}
              onClick={onAddPool}
              className={`inline-flex h-7 items-center gap-1 rounded-btn border px-2 ${inPool ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-accent/40 hover:text-accent'} disabled:opacity-60`}
            >
              {inPool ? <Check className="h-3.5 w-3.5" /> : <Crosshair className="h-3.5 w-3.5" />}
              {inPool ? '已加入' : '加入'}
            </button>
          </td>
          {mode === 'selected' ? (
            <td className="pr-3 text-right">
              <button type="button" title="移除精选跟踪" disabled={busy} onClick={onRemoveSelected} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger disabled:opacity-40">
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            </td>
          ) : null}
        </>
      )}
    </tr>
  )
}

interface TableProps {
  rows: LimitBoardRow[]
  mode: TableMode
  poolSymbols: Set<string>
  busy: boolean
  onOpen: (row: LimitBoardRow) => void
  onAddPool: (row: LimitBoardRow) => void
  onRemoveSelected: (row: LimitBoardRow) => void
  onToggleAuto: (row: LimitBoardRow, enabled: boolean) => void
  onRemovePool: (row: LimitBoardRow) => void
}

function Table(props: TableProps) {
  const { rows, mode } = props
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[900px] border-collapse">
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="py-2 pl-3 pr-2">标的</th><th className="px-2">现价</th><th className="px-2">涨停价</th><th className="px-2">距涨停</th><th className="px-2">状态</th><th className="px-2">炸板次数</th><th className="px-2">买一封单</th><th className="px-2">行情</th>
            {mode === 'pool' ? <><th className="px-2">自动打板</th><th className="px-2">委托状态</th><th className="px-2" /></> : <><th className="px-2">打板池</th>{mode === 'selected' ? <th className="px-2" /> : null}</>}
          </tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <Row
              key={row.symbol}
              row={row}
              mode={mode}
              inPool={props.poolSymbols.has(row.symbol)}
              busy={props.busy}
              onOpen={() => props.onOpen(row)}
              onAddPool={() => props.onAddPool(row)}
              onRemoveSelected={() => props.onRemoveSelected(row)}
              onToggleAuto={enabled => props.onToggleAuto(row, enabled)}
              onRemovePool={() => props.onRemovePool(row)}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function LimitBoard() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('first')
  const [search, setSearch] = useState('')
  const [preview, setPreview] = useState<LimitBoardRow | null>(null)
  const view = useQuery({ queryKey: QK.limitBoard, queryFn: api.limitBoard, refetchInterval: 5000, placeholderData: previous => previous })
  const searchQuery = useQuery({
    queryKey: QK.instrumentSearch(search, 'stock'),
    queryFn: () => api.instrumentSearch(search, 10, 'stock'),
    enabled: search.trim().length >= 2,
  })
  const refresh = () => queryClient.invalidateQueries({ queryKey: QK.limitBoard })
  const add = useMutation({
    mutationFn: (symbol: string) => api.limitBoardAdd(symbol, view.data?.revision ?? 0),
    onSuccess: () => { setSearch(''); refresh() },
  })
  const remove = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const addPool = useMutation({
    mutationFn: ({ row, source }: { row: LimitBoardRow; source: 'first_board' | 'selected' }) => api.limitBoardPoolAdd(row.symbol, source, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const updatePool = useMutation({
    mutationFn: ({ row, enabled }: { row: LimitBoardRow; enabled: boolean }) => api.limitBoardPoolUpdate(row.symbol, enabled, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const removePool = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardPoolRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
  })

  const selectedSymbols = useMemo(() => new Set((view.data?.selected ?? []).map(row => row.symbol)), [view.data?.selected])
  const poolSymbols = useMemo(() => new Set((view.data?.board_pool ?? []).map(row => row.symbol)), [view.data?.board_pool])
  const busy = add.isPending || remove.isPending || addPool.isPending || updatePool.isPending || removePool.isPending
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="打板专区加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'first' ? data.first_board : tab === 'selected' ? data.selected : data.board_pool
  const tableMode: TableMode = tab === 'pool' ? 'pool' : tab === 'selected' ? 'selected' : 'first'
  const tableTitle = tab === 'first' ? '全市场首板候选' : tab === 'selected' ? '手工精选股票' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? '自动打板默认关闭；开启后在新鲜行情首次触及涨停价时提交 1 手限价委托'
    : '临近涨停后自动进入 WS，可手动加入打板池'

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="打板专区"
        titleExtra={<span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-[10px] text-secondary"><Radio className="h-3 w-3 text-accent" />{runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>}
        right={<div className="flex items-center gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入精选" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchQuery.data?.results?.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchQuery.data.results.map((item: any) => <button type="button" key={item.symbol} disabled={selectedSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
      />

      <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2 text-[11px] text-muted sm:px-5">
        <span className={`inline-flex items-center gap-1.5 ${runtime.websocket_status === 'connected' ? 'text-bear' : 'text-muted'}`}><Wifi className="h-3.5 w-3.5" />{runtime.websocket_status === 'connected' ? '行情已接入 WS' : '等待 WS 候选'}</span>
        <span>默认通知：触板、炸板、回封</span>
        <span className={runtime.trading_enabled ? 'text-bear' : 'text-warning'}>{runtime.trading_reason}</span>
        {!runtime.first_board_enabled ? <span className="text-warning">首板扫描暂不可用：需要全市场实时行情和历史涨停校验</span> : <span>{runtime.history_reason}</span>}
      </div>

      <div className="flex items-center gap-1 overflow-x-auto border-b border-border px-4 pt-2 sm:px-5">
        {([
          ['first', '首板扫描', data.first_board.length, Flame],
          ['selected', '精选跟踪', data.selected.length, CheckCircle2],
          ['pool', '打板池', data.board_pool.length, Crosshair],
          ['events', '触发记录', data.events.length, Bell],
        ] as const).map(([id, label, count, Icon]) => (
          <button key={id} type="button" onClick={() => setTab(id)} className={`inline-flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === id ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}>
            <Icon className="h-3.5 w-3.5" />{label}<span className="font-mono text-[10px] text-muted">{count}</span>
          </button>
        ))}
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-4 py-3 sm:px-5">
        {tab !== 'events' ? (
          <section className="overflow-hidden rounded-btn border border-border bg-surface">
            <div className="flex items-center justify-between border-b border-border px-3 py-2.5"><div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>{runtime.last_error ? <span className="text-[10px] text-warning">{runtime.last_error}</span> : null}</div>
            <Table
              rows={rows}
              mode={tableMode}
              poolSymbols={poolSymbols}
              busy={busy}
              onOpen={setPreview}
              onAddPool={row => addPool.mutate({ row, source: tableMode === 'selected' ? 'selected' : 'first_board' })}
              onRemoveSelected={row => remove.mutate(row)}
              onToggleAuto={(row, enabled) => updatePool.mutate({ row, enabled })}
              onRemovePool={row => removePool.mutate(row)}
            />
          </section>
        ) : (
          <section className="divide-y divide-border overflow-hidden rounded-btn border border-border bg-surface">
            {data.events.length ? data.events.map((event: any, index: number) => <div key={`${event.ts}-${index}`} className="flex items-start gap-3 px-3 py-3 text-xs"><span className={event.type === 'broken' ? 'text-danger' : event.type === 'resealed' ? 'text-bull' : 'text-accent'}>{STATUS[event.type]?.label || event.type}</span><div className="min-w-0 flex-1"><button type="button" onClick={() => setPreview({ symbol: event.symbol, name: event.name })} className="font-medium hover:text-accent" title="查看 K 线与分时">{event.name} <span className="ml-1 font-mono text-[10px] text-muted">{event.symbol}</span></button><div className="mt-1 text-[11px] text-secondary">{event.reasons?.join('；')}</div></div><div className="text-right text-[10px] text-muted"><div>炸板 {event.break_count || 0} 次</div><div>{new Date(event.ts).toLocaleTimeString('zh-CN')}</div></div></div>) : <div className="px-4 py-12 text-center text-xs text-muted">今天还没有触板、炸板或回封记录</div>}
          </section>
        )}
      </div>

      {data.blacklist.length ? <div className="flex items-center gap-2 border-t border-border px-4 py-2 text-[10px] text-danger sm:px-5"><Ban className="h-3.5 w-3.5" />今日黑名单：{data.blacklist.join('、')}</div> : null}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
