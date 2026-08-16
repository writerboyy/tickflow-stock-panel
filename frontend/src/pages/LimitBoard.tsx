import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Ban,
  Bell,
  Check,
  CircleDot,
  Crosshair,
  Flame,
  ListFilter,
  Plus,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  Trash2,
  Wifi,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { api, type LimitBoardRow, type LimitBoardView } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

type Tab = 'first' | 'candidate' | 'pool' | 'events'
type TableMode = Exclude<Tab, 'events'>
type NotificationSettings = LimitBoardView['settings']['notifications']

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

function themes(value: unknown): string[] {
  const values = Array.isArray(value) ? value : [value]
  const result: string[] = []
  for (const raw of values) {
    for (const item of String(raw ?? '').split(/[、,，;；]/)) {
      const theme = item.trim()
      if (theme && !result.includes(theme)) result.push(theme)
    }
  }
  return result
}

function isStName(name: unknown): boolean {
  return String(name ?? '').toUpperCase().includes('ST')
}

interface RowProps {
  row: LimitBoardRow
  mode: TableMode
  inPool: boolean
  busy: boolean
  onOpen: () => void
  onAddPool: () => void
  onRemoveCandidate: () => void
  onToggleAuto: (enabled: boolean) => void
  onChangeOrderMode: (mode: 'sweep' | 'queue') => void
  onRemovePool: () => void
}

function Row({
  row,
  mode,
  inPool,
  busy,
  onOpen,
  onAddPool,
  onRemoveCandidate,
  onToggleAuto,
  onChangeOrderMode,
  onRemovePool,
}: RowProps) {
  const status = STATUS[row.status || 'watching'] || STATUS.watching
  const gap = row.limit_gap_pct == null ? '--' : `${(row.limit_gap_pct * 100).toFixed(2)}%`
  const rebound = row.source === 'rebound_board' || row.source_modes?.includes('rebound_board')
  const allThemes = themes(row.concept)
  const visibleThemes = allThemes.slice(0, 2)
  const orderMode = row.order_mode === 'queue' ? 'queue' : 'sweep'
  const orderStatus = !row.auto_trade && !row.auto_order_key
    ? { label: '未开启', tone: 'text-muted' }
    : row.auto_order_status
    ? ORDER_STATUS[row.auto_order_status] || { label: row.auto_order_status, tone: 'text-muted' }
    : { label: '等待触板', tone: 'text-muted' }

  return (
    <tr className="group border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="sticky left-0 z-10 w-[128px] min-w-[128px] max-w-[128px] bg-surface py-2.5 pl-3 pr-2 group-hover:bg-elevated">
        <button type="button" onClick={onOpen} className="block w-full text-left hover:text-accent" title="查看 K 线与分时">
          <div className="truncate font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
          {mode !== 'pool' && rebound ? <div className="mt-0.5 text-[10px] text-warning">反包候选</div> : null}
          {mode !== 'pool' && row.limit_up_count != null ? <div className="mt-0.5 whitespace-nowrap text-[9px] text-muted" title={`涨停 ${row.limit_up_count} 次；次日红盘率 ${((row.next_day_red_rate ?? 0) * 100).toFixed(0)}%；首板破板率 ${((row.first_board_broken_rate ?? 0) * 100).toFixed(0)}%`}>
            {row.limit_up_count}次 · 红{((row.next_day_red_rate ?? 0) * 100).toFixed(0)}% · 破{((row.first_board_broken_rate ?? 0) * 100).toFixed(0)}%
          </div> : null}
        </button>
      </td>
      <td className="w-[160px] max-w-[160px] px-2">
        <div className="truncate text-[10px] text-secondary" title={allThemes.join('、') || undefined}>
          {visibleThemes.length ? visibleThemes.join('、') : '--'}
        </div>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-accent">{row.limit_up?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-warning">{gap}</td>
      {mode === 'candidate' ? (
        <td className="px-2 font-mono tabular-nums text-accent" title={(row.candidate_reasons || []).join('；')}>
          #{row.candidate_rank ?? '--'} <span className="text-[10px] text-muted">{row.candidate_score?.toFixed(1) ?? '--'}</span>
        </td>
      ) : null}
      <td className="px-2">
        <span className={`inline-flex items-center gap-1 font-medium ${status.tone}`}>
          <CircleDot className="h-3 w-3" />{status.label}
        </span>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.break_count ? `${row.break_count} 次` : '0 次'}</td>
      <td className="px-2 font-mono tabular-nums text-secondary">{row.bid1_volume ? row.bid1_volume.toLocaleString('zh-CN') : '--'}</td>
      <td className="px-2">
        <span className={mode === 'pool' && row.ws_active ? 'text-bear' : 'text-muted'}>{mode === 'pool' && row.ws_active ? 'WS' : '轮询'}</span>
      </td>
      {mode === 'pool' ? (
        <>
          <td className={`px-2 font-medium ${orderStatus.tone}`} title={row.auto_order_error || undefined}>
            {orderStatus.label}
          </td>
          <td className="sticky right-0 z-10 border-l border-border bg-surface px-2 text-right group-hover:bg-elevated">
            <div className="flex items-center justify-end gap-1.5">
              <div className="inline-flex h-7 overflow-hidden rounded-btn border border-border" aria-label="打板方式">
                {([
                  ['sweep', '扫板', '新鲜五档盘口中卖一距涨停价不超过 5 个价位时提交'],
                  ['queue', '排板', '价格已触及涨停价后再提交排队'],
                ] as const).map(([mode, label, title]) => <button
                  key={mode}
                  type="button"
                  title={title}
                  disabled={busy}
                  onClick={() => onChangeOrderMode(mode)}
                  className={`px-2 text-[10px] ${orderMode === mode ? 'bg-accent/15 text-accent' : 'text-muted hover:bg-elevated hover:text-foreground'} disabled:opacity-40`}
                >{label}</button>)}
              </div>
              <label className="inline-flex items-center gap-1 whitespace-nowrap text-secondary" title="自动打板">
                <input
                  type="checkbox"
                  checked={row.auto_trade === true}
                  disabled={busy}
                  onChange={event => onToggleAuto(event.target.checked)}
                />
                {row.auto_trade ? '已开启' : '已关闭'}
              </label>
              <button type="button" title="移出打板池" disabled={busy} onClick={onRemovePool} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger disabled:opacity-40">
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            </div>
          </td>
        </>
      ) : (
        <td className="sticky right-0 z-10 border-l border-border bg-surface px-2 group-hover:bg-elevated">
          <div className="flex items-center justify-end gap-1">
            <button
              type="button"
              title={inPool ? '已在打板池' : '加入打板池'}
              disabled={inPool || busy}
              onClick={onAddPool}
              className={`inline-flex h-7 items-center gap-1 rounded-btn border px-2 ${inPool ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-accent/40 hover:text-accent'} disabled:opacity-60`}
            >
              {inPool ? <Check className="h-3.5 w-3.5" /> : <Crosshair className="h-3.5 w-3.5" />}
              {inPool ? '已加入' : '打板'}
            </button>
            {mode === 'candidate' ? <button type="button" title="从备选池删除，当日自动候选不再回流" disabled={busy} onClick={onRemoveCandidate} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger disabled:opacity-40">
              <Trash2 className="h-3.5 w-3.5" />
            </button> : null}
          </div>
        </td>
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
  onRemoveCandidate: (row: LimitBoardRow) => void
  onToggleAuto: (row: LimitBoardRow, enabled: boolean) => void
  onChangeOrderMode: (row: LimitBoardRow, mode: 'sweep' | 'queue') => void
  onRemovePool: (row: LimitBoardRow) => void
}

function Table(props: TableProps) {
  const { rows, mode } = props
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="max-w-full overflow-x-auto overscroll-x-contain" style={{ WebkitOverflowScrolling: 'touch' }}>
      <table className="w-full min-w-[1080px] border-collapse">
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="sticky left-0 z-20 w-[128px] bg-surface py-2 pl-3 pr-2">标的</th><th className="w-[160px] px-2">题材</th><th className="px-2">现价</th><th className="px-2">涨停价</th><th className="px-2">距涨停</th>{mode === 'candidate' ? <th className="px-2">排序</th> : null}<th className="px-2">状态</th><th className="px-2">炸板次数</th><th className="px-2">买一封单</th><th className="px-2">行情</th>
            {mode === 'pool' ? <><th className="px-2">委托状态</th><th className="sticky right-0 z-20 w-[220px] border-l border-border bg-surface px-2 text-right">操作</th></> : <th className="sticky right-0 z-20 w-[96px] border-l border-border bg-surface px-2 text-right">操作</th>}
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
              onRemoveCandidate={() => props.onRemoveCandidate(row)}
              onToggleAuto={enabled => props.onToggleAuto(row, enabled)}
              onChangeOrderMode={mode => props.onChangeOrderMode(row, mode)}
              onRemovePool={() => props.onRemovePool(row)}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

function NotificationDialog({
  value,
  pending,
  onClose,
  onSave,
}: {
  value: NotificationSettings
  pending: boolean
  onClose: () => void
  onSave: (value: NotificationSettings) => void
}) {
  const [draft, setDraft] = useState(value)
  return <Modal labelledBy="limit-board-notification-title" onClose={onClose} closeOnBackdrop={!pending} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl">
    <div className="border-b border-border px-4 py-3"><h2 id="limit-board-notification-title" className="text-sm font-semibold">通知设置</h2></div>
    <div className="divide-y divide-border px-4">
      {([
        ['touched', '触板'],
        ['broken', '炸板'],
        ['resealed', '回封'],
      ] as const).map(([key, label]) => <label key={key} className="flex items-center justify-between py-3 text-xs"><span>{label}</span><input type="checkbox" checked={draft[key]} disabled={pending} onChange={event => setDraft(current => ({ ...current, [key]: event.target.checked }))} /></label>)}
    </div>
    <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" onClick={onClose} disabled={pending} className="h-8 rounded-btn border border-border px-3 text-xs text-muted disabled:opacity-50">取消</button><button type="button" onClick={() => onSave(draft)} disabled={pending} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-50"><Check className="h-3.5 w-3.5" />{pending ? '保存中…' : '保存'}</button></div>
  </Modal>
}

export function LimitBoard() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('first')
  const [search, setSearch] = useState('')
  const [preview, setPreview] = useState<LimitBoardRow | null>(null)
  const [notificationOpen, setNotificationOpen] = useState(false)
  const view = useQuery({ queryKey: QK.limitBoard, queryFn: api.limitBoard, refetchInterval: 5000, placeholderData: previous => previous })
  const searchQuery = useQuery({
    queryKey: QK.instrumentSearch(search, 'stock'),
    queryFn: () => api.instrumentSearch(search, 10, 'stock'),
    enabled: search.trim().length >= 2,
  })
  const refresh = () => queryClient.invalidateQueries({ queryKey: QK.limitBoard })
  const add = useMutation({
    mutationFn: (symbol: string) => api.limitBoardCandidateAdd(symbol, view.data?.revision ?? 0),
    onSuccess: () => { setSearch(''); refresh() },
  })
  const addPool = useMutation({
    mutationFn: ({ row, source }: { row: LimitBoardRow; source: 'first_board' | 'rebound_board' | 'manual' }) => api.limitBoardPoolAdd(row.symbol, source, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const removeCandidate = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardCandidateRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const updatePool = useMutation({
    mutationFn: ({ row, enabled, orderMode }: { row: LimitBoardRow; enabled: boolean; orderMode: 'sweep' | 'queue' }) => api.limitBoardPoolUpdate(row.symbol, enabled, orderMode, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const removePool = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardPoolRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const updateNotifications = useMutation({
    mutationFn: (notifications: NotificationSettings) => (
      api.limitBoardNotificationsUpdate(notifications, view.data?.revision ?? 0)
    ),
    onSuccess: async () => {
      await refresh()
      setNotificationOpen(false)
    },
  })

  const poolSymbols = useMemo(() => new Set((view.data?.board_pool ?? []).map(row => row.symbol)), [view.data?.board_pool])
  const candidateSymbols = useMemo(() => new Set([
    ...(view.data?.candidate_pool ?? []).map(row => row.symbol),
    ...(view.data?.board_pool ?? []).map(row => row.symbol),
  ]), [view.data?.candidate_pool, view.data?.board_pool])
  const searchResults = (searchQuery.data?.results ?? []).filter(item => !isStName(item.name))
  const busy = add.isPending || addPool.isPending || removeCandidate.isPending || updatePool.isPending || removePool.isPending || updateNotifications.isPending
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="打板专区加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'first' ? data.first_board : tab === 'candidate' ? data.candidate_pool : data.board_pool
  const tableMode: TableMode = tab === 'pool' ? 'pool' : tab === 'candidate' ? 'candidate' : 'first'
  const tableTitle = tab === 'first' ? '全市场首板/反包候选' : tab === 'candidate' ? '备选池' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? '默认扫板：卖一距涨停不超过 5 个价位时提交；排板在触及涨停后提交'
    : tab === 'candidate'
    ? '自动候选通过历史门槛后与手工标的合并，备选池仅使用实时轮询'
    : '自动过滤：近 200 日涨停≥4次、次日红盘率≥80%、首板破板率≤75%；不接入 WS'

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="打板专区"
        titleExtra={<span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-[10px] text-secondary"><Radio className="h-3 w-3 text-accent" />打板池 {runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>}
        right={<div className="flex items-center gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入备选池" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchResults.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchResults.map(item => <button type="button" key={item.symbol} disabled={candidateSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" onClick={() => setNotificationOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><Bell className="h-3.5 w-3.5" />通知设置</button><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
      />

      <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2 text-[11px] text-muted sm:px-5">
        <span className={`inline-flex items-center gap-1.5 ${runtime.websocket_status === 'connected' ? 'text-bear' : 'text-muted'}`}><Wifi className="h-3.5 w-3.5" />{runtime.websocket_status === 'connected' ? '打板池已接入 WS' : '备选池仅实时轮询'}</span>
        <span className={runtime.trading_enabled ? 'text-bear' : 'text-warning'}>{runtime.trading_reason}</span>
        {!runtime.first_board_enabled ? <span className="text-warning">首板/反包扫描暂不可用：{runtime.history_reason}</span> : <span>{runtime.history_reason}</span>}
      </div>

      <div className="flex items-center gap-1 overflow-x-auto border-b border-border px-4 pt-2 sm:px-5">
        {([
          ['first', '首板/反包', data.first_board.length, Flame],
          ['candidate', '备选池', data.candidate_pool.length, ListFilter],
          ['pool', '打板池', data.board_pool.length, Crosshair],
          ['events', '触发记录', data.events.length, Bell],
        ] as const).map(([id, label, count, Icon]) => (
          <button key={id} type="button" onClick={() => setTab(id)} className={`inline-flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === id ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}>
            <Icon className="h-3.5 w-3.5" />{label}<span className="font-mono text-[10px] text-muted">{count}</span>
          </button>
        ))}
      </div>

      <div className="min-h-0 flex-1 overflow-x-hidden overflow-y-auto px-2 py-3 sm:px-5">
        {tab !== 'events' ? (
          <section className="overflow-hidden rounded-btn border border-border bg-surface">
            <div className="flex items-center justify-between border-b border-border px-3 py-2.5"><div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>{runtime.last_error ? <span className="text-[10px] text-warning">{runtime.last_error}</span> : null}</div>
            <Table
              rows={rows}
              mode={tableMode}
              poolSymbols={poolSymbols}
              busy={busy}
              onOpen={setPreview}
              onAddPool={row => addPool.mutate({
                row,
                source: row.source === 'manual' || row.source === 'selected'
                  ? 'manual'
                  : row.source === 'rebound_board' ? 'rebound_board' : 'first_board',
              })}
              onRemoveCandidate={row => removeCandidate.mutate(row)}
              onToggleAuto={(row, enabled) => updatePool.mutate({ row, enabled, orderMode: row.order_mode === 'queue' ? 'queue' : 'sweep' })}
              onChangeOrderMode={(row, orderMode) => updatePool.mutate({ row, enabled: row.auto_trade === true, orderMode })}
              onRemovePool={row => removePool.mutate(row)}
            />
          </section>
        ) : (
          <section className="divide-y divide-border overflow-hidden rounded-btn border border-border bg-surface">
            {data.events.length ? data.events.map((event: any, index: number) => {
              const eventThemes = themes(event.concept).slice(0, 2)
              return <div key={`${event.ts}-${index}`} className="flex items-start gap-3 px-3 py-3 text-xs"><span className={event.type === 'broken' ? 'text-danger' : event.type === 'resealed' ? 'text-bull' : 'text-accent'}>{STATUS[event.type]?.label || event.type}</span><div className="min-w-0 flex-1"><button type="button" onClick={() => setPreview({ symbol: event.symbol, name: event.name })} className="font-medium hover:text-accent" title="查看 K 线与分时">{event.name} <span className="ml-1 font-mono text-[10px] text-muted">{event.symbol}</span></button>{eventThemes.length ? <div className="mt-1 truncate text-[10px] text-secondary">题材：{eventThemes.join('、')}</div> : null}<div className="mt-1 text-[11px] text-secondary">{event.reasons?.join('；')}</div></div><div className="text-right text-[10px] text-muted"><div>炸板 {event.break_count || 0} 次</div><div>{new Date(event.ts).toLocaleTimeString('zh-CN')}</div></div></div>
            }) : <div className="px-4 py-12 text-center text-xs text-muted">今天还没有触板、炸板或回封记录</div>}
          </section>
        )}
      </div>

      {data.blacklist.length ? <div className="flex items-center gap-2 border-t border-border px-4 py-2 text-[10px] text-danger sm:px-5"><Ban className="h-3.5 w-3.5" />今日黑名单：{data.blacklist.join('、')}</div> : null}
      {notificationOpen ? <NotificationDialog value={data.settings.notifications} pending={updateNotifications.isPending} onClose={() => setNotificationOpen(false)} onSave={value => updateNotifications.mutate(value)} /> : null}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
