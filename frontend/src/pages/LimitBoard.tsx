import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Ban, Bell, CheckCircle2, CircleDot, Flame, Plus, Radio, RefreshCw, Search, ShieldAlert, Trash2, Wifi } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { api, type LimitBoardRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const STATUS: Record<string, { label: string; tone: string }> = {
  watching: { label: '观察中', tone: 'text-muted' },
  near_limit: { label: '临板', tone: 'text-warning' },
  touched: { label: '触板', tone: 'text-accent' },
  sealed: { label: '封板', tone: 'text-bull' },
  broken: { label: '炸板', tone: 'text-danger' },
  resealed: { label: '回封', tone: 'text-bull' },
  blacklisted: { label: '今日黑名单', tone: 'text-danger' },
}

function Row({ row, onRemove, removable }: { row: LimitBoardRow; onRemove?: () => void; removable?: boolean }) {
  const status = STATUS[row.status || 'watching'] || STATUS.watching
  const gap = row.limit_gap_pct == null ? '--' : `${(row.limit_gap_pct * 100).toFixed(2)}%`
  return (
    <tr className="border-t border-border/70 text-[11px]">
      <td className="py-2.5 pl-3 pr-2">
        <div className="font-medium">{row.name || row.symbol}</div>
        <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
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
        <span className={row.ws_active ? 'text-bull' : 'text-muted'}>{row.ws_active ? 'WS' : '快照'}</span>
      </td>
      {removable ? <td className="pr-3 text-right"><button type="button" title="移除精选跟踪" onClick={onRemove} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger"><Trash2 className="h-3.5 w-3.5" /></button></td> : null}
    </tr>
  )
}

function Table({ rows, onRemove, removable }: { rows: LimitBoardRow[]; onRemove?: (row: LimitBoardRow) => void; removable?: boolean }) {
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] border-collapse">
        <thead className="text-left text-[10px] uppercase tracking-wide text-muted"><tr><th className="py-2 pl-3 pr-2">标的</th><th className="px-2">现价</th><th className="px-2">涨停价</th><th className="px-2">距涨停</th><th className="px-2">状态</th><th className="px-2">炸板次数</th><th className="px-2">买一封单</th><th className="px-2">行情</th>{removable ? <th /> : null}</tr></thead>
        <tbody>{rows.map(row => <Row key={row.symbol} row={row} removable={removable} onRemove={() => onRemove?.(row)} />)}</tbody>
      </table>
    </div>
  )
}

export function LimitBoard() {
  const qc = useQueryClient()
  const [tab, setTab] = useState<'first' | 'selected' | 'events'>('first')
  const [search, setSearch] = useState('')
  const view = useQuery({ queryKey: QK.limitBoard, queryFn: api.limitBoard, refetchInterval: 5000, placeholderData: prev => prev })
  const searchQuery = useQuery({
    queryKey: QK.instrumentSearch(search, 'stock'),
    queryFn: () => api.instrumentSearch(search, 10, 'stock'),
    enabled: search.trim().length >= 2,
  })
  const add = useMutation({
    mutationFn: (symbol: string) => api.limitBoardAdd(symbol, view.data?.revision ?? 0),
    onSuccess: () => { setSearch(''); qc.invalidateQueries({ queryKey: QK.limitBoard }) },
  })
  const remove = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: () => qc.invalidateQueries({ queryKey: QK.limitBoard }),
  })
  const selectedSymbols = useMemo(() => new Set((view.data?.selected ?? []).map(row => row.symbol)), [view.data?.selected])
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="打板专区加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'first' ? data.first_board : data.selected
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader title="打板专区" titleExtra={<span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-[10px] text-secondary"><Radio className="h-3 w-3 text-accent" />{runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>} right={<div className="flex items-center gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入精选" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchQuery.data?.results?.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchQuery.data.results.map((item: any) => <button type="button" key={item.symbol} disabled={selectedSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>} />
      <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2 text-[11px] text-muted sm:px-5"><span className="inline-flex items-center gap-1.5"><Wifi className="h-3.5 w-3.5 text-bull" />{runtime.websocket_status === 'connected' ? '行情已接入 WS' : '等待 WS 候选'}</span><span>默认通知：触板、炸板、回封</span><span className="text-secondary">交易接口：{runtime.trading_enabled ? '已接入' : runtime.trading_reason}</span>{!runtime.first_board_enabled ? <span className="text-warning">首板扫描暂不可用：需要全市场实时行情和历史涨停校验</span> : <span className="text-muted">{runtime.history_reason}</span>}</div>
      <div className="flex items-center gap-1 border-b border-border px-4 pt-2 sm:px-5"><button type="button" onClick={() => setTab('first')} className={`inline-flex items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === 'first' ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}><Flame className="h-3.5 w-3.5" />首板扫描<span className="font-mono text-[10px] text-muted">{data.first_board.length}</span></button><button type="button" onClick={() => setTab('selected')} className={`inline-flex items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === 'selected' ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}><CheckCircle2 className="h-3.5 w-3.5" />精选跟踪<span className="font-mono text-[10px] text-muted">{data.selected.length}</span></button><button type="button" onClick={() => setTab('events')} className={`inline-flex items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === 'events' ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}><Bell className="h-3.5 w-3.5" />触发记录<span className="font-mono text-[10px] text-muted">{data.events.length}</span></button></div>
      <div className="min-h-0 flex-1 overflow-auto px-4 py-3 sm:px-5">{tab !== 'events' ? <section className="overflow-hidden rounded-btn border border-border bg-surface"><div className="flex items-center justify-between border-b border-border px-3 py-2.5"><div><div className="text-xs font-medium">{tab === 'first' ? '全市场首板候选' : '手工精选股票'}</div><div className="mt-0.5 text-[10px] text-muted">临近涨停后自动进入 WS，当前只发送通知</div></div>{runtime.last_error ? <span className="text-[10px] text-warning">{runtime.last_error}</span> : null}</div><Table rows={rows} removable={tab === 'selected'} onRemove={row => remove.mutate(row)} /></section> : <section className="divide-y divide-border overflow-hidden rounded-btn border border-border bg-surface">{data.events.length ? data.events.map((event: any, index: number) => <div key={`${event.ts}-${index}`} className="flex items-start gap-3 px-3 py-3 text-xs"><span className={event.type === 'broken' ? 'text-danger' : event.type === 'resealed' ? 'text-bull' : 'text-accent'}>{STATUS[event.type]?.label || event.type}</span><div className="min-w-0 flex-1"><div className="font-medium">{event.name} <span className="ml-1 font-mono text-[10px] text-muted">{event.symbol}</span></div><div className="mt-1 text-[11px] text-secondary">{event.reasons?.join('；')}</div></div><div className="text-right text-[10px] text-muted"><div>炸板 {event.break_count || 0} 次</div><div>{new Date(event.ts).toLocaleTimeString('zh-CN')}</div></div></div>) : <div className="px-4 py-12 text-center text-xs text-muted">今天还没有触板、炸板或回封记录</div>}</section>}</div>
      {data.blacklist.length ? <div className="flex items-center gap-2 border-t border-border px-4 py-2 text-[10px] text-danger sm:px-5"><Ban className="h-3.5 w-3.5" />今日黑名单：{data.blacklist.join('、')}</div> : null}
    </div>
  )
}
