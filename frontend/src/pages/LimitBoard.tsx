import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Ban,
  Bell,
  BookOpen,
  Check,
  CircleHelp,
  CircleDot,
  Crosshair,
  Database,
  Flame,
  GitBranch,
  Clock3,
  Layers3,
  ListFilter,
  PanelRightClose,
  PanelRightOpen,
  Plus,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  SlidersHorizontal,
  Trash2,
  WalletCards,
  Wifi,
  X,
  Zap,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { PageHeader } from '@/components/PageHeader'
import { QMT_ALLOCATION_OPTIONS, QmtTradePanel, type QmtAllocationMode } from '@/components/QmtTradePanel'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import {
  api,
  type LimitBoardEvent,
  type LimitBoardQuoteSnapshot,
  type LimitBoardRow,
  type LimitBoardSectorConstituent,
  type LimitBoardSectorStrengthRow,
  type LimitBoardView,
  type MarketHeatItem,
  type FourModeStrategyView,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const EmbeddedLimitLadder = lazy(() => import('./LimitUpLadder').then(module => ({ default: module.LimitUpLadder })))

type Tab = 'ladder' | 'sector' | 'candidate' | 'opportunity' | 'pool' | 'four_mode' | 'events'
type TableMode = 'candidate' | 'pool'
type NotificationSettings = LimitBoardView['settings']['notifications']
type AdvancedSettings = Omit<LimitBoardView['settings'], 'notifications'>

const STATUS: Record<string, { label: string; tone: string }> = {
  watching: { label: '观察中', tone: 'text-muted' },
  near_limit: { label: '临板', tone: 'text-warning' },
  touched: { label: '涨停', tone: 'text-accent' },
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

function scorePct(value: number | null | undefined, digits = 1): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(digits)}%`
}

function ratioPct(value: number | null | undefined, digits = 0): string {
  return value == null || !Number.isFinite(value) ? '--' : `${(value * 100).toFixed(digits)}%`
}

function scoreTime(value: string | null | undefined): string {
  if (!value) return '--'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? '--' : parsed.toLocaleTimeString('zh-CN', { hour12: false })
}

function exactTime(value: string | number | null | undefined): string {
  if (value == null || value === '') return '--'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return '--'
  return [
    String(parsed.getHours()).padStart(2, '0'),
    String(parsed.getMinutes()).padStart(2, '0'),
    String(parsed.getSeconds()).padStart(2, '0'),
  ].join(':') + `.${String(parsed.getMilliseconds()).padStart(3, '0')}`
}

function elapsedTime(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--'
  if (Math.abs(value) < 1000) return `${value} 毫秒`
  return `${(value / 1000).toFixed(3)} 秒`
}

function percentValue(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
}

function plainPercentValue(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value.toFixed(2)}%`
}

function moneyYi(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--'
  const amount = value / 100_000_000
  const digits = Math.abs(amount) >= 1000 ? 0 : Math.abs(amount) >= 100 ? 1 : 2
  return `${amount.toFixed(digits)}亿`
}

function financialTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value === 0) return 'text-secondary'
  return value > 0 ? 'text-bull' : 'text-bear'
}

const LEADERSHIP = {
  leader: '龙头',
  front: '前排',
  follower: '跟随',
} as const

interface RowProps {
  row: LimitBoardRow
  mode: TableMode
  inPool: boolean
  busy: boolean
  sweepPriceLevels: number
  queueWaitSeconds: number
  queueConfirmSnapshots: number
  onOpen: () => void
  onAddPool: () => void
  onRemoveCandidate: () => void
  onToggleAuto: (enabled: boolean) => void
  onChangeOrderMode: (mode: 'sweep' | 'queue') => void
  onRemovePool: () => void
  onTrade: () => void
}

function Row({
  row,
  mode,
  inPool,
  busy,
  sweepPriceLevels,
  queueWaitSeconds,
  queueConfirmSnapshots,
  onOpen,
  onAddPool,
  onRemoveCandidate,
  onToggleAuto,
  onChangeOrderMode,
  onRemovePool,
  onTrade,
}: RowProps) {
  const status = STATUS[row.status || 'watching'] || STATUS.watching
  const gap = row.limit_gap_pct == null ? '--' : `${(row.limit_gap_pct * 100).toFixed(2)}%`
  const atLimit = row.limit_gap_pct != null && row.limit_gap_pct <= 0.0001
  const change = row.change_pct == null ? '--' : `${scorePct(row.change_pct, 2)}${atLimit ? '（涨停）' : ''}`
  const rebound = row.source === 'rebound_board' || row.source_modes?.includes('rebound_board')
  const allThemes = themes(row.concept)
  const visibleThemes = allThemes.slice(0, 2)
  const scoreDetail = row.candidate_score_detail
  const intradayFlow = scoreDetail?.intraday_flow
  const sector = scoreDetail?.sector
  const gene = scoreDetail?.premium_gene
  const technical = scoreDetail?.technical
  const leadership = LEADERSHIP[sector?.leadership ?? 'follower']
  const orderMode = row.order_mode === 'queue' ? 'queue' : 'sweep'
  const orderStatus = !row.auto_trade && !row.auto_order_key
    ? { label: '未开启', tone: 'text-muted' }
    : row.auto_order_status
    ? ORDER_STATUS[row.auto_order_status] || { label: row.auto_order_status, tone: 'text-muted' }
    : { label: '等待涨停', tone: 'text-muted' }

  return (
    <tr className="group border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="sticky left-0 z-30 w-[128px] min-w-[128px] max-w-[128px] overflow-hidden bg-surface py-2.5 pl-3 pr-2 group-hover:bg-elevated">
        <button type="button" onClick={onOpen} className="block w-full text-left hover:text-accent" title="查看 K 线与分时">
          <div className="truncate font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
          {mode !== 'pool' && rebound ? <div className="mt-0.5 text-[10px] text-warning">反包候选</div> : null}
        </button>
      </td>
      <td className="w-[160px] max-w-[160px] px-2">
        <div className="truncate text-[10px] text-secondary" title={allThemes.join('、') || undefined}>
          {visibleThemes.length ? visibleThemes.join('、') : '--'}
        </div>
        {mode !== 'candidate' && sector?.realtime_available ? <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted" title="开盘啦实时板块强度">板强 {sector.realtime_strength?.toFixed(1) ?? '--'} · #{sector.realtime_rank ?? '--'}/{sector.realtime_rank_count ?? '--'} · {scorePct(sector.realtime_change_pct, 2)}</div> : null}
      </td>
      {mode === 'candidate' ? <>
        <td className="w-[116px] min-w-[116px] px-2" title={(row.candidate_reasons || []).join('；')}>
          {row.candidate_score == null ? <div className={intradayFlow?.capital_available === false ? 'text-warning' : 'text-muted'}>{intradayFlow?.capital_available === false ? '实时资金待补' : '待补数据'}</div> : <>
            <div className="font-mono text-sm font-semibold tabular-nums text-accent">强 {row.candidate_score.toFixed(1)}</div>
            <div className="mt-0.5 font-mono text-[9px] text-bull">机会 {row.entry_rank != null ? `#${row.entry_rank} ` : ''}{row.entry_score == null ? '--' : row.entry_score.toFixed(1)}{row.candidate_score_velocity != null ? ` · ${row.candidate_score_velocity >= 0 ? '+' : ''}${row.candidate_score_velocity.toFixed(1)}` : ''}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">板{sector?.score.toFixed(1)} 基{gene?.score.toFixed(1)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">分{intradayFlow?.score.toFixed(1)} 技{technical?.score.toFixed(1)}</div>
          </>}
          {row.candidate_score_state === 'cached' ? <div className="mt-0.5 whitespace-nowrap text-[9px] text-warning">缓存 · {scoreTime(row.candidate_score_as_of)}</div> : null}
        </td>
        <td className="w-[118px] min-w-[118px] px-2" title={`涨幅 ${change}；距涨停 ${gap}；${status.label}`}>
          <div className="font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'} <span className="text-secondary">{change}</span></div>
          <div className="mt-0.5 flex items-center gap-1.5 text-[9px]"><span className="font-mono text-muted">距涨停 {gap}</span><span className={status.tone}>{status.label}</span></div>
          <div className="mt-0.5 truncate text-[9px] text-secondary">{row.tradability_state === 'tradable' ? '可交易机会' : row.tradability_reason || '待观察'}</div>
        </td>
        <td className="w-[292px] min-w-[292px] px-2" title={(row.candidate_reasons || []).join('；')}>
          <div className="truncate text-[10px] text-secondary">板 {sector?.name || '--'} · {leadership} · {sector?.score.toFixed(1) ?? '--'}/50</div>
          <div className="mt-0.5 font-mono text-[9px] text-muted">基 涨{gene?.limit_up_count ?? '--'} · 红{scorePct(gene?.next_day_red_rate, 0)} · {gene?.score.toFixed(1) ?? '--'}/30</div>
          <div className="mt-0.5 font-mono text-[9px] text-muted">分 {intradayFlow?.trend_state === 'strong' ? '强' : intradayFlow?.trend_state === 'weak' ? '弱' : '中'} · 资金 {intradayFlow?.capital_available ? intradayFlow.capital_score?.toFixed(1) ?? '--' : '待补'} · {intradayFlow?.score.toFixed(1) ?? '--'}/15</div>
          <div className="mt-0.5 font-mono text-[9px] text-muted">技 量比 {technical?.vol_ratio_5d?.toFixed(2) ?? '--'} · RSI {technical?.rsi_14?.toFixed(0) ?? '--'} · {technical?.score.toFixed(1) ?? '--'}/5</div>
        </td>
      </> : null}
      {mode !== 'candidate' ? <>
        <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
        <td className="px-2 font-mono tabular-nums text-secondary">{row.limit_up?.toFixed(2) ?? '--'}</td>
        <td className="px-2 font-mono tabular-nums text-secondary">{gap}</td>
        <td className="px-2">
          <span className={`inline-flex items-center gap-1 font-medium ${status.tone}`}>
            <CircleDot className="h-3 w-3" />{status.label}
          </span>
        </td>
        <td className="px-2 font-mono tabular-nums">{row.break_count ? `${row.break_count} 次` : '0 次'}</td>
      </> : null}
      {mode !== 'candidate' ? <td className="px-2 font-mono tabular-nums text-secondary">{row.bid1_volume ? row.bid1_volume.toLocaleString('zh-CN') : '--'}</td> : null}
      {mode !== 'candidate' ? <td className="px-2">
        <span className={mode === 'pool' && row.ws_active ? 'text-bear' : 'text-muted'}>{mode === 'pool' && row.ws_active ? 'WS' : '轮询'}</span>
      </td> : null}
      {mode === 'pool' ? (
        <>
          <td className={`px-2 font-medium ${orderStatus.tone}`} title={row.auto_order_error || undefined}>
            <div>{orderStatus.label}</div>
            {row.auto_order_volume && row.auto_order_amount != null ? <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] font-normal text-muted">{row.auto_order_volume.toLocaleString('zh-CN')} 股 · {row.auto_order_amount.toLocaleString('zh-CN', { maximumFractionDigits: 2 })} 元</div> : null}
          </td>
          <td className="sticky right-0 z-30 border-l border-border bg-surface px-2 text-right group-hover:bg-elevated">
            <div className="flex items-center justify-end gap-1.5">
              <div className="inline-flex h-7 overflow-hidden rounded-btn border border-border" aria-label="打板方式">
                {([
                  ['sweep', '扫板', `新鲜盘口中卖一距涨停价不超过 ${sweepPriceLevels} 个价位时提交`],
                  ['queue', '排板', queueTriggerDescription(queueWaitSeconds, queueConfirmSnapshots)],
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
        <td className="sticky right-0 z-30 border-l border-border bg-surface px-2 group-hover:bg-elevated">
          <div className="flex items-center justify-end gap-1">
            {mode === 'candidate' ? <button type="button" title="打开 QMT 手动交易" disabled={busy} onClick={onTrade} className="inline-flex h-7 items-center gap-1 rounded-btn border border-border px-2 text-secondary hover:border-warning/50 hover:text-warning disabled:opacity-60"><WalletCards className="h-3.5 w-3.5" />交易</button> : null}
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
  sweepPriceLevels: number
  queueWaitSeconds: number
  queueConfirmSnapshots: number
  onOpen: (row: LimitBoardRow) => void
  onAddPool: (row: LimitBoardRow) => void
  onRemoveCandidate: (row: LimitBoardRow) => void
  onToggleAuto: (row: LimitBoardRow, enabled: boolean) => void
  onChangeOrderMode: (row: LimitBoardRow, mode: 'sweep' | 'queue') => void
  onRemovePool: (row: LimitBoardRow) => void
  onTrade: (row: LimitBoardRow) => void
}

function Table(props: TableProps) {
  const { rows, mode } = props
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="max-w-full overflow-x-auto overscroll-x-contain" style={{ WebkitOverflowScrolling: 'touch' }}>
      <table className={`w-full border-collapse ${mode === 'candidate' ? 'min-w-[1000px]' : 'min-w-[1080px]'}`}>
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="sticky left-0 z-40 w-[128px] overflow-hidden bg-surface py-2 pl-3 pr-2">标的</th>
            <th className="w-[160px] px-2">题材</th>
            {mode === 'candidate' ? <><th className="w-[116px] min-w-[116px] whitespace-nowrap px-2">评分</th><th className="w-[118px] min-w-[118px] whitespace-nowrap px-2">行情</th><th className="w-[292px] min-w-[292px] px-2">评分依据</th></> : null}
            {mode !== 'candidate' ? <><th className="px-2">现价</th><th className="px-2">涨停价</th><th className="px-2">距涨停</th><th className="px-2">状态</th><th className="px-2">炸板次数</th><th className="px-2">买一封单</th><th className="px-2">行情</th><th className="px-2">委托状态</th></> : null}
            <th className={`sticky right-0 z-40 border-l border-border bg-surface px-2 text-right ${mode === 'pool' ? 'w-[220px]' : 'w-[172px]'}`}>操作</th>
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
              sweepPriceLevels={props.sweepPriceLevels}
              queueWaitSeconds={props.queueWaitSeconds}
              queueConfirmSnapshots={props.queueConfirmSnapshots}
              onOpen={() => props.onOpen(row)}
              onAddPool={() => props.onAddPool(row)}
              onRemoveCandidate={() => props.onRemoveCandidate(row)}
              onToggleAuto={enabled => props.onToggleAuto(row, enabled)}
              onChangeOrderMode={mode => props.onChangeOrderMode(row, mode)}
              onRemovePool={() => props.onRemovePool(row)}
              onTrade={() => props.onTrade(row)}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

type SectorSortKey = 'strength' | 'main_net' | 'institution_increase'
const SECTOR_TIMELINE_START = 9 * 3600 + 25 * 60
const SECTOR_TIMELINE_END = 15 * 3600

function sectorTimelineOffset(value: string | null | undefined): number {
  const match = /T(\d{2}):(\d{2}):(\d{2})/.exec(value ?? '')
  if (!match) return 0
  const seconds = Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3])
  return Math.max(0, Math.min(SECTOR_TIMELINE_END - SECTOR_TIMELINE_START, seconds - SECTOR_TIMELINE_START))
}

function sectorTradingCapturedAt(
  value: string | null | undefined,
  historyState: 'live' | 'closed' | 'unavailable' | undefined,
): string | null {
  if (!value) return null
  if (historyState !== 'closed') return value
  const match = /T(\d{2}):(\d{2}):(\d{2})/.exec(value)
  if (!match || Number(match[1]) < 15) return value
  return value.replace(/T\d{2}:\d{2}:\d{2}/, 'T15:00:00')
}

function sectorConstituentStatus(row: LimitBoardSectorConstituent): string {
  if (row.limit_tag) return row.limit_tag
  if ((row.limit_count ?? 0) > 1) return `${row.limit_count}连板`
  if (row.limit_count === 1) return '首板'
  return '--'
}

function sectorStrengthSpeed(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--'
  return `${value > 0 ? '+' : ''}${value.toFixed(1)}`
}

function sectorNameKey(value: string | null | undefined): string {
  return String(value ?? '').replace(/\s+/g, '').trim()
}

function signalSectorNames(row: LimitBoardRow): string[] {
  const values = [
    ...(row.top_sector_names ?? []),
    row.candidate_score_detail?.sector?.name,
    ...themes(row.concept),
  ]
  const seen = new Set<string>()
  return values.filter((value): value is string => {
    const key = sectorNameKey(value)
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function SectorStrengthTable({
  snapshot,
  signalRows = [],
  hotRows = [],
  hotQuotes = {},
  hotSectorLinks = {},
  hotLoading = false,
  hotError = false,
  refreshIntervalSeconds = 5,
  refreshCycleUpdatedAt = 0,
  onOpenAlgorithm,
  onOpenStock,
}: {
  snapshot: LimitBoardView['sector_strength']
  signalRows?: LimitBoardRow[]
  hotRows?: MarketHeatItem[]
  hotQuotes?: LimitBoardQuoteSnapshot['quotes']
  hotSectorLinks?: LimitBoardQuoteSnapshot['sector_links']
  hotLoading?: boolean
  hotError?: boolean
  refreshIntervalSeconds?: number
  refreshCycleUpdatedAt?: number
  onOpenAlgorithm: () => void
  onOpenStock: (symbol: string, name?: string) => void
}) {
  const [sortKey, setSortKey] = useState<SectorSortKey>('strength')
  const [descending, setDescending] = useState(true)
  const [cursorIndex, setCursorIndex] = useState<number | null>(null)
  const [requestedAt, setRequestedAt] = useState<string | null>(null)
  const [selectedPlateId, setSelectedPlateId] = useState<string | null>(null)
  const [selectedStockSymbol, setSelectedStockSymbol] = useState<string | null>(null)
  const [rankingWindowMinutes, setRankingWindowMinutes] = useState<5 | 30>(5)
  const [rankingOpen, setRankingOpen] = useState(() => (
    typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches
  ))
  const [progressClock, setProgressClock] = useState(() => Date.now())
  const [cycleStartedAt, setCycleStartedAt] = useState(() => Date.now())
  const constituentRowRefs = useRef(new Map<string, HTMLTableRowElement>())
  const lastScrolledConstituent = useRef<string | null>(null)
  const constituentOrder = useRef<{ key: string; symbols: string[] }>({ key: '', symbols: [] })
  const timeline = snapshot?.timeline ?? []
  const latestIndex = Math.max(0, timeline.length - 1)
  const activeIndex = cursorIndex == null ? latestIndex : Math.min(cursorIndex, latestIndex)
  const cursorAt = timeline[activeIndex] ?? null
  const activeOffset = sectorTimelineOffset(cursorAt)
  const isLive = (!cursorAt || activeIndex === latestIndex) && snapshot?.history_state !== 'closed'
  const isClosedLatest = snapshot?.history_state === 'closed' && activeIndex === latestIndex
  useEffect(() => {
    const now = refreshCycleUpdatedAt || Date.now()
    setCycleStartedAt(now)
    setProgressClock(now)
  }, [refreshCycleUpdatedAt])
  useEffect(() => {
    if (!isLive) return undefined
    const handle = window.setInterval(() => setProgressClock(Date.now()), 200)
    return () => window.clearInterval(handle)
  }, [isLive])
  useEffect(() => {
    const handle = window.setTimeout(() => setRequestedAt(isLive ? null : cursorAt), 120)
    return () => window.clearTimeout(handle)
  }, [cursorAt, isLive])
  useEffect(() => {
    if (cursorIndex != null && cursorIndex > latestIndex) setCursorIndex(null)
  }, [cursorIndex, latestIndex])
  const historical = useQuery({
    queryKey: QK.limitBoardSectorStrength(requestedAt ?? 'live'),
    queryFn: () => api.limitBoardSectorStrength(requestedAt as string),
    enabled: requestedAt != null,
    placeholderData: previous => previous,
  })
  const activeSnapshot = requestedAt ? historical.data ?? snapshot : snapshot
  const historyLabel = snapshot?.history_state === 'live'
    ? '盘中时序已落库'
    : snapshot?.history_state === 'closed' ? '收盘快照已落库' : '盘中时序落库不可用'
  const rows = useMemo(() => {
    const values = [...(activeSnapshot?.rows ?? [])]
    const children = new Map<string, LimitBoardSectorStrengthRow[]>()
    const roots: LimitBoardSectorStrengthRow[] = []
    const orphans: LimitBoardSectorStrengthRow[] = []
    const rootIds = new Set(values.filter(row => !row.parent_plate_id).map(row => row.plate_id))
    for (const row of values) {
      if (!row.parent_plate_id) roots.push(row)
      else if (rootIds.has(row.parent_plate_id)) children.set(row.parent_plate_id, [...(children.get(row.parent_plate_id) ?? []), row])
      else orphans.push(row)
    }
    const compare = (left: LimitBoardSectorStrengthRow, right: LimitBoardSectorStrengthRow) => {
      const a = left[sortKey]
      const b = right[sortKey]
      const numeric = (Number(a ?? 0) - Number(b ?? 0)) * (descending ? -1 : 1)
      return numeric || left.plate_id.localeCompare(right.plate_id)
    }
    roots.sort(compare)
    orphans.sort(compare)
    return roots.flatMap(row => [row, ...(children.get(row.plate_id) ?? []).sort(compare)]).concat(orphans)
  }, [activeSnapshot?.rows, descending, sortKey])
  const selectedSignal = signalRows.find(row => row.symbol === selectedStockSymbol) ?? null
  const linkedPlateIds = useMemo(() => {
    if (!selectedStockSymbol) return new Set<string>()
    const heatLinks = hotSectorLinks[selectedStockSymbol] ?? []
    const ids = new Set([
      ...(selectedSignal?.top_sector_ids ?? []),
      ...heatLinks.map(link => link.plate_id),
    ])
    const names = new Set([
      ...(selectedSignal ? signalSectorNames(selectedSignal) : []),
      ...heatLinks.map(link => link.plate_name),
    ].map(sectorNameKey))
    return new Set(rows.filter(row => ids.has(row.plate_id) || names.has(sectorNameKey(row.plate_name))).map(row => row.plate_id))
  }, [hotSectorLinks, rows, selectedSignal, selectedStockSymbol])
  const selectedPlate = rows.find(row => row.plate_id === selectedPlateId) ?? rows[0] ?? null
  useEffect(() => {
    if (selectedPlateId == null || rows.some(row => row.plate_id === selectedPlateId)) return
    setSelectedPlateId(rows[0]?.plate_id ?? null)
    setSelectedStockSymbol(null)
  }, [rows, selectedPlateId])
  const activeCapturedAt = isLive
    ? activeSnapshot?.refreshed_at ?? cursorAt
    : cursorAt ?? sectorTradingCapturedAt(
      activeSnapshot?.refreshed_at,
      snapshot?.history_state,
    )
  const activeSnapshotReady = isLive
    || cursorAt == null
    || activeSnapshot?.refreshed_at === activeCapturedAt
  const constituents = useQuery({
    queryKey: QK.limitBoardSectorConstituents(
      selectedPlate?.plate_id ?? '',
      activeCapturedAt ?? '',
    ),
    queryFn: () => api.limitBoardSectorConstituents(
      selectedPlate!.plate_id,
      isLive ? undefined : activeCapturedAt!,
    ),
    enabled: selectedPlate != null && activeCapturedAt != null && activeSnapshotReady,
    placeholderData: previous => previous,
  })
  const constituentData = constituents.data?.plate_id === selectedPlate?.plate_id
    ? constituents.data
    : null
  const constituentRows = useMemo(() => {
    const values = constituentData?.rows ?? []
    const orderKey = `${selectedPlate?.plate_id ?? ''}:${isLive ? 'live' : activeCapturedAt ?? ''}`
    const available = new Set(values.map(row => row.symbol))
    if (constituentOrder.current.key !== orderKey) {
      constituentOrder.current = { key: orderKey, symbols: values.map(row => row.symbol) }
    } else {
      const retained = constituentOrder.current.symbols.filter(symbol => available.has(symbol))
      const retainedSet = new Set(retained)
      constituentOrder.current.symbols = [
        ...retained,
        ...values.map(row => row.symbol).filter(symbol => !retainedSet.has(symbol)),
      ]
    }
    const bySymbol = new Map(values.map(row => [row.symbol, row]))
    return constituentOrder.current.symbols
      .map(symbol => bySymbol.get(symbol))
      .filter((row): row is LimitBoardSectorConstituent => row != null)
  }, [activeCapturedAt, constituentData?.rows, isLive, selectedPlate?.plate_id])
  useEffect(() => {
    if (!selectedStockSymbol || !selectedPlate || !constituentData?.rows.some(row => row.symbol === selectedStockSymbol)) return
    const scrollKey = `${selectedPlate.plate_id}:${selectedStockSymbol}`
    if (lastScrolledConstituent.current === scrollKey) return
    const frame = window.requestAnimationFrame(() => {
      constituentRowRefs.current.get(selectedStockSymbol)?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      lastScrolledConstituent.current = scrollKey
    })
    return () => window.cancelAnimationFrame(frame)
  }, [constituentData?.rows, selectedPlate, selectedStockSymbol])
  const changeSort = (key: SectorSortKey) => {
    if (sortKey === key) setDescending(value => !value)
    else {
      setSortKey(key)
      setDescending(true)
    }
  }
  const header = (key: SectorSortKey, label: string) => (
    <button type="button" onClick={() => changeSort(key)} className="inline-flex items-center gap-1 whitespace-nowrap font-medium hover:text-foreground" aria-label={`按${label}${sortKey === key && descending ? '升序' : '降序'}排序`}>
      {label}<span className={sortKey === key ? 'text-accent' : 'text-muted'}>{sortKey === key ? (descending ? '↓' : '↑') : '↕'}</span>
    </button>
  )
  const selectStock = (symbol: string) => {
    const normalized = symbol.trim().toUpperCase()
    const signal = signalRows.find(row => row.symbol === normalized)
    const heatLinks = hotSectorLinks[normalized] ?? []
    setSelectedStockSymbol(normalized)
    lastScrolledConstituent.current = null
    const ids = new Set([
      ...(signal?.top_sector_ids ?? []),
      ...heatLinks.map(link => link.plate_id),
    ])
    const names = new Set([
      ...(signal ? signalSectorNames(signal) : []),
      ...heatLinks.map(link => link.plate_name),
    ].map(sectorNameKey))
    const matches = rows.filter(row => ids.has(row.plate_id) || names.has(sectorNameKey(row.plate_name)))
    if (!matches.length) return
    const strongest = matches.reduce((best, row) => Number(row.strength ?? 0) > Number(best.strength ?? 0) ? row : best)
    setSelectedPlateId(strongest.plate_id)
  }
  const selectPlate = (plateId: string) => {
    setSelectedStockSymbol(null)
    setSelectedPlateId(plateId)
  }
  const progressDuration = Math.max(1, refreshIntervalSeconds) * 1000
  const refreshProgress = isLive
    ? Math.min(100, Math.max(0, ((progressClock - cycleStartedAt) / progressDuration) * 100))
    : 100
  const trend = rankingWindowMinutes === 5
    ? activeSnapshot?.trend_5m
    : activeSnapshot?.trend_30m
  const strengthSpeed = (row: LimitBoardSectorStrengthRow) => (
    rankingWindowMinutes === 5
      ? row.strength_speed_per_min_5m
      : row.strength_speed_per_min_30m
  )
  const mainNetSpeed = (row: LimitBoardSectorStrengthRow) => (
    rankingWindowMinutes === 5
      ? row.main_net_speed_per_min_5m
      : row.main_net_speed_per_min_30m
  )
  const windowRisingRanking = [...rows]
    .filter(row => {
      const value = strengthSpeed(row)
      return value != null && Number.isFinite(value) && value > 0
    })
    .sort((left, right) => Number(strengthSpeed(right)) - Number(strengthSpeed(left)))
    .slice(0, 3)
  const windowFallingRanking = [...rows]
    .filter(row => {
      const value = strengthSpeed(row)
      return value != null && Number.isFinite(value) && value < 0
    })
    .sort((left, right) => Number(strengthSpeed(left)) - Number(strengthSpeed(right)))
    .slice(0, 3)
  return <div className="min-w-0">
    <section className="overflow-hidden rounded-btn border border-border bg-surface">
    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1"><div className="shrink-0 text-xs font-medium">板块强度</div><span className="truncate text-[10px] text-muted">强势股、板块与成分股按同一截面每 {refreshIntervalSeconds} 秒刷新</span></div>
      <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px] text-muted">
        <span className={snapshot?.history_state === 'unavailable' ? 'text-warning' : 'text-secondary'}>{historyLabel}</span>
        <span>{activeSnapshot?.state === 'live' ? `${isLive ? '实时' : isClosedLatest ? '收盘' : '回看'} ${scoreTime(activeCapturedAt)}` : '实时板块数据暂不可用'}</span>
        <button
          type="button"
          aria-expanded={rankingOpen}
          aria-controls="sector-interval-ranking"
          onClick={() => setRankingOpen(value => !value)}
          className="inline-flex h-6 items-center gap-1 rounded-btn border border-border px-2 text-[9px] text-secondary hover:bg-elevated hover:text-foreground"
          title={rankingOpen ? '收起板块区间排序' : '展开板块区间排序'}
        >
          {rankingOpen ? <PanelRightClose className="h-3 w-3" /> : <PanelRightOpen className="h-3 w-3" />}
          {rankingOpen ? '收起区间榜' : '区间榜'}
        </button>
      </div>
    </div>
    <div className="h-0.5 bg-elevated" aria-label={`板块三栏统一刷新进度 ${Math.round(refreshProgress)}%`}><div className="h-full bg-accent transition-[width] duration-200 ease-linear" style={{ width: `${refreshProgress}%` }} /></div>
    <div className="overflow-x-auto overscroll-x-contain">
    <div className={`grid min-w-0 lg:min-w-[1020px] ${rankingOpen ? 'lg:grid-cols-[14%_14%_22%_28%_22%]' : 'lg:grid-cols-[16%_16%_28%_40%]'}`}>
      <div className="min-w-0 border-b border-border lg:border-b-0 lg:border-r">
        <div className="flex min-h-12 items-center border-b border-border px-2 py-1.5">
          <div className="min-w-0"><div className="inline-flex items-center gap-1 text-[11px] font-medium"><Flame className="h-3.5 w-3.5 shrink-0 text-accent" /><span className="truncate">热股雷达</span></div><div className="mt-0.5 truncate pl-[18px] text-[8px] text-muted">榜60秒 · 行情5秒</div></div>
        </div>
        {hotRows.length ? <div className="max-w-full overflow-x-auto overscroll-contain p-2 lg:max-h-[62vh] lg:overflow-x-hidden lg:overflow-y-auto">
          <div className="flex w-max gap-2 lg:w-full lg:flex-col">
            {hotRows.slice(0, 30).map(item => {
              const quote = hotQuotes[item.thscode.toUpperCase()]
              const selected = item.thscode.toUpperCase() === selectedStockSymbol
              const atLimit = quote?.last_price != null && quote.limit_up != null
                && quote.last_price >= quote.limit_up - 0.001
              return <div
                key={item.thscode}
                role="button"
                tabIndex={0}
                aria-pressed={selected}
                onClick={() => selectStock(item.thscode)}
                onKeyDown={event => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    selectStock(item.thscode)
                  }
                }}
                className={`h-[68px] w-[164px] shrink-0 rounded-btn border px-2.5 py-2 text-left outline-none transition-colors hover:border-warning/60 hover:bg-warning/5 focus-visible:ring-1 focus-visible:ring-warning lg:w-full ${selected ? 'border-warning bg-warning/15 ring-1 ring-warning/60' : 'border-border bg-surface'}`}
                title="联动强势股、实时板块与成分股"
              >
                <div className="flex items-center justify-between gap-2"><button type="button" onClick={event => { event.stopPropagation(); onOpenStock(item.thscode, item.name || item.ticker) }} className="min-w-0 truncate text-left text-xs font-medium hover:text-accent focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-warning" title="查看 K 线与分时">{item.name || item.ticker}</button><span className="shrink-0 font-mono text-[10px] text-accent">#{item.rank ?? '--'}</span></div>
                <div className="mt-0.5 flex items-center justify-between gap-2 font-mono text-[9px]"><span className="truncate text-muted">{item.thscode}</span><span className="shrink-0"><span className="text-secondary">{quote?.last_price?.toFixed(2) ?? '--'}</span> <span className={financialTone(quote?.change_pct)}>{scorePct(quote?.change_pct, 2)}{atLimit ? '（涨停）' : ''}</span></span></div>
                <div className="mt-0.5 truncate font-mono text-[8px] text-muted">热度 {item.heat == null ? '--' : item.heat.toFixed(0)} · 排名变化 {item.rank_change == null ? '--' : `${item.rank_change > 0 ? '+' : ''}${item.rank_change}`}</div>
              </div>
            })}
          </div>
        </div> : <div className={`px-3 py-10 text-center text-xs ${hotError ? 'text-warning' : 'text-muted'}`}>{hotLoading ? '正在读取热股雷达' : hotError ? '热股雷达暂不可用' : '暂无热股数据'}</div>}
      </div>
      <div className="min-w-0 border-b border-border lg:border-b-0 lg:border-r">
        <div className="flex min-h-12 items-center justify-between gap-1 border-b border-border px-2 py-1.5">
          <div className="min-w-0"><div className="inline-flex items-center gap-1 text-[11px] font-medium"><Flame className="h-3.5 w-3.5 shrink-0 text-accent" /><span className="truncate">强势股打分</span></div><div className="mt-0.5 truncate pl-[18px] font-mono text-[8px] text-muted">{signalRows.length} 只</div></div>
          <div className="flex shrink-0 items-center">
            <button type="button" onClick={onOpenAlgorithm} className="inline-flex h-6 items-center gap-1 rounded-btn border border-border px-1.5 text-[9px] text-secondary hover:bg-elevated hover:text-foreground" title="查看强势股打分算法"><CircleHelp className="h-3 w-3" />算法</button>
          </div>
        </div>
        {signalRows.length ? <div className="max-w-full overflow-x-auto overscroll-contain p-2 lg:max-h-[62vh] lg:overflow-x-hidden lg:overflow-y-auto">
          <div className="flex w-max gap-2 lg:w-full lg:flex-col">
            {signalRows.map(signal => {
              const selected = signal.symbol === selectedStockSymbol
              const status = STATUS[signal.status || 'watching'] || STATUS.watching
              const rebound = signal.source === 'rebound_board' || signal.source_modes?.includes('rebound_board')
              const atLimit = signal.limit_gap_pct != null && signal.limit_gap_pct <= 0.0001
              const boardLabel = atLimit ? (rebound ? '反包' : '首板') : '观察'
              const names = new Set(signalSectorNames(signal).map(sectorNameKey))
              const matchedPlates = rows.filter(row => names.has(sectorNameKey(row.plate_name)))
              const displayThemes = matchedPlates.length
                ? matchedPlates.slice(0, 2).map(row => row.plate_name).filter((value): value is string => Boolean(value))
                : themes(signal.concept).slice(0, 2)
              return <div
                key={signal.symbol}
                role="button"
                tabIndex={0}
                aria-pressed={selected}
                onClick={() => selectStock(signal.symbol)}
                onKeyDown={event => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    selectStock(signal.symbol)
                  }
                }}
                className={`h-[92px] w-[164px] shrink-0 rounded-btn border px-2.5 py-2 text-left outline-none transition-colors hover:border-warning/60 hover:bg-warning/5 focus-visible:ring-1 focus-visible:ring-warning lg:w-full ${selected ? 'border-warning bg-warning/15 ring-1 ring-warning/60' : 'border-border bg-surface'}`}
              >
                <div className="flex items-start justify-between gap-2"><button type="button" onClick={event => { event.stopPropagation(); onOpenStock(signal.symbol, signal.name || signal.symbol) }} className="min-w-0 truncate text-left text-xs font-medium hover:text-accent focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-warning" title="查看 K 线与分时">{signal.name || signal.symbol}</button><span className="shrink-0 text-right text-[9px] text-secondary"><span className="block">{boardLabel}{signal.candidate_score_state === 'cached' ? ' · 缓存' : ''}</span><span className={`block font-mono ${signal.candidate_score_state === 'cached' ? 'text-warning' : 'text-accent'}`} title={signal.candidate_score_state === 'cached' ? `缓存评分 ${scoreTime(signal.candidate_score_as_of)}` : '强势股打分最终总分'}>总分 {signal.candidate_score == null ? '--' : signal.candidate_score.toFixed(1)}</span></span></div>
                <div className="mt-0.5 flex items-center justify-between gap-1 font-mono text-[9px] text-muted"><span>{signal.symbol}</span><span className={financialTone(signal.change_pct)}>{scorePct(signal.change_pct, 2)}{atLimit ? '（涨停）' : ''}</span></div>
                <div className="mt-1.5 flex min-w-0 items-center gap-1 text-[9px] text-secondary">
                  {displayThemes.length ? displayThemes.map(name => <span key={name} className="max-w-[70px] truncate rounded-sm bg-elevated px-1 py-0.5">{name}</span>) : <span className="truncate text-muted">未匹配实时板块</span>}
                </div>
                <div className="mt-1 flex items-center justify-between text-[9px]"><span className={status.tone}>{status.label}</span><span className="font-mono text-muted">距涨停 {signal.limit_gap_pct == null ? '--' : scorePct(signal.limit_gap_pct, 2)}</span></div>
              </div>
            })}
          </div>
        </div> : <div className="px-3 py-10 text-center text-xs text-muted">暂无标的</div>}
      </div>
      <div className="min-w-0 overflow-x-auto overscroll-x-contain border-b border-border lg:border-b-0 lg:border-r">
        <table className="w-full min-w-[420px] table-fixed border-collapse">
          <thead className="text-left text-[9px] text-muted"><tr><th className="w-[31%] px-2 py-1.5">板块</th><th className="w-[14%] px-2 py-1.5 text-right text-foreground">{header('strength', '强度')}</th><th className="w-[26%] px-2 py-1.5 text-right">{header('main_net', '主力净额')}</th><th className="w-[29%] px-2 py-1.5 text-right">{header('institution_increase', activeSnapshot?.institution_label || '机构增仓')}</th></tr></thead>
          <tbody>{rows.length ? rows.map(row => {
            const selected = row.plate_id === selectedPlate?.plate_id
            const linked = linkedPlateIds.has(row.plate_id)
            return <tr
              key={row.plate_id}
              role="button"
              tabIndex={0}
              aria-selected={selected}
              onClick={() => selectPlate(row.plate_id)}
              onKeyDown={event => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault()
                  selectPlate(row.plate_id)
                }
              }}
              className={`cursor-pointer border-t border-border/70 outline-none hover:bg-elevated/50 focus-visible:bg-elevated ${selected && linked ? 'bg-warning/25 ring-1 ring-inset ring-warning/60' : linked ? 'bg-warning/10' : selected ? 'bg-accent/20' : ''}`}
            >
              <td className="px-2 py-1.5"><div className={row.is_child ? 'relative ml-2 pl-3 before:absolute before:left-0 before:top-0 before:h-1/2 before:w-2 before:border-b before:border-l before:border-border' : ''}><div className="flex items-center gap-1 text-[11px] font-medium">{linked ? <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-warning" aria-label="首板或反包关联板块" /> : null}<span className="truncate">{row.plate_name || '--'}</span></div><div className="font-mono text-[8px] text-muted">{row.plate_id}</div></div></td>
              <td className="px-2 py-1.5 text-right font-mono text-xs font-semibold tabular-nums text-secondary">{row.strength?.toFixed(0) ?? '--'}</td>
              <td className={`px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums ${financialTone(row.main_net)}`}>{moneyYi(row.main_net)}</td>
              <td className={`px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums ${financialTone(row.institution_increase)}`}>{moneyYi(row.institution_increase)}</td>
            </tr>
          }) : <tr><td colSpan={4} className="px-3 py-10 text-center text-xs text-muted">实时板块数据暂不可用</td></tr>}</tbody>
        </table>
      </div>
      <div className="min-w-0">
        {constituents.isError && !constituentData && !constituents.isFetching ? <div className="flex flex-col items-center gap-2 px-4 py-12 text-center text-xs text-danger"><span>{selectedPlate?.plate_name || '实时板块'}成分股加载失败</span><button type="button" onClick={() => constituents.refetch()} className="inline-flex h-7 items-center gap-1 rounded-btn border border-danger/40 px-2.5 text-[10px] text-danger hover:bg-danger/10"><RefreshCw className="h-3 w-3" />重试</button></div> : constituentRows.length ? <div className="max-h-[62vh] max-w-full overflow-auto overscroll-contain">
          <table className="w-full min-w-[480px] table-fixed border-collapse">
            <thead className="sticky top-0 z-10 bg-surface text-left text-[9px] text-muted"><tr><th className="w-[28%] px-2 py-1.5">股票</th><th className="w-[12%] px-2 py-1.5 text-right">现价</th><th className="w-[12%] px-2 py-1.5 text-right">涨幅</th><th className="w-[14%] px-2 py-1.5 text-right">板状态</th><th className="w-[14%] px-2 py-1.5 text-right">换手率</th><th className="w-[20%] px-2 py-1.5 text-right">成交额</th></tr></thead>
            <tbody>{constituentRows.map(row => {
              const linked = row.symbol === selectedStockSymbol
              return <tr
                key={row.symbol}
                ref={element => {
                  if (element) constituentRowRefs.current.set(row.symbol, element)
                  else constituentRowRefs.current.delete(row.symbol)
                }}
                className={`border-t border-border/70 hover:bg-elevated/30 ${linked ? 'bg-warning/20 ring-1 ring-inset ring-warning/60' : ''}`}
              >
              <td className="px-2 py-1.5"><button type="button" onClick={() => onOpenStock(row.symbol, row.name ?? undefined)} className="block max-w-full text-left hover:text-accent" title="查看 K 线与分时"><span className="block truncate text-[11px] font-medium">{row.name || row.code}</span><span className="block truncate font-mono text-[8px] text-muted">#{row.rank} {row.symbol}{row.tags ? ` · ${row.tags}` : ''}</span></button></td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
              <td className={`px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums ${financialTone(row.change_pct)}`}>{scorePct(row.change_pct, 2)}</td>
              <td className="px-2 py-1.5 text-right text-[10px] text-secondary">{sectorConstituentStatus(row)}</td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums text-secondary">{ratioPct(row.turnover_rate, 2)}</td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums text-secondary">{moneyYi(row.amount)}</td>
              </tr>
            })}</tbody>
          </table>
        </div> : <div className="px-4 py-12 text-center text-xs text-muted">{constituents.isPending || constituents.isFetching ? '正在读取实时板块成分股' : '该时间点没有可用的成分股数据'}</div>}
      </div>
      {rankingOpen ? <div id="sector-interval-ranking" className="min-w-0 border-b border-border lg:border-b-0">
        <div className="flex min-h-12 items-center gap-1.5 border-b border-border px-2 py-1.5">
          <div className="min-w-0 flex-1">
            <div className="truncate text-[11px] font-medium">板块区间排序</div>
            <div
              className="truncate font-mono text-[8px] text-muted"
              title={trend ? `同板块强度变化除以实际 ${trend.elapsed_minutes.toFixed(1)} 分钟；主力净额速度仅作辅助` : undefined}
            >
              {trend ? `${scoreTime(trend.base_at)} → ${scoreTime(trend.captured_at)}` : `积累 ${rankingWindowMinutes} 分钟截面`}
            </div>
          </div>
          <div className="inline-flex shrink-0 overflow-hidden rounded-btn border border-border bg-base" aria-label="选择板块排序周期">
            {([5, 30] as const).map(minutes => <button key={minutes} type="button" aria-pressed={rankingWindowMinutes === minutes} onClick={() => setRankingWindowMinutes(minutes)} className={`h-6 px-1.5 text-[9px] ${rankingWindowMinutes === minutes ? 'bg-accent/15 text-accent' : 'text-muted hover:bg-elevated hover:text-foreground'}`}>{minutes}分</button>)}
          </div>
          <button type="button" onClick={() => setRankingOpen(false)} className="grid h-6 w-6 shrink-0 place-items-center rounded-btn text-muted hover:bg-elevated hover:text-foreground" aria-label="收起板块区间排序"><X className="h-3.5 w-3.5" /></button>
        </div>
        <div className="grid grid-cols-[24px_minmax(44px,1fr)_52px_62px] border-b border-border px-2 py-1.5 text-[8px] text-muted"><span>#</span><span>板块</span><span className="text-right">强度/分</span><span className="text-right">主力/分</span></div>
        <div className="border-b border-border">
          <div className="px-2 py-1.5 text-[10px] font-medium text-bull">强度涨速</div>
          {windowRisingRanking.length ? windowRisingRanking.map((row, index) => <button
            key={row.plate_id}
            type="button"
            aria-pressed={row.plate_id === selectedPlate?.plate_id}
            onClick={() => selectPlate(row.plate_id)}
            className={`grid w-full grid-cols-[24px_minmax(44px,1fr)_52px_62px] items-center border-t border-border/70 px-2 py-2 text-left text-[9px] hover:bg-elevated/50 ${row.plate_id === selectedPlate?.plate_id ? 'bg-accent/15' : ''}`}
            title="选择该板块并联动成分股"
          >
            <span className="font-mono text-muted">#{index + 1}</span><span className="truncate text-secondary">{row.plate_name || row.plate_id}</span><span className="text-right font-mono font-medium text-bull">{sectorStrengthSpeed(strengthSpeed(row))}</span><span className={`text-right font-mono ${financialTone(mainNetSpeed(row))}`}>{moneyYi(mainNetSpeed(row))}</span>
          </button>) : <div className="px-2 py-5 text-center text-[10px] text-muted">暂无强度上涨板块</div>}
        </div>
        <div>
          <div className="px-2 py-1.5 text-[10px] font-medium text-bear">强度跌速</div>
          {windowFallingRanking.length ? windowFallingRanking.map((row, index) => <button
            key={row.plate_id}
            type="button"
            aria-pressed={row.plate_id === selectedPlate?.plate_id}
            onClick={() => selectPlate(row.plate_id)}
            className={`grid w-full grid-cols-[24px_minmax(44px,1fr)_52px_62px] items-center border-t border-border/70 px-2 py-2 text-left text-[9px] hover:bg-elevated/50 ${row.plate_id === selectedPlate?.plate_id ? 'bg-accent/15' : ''}`}
            title="选择该板块并联动成分股"
          >
            <span className="font-mono text-muted">#{index + 1}</span><span className="truncate text-secondary">{row.plate_name || row.plate_id}</span><span className="text-right font-mono font-medium text-bear">{sectorStrengthSpeed(strengthSpeed(row))}</span><span className={`text-right font-mono ${financialTone(mainNetSpeed(row))}`}>{moneyYi(mainNetSpeed(row))}</span>
          </button>) : <div className="px-2 py-5 text-center text-[10px] text-muted">暂无强度下跌板块</div>}
        </div>
      </div> : null}
    </div>
    </div>
    <div className="border-t border-border px-4 py-3">
      <div className="mb-2 flex items-center justify-between font-mono text-[10px] text-muted"><span>09:25</span><span className="text-secondary">{isLive ? '实时' : scoreTime(cursorAt)}</span><span>15:00</span></div>
      <input
        type="range"
        min={0}
        max={SECTOR_TIMELINE_END - SECTOR_TIMELINE_START}
        step={5}
        value={activeOffset}
        disabled={timeline.length < 2}
        onInput={event => {
          const target = Number(event.currentTarget.value)
          let nearestIndex = 0
          let nearestDistance = Number.POSITIVE_INFINITY
          timeline.forEach((point, index) => {
            const distance = Math.abs(sectorTimelineOffset(point) - target)
            if (distance < nearestDistance) {
              nearestDistance = distance
              nearestIndex = index
            }
          })
          setCursorIndex(nearestIndex === latestIndex ? null : nearestIndex)
        }}
        aria-label="选择盘中板块强度时间点"
        className="h-1 w-full cursor-pointer accent-accent disabled:cursor-not-allowed disabled:opacity-40"
      />
    </div>
    </section>
  </div>
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
        ['touched', '涨停'],
        ['broken', '炸板'],
        ['resealed', '回封'],
      ] as const).map(([key, label]) => <label key={key} className="flex items-center justify-between py-3 text-xs"><span>{label}</span><input type="checkbox" checked={draft[key]} disabled={pending} onChange={event => setDraft(current => ({ ...current, [key]: event.target.checked }))} /></label>)}
    </div>
    <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" onClick={onClose} disabled={pending} className="h-8 rounded-btn border border-border px-3 text-xs text-muted disabled:opacity-50">取消</button><button type="button" onClick={() => onSave(draft)} disabled={pending} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-50"><Check className="h-3.5 w-3.5" />{pending ? '保存中…' : '保存'}</button></div>
  </Modal>
}

function CandidateAlgorithmDialog({ onClose }: { onClose: () => void }) {
  return <Modal labelledBy="limit-board-candidate-algorithm-title" onClose={onClose} panelClassName="flex max-h-[90vh] w-[94vw] max-w-3xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl">
    <div className="flex items-center justify-between border-b border-border px-4 py-3">
      <div>
        <h2 id="limit-board-candidate-algorithm-title" className="text-sm font-semibold">强势股打分算法</h2>
        <p className="mt-0.5 text-[10px] text-muted">板块优先的确定性排序，用于自动备选池优先级</p>
      </div>
      <button type="button" onClick={onClose} className="grid h-7 w-7 place-items-center rounded-btn text-muted hover:bg-elevated hover:text-foreground" aria-label="关闭排序算法"><X className="h-4 w-4" /></button>
    </div>
    <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4 text-xs text-secondary sm:px-5">
      <section>
        <h3 className="font-medium text-foreground">候选范围</h3>
        <ol className="mt-2 grid gap-2 sm:grid-cols-3">
          {[
            ['1', '板块入围', '只取开盘啦实时板块强度前 10 名。'],
            ['2', '成分去重', '盘中通过开盘啦 socket 获取当天实时板块成分。'],
            ['3', '保留 Top 30', '首板、反包合并打分后只保留自动排名前 30。'],
          ].map(([step, title, detail]) => <li key={step} className="flex gap-2 border-t border-border pt-2"><span className="font-mono text-accent">{step}</span><span><strong className="font-medium text-foreground">{title}</strong><span className="mt-0.5 block text-[11px] leading-5 text-muted">{detail}</span></span></li>)}
        </ol>
        <p className="mt-2 text-[11px] leading-5 text-muted">首板资格为回看窗口内无涨停记录；反包资格为窗口内曾涨停、随后炸板或断板，且最近一个完整交易日未涨停。两类只有当日真实触及涨停时才显示对应标签，未触及时显示“观察”。“仅沪深主板”只限制自动候选，手工加入不受影响；强势确认分不使用距涨停，但可交易机会榜会使用成交空间和行情新鲜度。</p>
      </section>

      <section className="mt-4 border-t border-border pt-4">
        <h3 className="font-medium text-foreground">总分构成</h3>
        <div className="mt-2 grid grid-cols-2 divide-x divide-y divide-border border border-border sm:grid-cols-4 sm:divide-y-0">
          {[
            ['50', '板块强度与轮动'],
            ['30', '涨停基因'],
            ['15', '日内分时与资金'],
            ['5', '技术面'],
          ].map(([score, label]) => <div key={label} className="px-3 py-2.5"><div className="font-mono text-lg font-semibold text-foreground">{score}<span className="ml-0.5 text-[10px] font-normal text-muted">分</span></div><div className="mt-0.5 text-[10px] text-muted">{label}</div></div>)}
        </div>
      </section>

      <section className="mt-4 divide-y divide-border border-y border-border">
        <div className="py-3">
          <div className="flex items-center justify-between"><h3 className="font-medium text-foreground">板块强度与轮动</h3><span className="font-mono text-accent">50 分</span></div>
          <div className="mt-2 grid gap-x-5 gap-y-2 text-[11px] leading-5 sm:grid-cols-2">
            <p><strong className="font-medium text-foreground">当日实时 30 分：</strong>板块涨跌 8、上涨家数占比 5、代表龙头涨幅 3、个股相对板块强度 4、龙头/前排/跟随 10/5/0。</p>
            <p><strong className="font-medium text-foreground">前 5 日轮动 20 分：</strong>复合涨跌 6、趋势斜率 4、排名百分位变化 4、前 20% 持续性 3、昨日强度 3。今日不进入 5 日窗口。</p>
            <p><strong className="font-medium text-foreground">线性区间：</strong>板块涨跌 -1%→+4%，龙头 0%→+10%，个股跑赢板块 -2%→+4%；5 日复合 -5%→+10%，斜率 -1%→+1%/日。</p>
            <p><strong className="font-medium text-foreground">板块选择：</strong>优先题材，无有效题材时回退二级行业；同类中先比实时排名、强度，再比板块分。覆盖率低于 80% 或成员少于 5 只时不计算。</p>
          </div>
        </div>
        <div className="py-3">
          <div className="flex items-center justify-between"><h3 className="font-medium text-foreground">涨停基因</h3><span className="font-mono text-accent">30 分</span></div>
          <p className="mt-2 text-[11px] leading-5">近 200 日涨停次数 7 分、次日红盘率 7 分、次日涨幅超 5% 比例 5 分、首板封板率 6 分、连板晋级率 5 分。除涨停次数外，比例分均乘以 <span className="font-mono text-foreground">min(有效样本 / 10, 1)</span> 的样本置信度。</p>
        </div>
        <div className="py-3">
          <div className="flex items-center justify-between"><h3 className="font-medium text-foreground">日内分时与资金</h3><span className="font-mono text-accent">15 分</span></div>
          <div className="mt-2 grid gap-x-5 gap-y-2 text-[11px] leading-5 sm:grid-cols-2">
            <p><strong className="font-medium text-foreground">分时强势 7.5 分：</strong>相对昨收涨幅 2.4、现价相对 VWAP 1.8、非水下时间占比 1.8、价涨量增 1.5。</p>
            <p><strong className="font-medium text-foreground">主动资金 7.5 分：</strong>大单净流向 5.4、资金持续性 2.1。一直水下、净流出或连续走弱会显著拉低该项。</p>
          </div>
        </div>
        <div className="py-3">
          <div className="flex items-center justify-between"><h3 className="font-medium text-foreground">技术面</h3><span className="font-mono text-accent">5 分</span></div>
          <p className="mt-2 text-[11px] leading-5">均线趋势 1.75 分（现价、MA5、MA10、MA20、MA60 多头关系），5/20 日动量 1.25 分，5 日量比 0.75 分，MACD 0.75 分，RSI14 0.5 分。RSI 50–85 为满分区，85 后逐步降分。</p>
        </div>
      </section>

      <section className="mt-4">
        <h3 className="font-medium text-foreground">数据门槛与排序</h3>
        <div className="mt-2 grid gap-x-5 gap-y-2 text-[11px] leading-5 sm:grid-cols-2">
          <p><strong className="font-medium text-foreground">完整性：</strong>板块及 5 日轮动、涨停基因、分时、实时主动资金和技术面必须全部可用，才会生成总分。缺实时资金时不把代理数据伪装成真实分数。</p>
          <p><strong className="font-medium text-foreground">排序顺序：</strong>可计算状态 → 实时板块可用 → 板块实时排名 → 板块强度 → 板块分 → 龙头地位与成分排名 → 总分 → 基因 → 分时资金 → 技术面 → 股票代码。</p>
          <p className="sm:col-span-2"><strong className="font-medium text-foreground">缓存：</strong>5 秒一轮批量更新。同一交易日某项短暂缺数时可沿用最后有效值并标记“缓存”；跨交易日清空。实时板块或当日实时成分缺失时，自动候选严格停止，不使用本地聚合降级。</p>
        </div>
      </section>

      <section className="mt-4 border-t border-border pt-4">
        <div className="flex items-center justify-between"><h3 className="font-medium text-foreground">可交易机会分</h3><span className="font-mono text-bull">100 分</span></div>
        <p className="mt-2 text-[11px] leading-5">强势确认分 50%、评分上升速度 20%、日内分时 15%、距涨停成交空间 15%。机会榜只保留行情 10 秒内、距涨停 0.5%–3%、未触板且强势分连续两轮上升的标的；涨停、封板、炸板和缓存评分只留在强势确认榜。</p>
      </section>
    </div>
    <div className="flex justify-end border-t border-border px-4 py-3"><button type="button" onClick={onClose} className="h-8 rounded-btn border border-border px-3 text-xs text-muted hover:bg-elevated hover:text-foreground">关闭</button></div>
  </Modal>
}

function queueTriggerDescription(waitSeconds: number, confirmSnapshots: number): string {
  const trigger = confirmSnapshots > 0
    ? `连续 ${confirmSnapshots} 个盘口快照确认封板`
    : '价格触及涨停'
  return waitSeconds > 0
    ? `${trigger}，且首次涨停已等待 ${waitSeconds} 秒后提交`
    : `${trigger}后提交`
}

function AdvancedSettingsDialog({
  value,
  pending,
  onClose,
  onSave,
}: {
  value: AdvancedSettings
  pending: boolean
  onClose: () => void
  onSave: (value: AdvancedSettings) => void
}) {
  const [draft, setDraft] = useState(value)
  const valid = Number.isInteger(draft.sweep_price_levels)
    && draft.sweep_price_levels >= 1
    && draft.sweep_price_levels <= 10
    && Number.isInteger(draft.queue_wait_seconds)
    && draft.queue_wait_seconds >= 0
    && draft.queue_wait_seconds <= 300
    && Number.isInteger(draft.queue_confirm_snapshots)
    && draft.queue_confirm_snapshots >= 0
    && draft.queue_confirm_snapshots <= 10
    && QMT_ALLOCATION_OPTIONS.some(option => option.value === draft.order_allocation_mode)
    && Number.isFinite(draft.order_amount_per_board)
    && draft.order_amount_per_board >= 0
    && draft.order_amount_per_board <= 10000000
    && Number.isInteger(draft.max_auto_board_count)
    && draft.max_auto_board_count >= 0
    && draft.max_auto_board_count <= 100
    && Number.isFinite(draft.max_market_broken_rate_pct)
    && draft.max_market_broken_rate_pct >= 0
    && draft.max_market_broken_rate_pct <= 100
    && draft.near_limit_pct >= 0.001
    && draft.near_limit_pct <= 0.10
    && draft.exit_limit_pct >= draft.near_limit_pct
    && draft.exit_limit_pct <= 0.20
    && draft.exit_sustain_seconds >= 1
    && draft.exit_sustain_seconds <= 300
    && draft.first_board_lookback_days >= 1
    && draft.first_board_lookback_days <= 60
    && draft.blacklist_after_breaks >= 0
    && draft.blacklist_after_breaks <= 20
  const inputClass = 'h-8 w-28 rounded-btn border border-border bg-base px-2 text-right font-mono text-xs outline-none focus:border-accent disabled:opacity-50'
  const update = <K extends keyof AdvancedSettings,>(key: K, next: AdvancedSettings[K]) => {
    setDraft(current => ({ ...current, [key]: next }))
  }

  return <Modal labelledBy="limit-board-advanced-title" onClose={onClose} closeOnBackdrop={!pending} panelClassName="max-h-[92vh] w-[94vw] max-w-xl overflow-y-auto rounded-card border border-border bg-surface shadow-xl">
    <div className="border-b border-border px-4 py-3"><h2 id="limit-board-advanced-title" className="text-sm font-semibold">高级设置</h2></div>
    <div className="grid grid-cols-1 divide-y divide-border px-4 sm:grid-cols-2 sm:gap-x-6 sm:divide-y-0">
      <label className="flex items-center justify-between gap-3 border-b border-border py-3 text-xs sm:col-span-2">
        <span><span className="block font-medium">扫板触发档位</span><span className="mt-0.5 block text-[10px] text-muted">卖一距涨停价的最大价格档位</span></span>
        <span className="flex items-center gap-2"><input type="number" min={1} max={10} step={1} value={draft.sweep_price_levels} disabled={pending} onChange={event => update('sweep_price_levels', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">档</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span>排板等待时间</span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={300} step={1} value={draft.queue_wait_seconds} disabled={pending} onChange={event => update('queue_wait_seconds', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">秒</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span><span className="block">排板确认快照</span><span className="mt-0.5 block text-[10px] text-muted">0 为涨停即排</span></span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={10} step={1} value={draft.queue_confirm_snapshots} disabled={pending} onChange={event => update('queue_confirm_snapshots', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">次</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span><span className="block">自动下单资金方式</span><span className="mt-0.5 block text-[10px] text-muted">按 QMT 提交时的最新可用资金计算</span></span>
        <select value={draft.order_allocation_mode} disabled={pending} onChange={event => update('order_allocation_mode', event.target.value as QmtAllocationMode)} className="h-8 w-36 rounded-btn border border-border bg-base px-2 text-xs outline-none focus:border-accent disabled:opacity-50">
          {QMT_ALLOCATION_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
        </select>
      </label>
      {draft.order_allocation_mode === 'fixed' ? <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span><span className="block">单板固定金额</span><span className="mt-0.5 block text-[10px] text-muted">0 保留旧配置的一手模式</span></span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={10000000} step={100} value={draft.order_amount_per_board} disabled={pending} onChange={event => update('order_amount_per_board', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">元</span></span>
      </label> : null}
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span><span className="block">每日自动打板上限</span><span className="mt-0.5 block text-[10px] text-muted">0 为不限制</span></span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={100} step={1} value={draft.max_auto_board_count} disabled={pending} onChange={event => update('max_auto_board_count', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">只</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span><span className="block">今日破板率停手阈值</span><span className="mt-0.5 block text-[10px] text-muted">达到后停止自动打板，默认 40%</span></span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={100} step={0.1} value={draft.max_market_broken_rate_pct} disabled={pending} onChange={event => update('max_market_broken_rate_pct', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">%</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 border-b border-border py-3 text-xs sm:col-span-2">
        <span><span className="block font-medium">自动候选仅沪深主板</span><span className="mt-0.5 block text-[10px] text-muted">只限制自动评分 Top 30，手工备选和打板池不受影响</span></span>
        <input type="checkbox" checked={draft.main_board_only} disabled={pending} onChange={event => update('main_board_only', event.target.checked)} />
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span>临板 WS 阈值</span>
        <span className="flex items-center gap-2"><input type="number" min={0.1} max={10} step={0.1} value={Number((draft.near_limit_pct * 100).toFixed(3))} disabled={pending} onChange={event => update('near_limit_pct', Number(event.target.value) / 100)} className={inputClass} /><span className="w-7 text-muted">%</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span>扫描退出阈值</span>
        <span className="flex items-center gap-2"><input type="number" min={0.1} max={20} step={0.1} value={Number((draft.exit_limit_pct * 100).toFixed(3))} disabled={pending} onChange={event => update('exit_limit_pct', Number(event.target.value) / 100)} className={inputClass} /><span className="w-7 text-muted">%</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 border-t border-border py-3 text-xs sm:border-t-0 sm:border-b sm:border-border">
        <span>退出持续时间</span>
        <span className="flex items-center gap-2"><input type="number" min={1} max={300} step={1} value={draft.exit_sustain_seconds} disabled={pending} onChange={event => update('exit_sustain_seconds', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">秒</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 py-3 text-xs sm:border-b sm:border-border">
        <span>首板回看</span>
        <span className="flex items-center gap-2"><input type="number" min={1} max={60} step={1} value={draft.first_board_lookback_days} disabled={pending} onChange={event => update('first_board_lookback_days', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">日</span></span>
      </label>
      <label className="flex items-center justify-between gap-3 border-t border-border py-3 text-xs sm:col-span-2 sm:border-t-0">
        <span>炸板黑名单阈值</span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={20} step={1} value={draft.blacklist_after_breaks} disabled={pending} onChange={event => update('blacklist_after_breaks', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">次</span></span>
      </label>
    </div>
    {!valid ? <div className="border-t border-border px-4 py-2 text-[11px] text-danger">请检查参数范围，且扫描退出阈值不能小于临板 WS 阈值。</div> : null}
    <div className="flex justify-end gap-2 border-t border-border px-4 py-3"><button type="button" onClick={onClose} disabled={pending} className="h-8 rounded-btn border border-border px-3 text-xs text-muted disabled:opacity-50">取消</button><button type="button" onClick={() => onSave(draft)} disabled={pending || !valid} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-50"><Check className="h-3.5 w-3.5" />{pending ? '保存中…' : '保存'}</button></div>
  </Modal>
}

function advancedSettings(value: LimitBoardView['settings']): AdvancedSettings {
  return {
    sweep_price_levels: value.sweep_price_levels,
    queue_wait_seconds: value.queue_wait_seconds,
    queue_confirm_snapshots: value.queue_confirm_snapshots,
    order_allocation_mode: value.order_allocation_mode ?? 'fixed',
    order_amount_per_board: value.order_amount_per_board,
    max_auto_board_count: value.max_auto_board_count,
    max_market_broken_rate_pct: value.max_market_broken_rate_pct,
    main_board_only: value.main_board_only,
    near_limit_pct: value.near_limit_pct,
    exit_limit_pct: value.exit_limit_pct,
    exit_sustain_seconds: value.exit_sustain_seconds,
    first_board_lookback_days: value.first_board_lookback_days,
    blacklist_after_breaks: value.blacklist_after_breaks,
  }
}

function fourModeConfigValue(value: unknown): string {
  if (typeof value === 'string') return value
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(2)
  if (typeof value === 'boolean') return value ? '是' : '否'
  try { return JSON.stringify(value) } catch { return '--' }
}

function FourModePanel({ report }: { report: FourModeStrategyView }) {
  if (report.state !== 'available') {
    return <EmptyState icon={BookOpen} title="四合一规则解析不可用" hint={report.reason} />
  }
  const unavailable = report.dependencies.filter(item => !item.available)
  return <div className="space-y-3">
    <section className="rounded-btn border border-border bg-surface px-3 py-3 sm:px-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-sm font-semibold"><BookOpen className="h-4 w-4 text-accent" />{report.source.title || '四合一策略'}</div>
          <div className="mt-1 text-[10px] text-muted">{report.reason}</div>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 font-mono text-[10px] text-muted">
            <span>源码 {report.source.path}</span>
            {report.source.sha256 ? <span>SHA256 {report.source.sha256.slice(0, 12)}…</span> : null}
            {report.source.parsed_at ? <span>解析 {scoreTime(report.source.parsed_at)}</span> : null}
          </div>
        </div>
        <span className="inline-flex shrink-0 items-center gap-1 rounded-md border border-warning/40 bg-warning/10 px-2 py-1 text-[10px] text-warning"><GitBranch className="h-3 w-3" />只读规则解析 · 不生成委托</span>
      </div>
    </section>

    <section className="grid gap-3 xl:grid-cols-2">
      {report.modes.map(mode => <article key={mode.id} className="rounded-btn border border-border bg-surface p-3">
        <div className="flex items-start justify-between gap-2">
          <div><div className="text-sm font-semibold">{mode.name}</div><div className="mt-1 text-[11px] leading-5 text-secondary">{mode.summary}</div></div>
          <span className="rounded-md bg-elevated px-2 py-1 font-mono text-[10px] text-accent">{mode.id}</span>
        </div>
        <div className="mt-3 grid gap-2 text-[10px] sm:grid-cols-2">
          <div className="rounded-md bg-elevated/60 px-2 py-2"><div className="text-muted">运行阶段</div><div className="mt-1 text-secondary">{mode.runtime}</div></div>
          <div className="rounded-md bg-elevated/60 px-2 py-2"><div className="text-muted">状态字段</div><div className="mt-1 truncate font-mono text-secondary" title={mode.state_fields.join('、')}>{mode.state_fields.length ? mode.state_fields.join('、') : '--'}</div></div>
        </div>
        <div className="mt-3 border-t border-border pt-2"><div className="text-[10px] text-muted">关键函数</div><div className="mt-1 flex flex-wrap gap-1.5">{mode.functions.map(fn => <span key={fn.name} className="rounded-md border border-border px-1.5 py-1 font-mono text-[9px] text-secondary">{fn.name}{fn.line ? `:${fn.line}` : ''}</span>)}</div></div>
        <div className="mt-3 border-t border-border pt-2"><div className="text-[10px] text-muted">源码参数</div><div className="mt-1 flex flex-wrap gap-x-3 gap-y-1">{mode.config.map(item => <span key={item.key} className="font-mono text-[10px] text-secondary">{item.key}={fourModeConfigValue(item.value)}</span>)}</div></div>
      </article>)}
    </section>

    <section className="overflow-hidden rounded-btn border border-border bg-surface">
      <div className="flex items-center gap-2 border-b border-border px-3 py-2.5 text-xs font-medium"><Clock3 className="h-3.5 w-3.5 text-accent" />日内调度</div>
      <div className="overflow-x-auto"><table className="w-full min-w-[620px] text-left text-[10px]"><thead className="bg-elevated/60 text-muted"><tr><th className="px-3 py-2 font-medium">时间</th><th className="px-3 py-2 font-medium">函数</th><th className="px-3 py-2 font-medium">用途</th></tr></thead><tbody>{report.schedule.map(item => <tr key={`${item.time}-${item.function}`} className="border-t border-border"><td className="px-3 py-2 font-mono text-accent">{item.time}</td><td className="px-3 py-2 font-mono text-secondary">{item.function}</td><td className="px-3 py-2 text-secondary">{item.description}</td></tr>)}</tbody></table></div>
    </section>

    <section className="overflow-hidden rounded-btn border border-border bg-surface">
      <div className="flex items-center gap-2 border-b border-border px-3 py-2.5 text-xs font-medium"><Database className="h-3.5 w-3.5 text-accent" />依赖与接入状态</div>
      <div className="overflow-x-auto"><table className="w-full min-w-[760px] text-left text-[10px]"><thead className="bg-elevated/60 text-muted"><tr><th className="px-3 py-2 font-medium">依赖</th><th className="px-3 py-2 font-medium">用途</th><th className="px-3 py-2 font-medium">当前状态</th><th className="px-3 py-2 font-medium">说明</th></tr></thead><tbody>{report.dependencies.map(item => <tr key={item.name} className="border-t border-border"><td className="px-3 py-2 font-mono text-secondary">{item.name}</td><td className="px-3 py-2 text-secondary">{item.kind}</td><td className={`px-3 py-2 font-medium ${item.available ? 'text-bear' : 'text-warning'}`}>{item.available ? '库可见' : '未接入'}{item.referenced ? '' : ' · 源码未直接导入'}</td><td className="px-3 py-2 text-muted">{item.note}</td></tr>)}</tbody></table></div>
      <div className="flex flex-wrap items-center gap-2 border-t border-border px-3 py-2.5 text-[10px] text-warning"><ShieldAlert className="h-3.5 w-3.5" />{unavailable.length} 项依赖尚未形成可验证的系统数据契约；当前仅供查看策略结构。</div>
    </section>
  </div>
}

export function LimitBoard() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('sector')
  const [search, setSearch] = useState('')
  const [preview, setPreview] = useState<{ symbol: string; name?: string } | null>(null)
  const [notificationOpen, setNotificationOpen] = useState(false)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [candidateAlgorithmOpen, setCandidateAlgorithmOpen] = useState(false)
  const [tradeRow, setTradeRow] = useState<LimitBoardRow | null>(null)
  const view = useQuery({
    queryKey: QK.limitBoard,
    queryFn: api.limitBoard,
    refetchInterval: query => Math.max(
      1,
      query.state.data?.runtime.refresh_cycle.interval_seconds ?? 5,
    ) * 1000,
    placeholderData: previous => previous,
  })
  const unifiedRefreshIntervalMs = Math.max(
    1,
    view.data?.runtime.refresh_cycle.interval_seconds ?? 5,
  ) * 1000
  const heat = useQuery({
    queryKey: QK.marketHeatRadar(30),
    queryFn: () => api.marketHeatRadar(30, true),
    enabled: tab === 'sector',
    refetchInterval: tab === 'sector' ? 60_000 : false,
    staleTime: 60_000,
    placeholderData: previous => previous,
  })
  const heatSymbols = useMemo(
    () => (heat.data?.lists.hot_day.items ?? []).slice(0, 30).map(item => item.thscode.toUpperCase()),
    [heat.data?.lists.hot_day.items],
  )
  const heatQuotes = useQuery({
    queryKey: QK.limitBoardQuotes(heatSymbols.join(',')),
    queryFn: () => api.limitBoardQuotes(heatSymbols, true),
    enabled: tab === 'sector' && heatSymbols.length > 0,
    refetchInterval: tab === 'sector' ? unifiedRefreshIntervalMs : false,
    staleTime: Math.max(1000, unifiedRefreshIntervalMs - 1000),
    placeholderData: previous => previous,
  })
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
  const updateAdvanced = useMutation({
    mutationFn: (settings: AdvancedSettings) => (
      api.limitBoardAdvancedSettingsUpdate(settings, view.data?.revision ?? 0)
    ),
    onSuccess: async () => {
      await refresh()
      setAdvancedOpen(false)
    },
  })

  const poolSymbols = useMemo(() => new Set((view.data?.board_pool ?? []).map(row => row.symbol)), [view.data?.board_pool])
  const candidateSymbols = useMemo(() => new Set([
    ...(view.data?.candidate_pool ?? []).map(row => row.symbol),
    ...(view.data?.opportunity_pool ?? []).map(row => row.symbol),
    ...(view.data?.board_pool ?? []).map(row => row.symbol),
  ]), [view.data?.candidate_pool, view.data?.opportunity_pool, view.data?.board_pool])
  const searchResults = (searchQuery.data?.results ?? []).filter(item => !isStName(item.name))
  const busy = add.isPending || addPool.isPending || removeCandidate.isPending || updatePool.isPending || removePool.isPending || updateNotifications.isPending || updateAdvanced.isPending
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="短线猎手加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'candidate' ? data.candidate_pool : tab === 'opportunity' ? data.opportunity_pool : tab === 'pool' ? data.board_pool : []
  const tableMode: TableMode = tab === 'pool' ? 'pool' : 'candidate'
  const tableTitle = tab === 'candidate' ? '备选池' : tab === 'opportunity' ? '可交易机会' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? `扫板：卖一距涨停不超过 ${data.settings.sweep_price_levels} 个价位时提交；排板：${queueTriggerDescription(data.settings.queue_wait_seconds, data.settings.queue_confirm_snapshots)}`
    : tab === 'opportunity'
      ? '独立机会分排序：强势确认、评分上升速度、日内分时和成交空间；只显示仍有成交空间的实时标的'
    : `前 10 板块强势股统一打分，自动候选只取 Top 30${data.settings.main_board_only ? ' · 仅沪深主板' : ''}；手工标的不受限制`
  const sentimentPanel = <section className="border-b border-border px-4 py-3 sm:px-5">
    <div className="grid min-w-[720px] grid-cols-5 divide-x divide-border overflow-x-auto rounded-btn border border-border bg-surface">
      {[
        ['今日破板率', data.market_sentiment ? plainPercentValue(data.market_sentiment.market_broken_rate_pct) : '--', runtime.sentiment_guard.blocked ? 'text-danger' : 'text-secondary'],
        ['昨日涨停今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_limitup_change_pct) : '--', 'text-secondary'],
        ['昨日连板今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_consecutive_change_pct) : '--', 'text-secondary'],
        ['昨日破板今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_broken_change_pct) : '--', 'text-secondary'],
        ['开盘啦情绪 / 连板高度', `${data.market_sentiment?.market_evaluation || '--'} / ${data.market_sentiment?.max_consecutive ?? '--'}板`, 'text-accent'],
      ].map(([label, value, tone]) => <div key={label} className="min-w-0 px-3 py-2.5"><div className="truncate text-[10px] text-muted">{label}</div><div className={`mt-1 truncate font-mono text-sm ${tone}`}>{value}</div></div>)}
    </div>
    <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted">
      {data.market_sentiment ? <span>{data.market_sentiment.state === 'live' ? '开盘啦实时情绪数据' : data.market_sentiment.state === 'stale' ? `${data.market_sentiment.as_of ?? '--'} 开盘啦收盘数据` : '开盘啦实时情绪数据暂不可用'}</span> : <span>开盘啦实时情绪数据暂不可用</span>}
      {data.market_sentiment ? <span>刷新 {scoreTime(data.market_sentiment.refreshed_at)}</span> : null}
      <span className={runtime.sentiment_guard.blocked ? 'text-danger' : 'text-secondary'}>{runtime.sentiment_guard.reason}</span>
    </div>
    {runtime.sentiment_guard.blocked ? <div className="mt-2 flex items-center gap-2 rounded-btn border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger"><ShieldAlert className="h-3.5 w-3.5" />自动打板已停止</div> : null}
  </section>
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="短线猎手"
        titleExtra={<div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted">
          <span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-secondary"><Radio className="h-3 w-3 text-accent" />打板池 {runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>
          <span className={`inline-flex items-center gap-1.5 ${runtime.websocket_status === 'connected' ? 'text-bear' : 'text-muted'}`}><Wifi className="h-3.5 w-3.5" />{runtime.websocket_status === 'connected' ? '打板池已接入 WS' : '备选池仅实时轮询'}</span>
          <span className={runtime.trading_enabled ? 'text-bear' : 'text-warning'}>{runtime.trading_reason}</span>
          {!runtime.first_board_enabled ? <span className="max-w-[520px] truncate text-warning" title={`强势股打分暂不可用：${runtime.candidate_scope.state === 'unavailable' ? runtime.candidate_scope.reason : runtime.history_reason}`}>
            强势股打分暂不可用：{runtime.candidate_scope.state === 'unavailable' ? runtime.candidate_scope.reason : runtime.history_reason}
          </span> : null}
        </div>}
        right={<div className="flex flex-wrap items-center justify-end gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入备选池" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchResults.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchResults.map(item => <button type="button" key={item.symbol} disabled={candidateSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" onClick={() => setAdvancedOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><SlidersHorizontal className="h-3.5 w-3.5" />高级设置</button><button type="button" onClick={() => setNotificationOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><Bell className="h-3.5 w-3.5" />通知设置</button><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
      />

      <div className="flex items-center gap-1 overflow-x-auto border-b border-border px-4 pt-2 sm:px-5">
        {([
          ['ladder', '连板天梯', null, Flame],
          ['sector', '板块强度', data.sector_strength?.rows.length ?? 0, Layers3],
          ['candidate', '备选池', data.candidate_pool.length, ListFilter],
          ['opportunity', '机会榜', data.opportunity_pool.length, Zap],
          ['pool', '打板池', data.board_pool.length, Crosshair],
          ['four_mode', '四合一', data.four_mode.modes.length, BookOpen],
          ['events', '触发记录', data.events.length, Bell],
        ] as const).map(([id, label, count, Icon]) => (
          <button key={id} type="button" onClick={() => setTab(id)} className={`inline-flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === id ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}>
            <Icon className="h-3.5 w-3.5" />{label}{count == null ? null : <span className="font-mono text-[10px] text-muted">{count}</span>}
          </button>
        ))}
      </div>

      <div className={`min-h-0 flex-1 ${tab === 'ladder' ? 'overflow-hidden' : 'overflow-x-hidden overflow-y-auto px-2 py-3 sm:px-5'}`}>
        {tab === 'ladder' ? <Suspense fallback={<div className="grid h-full place-items-center"><RefreshCw className="h-5 w-5 animate-spin text-muted" /></div>}><EmbeddedLimitLadder headerContent={sentimentPanel} /></Suspense> : tab === 'sector' ? <SectorStrengthTable snapshot={data.sector_strength} signalRows={data.first_board} hotRows={heat.data?.lists.hot_day.items ?? []} hotQuotes={heatQuotes.data?.quotes} hotSectorLinks={heatQuotes.data?.sector_links} hotLoading={heat.isPending} hotError={heat.isError} refreshIntervalSeconds={runtime.refresh_cycle.interval_seconds} refreshCycleUpdatedAt={view.dataUpdatedAt} onOpenAlgorithm={() => setCandidateAlgorithmOpen(true)} onOpenStock={(symbol, name) => setPreview({ symbol, name })} /> : tab === 'four_mode' ? <FourModePanel report={data.four_mode} /> : tab !== 'events' ? (
          <section className="overflow-hidden rounded-btn border border-border bg-surface">
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
              <div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>
              <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px]">
                {tab === 'candidate' || tab === 'opportunity' ? <button type="button" onClick={() => setCandidateAlgorithmOpen(true)} className="inline-flex items-center gap-1 rounded-btn border border-border px-2 py-1 text-secondary hover:bg-elevated hover:text-foreground"><CircleHelp className="h-3.5 w-3.5" />排序算法</button> : null}
                {runtime.last_error ? <span className="text-warning">{runtime.last_error}</span> : null}
              </div>
            </div>
            <Table
              rows={rows}
              mode={tableMode}
              poolSymbols={poolSymbols}
              busy={busy}
              sweepPriceLevels={data.settings.sweep_price_levels}
              queueWaitSeconds={data.settings.queue_wait_seconds}
              queueConfirmSnapshots={data.settings.queue_confirm_snapshots}
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
              onTrade={setTradeRow}
            />
          </section>
        ) : (
          <section className="divide-y divide-border overflow-hidden rounded-btn border border-border bg-surface">
            {data.events.length ? data.events.map((event: LimitBoardEvent, index: number) => {
              const eventThemes = themes(event.concept).slice(0, 2)
              const timeline = event.order_timeline
              const brokerTime = timeline?.broker_order_at
                ? exactTime(timeline.broker_order_at)
                : timeline?.broker_order_time_raw != null
                  ? `原始值 ${String(timeline.broker_order_time_raw)}`
                  : 'QMT 未返回券商委托时间'
              return <div key={`${event.ts}-${index}`} className="flex items-start gap-3 px-3 py-3 text-xs"><span className={event.type === 'broken' ? 'text-danger' : event.type === 'resealed' ? 'text-bull' : 'text-accent'}>{STATUS[event.type]?.label || event.type}</span><div className="min-w-0 flex-1"><button type="button" onClick={() => setPreview({ symbol: event.symbol, name: event.name })} className="font-medium hover:text-accent" title="查看 K 线与分时">{event.name} <span className="ml-1 font-mono text-[10px] text-muted">{event.symbol}</span></button>{eventThemes.length ? <div className="mt-1 truncate text-[10px] text-secondary">题材：{eventThemes.join('、')}</div> : null}<div className="mt-1 text-[11px] text-secondary">{event.reasons?.join('；')}</div>{timeline ? <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 border-t border-border pt-2 font-mono text-[10px] text-muted"><span>策略触发 {exactTime(timeline.trigger_at || event.trigger_at || event.ts)}</span><span>系统送单 {exactTime(timeline.system_order_at)}</span><span>QMT 提交 {exactTime(timeline.qmt_submit_at)}</span><span>QMT 返回 {exactTime(timeline.qmt_response_at || timeline.qmt_accepted_at)}</span><span>券商委托 {brokerTime}</span>{timeline.system_to_broker_delay_ms != null ? <span className="text-foreground">送单到券商 {elapsedTime(timeline.system_to_broker_delay_ms)}</span> : null}{timeline.status ? <span className={ORDER_STATUS[timeline.status]?.tone || 'text-muted'}>{ORDER_STATUS[timeline.status]?.label || timeline.status}</span> : null}</div> : event.type === 'touched' ? <div className="mt-2 border-t border-border pt-2 text-[10px] text-muted">未发送自动委托</div> : null}</div><div className="shrink-0 text-right font-mono text-[10px] text-muted"><div>炸板 {event.break_count || 0} 次</div><div>{exactTime(event.trigger_at || event.ts)}</div></div></div>
            }) : <div className="px-4 py-12 text-center text-xs text-muted">今天还没有涨停、炸板或回封记录</div>}
          </section>
        )}
      </div>

      {data.blacklist.length ? <div className="flex items-center gap-2 border-t border-border px-4 py-2 text-[10px] text-danger sm:px-5"><Ban className="h-3.5 w-3.5" />今日黑名单：{data.blacklist.join('、')}</div> : null}
      {advancedOpen ? <AdvancedSettingsDialog value={advancedSettings(data.settings)} pending={updateAdvanced.isPending} onClose={() => setAdvancedOpen(false)} onSave={value => updateAdvanced.mutate(value)} /> : null}
      {notificationOpen ? <NotificationDialog value={data.settings.notifications} pending={updateNotifications.isPending} onClose={() => setNotificationOpen(false)} onSave={value => updateNotifications.mutate(value)} /> : null}
      {candidateAlgorithmOpen ? <CandidateAlgorithmDialog onClose={() => setCandidateAlgorithmOpen(false)} /> : null}
      {tradeRow ? <QmtTradePanel instrument={{ symbol: tradeRow.symbol, name: tradeRow.name, price: tradeRow.last_price }} preset={{ action: 'BUY' }} onClose={() => setTradeRow(null)} /> : null}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
