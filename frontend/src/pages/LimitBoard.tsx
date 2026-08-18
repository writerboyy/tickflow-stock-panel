import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Ban,
  Bell,
  Check,
  CircleHelp,
  CircleDot,
  Crosshair,
  Flame,
  Layers3,
  ListFilter,
  Plus,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  SlidersHorizontal,
  Trash2,
  Wifi,
  X,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { PageHeader } from '@/components/PageHeader'
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
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const EmbeddedLimitLadder = lazy(() => import('./LimitUpLadder').then(module => ({ default: module.LimitUpLadder })))

type Tab = 'ladder' | 'sector' | 'candidate' | 'pool' | 'events'
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
  const rotationTitle = (sector?.days ?? []).map(day => `${day.date.slice(5)} ${scorePct(day.change_pct)} #${day.rank}/${day.rank_count}`).join('；')
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
            <div className="font-mono text-sm font-semibold tabular-nums text-accent">#{row.candidate_rank} · {row.candidate_score.toFixed(1)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">板{sector?.score.toFixed(1)} 基{gene?.score.toFixed(1)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">分{intradayFlow?.score.toFixed(1)} 技{technical?.score.toFixed(1)}</div>
          </>}
          {row.candidate_score_state === 'cached' ? <div className="mt-0.5 whitespace-nowrap text-[9px] text-warning">缓存 · {scoreTime(row.candidate_score_as_of)}</div> : null}
        </td>
        <td className="w-[92px] min-w-[92px] px-2 font-mono tabular-nums text-secondary">{change}</td>
        <td className="w-[190px] min-w-[190px] px-2" title={intradayFlow ? `日内走势 ${intradayFlow.trend_score?.toFixed(1) ?? '--'}/${intradayFlow.trend_max_score?.toFixed(1) ?? '--'}；${intradayFlow.price_volume_rising ? '量价齐升' : '未形成量价齐升'}；资金源 ${intradayFlow.capital_source_label ?? '暂无'}` : undefined}>
          {intradayFlow ? <>
            <div className="font-mono text-[10px] text-secondary">走势 {intradayFlow.trend_score?.toFixed(1) ?? '--'}/{intradayFlow.trend_max_score?.toFixed(1) ?? '--'} · 资金 {intradayFlow.capital_available ? `${intradayFlow.capital_score?.toFixed(1) ?? '--'}/${intradayFlow.capital_max_score?.toFixed(1) ?? '--'}` : '待补'}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">{intradayFlow.trend_state === 'strong' ? '日内强势' : intradayFlow.trend_state === 'weak' ? '日内偏弱' : '日内中性'} · 水下 {ratioPct(intradayFlow.underwater_ratio)} · {intradayFlow.price_volume_rising ? '量价齐升' : '量价未齐升'}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">{intradayFlow.capital_available ? intradayFlow.flow_metric === 'main_net_speed' ? `${intradayFlow.capital_source_label ?? '主力净额涨速'} ${moneyYi(intradayFlow.net_flow_speed)}/分 · 净流向 ${scorePct(intradayFlow.net_flow_ratio, 0)}` : `${intradayFlow.capital_source_label ?? '实时主动资金'} · 净流向 ${scorePct(intradayFlow.net_flow_ratio, 0)} · 连续流出 ${intradayFlow.outflow_streak ?? 0} 根` : intradayFlow.capital_source_label ?? '实时主动资金待补'}</div>
          </> : <div className="text-muted">分时待补</div>}
        </td>
        <td className="w-[210px] min-w-[210px] px-2" title={rotationTitle || allThemes.join('、') || undefined}>
          {sector ? <>
            <div className="flex items-center gap-1.5"><span className="max-w-[110px] truncate font-medium">{sector.name}</span><span className="text-secondary">{scorePct(sector.change_pct)}</span></div>
            <div className="mt-0.5 flex items-center gap-1.5 text-[9px]"><span className="text-secondary">{leadership}</span><span className="font-mono text-muted">#{sector.stock_rank ?? '--'}/{sector.member_count ?? '--'}</span><span className="text-secondary">{sector.rotation_label ?? '数据不足'}</span></div>
            {sector.realtime_available ? <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-secondary" title="开盘啦实时板块强度"><span>强 {sector.realtime_strength?.toFixed(1) ?? '--'}</span><span className="ml-1.5">板 #{sector.realtime_rank ?? '--'}/{sector.realtime_rank_count ?? '--'}</span><span className="ml-1.5">速 {scorePct(sector.realtime_speed_pct, 2)}</span></div> : <div className="mt-0.5 whitespace-nowrap text-[9px] text-muted">实时板块强度待补</div>}
            {sector.realtime_available ? <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">主净 {sector.realtime_main_net == null ? '--' : sector.realtime_main_net.toFixed(0)} · 量比 {sector.realtime_volume_ratio?.toFixed(2) ?? '--'}</div> : null}
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">5日 {scorePct(sector.five_day_change_pct)} · 昨 {scorePct(sector.yesterday_change_pct)}</div>
            {sector.leader && !sector.is_sector_leader ? <div className="mt-0.5 max-w-[190px] truncate text-[9px] text-muted">龙头 {sector.leader.name || sector.leader.symbol} {scorePct(sector.leader.change_pct)}</div> : null}
          </> : <div className="text-muted">实时板块强度待补</div>}
        </td>
        <td className="w-[170px] min-w-[170px] px-2" title={gene ? `快照 ${gene.as_of || '--'}；样本 ${gene.next_day_observation_count ?? 0}` : undefined}>
          {gene ? <>
            <div className="font-mono text-[10px] text-secondary">涨 {gene.limit_up_count ?? '--'} · 红 {scorePct(gene.next_day_red_rate, 0)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">溢 {scorePct(gene.premium_5_rate, 0)} · 封 {scorePct(gene.first_board_seal_rate, 0)} · 晋 {scorePct(gene.consecutive_rate, 0)}</div>
            <div className="mt-0.5 font-mono text-[9px] text-secondary">{gene.score.toFixed(1)}/30</div>
          </> : <div className="text-muted">基因待补</div>}
        </td>
        <td className="w-[180px] min-w-[180px] px-2" title={technical ? `MA5 ${technical.ma5?.toFixed(2) ?? '--'}；MA10 ${technical.ma10?.toFixed(2) ?? '--'}；MA20 ${technical.ma20?.toFixed(2) ?? '--'}；MA60 ${technical.ma60?.toFixed(2) ?? '--'}` : undefined}>
          {technical ? <>
            <div className="whitespace-nowrap font-mono text-[9px] text-secondary">均 {technical.components?.trend?.toFixed(2)}/1.75 · 动 {technical.components?.momentum?.toFixed(2)}/1.25</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">量 {technical.components?.volume?.toFixed(2)}/0.75 · MACD {technical.components?.macd?.toFixed(2)}/0.75 · RSI {technical.components?.rsi?.toFixed(2)}/0.50</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">量比 {technical.vol_ratio_5d?.toFixed(2) ?? '--'} · RSI {technical.rsi_14?.toFixed(0) ?? '--'}</div>
          </> : <div className="text-muted">技术面待补</div>}
        </td>
      </> : null}
      <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
      {mode !== 'candidate' ? <td className="px-2 font-mono tabular-nums text-secondary">{row.limit_up?.toFixed(2) ?? '--'}</td> : null}
      <td className="px-2 font-mono tabular-nums text-secondary">{gap}</td>
      <td className="px-2">
        <span className={`inline-flex items-center gap-1 font-medium ${status.tone}`}>
          <CircleDot className="h-3 w-3" />{status.label}
        </span>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.break_count ? `${row.break_count} 次` : '0 次'}</td>
      {mode !== 'candidate' ? <td className="px-2 font-mono tabular-nums text-secondary">{row.bid1_volume ? row.bid1_volume.toLocaleString('zh-CN') : '--'}</td> : null}
      {mode !== 'candidate' ? <td className="px-2">
        <span className={mode === 'pool' && row.ws_active ? 'text-bear' : 'text-muted'}>{mode === 'pool' && row.ws_active ? 'WS' : '轮询'}</span>
      </td> : null}
      {mode === 'pool' ? (
        <>
          <td className={`px-2 font-medium ${orderStatus.tone}`} title={row.auto_order_error || undefined}>
            {orderStatus.label}
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
}

function Table(props: TableProps) {
  const { rows, mode } = props
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="max-w-full overflow-x-auto overscroll-x-contain" style={{ WebkitOverflowScrolling: 'touch' }}>
      <table className={`w-full border-collapse ${mode === 'candidate' ? 'min-w-[1670px]' : 'min-w-[1080px]'}`}>
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="sticky left-0 z-40 w-[128px] overflow-hidden bg-surface py-2 pl-3 pr-2">标的</th>
            <th className="w-[160px] px-2">题材</th>
            {mode === 'candidate' ? <><th className="w-[116px] min-w-[116px] whitespace-nowrap px-2">总分</th><th className="w-[92px] min-w-[92px] whitespace-nowrap px-2">涨幅</th><th className="px-2">分时强度</th><th className="px-2">当前板块</th><th className="px-2">涨停基因</th><th className="px-2">技术面</th></> : null}
            <th className="px-2">现价</th>{mode !== 'candidate' ? <th className="px-2">涨停价</th> : null}<th className="px-2">距涨停</th><th className="px-2">状态</th><th className="px-2">炸板次数</th>{mode !== 'candidate' ? <><th className="px-2">买一封单</th><th className="px-2">行情</th></> : null}
            {mode === 'pool' ? <><th className="px-2">委托状态</th><th className="sticky right-0 z-40 w-[220px] border-l border-border bg-surface px-2 text-right">操作</th></> : <th className="sticky right-0 z-40 w-[96px] border-l border-border bg-surface px-2 text-right">操作</th>}
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
  useEffect(() => {
    const now = Date.now()
    setCycleStartedAt(now)
    setProgressClock(now)
  }, [snapshot?.refreshed_at])
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
    : snapshot?.history_state === 'closed' ? '非落库时段' : '盘中时序落库不可用'
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
  const progressDuration = Math.max(1, refreshIntervalSeconds) * 1000
  const refreshProgress = isLive
    ? Math.min(100, Math.max(0, ((progressClock - cycleStartedAt) / progressDuration) * 100))
    : 100
  const trend = rankingWindowMinutes === 5
    ? activeSnapshot?.trend_5m
    : activeSnapshot?.trend_30m
  const mainNetDelta = (row: LimitBoardSectorStrengthRow) => (
    rankingWindowMinutes === 5 ? row.main_net_delta_5m : row.main_net_delta_30m
  )
  const windowRisingRanking = [...rows]
    .filter(row => {
      const value = mainNetDelta(row)
      return value != null && Number.isFinite(value) && value > 0
    })
    .sort((left, right) => Number(mainNetDelta(right)) - Number(mainNetDelta(left)))
    .slice(0, 3)
  const windowFallingRanking = [...rows]
    .filter(row => {
      const value = mainNetDelta(row)
      return value != null && Number.isFinite(value) && value < 0
    })
    .sort((left, right) => Number(mainNetDelta(left)) - Number(mainNetDelta(right)))
    .slice(0, 3)
  return <div className="space-y-3">
    <section className="overflow-hidden rounded-btn border border-border bg-surface">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
        <div><div className="text-xs font-medium">板块区间排序</div><div className="mt-0.5 text-[10px] text-muted">{trend ? `${scoreTime(trend.base_at)} → ${scoreTime(trend.captured_at)} · 按同一板块两个截面的变化量排序` : `正在积累 ${rankingWindowMinutes} 分钟可比截面`}</div></div>
        <div className="inline-flex overflow-hidden rounded-btn border border-border bg-base" aria-label="选择板块排序周期">
          {([5, 30] as const).map(minutes => <button key={minutes} type="button" aria-pressed={rankingWindowMinutes === minutes} onClick={() => setRankingWindowMinutes(minutes)} className={`h-7 px-3 text-[10px] ${rankingWindowMinutes === minutes ? 'bg-accent/15 text-accent' : 'text-muted hover:bg-elevated hover:text-foreground'}`}>{minutes} 分钟</button>)}
        </div>
      </div>
      <div className="grid min-w-[720px] grid-cols-2 divide-x divide-border overflow-x-auto">
        <div className="min-w-0">
          <div className="grid grid-cols-[42px_1fr_100px] border-b border-border px-3 py-2 text-[10px] text-muted"><span>排名</span><span>板块</span><span className="text-right">{rankingWindowMinutes} 分钟主力净额涨速</span></div>
          {windowRisingRanking.length ? windowRisingRanking.map((row, index) => <div key={row.plate_id} className="grid grid-cols-[42px_1fr_100px] border-b border-border/70 px-3 py-2 text-xs last:border-b-0"><span className="font-mono text-muted">#{index + 1}</span><span className="truncate text-secondary">{row.plate_name || row.plate_id}</span><span className="text-right font-mono font-medium text-bull">{moneyYi(mainNetDelta(row))}</span></div>) : <div className="px-3 py-8 text-center text-xs text-muted">暂无主力净额涨速数据</div>}
        </div>
        <div className="min-w-0">
          <div className="grid grid-cols-[42px_1fr_100px] border-b border-border px-3 py-2 text-[10px] text-muted"><span>排名</span><span>板块</span><span className="text-right">{rankingWindowMinutes} 分钟主力净额跌速</span></div>
          {windowFallingRanking.length ? windowFallingRanking.map((row, index) => <div key={row.plate_id} className="grid grid-cols-[42px_1fr_100px] border-b border-border/70 px-3 py-2 text-xs last:border-b-0"><span className="font-mono text-muted">#{index + 1}</span><span className="truncate text-secondary">{row.plate_name || row.plate_id}</span><span className="text-right font-mono font-medium text-bear">{moneyYi(mainNetDelta(row))}</span></div>) : <div className="px-3 py-8 text-center text-xs text-muted">暂无主力净额跌速数据</div>}
        </div>
      </div>
    </section>
    <section className="overflow-hidden rounded-btn border border-border bg-surface">
    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
      <div><div className="text-xs font-medium">板块强度</div><div className="mt-0.5 text-[10px] text-muted">热股雷达在左侧独立展示；强势股打分、板块和成分股 {refreshIntervalSeconds} 秒统一刷新</div></div>
      <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px] text-muted">
        <span className={snapshot?.history_state === 'unavailable' ? 'text-warning' : 'text-secondary'}>{historyLabel}</span>
        <span>{activeSnapshot?.state === 'live' ? `${isLive ? '实时' : cursorAt ? '回看' : '收盘'} ${scoreTime(activeCapturedAt)}` : '实时板块数据暂不可用'}</span>
      </div>
    </div>
    <div className="h-0.5 bg-elevated" aria-label={`板块三栏统一刷新进度 ${Math.round(refreshProgress)}%`}><div className="h-full bg-accent transition-[width] duration-200 ease-linear" style={{ width: `${refreshProgress}%` }} /></div>
    <div className="overflow-x-auto overscroll-x-contain">
    <div className="grid min-w-0 lg:min-w-[1020px] lg:grid-cols-[18%_18%_24%_40%]">
      <div className="min-w-0 border-b border-border lg:border-b-0 lg:border-r">
        <div className="flex min-h-12 items-center justify-between gap-2 border-b border-border px-3 py-2">
          <div className="inline-flex items-center gap-1.5 text-xs font-medium"><Flame className="h-3.5 w-3.5 text-accent" />热股雷达</div>
          <div className="shrink-0 text-[9px] text-muted">榜60秒 · 行情5秒</div>
        </div>
        {hotRows.length ? <div className="max-w-full overflow-x-auto overscroll-contain p-2 lg:max-h-[62vh] lg:overflow-x-hidden lg:overflow-y-auto">
          <div className="flex w-max gap-2 lg:w-full lg:flex-col">
            {hotRows.slice(0, 30).map(item => {
              const quote = hotQuotes[item.thscode.toUpperCase()]
              const selected = item.thscode.toUpperCase() === selectedStockSymbol
              const atLimit = quote?.last_price != null && quote.limit_up != null
                && quote.last_price >= quote.limit_up - 0.001
              return <button
                key={item.thscode}
                type="button"
                aria-pressed={selected}
                onClick={() => selectStock(item.thscode)}
                className={`h-[68px] w-[164px] shrink-0 rounded-btn border px-2.5 py-2 text-left outline-none transition-colors hover:border-warning/60 hover:bg-warning/5 focus-visible:ring-1 focus-visible:ring-warning lg:w-full ${selected ? 'border-warning bg-warning/15 ring-1 ring-warning/60' : 'border-border bg-surface'}`}
                title="联动强势股、实时板块与成分股"
              >
                <div className="flex items-center justify-between gap-2"><span className="min-w-0 truncate text-xs font-medium">{item.name || item.ticker}</span><span className="shrink-0 font-mono text-[10px] text-accent">#{item.rank ?? '--'}</span></div>
                <div className="mt-0.5 flex items-center justify-between gap-2 font-mono text-[9px]"><span className="truncate text-muted">{item.thscode}</span><span className="shrink-0"><span className="text-secondary">{quote?.last_price?.toFixed(2) ?? '--'}</span> <span className={financialTone(quote?.change_pct)}>{scorePct(quote?.change_pct, 2)}{atLimit ? '（涨停）' : ''}</span></span></div>
                <div className="mt-0.5 truncate font-mono text-[8px] text-muted">热度 {item.heat == null ? '--' : item.heat.toFixed(0)} · 排名变化 {item.rank_change == null ? '--' : `${item.rank_change > 0 ? '+' : ''}${item.rank_change}`}</div>
              </button>
            })}
          </div>
        </div> : <div className={`px-3 py-10 text-center text-xs ${hotError ? 'text-warning' : 'text-muted'}`}>{hotLoading ? '正在读取热股雷达' : hotError ? '热股雷达暂不可用' : '暂无热股数据'}</div>}
      </div>
      <div className="min-w-0 border-b border-border lg:border-b-0 lg:border-r">
        <div className="flex min-h-12 items-center justify-between gap-2 border-b border-border px-3 py-2">
          <div className="inline-flex items-center gap-1.5 text-xs font-medium"><Flame className="h-3.5 w-3.5 text-accent" />强势股打分</div>
          <div className="flex shrink-0 items-center gap-2">
            <span className="font-mono text-[10px] text-muted">{signalRows.length} 只</span>
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
              const names = new Set(signalSectorNames(signal).map(sectorNameKey))
              const matchedPlates = rows.filter(row => names.has(sectorNameKey(row.plate_name)))
              const displayThemes = matchedPlates.length
                ? matchedPlates.slice(0, 2).map(row => row.plate_name).filter((value): value is string => Boolean(value))
                : themes(signal.concept).slice(0, 2)
              return <button
                key={signal.symbol}
                type="button"
                aria-pressed={selected}
                onClick={() => selectStock(signal.symbol)}
                className={`h-[92px] w-[164px] shrink-0 rounded-btn border px-2.5 py-2 text-left outline-none transition-colors hover:border-warning/60 hover:bg-warning/5 focus-visible:ring-1 focus-visible:ring-warning lg:w-full ${selected ? 'border-warning bg-warning/15 ring-1 ring-warning/60' : 'border-border bg-surface'}`}
              >
                <div className="flex items-start justify-between gap-2"><span className="min-w-0 truncate text-xs font-medium">{signal.name || signal.symbol}</span><span className="shrink-0 text-right text-[9px] text-secondary"><span className="block">{rebound ? '反包' : '首板'}</span><span className="block font-mono text-accent" title="强势股打分最终总分">总分 {signal.candidate_score == null ? '--' : signal.candidate_score.toFixed(1)}</span></span></div>
                <div className="mt-0.5 flex items-center justify-between gap-1 font-mono text-[9px] text-muted"><span>{signal.symbol}</span><span className={financialTone(signal.change_pct)}>{scorePct(signal.change_pct, 2)}{atLimit ? '（涨停）' : ''}</span></div>
                <div className="mt-1.5 flex min-w-0 items-center gap-1 text-[9px] text-secondary">
                  {displayThemes.length ? displayThemes.map(name => <span key={name} className="max-w-[70px] truncate rounded-sm bg-elevated px-1 py-0.5">{name}</span>) : <span className="truncate text-muted">未匹配实时板块</span>}
                </div>
                <div className="mt-1 flex items-center justify-between text-[9px]"><span className={status.tone}>{status.label}</span><span className="font-mono text-muted">距涨停 {signal.limit_gap_pct == null ? '--' : scorePct(signal.limit_gap_pct, 2)}</span></div>
              </button>
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
              onClick={() => {
                setSelectedStockSymbol(null)
                setSelectedPlateId(row.plate_id)
              }}
              onKeyDown={event => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault()
                  setSelectedStockSymbol(null)
                  setSelectedPlateId(row.plate_id)
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
        {constituents.isError && !constituentData ? <div className="px-4 py-12 text-center text-xs text-danger">实时板块成分股加载失败</div> : constituentRows.length ? <div className="max-h-[62vh] max-w-full overflow-auto overscroll-contain">
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
        </div> : <div className="px-4 py-12 text-center text-xs text-muted">{constituents.isPending ? '正在读取实时板块成分股' : '该时间点没有可用的成分股数据'}</div>}
      </div>
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
            ['2', '成分去重', '使用上一完整交易日盘前落库的板块成分。'],
            ['3', '保留 Top 30', '首板、反包合并打分后只保留自动排名前 30。'],
          ].map(([step, title, detail]) => <li key={step} className="flex gap-2 border-t border-border pt-2"><span className="font-mono text-accent">{step}</span><span><strong className="font-medium text-foreground">{title}</strong><span className="mt-0.5 block text-[11px] leading-5 text-muted">{detail}</span></span></li>)}
        </ol>
        <p className="mt-2 text-[11px] leading-5 text-muted">默认近 10 个交易日无涨停记录为首板；窗口内曾涨停，随后出现炸板或断板，且最近一日未涨停为反包。“仅沪深主板”只限制自动候选，手工加入不受影响；距涨停不参与入选和打分。</p>
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
          <p className="sm:col-span-2"><strong className="font-medium text-foreground">缓存：</strong>5 秒一轮批量更新。同一交易日某项短暂缺数时可沿用最后有效值并标记“缓存”；跨交易日清空。实时板块或盘前成分数据缺失时，自动候选严格停止，不使用本地聚合降级。</p>
        </div>
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
        <span><span className="block">单板下单资金</span><span className="mt-0.5 block text-[10px] text-muted">0 为当前一手模式</span></span>
        <span className="flex items-center gap-2"><input type="number" min={0} max={10000000} step={100} value={draft.order_amount_per_board} disabled={pending} onChange={event => update('order_amount_per_board', Number(event.target.value))} className={inputClass} /><span className="w-7 text-muted">元</span></span>
      </label>
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

export function LimitBoard() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('sector')
  const [search, setSearch] = useState('')
  const [preview, setPreview] = useState<{ symbol: string; name?: string } | null>(null)
  const [notificationOpen, setNotificationOpen] = useState(false)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [candidateAlgorithmOpen, setCandidateAlgorithmOpen] = useState(false)
  const view = useQuery({ queryKey: QK.limitBoard, queryFn: api.limitBoard, refetchInterval: 5000, placeholderData: previous => previous })
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
    refetchInterval: tab === 'sector' ? 5000 : false,
    staleTime: 4000,
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
    ...(view.data?.board_pool ?? []).map(row => row.symbol),
  ]), [view.data?.candidate_pool, view.data?.board_pool])
  const searchResults = (searchQuery.data?.results ?? []).filter(item => !isStName(item.name))
  const busy = add.isPending || addPool.isPending || removeCandidate.isPending || updatePool.isPending || removePool.isPending || updateNotifications.isPending || updateAdvanced.isPending
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="短线猎手加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'candidate' ? data.candidate_pool : tab === 'pool' ? data.board_pool : []
  const tableMode: TableMode = tab === 'pool' ? 'pool' : 'candidate'
  const tableTitle = tab === 'candidate' ? '备选池' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? `扫板：卖一距涨停不超过 ${data.settings.sweep_price_levels} 个价位时提交；排板：${queueTriggerDescription(data.settings.queue_wait_seconds, data.settings.queue_confirm_snapshots)}`
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
        titleExtra={<span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-[10px] text-secondary"><Radio className="h-3 w-3 text-accent" />打板池 {runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>}
        right={<div className="flex flex-wrap items-center justify-end gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入备选池" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchResults.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchResults.map(item => <button type="button" key={item.symbol} disabled={candidateSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" onClick={() => setAdvancedOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><SlidersHorizontal className="h-3.5 w-3.5" />高级设置</button><button type="button" onClick={() => setNotificationOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><Bell className="h-3.5 w-3.5" />通知设置</button><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
      />

      <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2 text-[11px] text-muted sm:px-5">
        <span className={`inline-flex items-center gap-1.5 ${runtime.websocket_status === 'connected' ? 'text-bear' : 'text-muted'}`}><Wifi className="h-3.5 w-3.5" />{runtime.websocket_status === 'connected' ? '打板池已接入 WS' : '备选池仅实时轮询'}</span>
        <span className={runtime.trading_enabled ? 'text-bear' : 'text-warning'}>{runtime.trading_reason}</span>
        {!runtime.first_board_enabled ? <span className="text-warning">强势股打分暂不可用：{runtime.candidate_scope.state === 'unavailable' ? runtime.candidate_scope.reason : runtime.history_reason}</span> : <span className={runtime.candidate_scope.state === 'partial' ? 'text-warning' : undefined}>{runtime.candidate_scope.reason}</span>}
      </div>

      <div className="flex items-center gap-1 overflow-x-auto border-b border-border px-4 pt-2 sm:px-5">
        {([
          ['ladder', '连板天梯', null, Flame],
          ['sector', '板块强度', data.sector_strength?.rows.length ?? 0, Layers3],
          ['candidate', '备选池', data.candidate_pool.length, ListFilter],
          ['pool', '打板池', data.board_pool.length, Crosshair],
          ['events', '触发记录', data.events.length, Bell],
        ] as const).map(([id, label, count, Icon]) => (
          <button key={id} type="button" onClick={() => setTab(id)} className={`inline-flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === id ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}>
            <Icon className="h-3.5 w-3.5" />{label}{count == null ? null : <span className="font-mono text-[10px] text-muted">{count}</span>}
          </button>
        ))}
      </div>

      <div className={`min-h-0 flex-1 ${tab === 'ladder' ? 'overflow-hidden' : 'overflow-x-hidden overflow-y-auto px-2 py-3 sm:px-5'}`}>
        {tab === 'ladder' ? <Suspense fallback={<div className="grid h-full place-items-center"><RefreshCw className="h-5 w-5 animate-spin text-muted" /></div>}><EmbeddedLimitLadder headerContent={sentimentPanel} /></Suspense> : tab === 'sector' ? <SectorStrengthTable snapshot={data.sector_strength} signalRows={data.first_board} hotRows={heat.data?.lists.hot_day.items ?? []} hotQuotes={heatQuotes.data?.quotes} hotSectorLinks={heatQuotes.data?.sector_links} hotLoading={heat.isPending} hotError={heat.isError} refreshIntervalSeconds={runtime.refresh_cycle.interval_seconds} onOpenAlgorithm={() => setCandidateAlgorithmOpen(true)} onOpenStock={(symbol, name) => setPreview({ symbol, name })} /> : tab !== 'events' ? (
          <section className="overflow-hidden rounded-btn border border-border bg-surface">
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
              <div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>
              <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px]">
                {tab === 'candidate' ? <button type="button" onClick={() => setCandidateAlgorithmOpen(true)} className="inline-flex items-center gap-1 rounded-btn border border-border px-2 py-1 text-secondary hover:bg-elevated hover:text-foreground"><CircleHelp className="h-3.5 w-3.5" />排序算法</button> : null}
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
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
