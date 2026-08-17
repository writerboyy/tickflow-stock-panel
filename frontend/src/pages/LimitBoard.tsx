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
  SlidersHorizontal,
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
type AdvancedSettings = Omit<LimitBoardView['settings'], 'notifications'>

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

function scorePct(value: number | null | undefined, digits = 1): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(digits)}%`
}

function scoreTime(value: string | null | undefined): string {
  if (!value) return '--'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? '--' : parsed.toLocaleTimeString('zh-CN', { hour12: false })
}

const LEADERSHIP = {
  leader: { label: '龙头', tone: 'text-bear' },
  front: { label: '前排', tone: 'text-warning' },
  follower: { label: '跟随', tone: 'text-muted' },
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
  const rebound = row.source === 'rebound_board' || row.source_modes?.includes('rebound_board')
  const allThemes = themes(row.concept)
  const visibleThemes = allThemes.slice(0, 2)
  const scoreDetail = row.candidate_score_detail
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
    : { label: '等待触板', tone: 'text-muted' }

  return (
    <tr className="group border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="sticky left-0 z-30 w-[128px] min-w-[128px] max-w-[128px] overflow-hidden bg-surface py-2.5 pl-3 pr-2 group-hover:bg-elevated">
        <button type="button" onClick={onOpen} className="block w-full text-left hover:text-accent" title="查看 K 线与分时">
          <div className="truncate font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
          {mode !== 'pool' && rebound ? <div className="mt-0.5 text-[10px] text-warning">反包候选</div> : null}
          {mode === 'first' && row.limit_up_count != null ? <div className="mt-0.5 whitespace-nowrap text-[9px] text-muted" title={`涨停 ${row.limit_up_count} 次；次日红盘率 ${((row.next_day_red_rate ?? 0) * 100).toFixed(0)}%；首板破板率 ${((row.first_board_broken_rate ?? 0) * 100).toFixed(0)}%`}>
            {row.limit_up_count}次 · 红{((row.next_day_red_rate ?? 0) * 100).toFixed(0)}% · 破{((row.first_board_broken_rate ?? 0) * 100).toFixed(0)}%
          </div> : null}
        </button>
      </td>
      {mode === 'candidate' ? <>
        <td className="w-[116px] min-w-[116px] px-2" title={(row.candidate_reasons || []).join('；')}>
          {row.candidate_score == null ? <div className="text-muted">待补数据</div> : <>
            <div className="font-mono text-sm font-semibold tabular-nums text-accent">#{row.candidate_rank} · {row.candidate_score.toFixed(1)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">板{sector?.score.toFixed(1)} 基{gene?.score.toFixed(1)} 技{technical?.score.toFixed(1)}</div>
          </>}
          {row.candidate_score_state === 'cached' ? <div className="mt-0.5 whitespace-nowrap text-[9px] text-warning">缓存 · {scoreTime(row.candidate_score_as_of)}</div> : null}
        </td>
        <td className="w-[210px] min-w-[210px] px-2" title={rotationTitle || allThemes.join('、') || undefined}>
          {sector ? <>
            <div className="flex items-center gap-1.5"><span className="max-w-[110px] truncate font-medium">{sector.name}</span><span className={sector.change_pct != null && sector.change_pct >= 0 ? 'text-bear' : 'text-danger'}>{scorePct(sector.change_pct)}</span></div>
            <div className="mt-0.5 flex items-center gap-1.5 text-[9px]"><span className={leadership.tone}>{leadership.label}</span><span className="font-mono text-muted">#{sector.stock_rank ?? '--'}/{sector.member_count ?? '--'}</span><span className="text-secondary">{sector.rotation_label ?? '震荡'}</span></div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">5日 {scorePct(sector.five_day_change_pct)} · 昨 {scorePct(sector.yesterday_change_pct)}</div>
            {sector.leader && !sector.is_sector_leader ? <div className="mt-0.5 max-w-[190px] truncate text-[9px] text-muted">龙头 {sector.leader.name || sector.leader.symbol} {scorePct(sector.leader.change_pct)}</div> : null}
          </> : <div className="text-muted">板块待补</div>}
        </td>
        <td className="w-[170px] min-w-[170px] px-2" title={gene ? `快照 ${gene.as_of || '--'}；样本 ${gene.next_day_observation_count ?? 0}` : undefined}>
          {gene ? <>
            <div className="font-mono text-[10px] text-secondary">涨 {gene.limit_up_count ?? '--'} · 红 {scorePct(gene.next_day_red_rate, 0)}</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">溢 {scorePct(gene.premium_5_rate, 0)} · 封 {scorePct(gene.first_board_seal_rate, 0)} · 晋 {scorePct(gene.consecutive_rate, 0)}</div>
            <div className="mt-0.5 font-mono text-[9px] text-accent">{gene.score.toFixed(1)}/30</div>
          </> : <div className="text-muted">基因待补</div>}
        </td>
        <td className="w-[180px] min-w-[180px] px-2" title={technical ? `MA5 ${technical.ma5?.toFixed(2) ?? '--'}；MA10 ${technical.ma10?.toFixed(2) ?? '--'}；MA20 ${technical.ma20?.toFixed(2) ?? '--'}；MA60 ${technical.ma60?.toFixed(2) ?? '--'}` : undefined}>
          {technical ? <>
            <div className="whitespace-nowrap font-mono text-[9px] text-secondary">均 {technical.components?.trend?.toFixed(1)}/7 · 动 {technical.components?.momentum?.toFixed(1)}/5</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">量 {technical.components?.volume?.toFixed(1)}/3 · MACD {technical.components?.macd?.toFixed(1)}/3 · RSI {technical.components?.rsi?.toFixed(1)}/2</div>
            <div className="mt-0.5 whitespace-nowrap font-mono text-[9px] text-muted">量比 {technical.vol_ratio_5d?.toFixed(2) ?? '--'} · RSI {technical.rsi_14?.toFixed(0) ?? '--'}</div>
          </> : <div className="text-muted">技术面待补</div>}
        </td>
      </> : <td className="w-[160px] max-w-[160px] px-2">
        <div className="truncate text-[10px] text-secondary" title={allThemes.join('、') || undefined}>
          {visibleThemes.length ? visibleThemes.join('、') : '--'}
        </div>
      </td>}
      <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
      {mode !== 'candidate' ? <td className="px-2 font-mono tabular-nums text-accent">{row.limit_up?.toFixed(2) ?? '--'}</td> : null}
      <td className="px-2 font-mono tabular-nums text-warning">{gap}</td>
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
      <table className={`w-full border-collapse ${mode === 'candidate' ? 'min-w-[1320px]' : 'min-w-[1080px]'}`}>
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="sticky left-0 z-40 w-[128px] overflow-hidden bg-surface py-2 pl-3 pr-2">标的</th>
            {mode === 'candidate' ? <><th className="px-2">总分</th><th className="px-2">当前板块</th><th className="px-2">涨停基因</th><th className="px-2">技术面</th></> : <th className="w-[160px] px-2">题材</th>}
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

function queueTriggerDescription(waitSeconds: number, confirmSnapshots: number): string {
  const trigger = confirmSnapshots > 0
    ? `连续 ${confirmSnapshots} 个盘口快照确认封板`
    : '价格触及涨停'
  return waitSeconds > 0
    ? `${trigger}，且首次触板已等待 ${waitSeconds} 秒后提交`
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
        <span><span className="block">排板确认快照</span><span className="mt-0.5 block text-[10px] text-muted">0 为触板即排</span></span>
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
    near_limit_pct: value.near_limit_pct,
    exit_limit_pct: value.exit_limit_pct,
    exit_sustain_seconds: value.exit_sustain_seconds,
    first_board_lookback_days: value.first_board_lookback_days,
    blacklist_after_breaks: value.blacklist_after_breaks,
  }
}

export function LimitBoard() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('first')
  const [search, setSearch] = useState('')
  const [preview, setPreview] = useState<LimitBoardRow | null>(null)
  const [notificationOpen, setNotificationOpen] = useState(false)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const view = useQuery({ queryKey: QK.limitBoard, queryFn: api.limitBoard, refetchInterval: 5000, placeholderData: previous => previous })
  const overview = useQuery({
    queryKey: QK.overviewMarket(undefined),
    queryFn: () => api.overviewMarket(),
    enabled: tab === 'candidate',
    refetchInterval: 15000,
    staleTime: 5000,
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
  if (view.isError || !view.data) return <EmptyState icon={ShieldAlert} title="打板专区加载失败" hint="请检查后端服务后重试" />
  const data = view.data
  const runtime = data.runtime
  const rows = tab === 'first' ? data.first_board : tab === 'candidate' ? data.candidate_pool : data.board_pool
  const tableMode: TableMode = tab === 'pool' ? 'pool' : tab === 'candidate' ? 'candidate' : 'first'
  const tableTitle = tab === 'first' ? '全市场首板/反包候选' : tab === 'candidate' ? '备选池' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? `扫板：卖一距涨停不超过 ${data.settings.sweep_price_levels} 个价位时提交；排板：${queueTriggerDescription(data.settings.queue_wait_seconds, data.settings.queue_confirm_snapshots)}`
    : tab === 'candidate'
    ? '自动候选通过历史门槛后与手工标的合并，备选池仅使用实时轮询'
    : '自动过滤：近 200 日涨停≥4次、次日红盘率≥80%、首板破板率≤75%；不接入 WS'
  const marketEmotion = overview.data?.emotion
  const marketRadar = (overview.data?.radar ?? []).filter(item => (
    ['profit', 'money', 'speculation', 'mainline'].includes(item.key)
  ))

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="打板专区"
        titleExtra={<span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-[10px] text-secondary"><Radio className="h-3 w-3 text-accent" />打板池 {runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>}
        right={<div className="flex flex-wrap items-center justify-end gap-2"><div className="relative"><Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索股票加入备选池" className="h-8 w-48 rounded-btn border border-border bg-elevated pl-7 pr-2 text-xs outline-none focus:border-accent" />{searchResults.length && search.trim() ? <div className="absolute right-0 z-20 mt-1 w-64 overflow-hidden rounded-btn border border-border bg-surface shadow-lg">{searchResults.map(item => <button type="button" key={item.symbol} disabled={candidateSymbols.has(item.symbol) || add.isPending} onClick={() => add.mutate(item.symbol)} className="flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-elevated disabled:opacity-50"><span>{item.name}<span className="ml-2 font-mono text-[10px] text-muted">{item.symbol}</span></span><Plus className="h-3.5 w-3.5 text-accent" /></button>)}</div> : null}</div><button type="button" onClick={() => setAdvancedOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><SlidersHorizontal className="h-3.5 w-3.5" />高级设置</button><button type="button" onClick={() => setNotificationOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><Bell className="h-3.5 w-3.5" />通知设置</button><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
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
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
              <div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>
              <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px]">
                {tab === 'candidate' && marketEmotion ? <>
                  <span className={marketEmotion.score >= 55 ? 'text-bear' : marketEmotion.score < 45 ? 'text-bull' : 'text-secondary'} title={`看板日期 ${overview.data?.as_of ?? '--'}`}>情绪 {marketEmotion.label} <span className="font-mono">{marketEmotion.score}</span></span>
                  {marketRadar.map(item => <span key={item.key} className="text-muted">{item.label} <span className="font-mono text-secondary">{item.value}</span></span>)}
                </> : null}
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
            {data.events.length ? data.events.map((event: any, index: number) => {
              const eventThemes = themes(event.concept).slice(0, 2)
              return <div key={`${event.ts}-${index}`} className="flex items-start gap-3 px-3 py-3 text-xs"><span className={event.type === 'broken' ? 'text-danger' : event.type === 'resealed' ? 'text-bull' : 'text-accent'}>{STATUS[event.type]?.label || event.type}</span><div className="min-w-0 flex-1"><button type="button" onClick={() => setPreview({ symbol: event.symbol, name: event.name })} className="font-medium hover:text-accent" title="查看 K 线与分时">{event.name} <span className="ml-1 font-mono text-[10px] text-muted">{event.symbol}</span></button>{eventThemes.length ? <div className="mt-1 truncate text-[10px] text-secondary">题材：{eventThemes.join('、')}</div> : null}<div className="mt-1 text-[11px] text-secondary">{event.reasons?.join('；')}</div></div><div className="text-right text-[10px] text-muted"><div>炸板 {event.break_count || 0} 次</div><div>{new Date(event.ts).toLocaleTimeString('zh-CN')}</div></div></div>
            }) : <div className="px-4 py-12 text-center text-xs text-muted">今天还没有触板、炸板或回封记录</div>}
          </section>
        )}
      </div>

      {data.blacklist.length ? <div className="flex items-center gap-2 border-t border-border px-4 py-2 text-[10px] text-danger sm:px-5"><Ban className="h-3.5 w-3.5" />今日黑名单：{data.blacklist.join('、')}</div> : null}
      {advancedOpen ? <AdvancedSettingsDialog value={advancedSettings(data.settings)} pending={updateAdvanced.isPending} onClose={() => setAdvancedOpen(false)} onSave={value => updateAdvanced.mutate(value)} /> : null}
      {notificationOpen ? <NotificationDialog value={data.settings.notifications} pending={updateNotifications.isPending} onClose={() => setNotificationOpen(false)} onSave={value => updateNotifications.mutate(value)} /> : null}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} defaultShowIntraday onClose={() => setPreview(null)} />
    </div>
  )
}
