import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as echarts from 'echarts'
import {
  AlertTriangle,
  Ban,
  Bell,
  Check,
  CircleDot,
  Crosshair,
  Flame,
  Layers3,
  LineChart,
  Loader2,
  PanelRightClose,
  PanelRightOpen,
  Radio,
  RefreshCw,
  Settings2,
  ShieldAlert,
  ShoppingCart,
  SlidersHorizontal,
  Trash2,
  Wifi,
  X,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { PageHeader } from '@/components/PageHeader'
import { type QmtAllocationMode } from '@/components/QmtTradePanel'
import { QmtTradeAllocationControls, type QmtTradeAllocationMode } from '@/components/QmtTradeAllocation'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { useQuoteStatus } from '@/lib/useSharedQueries'
import {
  api,
  type QmtCreditBuyMode,
  type LimitBoardEvent,
  type LimitBoardApproachingLimitUpItem,
  type LimitBoardQuoteSnapshot,
  type LimitBoardRow,
  type LimitBoardSectorConstituent,
  type LimitBoardSectorStrengthRow,
  type LimitBoardSentimentPoint,
  type LimitBoardView,
  type LimitLadderStock,
  type PremiumGene,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { getBoardType } from '@/lib/board'
import { useChartTheme } from '@/lib/theme'

const EmbeddedLimitLadder = lazy(() => import('./LimitUpLadder').then(module => ({ default: module.LimitUpLadder })))

type Tab = 'ladder' | 'sector' | 'buy_pool' | 'pool' | 'events'
type TableMode = 'buy_pool' | 'pool'
type AdvancedSettings = LimitBoardView['settings']
type PoolAllocationMode = 'global' | 'available' | 'sixth' | 'fifth' | 'quarter' | 'lot' | 'fixed' | 'volume'
type AllocationDialogState = {
  row: LimitBoardRow
  kind: 'buy' | 'board' | 'edit'
  initialMode: PoolAllocationMode
  initialValue?: number | null
  initialCreditBuyMode?: QmtCreditBuyMode
}

const ADVANCED_QMT_ALLOCATION_OPTIONS: ReadonlyArray<{ value: QmtAllocationMode; label: string }> = [
  { value: 'available', label: '当前可用金额' },
  { value: 'quarter', label: '可用金额 1/4' },
  { value: 'third', label: '可用金额 1/3' },
  { value: 'half', label: '可用金额 1/2' },
  { value: 'fixed', label: '固定金额' },
]

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
  accepted_pending: { label: '已委托', tone: 'text-warning' },
  filled: { label: '已成交', tone: 'text-bear' },
  rejected: { label: '已拒绝', tone: 'text-danger' },
  unknown: { label: '委托未确认', tone: 'text-danger' },
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

function scorePct(value: number | null | undefined, digits = 1): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(digits)}%`
}

function ratioPct(value: number | null | undefined, digits = 0): string {
  return value == null || !Number.isFinite(value) ? '--' : `${(value * 100).toFixed(digits)}%`
}

function geneRateTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return 'text-secondary'
  if (value >= 0.6) return 'text-bull'
  if (value < 0.4) return 'text-bear'
  return 'text-warning'
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

function mergeSentimentHistory(
  history: LimitBoardSentimentPoint[] | undefined,
  realtime: LimitBoardSentimentPoint | undefined,
): LimitBoardSentimentPoint[] {
  const byDate = new Map((history ?? []).map(point => [point.as_of, point]))
  if (realtime?.as_of) byDate.set(realtime.as_of, realtime)
  return [...byDate.values()].sort((left, right) => left.as_of.localeCompare(right.as_of))
}

function SentimentHistoryChart({ points, className = 'h-36' }: { points: LimitBoardSentimentPoint[]; className?: string }) {
  const chartRef = useRef<HTMLDivElement>(null)
  const chartTheme = useChartTheme()
  const option = useMemo<echarts.EChartsOption>(() => {
    const dates = points.map(point => point.as_of)
    return {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: chartTheme.tooltipBg,
        borderColor: chartTheme.tooltipBorder,
        textStyle: { color: chartTheme.tooltipText, fontSize: 11 },
        valueFormatter: value => value == null ? '--' : String(value),
      },
      grid: [
        { left: 26, right: 22, top: 4, height: '70%' },
        { left: 26, right: 22, top: '84%', bottom: 32 },
      ],
      xAxis: [
        {
          type: 'category',
          gridIndex: 0,
          data: dates,
          boundaryGap: false,
          axisLabel: { show: false },
          axisLine: { show: false },
          axisTick: { show: false },
        },
        {
          type: 'category',
          gridIndex: 1,
          data: dates,
          boundaryGap: true,
          axisLabel: { color: chartTheme.text, fontSize: 8, hideOverlap: true, formatter: (value: string) => value.slice(5) },
          axisLine: { lineStyle: { color: chartTheme.border } },
        },
      ],
      yAxis: [
        {
          type: 'value',
          gridIndex: 0,
          min: 0,
          max: 100,
          axisLabel: { color: chartTheme.text, fontSize: 9 },
          splitLine: { lineStyle: { color: chartTheme.grid } },
        },
        {
          type: 'value',
          gridIndex: 1,
          min: 0,
          axisLabel: { show: false },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: '涨停家数',
          type: 'bar',
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: points.map(point => point.limit_up_count ?? null),
          barMaxWidth: 4,
          itemStyle: { color: '#22c55e', opacity: 0.45 },
        },
        {
          name: '情绪强度',
          type: 'line',
          xAxisIndex: 0,
          yAxisIndex: 0,
          data: points.map(point => point.emotion_strength ?? null),
          smooth: false,
          symbol: 'circle',
          symbolSize: 5,
          lineStyle: { color: '#f97316', width: 2.5 },
          itemStyle: { color: '#f97316' },
          markLine: {
            symbol: ['none', 'none'],
            silent: true,
            label: { show: false },
            lineStyle: { color: chartTheme.accent, width: 1.5, type: 'solid', opacity: 0.9 },
            data: [{ yAxis: 25 }, { yAxis: 75 }],
          },
        },
      ],
      dataZoom: [
        { type: 'inside', xAxisIndex: [0, 1], start: Math.max(0, 100 - (30 / Math.max(points.length, 30)) * 100), end: 100 },
        { type: 'slider', xAxisIndex: [0, 1], bottom: 2, height: 12, borderColor: chartTheme.border, fillerColor: chartTheme.zoomFill, textStyle: { color: chartTheme.text, fontSize: 8 } },
      ],
    }
  }, [chartTheme, points])

  useEffect(() => {
    if (!chartRef.current) return
    const chart = echarts.init(chartRef.current, undefined, { renderer: 'canvas' })
    chart.setOption(option, { notMerge: true })
    const onResize = () => chart.resize()
    window.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      chart.dispose()
    }
  }, [option])

  if (points.length === 0) return <div className={`grid ${className} place-items-center text-[11px] text-muted`}>暂无情绪历史</div>
  return <div ref={chartRef} className={`${className} w-full`} aria-label="开盘啦情绪强度历史折线图" />
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

type HotQuoteState = 'limit' | 'near_limit' | 'normal' | 'sharp_drop' | 'unavailable'
type HotSortKey = 'change_pct' | 'rise_speed_pct'
const HOT_DISPLAY_LIMIT = 15

// Only used for visual highlighting when a quote snapshot lacks an authoritative limit_up.
function fallbackLimitGap(symbol: string | null | undefined, change: number | null | undefined): number | null {
  if (!symbol || change == null || !Number.isFinite(change)) return null
  const normalized = symbol.trim().toUpperCase()
  const growthBoard = ['300', '301', '688', '689'].some(prefix => normalized.startsWith(prefix))
  const limitRate = normalized.endsWith('.BJ')
    ? 0.30
    : growthBoard
      ? 0.20
      : 0.10
  return (limitRate - change) / (1 + limitRate)
}

function changeTextForLimitGap(limitGap: number | null, fallback: string): string {
  if (limitGap == null) return fallback
  if (limitGap <= 0.005) return 'text-danger'
  if (limitGap <= 0.01) return 'text-orange-400'
  if (limitGap <= 0.03) return 'text-yellow-300'
  return 'text-secondary'
}

function hotQuoteVisual(quote: LimitBoardQuoteSnapshot['quotes'][string] | undefined): {
  state: HotQuoteState
  label: string
  changeText: string
} {
  const price = quote?.last_price
  const limitUp = quote?.limit_up
  const change = quote?.change_pct
  const fallbackChangeText = financialTone(change)
  const actualLimitGap = price != null && limitUp != null && limitUp > 0
    ? (limitUp - price) / limitUp
    : null
  const inferredLimitGap = actualLimitGap == null ? fallbackLimitGap(quote?.symbol, change) : null
  const changeLimitGap = actualLimitGap ?? inferredLimitGap
  const changeText = changeTextForLimitGap(changeLimitGap, fallbackChangeText)
  const atLimit = actualLimitGap != null && price != null && limitUp != null && price >= limitUp - 0.001
  if (atLimit) return { state: 'limit', label: '已涨停', changeText }
  if (actualLimitGap != null && actualLimitGap >= 0 && actualLimitGap <= 0.01) return { state: 'near_limit', label: '临板', changeText }
  if (change != null && Number.isFinite(change) && change <= -0.05) return { state: 'sharp_drop', label: '', changeText: fallbackChangeText }
  if (price == null && change == null) return { state: 'unavailable', label: '行情待更新', changeText: 'text-muted' }
  return { state: 'normal', label: '', changeText }
}

function moneyValue(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value)
    ? '--'
    : `${value.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 元`
}

function queueAmount(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value.toLocaleString('zh-CN')}万`
}

function queueStatusLabel(row: LimitBoardRow): string {
  const queue = row.queue
  if (!queue) return '未接入'
  if (queue.limit_up_gone) return '炸板'
  if (queue.order_status === 'filled_estimate') return '推测成交'
  if (queue.order_status === 'queueing') return '排队中'
  if (queue.order_status === 'queueing_unmatched') return '待匹配'
  if (queue.order_status === 'cancelled') return '已撤'
  return '监听中'
}

function queueCell(row: LimitBoardRow): JSX.Element {
  const queue = row.queue
  if (!queue) return <span className="text-muted">未接入</span>
  const front = queue.order?.front
  const back = queue.order?.back
  return <div className="min-w-[132px]" title="D202 排队量分布估算，位置由新增手数匹配推测">
    <div className={`font-medium ${queue.order_status === 'filled_estimate' ? 'text-bear' : queue.limit_up_gone ? 'text-danger' : 'text-secondary'}`}>{queueStatusLabel(row)}</div>
    <div className="mt-0.5 font-mono text-[9px] text-muted">
      {front ? `前 ${front.volume.toLocaleString('zh-CN')} 手 · ${queueAmount(front.amount)}` : `封单 ${queue.current?.volume?.toLocaleString('zh-CN') ?? '--'} 手`}
    </div>
    {front && back ? <div className="font-mono text-[9px] text-muted">后 {back.volume.toLocaleString('zh-CN')} 手 · {queueAmount(back.amount)}</div> : null}
    <div className="font-mono text-[9px] text-muted">封单 {queue.current?.volume?.toLocaleString('zh-CN') ?? '--'} 手 · 减少 {queue.cancelled?.volume?.toLocaleString('zh-CN') ?? '--'}</div>
  </div>
}

function poolAllocationLabel(mode: PoolAllocationMode): string {
  if (mode === 'global') return '旧配置'
  if (mode === 'available') return '当前可用金额'
  if (mode === 'sixth') return '可用金额 1/6'
  if (mode === 'fifth') return '可用金额 1/5'
  if (mode === 'quarter') return '可用金额 1/4'
  if (mode === 'lot') return '一手（100 股）'
  if (mode === 'fixed') return '固定金额'
  return '固定数量'
}

function LimitBoardAllocationDialog({
  row,
  kind,
  initialMode,
  initialValue,
  initialCreditBuyMode,
  pending,
  onClose,
  onConfirm,
}: {
  row: LimitBoardRow
  kind: 'buy' | 'board' | 'edit'
  initialMode: PoolAllocationMode
  initialValue?: number | null
  initialCreditBuyMode?: QmtCreditBuyMode
  pending: boolean
  onClose: () => void
  onConfirm: (mode: PoolAllocationMode, value: number | undefined, creditBuyMode: QmtCreditBuyMode, orderPrice?: number) => void
}) {
  const [mode, setMode] = useState<PoolAllocationMode>(kind === 'edit' ? (initialMode === 'global' ? 'lot' : initialMode) : 'lot')
  const [value, setValue] = useState<number>(initialValue ?? 0)
  const [creditBuyMode, setCreditBuyMode] = useState<QmtCreditBuyMode>(
    kind === 'edit' ? (initialCreditBuyMode ?? row.credit_buy_mode ?? 'financing') : (initialCreditBuyMode ?? 'financing'),
  )
  const price = row.last_price ?? row.order_price ?? null
  const qmt = useQuery({
    queryKey: QK.positionRiskQmt,
    queryFn: api.qmtStatus,
    refetchInterval: 30_000,
    staleTime: 5_000,
    placeholderData: previous => previous,
  })
  const qmtReady = qmt.data?.configured === true && qmt.data.state === 'ready'
  const creditBuy = String(qmt.data?.account_type || '').toUpperCase() === 'CREDIT'
  const cachedAccount = qmt.data?.account
  const cachedBuyingPower = creditBuy
    ? creditBuyMode === 'financing'
      ? cachedAccount?.fin_enbuy_balance ?? cachedAccount?.credit_financing_buying_power
      : cachedAccount?.assure_enbuy_balance ?? cachedAccount?.credit_assure_buying_power
    : cachedAccount?.cash
  const cachedBasisLabel = creditBuy
    ? creditBuyMode === 'financing' ? '可买融资标的资金' : '可买担保品资金'
    : '可用资金'
  const cachedFinancingAvailable = cachedAccount?.fin_enable_balance
    ?? cachedAccount?.fin_enable_quota
    ?? cachedAccount?.financing_available_amount
  const cachedFinancingBuyingPower = cachedAccount?.fin_enbuy_balance
    ?? cachedAccount?.credit_financing_buying_power
  const validPrice = price != null && Number.isFinite(price) && price > 0
  const geneDetail = row.candidate_score_detail?.premium_gene
  const localGeneReady = Boolean(
    geneDetail
    && geneDetail.limit_up_count != null
    && geneDetail.next_day_red_rate != null
    && geneDetail.first_board_broken_rate != null,
  )
  const premiumGeneQuery = useQuery({
    queryKey: QK.stockPremiumGene(row.symbol, false),
    queryFn: () => api.stockAnalysisPremiumGene(row.symbol, false),
    // Limit-board rows already carry the latest computed gene snapshot. Avoid
    // an extra live request when that detail is complete; it delayed the dialog
    // without changing the displayed score.
    enabled: kind === 'board' && Boolean(row.symbol) && !localGeneReady,
    staleTime: 5 * 60_000,
    gcTime: 15 * 60_000,
    retry: false,
  })
  const geneData: PremiumGene | undefined = premiumGeneQuery.data?.available ? premiumGeneQuery.data : undefined
  const geneScore = geneData?.score ?? geneDetail?.score
  const geneMaxScore = geneData?.max_score ?? geneDetail?.max_score ?? 10
  const genePassed = geneData?.passed ?? geneDetail?.passed
  const ratioMode = mode === 'available' || mode === 'sixth' || mode === 'fifth' || mode === 'quarter'
  // Fetch one authoritative account basis and derive the other lot/ratio modes
  // locally. Switching the selector should not issue another QMT RPC request.
  const previewMode: Exclude<QmtTradeAllocationMode, 'volume' | 'global'> = mode === 'fixed' ? 'fixed' : 'quarter'
  const previewValue = mode === 'fixed' ? value : null
  const allocationPreview = useQuery({
    queryKey: QK.positionRiskQmtPreview(row.symbol, 'BUY', 'LIMIT', validPrice ? price : null, previewMode, previewValue, creditBuyMode),
    queryFn: () => api.qmtPreviewOrder({
      action: 'BUY',
      symbol: row.symbol,
      price,
      price_type: 'LIMIT',
      reference_price: price,
      allocation_mode: previewMode,
      allocation_value: previewValue,
      credit_buy_mode: creditBuyMode,
    }, true),
    enabled: Boolean(qmtReady && validPrice),
    retry: false,
    placeholderData: previous => previous,
    staleTime: 500,
  })
  const basePreview = allocationPreview.data?.preview
  const previewOrder = useMemo(() => {
    if (!basePreview || mode === 'fixed') return basePreview
    const ratio = mode === 'available'
      ? 1
      : mode === 'sixth'
        ? 1 / 6
        : mode === 'fifth'
          ? 0.2
          : mode === 'quarter'
            ? 0.25
            : 0
    const basisAmount = Number(basePreview.basis_amount) || 0
    const requestedVolume = mode === 'lot' ? 100 : mode === 'volume' ? Math.max(0, value) : null
    const requestedAmount = requestedVolume != null
      ? requestedVolume * (basePreview.price || 0)
      : basisAmount * ratio
    const targetAmount = Math.min(requestedAmount, basisAmount)
    const lotVolume = Math.floor(targetAmount / basePreview.price / 100) * 100
    const volume = requestedVolume != null ? Math.min(requestedVolume, lotVolume) : lotVolume
    const actualAmount = Math.round(volume * basePreview.price * 100) / 100
    return {
      ...basePreview,
      allocation_mode: mode,
      allocation_value: mode === 'volume' ? value : null,
      target_amount: Math.round(targetAmount * 100) / 100,
      actual_amount: actualAmount,
      volume,
      capped: targetAmount < requestedAmount || actualAmount < requestedAmount,
      reason: volume < 100 ? '金额不足一手' : null,
    }
  }, [basePreview, mode, value])
  const effectiveCreditBuyMode = previewOrder?.credit_buy_mode ?? creditBuyMode
  const estimatedVolume = price != null && price > 0
    ? previewOrder?.volume ?? 0
    : 0
  const estimatedAmount = mode !== 'volume' && mode !== 'global'
    ? previewOrder?.actual_amount ?? null
    : price != null && estimatedVolume > 0 ? price * estimatedVolume : null
  const validValue = ratioMode || mode === 'lot' || (Number.isFinite(value) && value > 0)
  const validVolume = mode !== 'volume' || Number.isInteger(value) && value >= 100 && value % 100 === 0
  const previewRequired = kind !== 'edit'
  const previewReady = !previewRequired || (qmtReady && !allocationPreview.isFetching && previewOrder != null)
  const allocationPreviewState = allocationPreview.isFetching
    ? 'loading'
    : allocationPreview.isError
      ? 'error'
      : previewOrder?.reason
        ? 'error'
      : previewOrder
        ? 'ready'
        : qmtReady
          ? 'idle'
          : 'unavailable'
  const allocationPreviewMessage = allocationPreview.isError
    ? allocationPreview.error instanceof Error ? allocationPreview.error.message : '当前可用金额暂时无法读取'
    : !qmtReady
      ? qmt.data?.reason || 'QMT 未就绪，无法读取账户可用金额'
      : previewOrder?.reason
        || previewOrder?.credit_buy_mode_reason
        || (previewOrder?.capped ? '目标金额已按账户可用资金和 100 股整手向下调整。' : null)
  const canConfirm = Boolean(
    validValue
    && validVolume
    && (kind === 'edit' || previewReady)
    && (kind === 'edit' || (price != null && price > 0 && estimatedVolume >= 100))
    && (kind === 'edit' || estimatedVolume >= 100),
  )
  const title = kind === 'buy' ? '确认加入买入池' : kind === 'edit' ? '设置打板交易金额' : '确认加入打板池'
  const allocationOptions: ReadonlyArray<{ value: QmtTradeAllocationMode; label: string }> = [
    { value: 'available', label: poolAllocationLabel('available') },
    { value: 'sixth', label: poolAllocationLabel('sixth') },
    { value: 'fifth', label: poolAllocationLabel('fifth') },
    { value: 'quarter', label: poolAllocationLabel('quarter') },
    { value: 'lot', label: poolAllocationLabel('lot') },
    { value: 'fixed', label: poolAllocationLabel('fixed') },
    { value: 'volume', label: poolAllocationLabel('volume') },
  ]

  return <Modal
    labelledBy="limit-board-allocation-title"
    onClose={() => { if (!pending) onClose() }}
    closeOnBackdrop={!pending}
    panelClassName="w-[92vw] max-w-md rounded-card border border-border bg-surface shadow-xl"
  >
    <div className="flex items-start gap-3 border-b border-border px-4 py-4">
      <div className="grid h-8 w-8 shrink-0 place-items-center rounded-btn bg-warning/10 text-warning"><AlertTriangle className="h-4 w-4" /></div>
      <div className="min-w-0">
        <h2 id="limit-board-allocation-title" className="text-sm font-semibold">{title}</h2>
        <p className="mt-1 text-[11px] leading-4 text-muted">{row.name || row.symbol}<span className="ml-2 font-mono text-[10px]">{row.symbol}</span></p>
      </div>
      <button type="button" disabled={pending} onClick={onClose} className="ml-auto grid h-7 w-7 shrink-0 place-items-center rounded-btn text-muted hover:bg-elevated disabled:opacity-40" aria-label="关闭金额设置"><X className="h-4 w-4" /></button>
    </div>
    <div className="space-y-3 px-4 py-4 text-xs">
      <div className="grid grid-cols-2 gap-3">
        <div><div className="text-[10px] text-muted">当前限价参考</div><div className="mt-1 font-mono text-foreground">{price == null ? '--' : price.toFixed(3)}</div></div>
        <div><div className="text-[10px] text-muted">预计委托金额</div><div className="mt-1 font-mono text-foreground">{moneyValue(estimatedAmount)}</div></div>
      </div>
      {kind === 'board' ? <div className="border-y border-border py-3 text-[10px]">
        <div className="mb-2 font-medium text-secondary">涨停基因</div>
        {geneData || geneDetail ? <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
          {geneScore != null ? <span className="col-span-2">综合评分 <b className="font-mono text-foreground">{geneScore.toFixed(1)} / {geneMaxScore.toFixed(1)}</b>{genePassed != null ? <em className={genePassed ? 'ml-1 not-italic text-bull' : 'ml-1 not-italic text-warning'}>{genePassed ? '达标' : '未达标'}</em> : null}</span> : null}
          <span>近{geneData?.window_days ?? geneDetail?.window_days ?? '--'}日涨停 <b className="font-mono text-foreground">{geneData?.limit_up_count ?? geneDetail?.limit_up_count ?? '--'} 次</b></span>
          <span>溢价5% <b className="font-mono text-foreground">{geneData?.premium_5_count != null ? String(geneData.premium_5_count) + ' 次' : ratioPct(geneDetail?.premium_5_rate, 1)}</b></span>
          <span>次日收红 <b className={`font-mono font-semibold ${geneRateTone(geneData?.next_day_red_rate ?? geneDetail?.next_day_red_rate)}`}>{ratioPct(geneData?.next_day_red_rate ?? geneDetail?.next_day_red_rate, 1)}</b></span>
          <span>首板封板 <b className={`font-mono font-semibold ${geneRateTone(geneData?.first_board_seal_rate ?? geneDetail?.first_board_seal_rate)}`}>{ratioPct(geneData?.first_board_seal_rate ?? geneDetail?.first_board_seal_rate, 1)}</b></span>
          <span>首板破板 <b className="font-mono text-foreground">{ratioPct(geneData?.first_board_broken_rate ?? geneDetail?.first_board_broken_rate, 1)}</b></span>
          <span>连板率 <b className="font-mono text-foreground">{ratioPct(geneData?.consecutive_rate ?? geneDetail?.consecutive_rate, 1)}</b></span>
        </div> : premiumGeneQuery.isLoading ? <div className="text-muted">正在读取涨停基因…</div> : <div className="text-muted">暂无涨停基因数据</div>}
      </div> : null}
      <QmtTradeAllocationControls
        action="BUY"
        mode={mode as QmtTradeAllocationMode}
        value={value}
        onModeChange={next => setMode(next as PoolAllocationMode)}
        onValueChange={setValue}
        disabled={pending}
        options={allocationOptions}
        disabledModes={{ available: !qmtReady }}
        basisLabel={previewOrder?.basis_label ?? cachedBasisLabel}
        basisAmount={previewOrder?.basis_amount ?? cachedBuyingPower}
        accountType={qmt.data?.account_type}
        cashAmount={previewOrder?.cash_amount ?? cachedAccount?.cash}
        financingBuyingPowerAmount={cachedFinancingBuyingPower}
        financingAvailableAmount={previewOrder?.financing_available_amount ?? cachedFinancingAvailable}
        previewState={allocationPreviewState}
        previewMessage={allocationPreviewMessage}
      />
      {creditBuy ? <label className="block text-[10px] text-muted">信用账户买入方式
        <select
          value={creditBuyMode}
          disabled={pending}
          onChange={event => setCreditBuyMode(event.target.value as QmtCreditBuyMode)}
          className="mt-1 h-8 w-full rounded border border-border bg-surface px-2 text-xs outline-none focus:border-accent disabled:opacity-50"
        >
          <option value="collateral">担保品买入</option>
          <option value="financing">融资买入</option>
        </select>
      </label> : null}
      {previewOrder?.credit_buy_mode_switched ? <div className="border-y border-warning/25 bg-warning/5 px-3 py-2 text-[10px] leading-4 text-warning">首选买入额度不足，实际委托将自动切换为{effectiveCreditBuyMode === 'financing' ? '融资买入' : '担保品买入'}。</div> : null}
      {kind === 'buy' ? <div className="border-y border-warning/25 bg-warning/5 px-3 py-2 text-[10px] leading-4 text-warning">确认后立即按当前 TickFlow 价格发送限价买入委托，委托结果以 QMT 与券商回报为准。</div> : null}
    </div>
    <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
      <button type="button" disabled={pending} onClick={onClose} className="h-8 rounded-btn border border-border px-3 text-xs text-muted hover:bg-elevated disabled:opacity-40">取消</button>
      <button type="button" disabled={!canConfirm || pending} onClick={() => onConfirm(mode, mode === 'fixed' || mode === 'volume' ? value : undefined, creditBuyMode, kind === 'buy' ? price ?? undefined : undefined)} className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs font-medium text-white disabled:cursor-not-allowed disabled:opacity-40">
        {pending ? '提交中…' : kind === 'buy' ? '确认买入并挂单' : kind === 'edit' ? '保存设置' : '确认加入打板池'}
      </button>
    </div>
  </Modal>
}

interface RowProps {
  row: LimitBoardRow
  mode: TableMode
  busy: boolean
  sweepPriceLevels: number
  queueWaitSeconds: number
  queueConfirmSnapshots: number
  onOpen: () => void
  onEditAllocation: () => void
  onToggleAuto: (enabled: boolean) => void
  onChangeOrderMode: (mode: 'sweep' | 'queue') => void
  onRemovePool: () => void
}

function Row({
  row,
  mode,
  busy,
  sweepPriceLevels,
  queueWaitSeconds,
  queueConfirmSnapshots,
  onOpen,
  onEditAllocation,
  onToggleAuto,
  onChangeOrderMode,
  onRemovePool,
}: RowProps) {
  const status = STATUS[row.status || 'watching'] || STATUS.watching
  const gap = row.limit_gap_pct == null ? '--' : `${(row.limit_gap_pct * 100).toFixed(2)}%`
  const allThemes = themes(row.concept)
  const visibleThemes = allThemes.slice(0, 2)
  const orderMode = row.order_mode === 'queue' ? 'queue' : 'sweep'
  const orderStatus = !row.auto_trade && !row.auto_order_key
    ? { label: '未开启', tone: 'text-muted' }
    : row.auto_order_status
    ? ORDER_STATUS[row.auto_order_status] || { label: row.auto_order_status, tone: 'text-muted' }
    : { label: '等待涨停', tone: 'text-muted' }
  const buyOrderStatus = row.order_status
    ? ORDER_STATUS[row.order_status] || { label: row.order_status, tone: 'text-muted' }
    : { label: '未读取', tone: 'text-muted' }

  if (mode === 'buy_pool') {
    return <tr className="group border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="sticky left-0 z-30 w-[128px] min-w-[128px] max-w-[128px] overflow-hidden bg-surface py-2.5 pl-3 pr-2 group-hover:bg-elevated">
        <button type="button" onClick={onOpen} className="block w-full text-left hover:text-accent" title="查看 K 线与分时">
          <div className="truncate font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
        </button>
      </td>
      <td className="w-[150px] px-2"><div className="truncate text-[10px] text-secondary">{themes(row.concept).slice(0, 2).join('、') || '--'}</div><div className="mt-0.5 text-[9px] text-muted">{row.source === 'rebound_board' ? '反包来源' : row.source === 'first_board' ? '首板来源' : '手工来源'}</div></td>
      <td className="px-2 font-mono tabular-nums">{row.order_price?.toFixed(3) ?? row.last_price?.toFixed(3) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums">{row.order_volume ? `${row.order_volume.toLocaleString('zh-CN')} 股` : '--'}</td>
      <td className="px-2 font-mono tabular-nums">{moneyValue(row.order_amount)}</td>
      <td className={`px-2 font-medium ${buyOrderStatus.tone}`} title={row.order_error || undefined}>{buyOrderStatus.label}</td>
      <td className="px-2"><span className={row.ws_active ? 'text-bear' : 'text-muted'}>{row.ws_active ? 'WS' : '未接入'}</span></td>
      <td className="sticky right-0 z-30 border-l border-border bg-surface px-2 text-right group-hover:bg-elevated">
        <button type="button" title="移出买入池；不会自动撤销已发委托" disabled={busy} onClick={onRemovePool} className="inline-flex h-7 items-center gap-1 rounded-btn border border-border px-2 text-secondary hover:border-danger/40 hover:text-danger disabled:opacity-40"><Trash2 className="h-3.5 w-3.5" />移除</button>
      </td>
    </tr>
  }

  return (
    <tr className="group border-t border-border/70 text-[11px] hover:bg-elevated/30">
      <td className="sticky left-0 z-30 w-[128px] min-w-[128px] max-w-[128px] overflow-hidden bg-surface py-2.5 pl-3 pr-2 group-hover:bg-elevated">
        <button type="button" onClick={onOpen} className="block w-full text-left hover:text-accent" title="查看 K 线与分时">
          <div className="truncate font-medium">{row.name || row.symbol}</div>
          <div className="mt-0.5 font-mono text-[10px] text-muted">{row.symbol}</div>
        </button>
      </td>
      <td className="w-[160px] max-w-[160px] px-2">
        <div className="truncate text-[10px] text-secondary" title={allThemes.join('、') || undefined}>
          {visibleThemes.length ? visibleThemes.join('、') : '--'}
        </div>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.last_price?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-secondary">{row.limit_up?.toFixed(2) ?? '--'}</td>
      <td className="px-2 font-mono tabular-nums text-secondary">{gap}</td>
      <td className="px-2">
        <span className={`inline-flex items-center gap-1 font-medium ${status.tone}`}>
          <CircleDot className="h-3 w-3" />{status.label}
        </span>
      </td>
      <td className="px-2 font-mono tabular-nums">{row.break_count ? `${row.break_count} 次` : '0 次'}</td>
      <td className="px-2 font-mono tabular-nums text-secondary">{row.bid1_volume ? row.bid1_volume.toLocaleString('zh-CN') : '--'}</td>
      <td className="px-2">{queueCell(row)}</td>
      <td className="px-2"><span className={row.ws_active ? 'text-bear' : 'text-muted'}>{row.ws_active ? 'WS' : '轮询'}</span></td>
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
          <button type="button" title="设置该股票的交易数量或金额" disabled={busy} onClick={onEditAllocation} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-elevated hover:text-accent disabled:opacity-40"><Settings2 className="h-3.5 w-3.5" /></button>
          <button type="button" title="移出打板池" disabled={busy} onClick={onRemovePool} className="inline-flex h-7 w-7 items-center justify-center rounded-btn text-muted hover:bg-danger/10 hover:text-danger disabled:opacity-40">
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      </td>
    </tr>
  )
}

interface TableProps {
  rows: LimitBoardRow[]
  mode: TableMode
  busy: boolean
  sweepPriceLevels: number
  queueWaitSeconds: number
  queueConfirmSnapshots: number
  onOpen: (row: LimitBoardRow) => void
  onEditAllocation: (row: LimitBoardRow) => void
  onToggleAuto: (row: LimitBoardRow, enabled: boolean) => void
  onChangeOrderMode: (row: LimitBoardRow, mode: 'sweep' | 'queue') => void
  onRemovePool: (row: LimitBoardRow) => void
}

function Table(props: TableProps) {
  const { rows, mode } = props
  if (!rows.length) return <div className="px-4 py-12 text-center text-xs text-muted">当前没有符合条件的标的</div>
  return (
    <div className="max-w-full overflow-x-auto overscroll-x-contain" style={{ WebkitOverflowScrolling: 'touch' }}>
      <table className={`w-full border-collapse ${mode === 'buy_pool' ? 'min-w-[980px]' : 'min-w-[1210px]'}`}>
        <thead className="text-left text-[10px] text-muted">
          <tr>
            <th className="sticky left-0 z-40 w-[128px] overflow-hidden bg-surface py-2 pl-3 pr-2">标的</th>
            <th className="w-[160px] px-2">题材</th>
            {mode === 'buy_pool' ? <><th className="px-2">限价</th><th className="px-2">数量</th><th className="px-2">金额</th><th className="px-2">委托状态</th><th className="px-2">行情</th></> : null}
            {mode === 'pool' ? <><th className="px-2">现价</th><th className="px-2">涨停价</th><th className="px-2">距涨停</th><th className="px-2">状态</th><th className="px-2">炸板次数</th><th className="px-2">买一封单</th><th className="px-2">排队</th><th className="px-2">行情</th><th className="px-2">委托状态</th></> : null}
            <th className={`sticky right-0 z-40 border-l border-border bg-surface px-2 text-right ${mode === 'pool' ? 'w-[250px]' : 'w-[172px]'}`}>操作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <Row
              key={row.symbol}
              row={row}
              mode={mode}
              busy={props.busy}
              sweepPriceLevels={props.sweepPriceLevels}
              queueWaitSeconds={props.queueWaitSeconds}
              queueConfirmSnapshots={props.queueConfirmSnapshots}
              onOpen={() => props.onOpen(row)}
              onEditAllocation={() => props.onEditAllocation(row)}
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

function isMainBoardSymbol(symbol: string): boolean {
  const board = getBoardType(symbol)
  return board === '沪主板' || board === '深主板'
}

function manualActionRow(
  symbol: string,
  name: string | null | undefined,
  lastPrice: number | null | undefined,
  changePct: number | null | undefined,
  limitUp: number | null | undefined,
): LimitBoardRow {
  return {
    symbol,
    name: name || symbol,
    source: 'manual',
    last_price: lastPrice ?? undefined,
    change_pct: changePct,
    limit_up: limitUp ?? undefined,
  }
}

function sectorStrengthSpeed(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--'
  return `${value > 0 ? '+' : ''}${value.toFixed(1)}`
}

function sectorNameKey(value: string | null | undefined): string {
  return String(value ?? '').replace(/\s+/g, '').trim()
}

function SectorStrengthTable({
  snapshot,
  hotRows = [],
  hotQuotes = {},
  hotSectorLinks = {},
  hotLoading = false,
  hotError = false,
  refreshIntervalSeconds = 5,
  refreshCycleUpdatedAt = 0,
  onOpenStock,
  onAddPool,
  onAddBuyPool,
  poolSymbols,
  buyPoolSymbols,
  busy,
}: {
  snapshot: LimitBoardView['sector_strength']
  hotRows?: LimitBoardApproachingLimitUpItem[]
  hotQuotes?: LimitBoardQuoteSnapshot['quotes']
  hotSectorLinks?: LimitBoardQuoteSnapshot['sector_links']
  hotLoading?: boolean
  hotError?: boolean
  refreshIntervalSeconds?: number
  refreshCycleUpdatedAt?: number
  onOpenStock: (symbol: string, name?: string) => void
  onAddPool: (row: LimitBoardRow) => void
  onAddBuyPool: (row: LimitBoardRow) => void
  poolSymbols: ReadonlySet<string>
  buyPoolSymbols: ReadonlySet<string>
  busy: boolean
}) {
  const [sortKey, setSortKey] = useState<SectorSortKey>('strength')
  const [descending, setDescending] = useState(true)
  const [cursorIndex, setCursorIndex] = useState<number | null>(null)
  const [requestedAt, setRequestedAt] = useState<string | null>(null)
  const [selectedPlateId, setSelectedPlateId] = useState<string | null>(null)
  const [selectedStockSymbol, setSelectedStockSymbol] = useState<string | null>(null)
  const [hotSortKey, setHotSortKey] = useState<HotSortKey>('change_pct')
  const [rankingWindowMinutes, setRankingWindowMinutes] = useState<5 | 30>(5)
  const [rankingOpen, setRankingOpen] = useState(false)
  const [mainBoardOnly, setMainBoardOnly] = useState(false)
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
  const sortedHotRows = useMemo(() => {
    return [...hotRows].sort((left, right) => {
      const leftValue = hotSortKey === 'change_pct'
        ? hotQuotes[left.thscode.toUpperCase()]?.change_pct ?? left.change_pct
        : left[hotSortKey]
      const rightValue = hotSortKey === 'change_pct'
        ? hotQuotes[right.thscode.toUpperCase()]?.change_pct ?? right.change_pct
        : right[hotSortKey]
      const leftNumber = typeof leftValue === 'number' && Number.isFinite(leftValue) ? leftValue : null
      const rightNumber = typeof rightValue === 'number' && Number.isFinite(rightValue) ? rightValue : null
      if (leftNumber == null && rightNumber == null) return left.rank - right.rank
      if (leftNumber == null) return 1
      if (rightNumber == null) return -1
      return (rightNumber - leftNumber) || (left.rank - right.rank)
    })
  }, [hotQuotes, hotRows, hotSortKey])
  const linkedPlateIds = useMemo(() => {
    if (!selectedStockSymbol) return new Set<string>()
    const heatLinks = hotSectorLinks[selectedStockSymbol] ?? []
    const heatItem = hotRows.find(item => item.thscode.toUpperCase() === selectedStockSymbol)
    const ids = new Set([
      ...heatLinks.map(link => link.plate_id),
    ])
    const names = new Set([
      ...heatLinks.map(link => link.plate_name),
      ...(heatItem?.sector ? themes(heatItem.sector) : []),
    ].map(sectorNameKey))
    return new Set(rows.filter(row => ids.has(row.plate_id) || names.has(sectorNameKey(row.plate_name))).map(row => row.plate_id))
  }, [hotRows, hotSectorLinks, rows, selectedStockSymbol])
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
      isLive ? 'live' : activeCapturedAt ?? '',
    ),
    queryFn: () => api.limitBoardSectorConstituents(
      selectedPlate!.plate_id,
      isLive ? undefined : activeCapturedAt!,
    ),
    enabled: selectedPlate != null && activeCapturedAt != null && activeSnapshotReady,
    placeholderData: previous => previous,
    refetchInterval: isLive ? Math.max(5_000, refreshIntervalSeconds * 3_000) : false,
    staleTime: isLive ? Math.max(1_000, refreshIntervalSeconds * 1_000 - 1_000) : 60_000,
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
  const visibleConstituentRows = useMemo(
    () => mainBoardOnly ? constituentRows.filter(row => isMainBoardSymbol(row.symbol)) : constituentRows,
    [constituentRows, mainBoardOnly],
  )
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
    const heatLinks = hotSectorLinks[normalized] ?? []
    const heatItem = hotRows.find(item => item.thscode.toUpperCase() === normalized)
    setSelectedStockSymbol(normalized)
    lastScrolledConstituent.current = null
    const ids = new Set([
      ...heatLinks.map(link => link.plate_id),
    ])
    const names = new Set([
      ...heatLinks.map(link => link.plate_name),
      ...(heatItem?.sector ? themes(heatItem.sector) : []),
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
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1"><div className="shrink-0 text-xs font-medium">板块强度</div><span className="truncate text-[10px] text-muted">板块与成分股按同一截面每 {refreshIntervalSeconds} 秒刷新</span></div>
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
    <div className={`grid min-w-0 lg:min-w-[1020px] ${rankingOpen ? 'lg:grid-cols-[22%_20%_34%_24%]' : 'lg:grid-cols-[25%_30%_45%]'}`}>
      <div className="min-w-0 border-b border-border lg:border-b-0 lg:border-r">
        <div className="flex min-h-12 flex-wrap items-center justify-between gap-1 border-b border-border px-2 py-1.5">
          <div className="min-w-0"><div className="inline-flex items-center gap-1 text-[11px] font-medium"><Flame className="h-3.5 w-3.5 shrink-0 text-accent" /><span className="truncate">即将涨停 Top{HOT_DISPLAY_LIMIT}</span></div><div className="mt-0.5 truncate pl-[18px] text-[8px] text-muted">行情5秒</div></div>
          <div className="flex shrink-0 flex-wrap items-center justify-end gap-1 text-[8px] font-medium">
            <div className="inline-flex h-5 overflow-hidden rounded border border-border" aria-label="即将涨停排序">
              {([['change_pct', '涨幅'], ['rise_speed_pct', '涨速']] as const).map(([key, label]) => <button key={key} type="button" aria-pressed={hotSortKey === key} onClick={() => setHotSortKey(key)} className={`px-1.5 ${hotSortKey === key ? 'bg-accent/15 text-accent' : 'text-muted hover:bg-elevated hover:text-foreground'}`}>{label}</button>)}
            </div>
            <span className="rounded border border-danger/40 bg-danger/10 px-1 py-0.5 text-danger">涨停</span><span className="rounded border border-danger/40 bg-danger/10 px-1 py-0.5 text-danger">≤0.5%</span><span className="rounded border border-orange-400/40 bg-orange-400/10 px-1 py-0.5 text-orange-400">≤1%</span><span className="rounded border border-yellow-300/40 bg-yellow-300/10 px-1 py-0.5 text-yellow-300">≤3%</span>
          </div>
        </div>
        {hotRows.length ? <div className="max-w-full overflow-x-auto overscroll-contain p-2 lg:max-h-[62vh] lg:overflow-x-hidden lg:overflow-y-auto">
          <div className="flex w-max gap-2 lg:w-full lg:flex-col">
            {sortedHotRows.slice(0, HOT_DISPLAY_LIMIT).map((item, index) => {
              const quote = hotQuotes[item.thscode.toUpperCase()] ?? {
                symbol: item.thscode,
                name: item.name,
                last_price: item.last_price,
                change_pct: item.change_pct,
              }
              const selected = item.thscode.toUpperCase() === selectedStockSymbol
              const visual = hotQuoteVisual(quote)
              const actionRow = manualActionRow(item.thscode.toUpperCase(), item.name || item.ticker, quote?.last_price, quote?.change_pct, quote?.limit_up)
              const inPool = poolSymbols.has(actionRow.symbol)
              const inBuyPool = buyPoolSymbols.has(actionRow.symbol)
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
                className={`h-[68px] w-[184px] shrink-0 rounded-btn border border-border bg-surface px-2.5 py-2 text-left outline-none transition-colors hover:border-warning/60 hover:bg-warning/5 focus-visible:ring-1 focus-visible:ring-warning lg:w-full ${selected ? 'border-warning bg-warning/15 ring-1 ring-warning/60' : ''}`}
                title="联动强势股、实时板块与成分股"
              >
                <div className="flex items-center gap-1.5"><button type="button" onClick={event => { event.stopPropagation(); onOpenStock(item.thscode, item.name || item.ticker) }} className="min-w-0 flex-1 truncate text-left text-xs font-medium hover:text-accent focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-warning" title="查看 K 线与分时">{item.name || item.ticker}</button><span className="shrink-0 font-mono text-[10px] text-secondary">{quote?.last_price?.toFixed(2) ?? '--'}</span><span className={`shrink-0 font-mono text-[10px] ${visual.changeText}`}>{scorePct(quote?.change_pct, 2)}</span><span className="shrink-0 font-mono text-[10px] text-accent">#{index + 1}</span><div className="flex shrink-0 items-center gap-0.5">
                  <button type="button" aria-label={inBuyPool ? '已在买入池' : '加入买入池'} title={inBuyPool ? '已在买入池' : '加入买入池'} disabled={inBuyPool || busy} onClick={event => { event.stopPropagation(); onAddBuyPool(actionRow) }} className={`grid h-6 w-6 place-items-center rounded-btn border ${inBuyPool ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-bull/40 hover:text-bull'} disabled:opacity-50`}>{inBuyPool ? <Check className="h-3 w-3" /> : <ShoppingCart className="h-3 w-3" />}</button>
                  <button type="button" aria-label={inPool ? '已在打板池' : '加入打板池'} title={inPool ? '已在打板池' : '加入打板池'} disabled={inPool || busy} onClick={event => { event.stopPropagation(); onAddPool(actionRow) }} className={`grid h-6 w-6 place-items-center rounded-btn border ${inPool ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-accent/40 hover:text-accent'} disabled:opacity-50`}>{inPool ? <Check className="h-3 w-3" /> : <Crosshair className="h-3 w-3" />}</button>
                </div></div>
                <div className="mt-0.5 flex items-center gap-2 font-mono text-[9px]"><span className="truncate text-muted">{item.thscode}</span>{item.yesterday_boards && item.yesterday_boards > 0 ? <span className="shrink-0 rounded border border-accent/30 bg-accent/10 px-1 text-accent">{item.yesterday_boards === 1 ? '昨日首板' : `昨日${item.yesterday_boards}板`}</span> : null}{visual.label ? <span className="shrink-0 text-secondary">{visual.label}</span> : null}</div>
                <div className="mt-0.5 flex items-center gap-2 truncate text-[9px]"><span className="shrink-0 font-mono text-muted">涨速 {scorePct(item.rise_speed_pct, 2)}</span>{item.sector ? <span className="truncate text-accent/80">{item.sector}</span> : null}</div>
              </div>
            })}
          </div>
        </div> : <div className={`px-3 py-10 text-center text-xs ${hotError ? 'text-warning' : 'text-muted'}`}>{hotLoading ? '正在读取即将涨停' : hotError ? '即将涨停暂不可用' : '暂无即将涨停数据'}</div>}
      </div>
      <div className="min-w-0 overflow-x-auto overscroll-x-contain border-b border-border lg:border-b-0 lg:border-r">
        <table className="w-full min-w-[360px] table-fixed border-collapse">
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
              <td className="px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums text-secondary">{moneyYi(row.main_net)}</td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums text-secondary">{moneyYi(row.institution_increase)}</td>
            </tr>
          }) : <tr><td colSpan={4} className="px-3 py-10 text-center text-xs text-muted">实时板块数据暂不可用</td></tr>}</tbody>
        </table>
      </div>
      <div className="min-w-0">
        <div className="flex min-h-12 items-center justify-between gap-1.5 border-b border-border px-2 py-1.5">
          <div className="min-w-0">
            <div className="truncate text-[11px] font-medium">{selectedPlate?.plate_name || '板块成分股'}</div>
            <div className="mt-0.5 truncate text-[8px] text-muted">
              {mainBoardOnly ? `主板 ${visibleConstituentRows.length}/${constituentRows.length}` : `全部 ${constituentRows.length} 只`}
            </div>
          </div>
          <button
            type="button"
            aria-pressed={mainBoardOnly}
            onClick={() => setMainBoardOnly(value => !value)}
            className={`inline-flex h-6 shrink-0 items-center gap-1 rounded-btn border px-2 text-[9px] transition-colors ${mainBoardOnly ? 'border-accent/50 bg-accent/15 text-accent' : 'border-border text-muted hover:bg-elevated hover:text-foreground'}`}
            title={mainBoardOnly ? '显示全部板块成分股' : '仅显示主板成分股'}
          >
            {mainBoardOnly ? <Check className="h-3 w-3" /> : null}
            主板
          </button>
        </div>
        {constituents.isError && !constituentData && !constituents.isFetching ? <div className="flex flex-col items-center gap-2 px-4 py-12 text-center text-xs text-danger"><span>{selectedPlate?.plate_name || '实时板块'}成分股加载失败</span><button type="button" onClick={() => constituents.refetch()} className="inline-flex h-7 items-center gap-1 rounded-btn border border-danger/40 px-2.5 text-[10px] text-danger hover:bg-danger/10"><RefreshCw className="h-3 w-3" />重试</button></div> : visibleConstituentRows.length ? <div className="max-h-[62vh] max-w-full overflow-auto overscroll-contain">
          <table className="w-full min-w-[540px] table-fixed border-collapse">
            <thead className="sticky top-0 z-10 bg-surface text-left text-[9px] text-muted"><tr><th className="w-[29%] px-2 py-1.5">股票</th><th className="w-[11%] px-2 py-1.5 text-right">现价</th><th className="w-[11%] px-2 py-1.5 text-right">涨幅</th><th className="w-[13%] px-2 py-1.5 text-right">板状态</th><th className="w-[13%] px-2 py-1.5 text-right">换手率</th><th className="w-[13%] px-2 py-1.5 text-right">成交额</th><th className="w-[10%] px-2 py-1.5 text-right">操作</th></tr></thead>
            <tbody>{visibleConstituentRows.map(row => {
              const linked = row.symbol === selectedStockSymbol
              const visual = hotQuoteVisual({ symbol: row.symbol, last_price: row.last_price, limit_up: row.limit_up, change_pct: row.change_pct })
              return <tr
                key={row.symbol}
                ref={element => {
                  if (element) constituentRowRefs.current.set(row.symbol, element)
                  else constituentRowRefs.current.delete(row.symbol)
                }}
                className={`border-t border-border/70 hover:bg-elevated/30 ${linked ? 'bg-warning/20 ring-1 ring-inset ring-warning/60' : ''}`}
              >
              <td className="px-2 py-1.5"><button type="button" onClick={() => onOpenStock(row.symbol, row.name ?? undefined)} className="block max-w-full text-left hover:text-accent" title="查看 K 线与分时"><span className="block truncate text-[11px] font-medium"><span>{row.name || row.code}</span>{row.tags ? <span className="ml-1 text-[9px] font-normal text-accent/80">· {row.tags}</span> : null}</span><span className="block truncate font-mono text-[8px] text-muted">#{row.rank} · {row.symbol}</span></button></td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums text-secondary">{row.last_price?.toFixed(2) ?? '--'}</td>
              <td className={`px-2 py-1.5 text-right font-mono text-[10px] font-medium tabular-nums ${visual.changeText}`}>{scorePct(row.change_pct, 2)}</td>
              <td className="px-2 py-1.5 text-right text-[10px] text-secondary">{sectorConstituentStatus(row)}</td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums text-secondary">{ratioPct(row.turnover_rate, 2)}</td>
              <td className="px-2 py-1.5 text-right font-mono text-[10px] tabular-nums text-secondary">{moneyYi(row.amount)}</td>
              <td className="sticky right-0 z-20 w-[60px] min-w-[60px] border-l border-border bg-surface px-1.5 py-1.5">
                <div className="flex justify-end gap-0.5">
                <button type="button" aria-label={`加入${row.name || row.symbol}买入池`} title="加入买入池" disabled={busy} onClick={() => onAddBuyPool(manualActionRow(row.symbol, row.name, row.last_price, row.change_pct, null))} className={`grid h-6 w-6 place-items-center rounded-btn border ${buyPoolSymbols.has(row.symbol) ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-bull/40 hover:text-bull'} disabled:opacity-50`}>{buyPoolSymbols.has(row.symbol) ? <Check className="h-3 w-3" /> : <ShoppingCart className="h-3 w-3" />}</button>
                <button type="button" aria-label={`加入${row.name || row.symbol}打板池`} title="加入打板池" disabled={busy} onClick={() => onAddPool(manualActionRow(row.symbol, row.name, row.last_price, row.change_pct, null))} className={`grid h-6 w-6 place-items-center rounded-btn border ${poolSymbols.has(row.symbol) ? 'border-bear/30 text-bear' : 'border-border text-secondary hover:border-accent/40 hover:text-accent'} disabled:opacity-50`}>{poolSymbols.has(row.symbol) ? <Check className="h-3 w-3" /> : <Crosshair className="h-3 w-3" />}</button>
                </div>
              </td>
              </tr>
            })}</tbody>
          </table>
        </div> : <div className="px-4 py-12 text-center text-xs text-muted">{constituents.isPending || constituents.isFetching ? '正在读取实时板块成分股' : mainBoardOnly && constituentRows.length ? '当前板块没有主板成分股' : '该时间点没有可用的成分股数据'}</div>}
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
    && ADVANCED_QMT_ALLOCATION_OPTIONS.some(option => option.value === draft.order_allocation_mode)
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
          {ADVANCED_QMT_ALLOCATION_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
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

export function LimitBoard() {
  const queryClient = useQueryClient()
  const { data: quoteStatus } = useQuoteStatus({ poll: true })
  const isTradingHours = quoteStatus?.is_trading_hours ?? false
  const [tab, setTab] = useState<Tab>('sector')
  const [preview, setPreview] = useState<{ symbol: string; name?: string } | null>(null)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [sentimentChartOpen, setSentimentChartOpen] = useState(false)
  const [allocationDialog, setAllocationDialog] = useState<AllocationDialogState | null>(null)
  const view = useQuery({
    queryKey: QK.limitBoard,
    queryFn: api.limitBoard,
    refetchOnMount: 'always',
    retry: 5,
    retryDelay: attemptIndex => Math.min(5_000, 1_000 * 2 ** attemptIndex),
    refetchInterval: query => {
      if (!query.state.data) return 5_000
      return isTradingHours
        ? Math.max(
          1,
          query.state.data.runtime.refresh_cycle.interval_seconds ?? 5,
        ) * 1000
        : false
    },
    placeholderData: previous => previous,
  })
  const unifiedRefreshIntervalMs = Math.max(
    1,
    view.data?.runtime.refresh_cycle.interval_seconds ?? 5,
  ) * 1000
  const approachingLimitUp = useQuery({
    queryKey: QK.limitBoardApproachingLimitUp,
    queryFn: () => api.limitBoardApproachingLimitUp(true),
    enabled: tab === 'sector',
    refetchInterval: tab === 'sector' && isTradingHours ? unifiedRefreshIntervalMs : false,
    staleTime: Math.max(1000, unifiedRefreshIntervalMs - 1000),
    placeholderData: previous => previous,
  })
  const heatRows = approachingLimitUp.data?.rows ?? []
  const heatSymbols = useMemo(
    () => heatRows.map(item => item.thscode.toUpperCase()),
    [heatRows],
  )
  const heatQuotes = useQuery({
    queryKey: QK.limitBoardQuotes(heatSymbols.join(',')),
    queryFn: () => api.limitBoardQuotes(heatSymbols, true),
    enabled: tab === 'sector' && heatSymbols.length > 0,
    refetchInterval: tab === 'sector' && isTradingHours ? unifiedRefreshIntervalMs : false,
    staleTime: Math.max(1000, unifiedRefreshIntervalMs - 1000),
    placeholderData: previous => previous,
  })
  const refresh = () => queryClient.invalidateQueries({ queryKey: QK.limitBoard })
  const addPool = useMutation({
    mutationFn: ({ row, source, allocationMode, allocationValue, creditBuyMode }: { row: LimitBoardRow; source: 'first_board' | 'rebound_board' | 'manual'; allocationMode: PoolAllocationMode; allocationValue?: number; creditBuyMode: QmtCreditBuyMode }) => api.limitBoardPoolAdd(row.symbol, source, view.data?.revision ?? 0, allocationMode, allocationValue, creditBuyMode),
    onSuccess: () => { setAllocationDialog(null); void refresh() },
  })
  const addBuyPool = useMutation({
    mutationFn: ({ row, source, allocationMode, allocationValue, creditBuyMode, orderPrice }: { row: LimitBoardRow; source: 'first_board' | 'rebound_board' | 'manual'; allocationMode: 'available' | 'sixth' | 'fifth' | 'quarter' | 'lot' | 'fixed' | 'volume'; allocationValue?: number; creditBuyMode: QmtCreditBuyMode; orderPrice?: number }) => api.limitBoardBuyPoolAdd(row.symbol, source, view.data?.revision ?? 0, allocationMode, allocationValue, creditBuyMode, orderPrice),
    onSuccess: () => { setAllocationDialog(null); void refresh() },
  })
  const updatePool = useMutation({
    mutationFn: ({ row, enabled, orderMode, allocationMode, allocationValue, creditBuyMode }: { row: LimitBoardRow; enabled: boolean; orderMode: 'sweep' | 'queue'; allocationMode?: PoolAllocationMode; allocationValue?: number; creditBuyMode?: QmtCreditBuyMode }) => api.limitBoardPoolUpdate(row.symbol, enabled, orderMode, view.data?.revision ?? 0, allocationMode, allocationValue, creditBuyMode),
    onSuccess: () => { setAllocationDialog(null); void refresh() },
  })
  const removePool = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardPoolRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
  })
  const removeBuyPool = useMutation({
    mutationFn: (row: LimitBoardRow) => api.limitBoardBuyPoolRemove(row.symbol, view.data?.revision ?? 0),
    onSuccess: refresh,
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
  const buyPoolSymbols = useMemo(() => new Set((view.data?.buy_pool ?? []).map(row => row.symbol)), [view.data?.buy_pool])
  const busy = addPool.isPending || addBuyPool.isPending || updatePool.isPending || removePool.isPending || removeBuyPool.isPending || updateAdvanced.isPending
  const data = view.data
  const sentimentHistory = useMemo(() => mergeSentimentHistory(
    data?.market_sentiment?.emotion_history,
    data?.market_sentiment?.emotion_strength == null || !data.market_sentiment.as_of
      ? undefined
      : {
          as_of: data.market_sentiment.as_of,
          emotion_strength: data.market_sentiment.emotion_strength,
          limit_up_count: data.market_sentiment.emotion_limit_up_count,
          max_consecutive: data.market_sentiment.emotion_max_consecutive,
          pullback_count: data.market_sentiment.emotion_pullback_count,
        },
  ), [data?.market_sentiment])
  if (!data && view.isLoading) {
    return <EmptyState icon={Loader2} title="短线猎手加载中" hint="正在等待后端服务响应" />
  }
  if (!data) return <EmptyState icon={ShieldAlert} title="短线猎手加载失败" hint="请检查后端服务后重试" />
  const runtime = data.runtime
  const rows = tab === 'buy_pool' ? data.buy_pool : tab === 'pool' ? data.board_pool : []
  const tableMode: TableMode = tab === 'pool' ? 'pool' : 'buy_pool'
  const tableTitle = tab === 'buy_pool' ? '买入池' : '实盘打板池'
  const tableHint = tab === 'pool'
    ? `扫板：卖一距涨停不超过 ${data.settings.sweep_price_levels} 个价位时提交；排板：${queueTriggerDescription(data.settings.queue_wait_seconds, data.settings.queue_confirm_snapshots)}`
    : '加入后立即按当前 TickFlow 价格发送限价买入委托；移出买入池不会自动撤销已发委托'
  const sentimentPanel = <>
    <section className="border-b border-border px-4 py-3 sm:px-5">
      <div className="grid min-w-[960px] grid-cols-[repeat(4,minmax(130px,1fr))_minmax(220px,1.8fr)] divide-x divide-border overflow-x-auto rounded-btn border border-border bg-surface">
      {[
        ['今日破板率', data.market_sentiment ? plainPercentValue(data.market_sentiment.market_broken_rate_pct) : '--', runtime.sentiment_guard.blocked ? 'text-danger' : 'text-secondary'],
        ['昨日涨停今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_limitup_change_pct) : '--', 'text-secondary'],
        ['昨日连板今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_consecutive_change_pct) : '--', 'text-secondary'],
        ['昨日破板今表现', data.market_sentiment ? percentValue(data.market_sentiment.yesterday_broken_change_pct) : '--', 'text-secondary'],
      ].map(([label, value, tone]) => <div key={label} className="min-w-0 px-3 py-2.5"><div className="truncate text-[10px] text-muted">{label}</div><div className={`mt-1 truncate font-mono text-sm ${tone}`}>{value}</div></div>)}
      <div className="flex min-w-0 items-center gap-2 px-3 py-2.5">
        <span className="min-w-0 truncate text-accent">情绪分数 {data.market_sentiment?.emotion_strength ?? '--'} / {data.market_sentiment?.max_consecutive ?? '--'}板</span>
        <button
          type="button"
          onClick={() => setSentimentChartOpen(true)}
          className="grid h-7 w-7 shrink-0 place-items-center rounded-btn border border-border text-muted hover:bg-elevated hover:text-accent"
          aria-label="查看情绪历史图表"
          title="查看情绪历史图表"
        >
          <LineChart className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
    <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted">
      {data.market_sentiment ? <span>{data.market_sentiment.state === 'live' ? '开盘啦实时情绪数据' : data.market_sentiment.state === 'stale' ? `${data.market_sentiment.as_of ?? '--'} 开盘啦收盘数据` : '开盘啦实时情绪数据暂不可用'}</span> : <span>开盘啦实时情绪数据暂不可用</span>}
      {data.market_sentiment ? <span>刷新 {scoreTime(data.market_sentiment.refreshed_at)}</span> : null}
      <span className={runtime.sentiment_guard.blocked ? 'text-danger' : 'text-secondary'}>{runtime.sentiment_guard.reason}</span>
    </div>
    {runtime.sentiment_guard.blocked ? <div className="mt-2 flex items-center gap-2 rounded-btn border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger"><ShieldAlert className="h-3.5 w-3.5" />自动打板已停止</div> : null}
    </section>
    {sentimentChartOpen ? <Modal labelledBy="limit-board-sentiment-title" onClose={() => setSentimentChartOpen(false)} panelClassName="flex max-h-[90vh] w-[94vw] max-w-4xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 id="limit-board-sentiment-title" className="text-sm font-semibold">市场情绪历史</h2>
          <p className="mt-0.5 text-[10px] text-muted">情绪强度与涨停家数</p>
        </div>
        <button type="button" onClick={() => setSentimentChartOpen(false)} className="grid h-7 w-7 place-items-center rounded-btn text-muted hover:bg-elevated hover:text-foreground" aria-label="关闭情绪历史图表"><X className="h-4 w-4" /></button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3 sm:px-4 sm:py-4">
        <div className="mb-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-[10px] text-muted">
          <span className="font-mono text-accent">情绪分数 {data.market_sentiment?.emotion_strength ?? '--'} / {data.market_sentiment?.max_consecutive ?? '--'}板</span>
          <span className="inline-flex items-center gap-1"><span className="h-1.5 w-1.5 rounded-full bg-orange-500" aria-hidden="true" />情绪强度</span>
          <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-sm bg-green-500/70" aria-hidden="true" />涨停家数</span>
        </div>
        <SentimentHistoryChart points={sentimentHistory} className="h-[min(74vh,620px)] min-h-[320px]" />
      </div>
    </Modal> : null}
  </>
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="短线猎手"
        titleExtra={<div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted">
          <span className="inline-flex items-center gap-1 rounded-md bg-elevated px-2 py-1 text-secondary"><Radio className="h-3 w-3 text-accent" />买入/打板池 {runtime.websocket_symbols}/{runtime.websocket_capacity} WS</span>
          <span className={`inline-flex items-center gap-1.5 ${runtime.websocket_status === 'connected' ? 'text-bear' : 'text-muted'}`}><Wifi className="h-3.5 w-3.5" />{runtime.websocket_status === 'connected' ? '买入池与打板池已接入 WS' : '买入池与打板池未接入 WS'}</span>
          <span className={`inline-flex items-center gap-1.5 ${runtime.limit_up_queue.state === 'connected' ? 'text-bear' : runtime.limit_up_queue.state === 'unavailable' ? 'text-warning' : 'text-muted'}`} title={runtime.limit_up_queue.last_error || runtime.limit_up_queue.url}>D202 排队 {runtime.limit_up_queue.state === 'connected' ? '已连接' : runtime.limit_up_queue.state === 'connecting' ? '连接中' : runtime.limit_up_queue.state === 'unavailable' ? '不可用' : '待接入'}</span>
          <span className={runtime.trading_enabled ? 'text-bear' : 'text-warning'}>{runtime.trading_reason}</span>
        </div>}
        right={<div className="flex flex-wrap items-center justify-end gap-2"><button type="button" onClick={() => setAdvancedOpen(true)} className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-2.5 text-xs text-secondary hover:bg-elevated hover:text-foreground"><SlidersHorizontal className="h-3.5 w-3.5" />高级设置</button><button type="button" title="刷新" onClick={() => view.refetch()} className="inline-flex h-8 w-8 items-center justify-center rounded-btn bg-elevated text-secondary hover:text-foreground"><RefreshCw className={`h-3.5 w-3.5 ${view.isFetching ? 'animate-spin' : ''}`} /></button></div>}
      />

      <div className="flex items-center gap-1 overflow-x-auto border-b border-border px-4 pt-2 sm:px-5">
        {([
          ['ladder', '连板天梯', null, Flame],
          ['sector', '板块强度', data.sector_strength?.rows.length ?? 0, Layers3],
          ['buy_pool', '买入池', data.buy_pool.length, ShoppingCart],
          ['pool', '打板池', data.board_pool.length, Crosshair],
          ['events', '触发记录', data.events.length, Bell],
        ] as const).map(([id, label, count, Icon]) => (
          <button key={id} type="button" onClick={() => setTab(id)} className={`inline-flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-xs font-medium ${tab === id ? 'border-accent text-foreground' : 'border-transparent text-muted'}`}>
            <Icon className="h-3.5 w-3.5" />{label}{count == null ? null : <span className="font-mono text-[10px] text-muted">{count}</span>}
          </button>
        ))}
      </div>

      <div className={`min-h-0 flex-1 ${tab === 'ladder' ? 'overflow-hidden' : 'overflow-x-hidden overflow-y-auto px-2 py-3 sm:px-5'}`}>
        {tab === 'ladder' ? <Suspense fallback={<div className="grid h-full place-items-center"><RefreshCw className="h-5 w-5 animate-spin text-muted" /></div>}><EmbeddedLimitLadder
          headerContent={sentimentPanel}
          onAddToPool={(stock: LimitLadderStock) => setAllocationDialog({
            row: manualActionRow(stock.symbol, stock.name, stock.close, stock.change_pct, null),
            kind: 'board',
            initialMode: 'lot',
            initialValue: null,
          })}
        /></Suspense> : tab === 'sector' ? <SectorStrengthTable snapshot={data.sector_strength} hotRows={heatRows} hotQuotes={heatQuotes.data?.quotes} hotSectorLinks={heatQuotes.data?.sector_links} hotLoading={approachingLimitUp.isPending} hotError={approachingLimitUp.isError || approachingLimitUp.data?.state === 'unavailable'} refreshIntervalSeconds={runtime.refresh_cycle.interval_seconds} refreshCycleUpdatedAt={view.dataUpdatedAt} onOpenStock={(symbol, name) => setPreview({ symbol, name })} onAddPool={row => setAllocationDialog({ row, kind: 'board', initialMode: row.allocation_mode ?? 'global', initialValue: row.allocation_value })} onAddBuyPool={row => setAllocationDialog({ row, kind: 'buy', initialMode: row.allocation_mode === 'available' || row.allocation_mode === 'sixth' || row.allocation_mode === 'fifth' || row.allocation_mode === 'quarter' || row.allocation_mode === 'fixed' || row.allocation_mode === 'volume' ? row.allocation_mode : 'lot', initialValue: row.allocation_value })} poolSymbols={poolSymbols} buyPoolSymbols={buyPoolSymbols} busy={busy} /> : tab !== 'events' ? (
          <section className="overflow-hidden rounded-btn border border-border bg-surface">
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5">
              <div><div className="text-xs font-medium">{tableTitle}</div><div className="mt-0.5 text-[10px] text-muted">{tableHint}</div></div>
              <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-[10px]">
                {runtime.last_error ? <span className="text-warning">{runtime.last_error}</span> : null}
              </div>
            </div>
            <Table
              rows={rows}
              mode={tableMode}
              busy={busy}
              sweepPriceLevels={data.settings.sweep_price_levels}
              queueWaitSeconds={data.settings.queue_wait_seconds}
              queueConfirmSnapshots={data.settings.queue_confirm_snapshots}
              onOpen={setPreview}
              onEditAllocation={row => setAllocationDialog({
                row,
                kind: 'edit',
                initialMode: row.allocation_mode ?? 'global',
                initialValue: row.allocation_value,
              })}
              onToggleAuto={(row, enabled) => updatePool.mutate({ row, enabled, orderMode: row.order_mode === 'queue' ? 'queue' : 'sweep' })}
              onChangeOrderMode={(row, orderMode) => updatePool.mutate({ row, enabled: row.auto_trade === true, orderMode })}
              onRemovePool={row => tableMode === 'buy_pool' ? removeBuyPool.mutate(row) : removePool.mutate(row)}
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
      {allocationDialog ? <LimitBoardAllocationDialog
        key={`${allocationDialog.kind}:${allocationDialog.row.symbol}`}
        row={allocationDialog.row}
        kind={allocationDialog.kind}
        initialMode={allocationDialog.initialMode}
        initialValue={allocationDialog.initialValue}
        pending={addPool.isPending || addBuyPool.isPending || updatePool.isPending}
        onClose={() => setAllocationDialog(null)}
        onConfirm={(allocationMode, allocationValue, creditBuyMode, orderPrice) => {
          const row = allocationDialog.row
          const source = row.source === 'manual' || row.source === 'selected'
            ? 'manual'
            : row.source === 'rebound_board' ? 'rebound_board' : 'first_board'
          if (allocationDialog.kind === 'buy') {
            if (allocationMode === 'global') return
            addBuyPool.mutate({
              row,
              source,
              allocationMode: allocationMode as 'available' | 'sixth' | 'fifth' | 'quarter' | 'lot' | 'fixed' | 'volume',
              allocationValue,
              creditBuyMode,
              orderPrice,
            })
            return
          }
          if (allocationDialog.kind === 'board') {
            addPool.mutate({ row, source, allocationMode, allocationValue, creditBuyMode })
            return
          }
          updatePool.mutate({
            row,
            enabled: row.auto_trade === true,
            orderMode: row.order_mode === 'queue' ? 'queue' : 'sweep',
            allocationMode,
            allocationValue,
            creditBuyMode,
          })
        }}
      /> : null}
      <StockPreviewDialog symbol={preview?.symbol ?? null} name={preview?.name} onClose={() => setPreview(null)} />
    </div>
  )
}
