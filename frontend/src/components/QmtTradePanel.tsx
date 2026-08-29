import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, X } from 'lucide-react'
import { api, type QmtCreditBuyMode } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { toast } from '@/components/Toast'
import { Modal } from '@/components/Modal'
import { cn } from '@/lib/cn'
import {
  QMT_ALLOCATION_OPTIONS,
  QmtTradeAllocationControls,
  qmtAllocationLabel,
  type QmtTradeAllocationMode,
} from '@/components/QmtTradeAllocation'

export type QmtTradeInstrument = {
  symbol: string
  name?: string
  price?: number | null
  limitUp?: number | null
  limitDown?: number | null
}

type QmtTradePriceType = 'LIMIT' | 'LATEST' | 'LIMIT_UP' | 'LIMIT_DOWN'

/** 旧调用方可继续使用的持仓风控资金方式类型。 */
export type QmtAllocationMode = Extract<QmtTradeAllocationMode, 'available' | 'quarter' | 'third' | 'half' | 'fixed'>
type QmtPanelAllocationMode = Exclude<QmtTradeAllocationMode, 'lot' | 'volume'>

export { QMT_ALLOCATION_OPTIONS, QmtTradeAllocationControls }

export type QmtTradePreset = {
  action?: 'BUY' | 'SELL'
  price?: number | null
  volume?: number | null
  allocationMode?: QmtPanelAllocationMode
  allocationValue?: number | null
  creditBuyMode?: QmtCreditBuyMode
}

export type QmtRiskTradeContext = {
  fingerprint: string
}

const QMT_ORDER_STATUS: Record<string, string> = {
  submitting: '提交中',
  unknown: '状态待人工核对',
  rejected: '已拒绝',
  accepted_pending: '已受理待回查',
  confirmed: '委托已确认',
  '48': '未报',
  '49': '待报',
  '50': '已报',
  '51': '已报待撤',
  '52': '部成待撤',
  '53': '部撤',
  '54': '已撤',
  '55': '部成',
  '56': '已成',
  '57': '废单',
}

const ALLOCATION_RATIOS: Record<Exclude<QmtPanelAllocationMode, 'fixed'>, number> = {
  available: 1,
  sixth: 1 / 6,
  fifth: 0.2,
  quarter: 0.25,
  third: 1 / 3,
  half: 0.5,
}

const MONEY = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

function qmtOrderStatus(value?: string) {
  return value ? QMT_ORDER_STATUS[value] ?? `状态 ${value}` : '状态未知'
}

function qmtOrderCanCancel(value?: string) {
  return ['48', '49', '50', '55'].includes(String(value ?? '').trim())
}

function qmtOrderCancelPending(value?: string) {
  return ['51', '52'].includes(String(value ?? '').trim())
}

function defaultPrice(value: number | null | undefined): string {
  return value != null && Number.isFinite(value) ? String(value) : ''
}

function finitePrice(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) && value > 0 ? value : null
}

function priceOptionLabel(label: string, value: number | null): string {
  return value == null ? label : `${label}（${value.toFixed(3)}）`
}

function initialAllocationValue(instrument: QmtTradeInstrument, preset?: QmtTradePreset | null) {
  if (preset?.allocationValue != null) return preset.allocationValue
  const price = preset?.price ?? instrument.price
  if (preset?.volume != null && price != null && Number.isFinite(price)) return preset.volume * price
  return 10_000
}

export function QmtTradePanel({
  instrument,
  preset,
  riskContext,
  onClose,
}: {
  instrument: QmtTradeInstrument
  preset?: QmtTradePreset | null
  riskContext?: QmtRiskTradeContext | null
  onClose: () => void
}) {
  const queryClient = useQueryClient()
  const qmt = useQuery({
    queryKey: QK.positionRiskQmt,
    queryFn: api.qmtStatus,
    refetchInterval: 30_000,
    staleTime: 5_000,
    placeholderData: previous => previous,
  })
  const qmtReady = qmt.data?.configured === true && qmt.data.state === 'ready'
  const orders = useQuery({
    queryKey: QK.positionRiskQmtOrders,
    queryFn: api.qmtOrders,
    enabled: Boolean(qmt.data?.configured),
    refetchInterval: 15_000,
  })
  const priceLimits = useQuery({
    queryKey: QK.positionRiskQmtPriceLimit(instrument.symbol),
    queryFn: () => api.klineMinute(instrument.symbol),
    enabled: Boolean(instrument.symbol),
    staleTime: 30_000,
    retry: false,
  })
  const [tradeAction, setTradeAction] = useState<'BUY' | 'SELL'>(preset?.action ?? 'SELL')
  const [tradePrice, setTradePrice] = useState(defaultPrice(preset?.price ?? instrument.price))
  const [tradePriceType, setTradePriceType] = useState<QmtTradePriceType>('LIMIT')
  const [allocationMode, setAllocationMode] = useState<QmtPanelAllocationMode>(preset?.allocationMode ?? 'quarter')
  const [allocationValue, setAllocationValue] = useState(initialAllocationValue(instrument, preset))
  const [creditBuyMode, setCreditBuyMode] = useState<QmtCreditBuyMode>(preset?.creditBuyMode ?? 'collateral')
  const [confirmOpen, setConfirmOpen] = useState(false)

  const limitUp = finitePrice(priceLimits.data?.price_limit?.limit_up ?? instrument.limitUp)
  const limitDown = finitePrice(priceLimits.data?.price_limit?.limit_down ?? instrument.limitDown)
  const quickPrice = tradePriceType === 'LIMIT_UP' ? limitUp : tradePriceType === 'LIMIT_DOWN' ? limitDown : null
  const limitPrice = quickPrice ?? Number(tradePrice)
  const referencePrice = tradePriceType === 'LATEST' ? Number(instrument.price) : limitPrice
  const backendPriceType = tradePriceType === 'LATEST' ? 'LATEST' : 'LIMIT'
  const creditBuy = tradeAction === 'BUY' && String(qmt.data?.account_type || '').toUpperCase() === 'CREDIT'
  const cachedAccount = qmt.data?.account
  const cachedBuyingPower = tradeAction === 'BUY'
    ? creditBuy
      ? creditBuyMode === 'financing'
        ? cachedAccount?.fin_enbuy_balance
        : cachedAccount?.assure_enbuy_balance ?? cachedAccount?.credit_assure_buying_power
      : cachedAccount?.cash
    : null
  const cachedBasisLabel = tradeAction === 'BUY'
    ? creditBuy
      ? creditBuyMode === 'financing' ? '可买融资标的资金' : '可买担保品资金'
      : '可用资金'
    : null
  const validReferencePrice = Number.isFinite(referencePrice) && referencePrice > 0
  const validAllocation = allocationMode !== 'fixed' || (Number.isFinite(allocationValue) && allocationValue > 0)
  const previewRequestMode = allocationMode === 'fixed' ? 'fixed' : 'quarter'
  const previewRequestValue = allocationMode === 'fixed' ? allocationValue : null
  const previewPayload = {
    action: tradeAction,
    symbol: instrument.symbol,
    price: backendPriceType === 'LIMIT' ? referencePrice : null,
    price_type: backendPriceType,
    reference_price: validReferencePrice ? referencePrice : null,
    allocation_mode: allocationMode,
    allocation_value: allocationMode === 'fixed' ? allocationValue : null,
    credit_buy_mode: creditBuyMode,
  }
  const preview = useQuery({
    queryKey: QK.positionRiskQmtPreview(
      instrument.symbol,
      tradeAction,
      backendPriceType,
      validReferencePrice ? referencePrice : null,
      previewRequestMode,
      previewRequestValue,
      creditBuyMode,
    ),
    queryFn: () => api.qmtPreviewOrder({ ...previewPayload, allocation_mode: previewRequestMode, allocation_value: previewRequestValue }, true),
    enabled: qmtReady && validReferencePrice && validAllocation,
    retry: false,
    placeholderData: previous => previous,
    staleTime: 500,
  })
  const basePreview = preview.data?.preview
  const serverPreview = useMemo(() => {
    if (!basePreview || allocationMode === 'fixed') return basePreview
    const ratio = ALLOCATION_RATIOS[allocationMode]
    const requestedAmount = basePreview.basis_amount * ratio
    const targetAmount = Math.min(requestedAmount, basePreview.basis_amount)
    let volume = Math.floor(targetAmount / basePreview.price / 100) * 100
    if (basePreview.available_volume != null) volume = Math.min(volume, Math.floor(basePreview.available_volume / 100) * 100)
    const actualAmount = Math.round(volume * basePreview.price * 100) / 100
    return {
      ...basePreview,
      allocation_mode: allocationMode,
      allocation_value: null,
      target_amount: Math.round(targetAmount * 100) / 100,
      actual_amount: actualAmount,
      volume,
      capped: targetAmount < requestedAmount || actualAmount < targetAmount,
      reason: volume < 100 ? '金额不足一手' : null,
    }
  }, [allocationMode, basePreview])
  const effectiveCreditBuyMode = serverPreview?.credit_buy_mode ?? creditBuyMode
  useEffect(() => {
    if (!creditBuy || preview.isFetching || !serverPreview) return
    const requestedMode = serverPreview.requested_credit_buy_mode ?? creditBuyMode
    const effectiveMode = serverPreview.credit_buy_mode
    if (requestedMode === creditBuyMode && effectiveMode && effectiveMode !== creditBuyMode) {
      setCreditBuyMode(effectiveMode)
    }
  }, [creditBuy, creditBuyMode, preview.isFetching, serverPreview])
  const tradeVolume = serverPreview?.volume ?? 0
  const actualAmount = serverPreview ? Math.round(tradeVolume * serverPreview.price * 100) / 100 : 0
  const actionLabel = tradeAction === 'BUY' ? '买入' : '卖出'
  const priceLabel = tradePriceType === 'LATEST'
    ? `最新价（参考 ${referencePrice.toFixed(3)}）`
    : tradePriceType === 'LIMIT_UP'
      ? `涨停价 ${referencePrice.toFixed(3)}`
      : tradePriceType === 'LIMIT_DOWN'
        ? `跌停价 ${referencePrice.toFixed(3)}`
        : `限价 ${limitPrice.toFixed(3)}`
  const tradeReady = qmt.data?.trade_enabled === true && qmt.data.state === 'ready'
  const canSubmit = tradeReady && tradeVolume >= 100 && validReferencePrice && validAllocation && !preview.isFetching
  const instrumentOrders = useMemo(
    () => (orders.data?.orders ?? []).filter(order => order.symbol === instrument.symbol && order.order_sys_id).slice(0, 5),
    [instrument.symbol, orders.data?.orders],
  )

  const tradeMutation = useMutation({
    mutationFn: () => riskContext
      ? api.qmtConfirmRiskAction({ fingerprint: riskContext.fingerprint, symbol: instrument.symbol, action: tradeAction, volume: tradeVolume, credit_buy_mode: creditBuyMode })
      : api.qmtSubmitOrder({
          ...previewPayload,
          volume: tradeVolume,
          idempotency_key: `manual-${instrument.symbol}-${tradeAction}-${Date.now()}`,
        }),
    onSuccess: result => {
      toast(`委托结果：${qmtOrderStatus(result.order.status)}`, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmt })
      queryClient.invalidateQueries({ queryKey: QK.positionRisk })
      queryClient.invalidateQueries({ queryKey: QK.positionRiskEvents })
    },
    onError: error => toast(error instanceof Error ? error.message : '委托失败，请检查 QMT 状态和交易开关'),
  })
  const cancelMutation = useMutation({
    mutationFn: api.qmtCancelOrder,
    onSuccess: () => {
      toast('已请求撤单', 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
    },
    onError: error => toast(error instanceof Error ? error.message : '撤单失败，请在 QMT 核对委托状态'),
  })

  const submit = () => {
    if (!validReferencePrice) {
      toast('请输入有效价格')
      return
    }
    if (!validAllocation) {
      toast('固定金额必须大于 0')
      return
    }
    if (tradeVolume < 100) {
      toast(serverPreview?.reason || (preview.error instanceof Error ? preview.error.message : '可用金额不足一手'))
      return
    }
    if (!tradeReady) {
      toast(qmt.data?.reason || 'QMT 未就绪，无法发送委托')
      return
    }
    setConfirmOpen(true)
  }

  const choosePriceType = (value: QmtTradePriceType) => {
    setTradePriceType(value)
    if (value === 'LIMIT_UP' && limitUp != null) setTradePrice(defaultPrice(limitUp))
    if (value === 'LIMIT_DOWN' && limitDown != null) setTradePrice(defaultPrice(limitDown))
  }

  const readiness = qmt.isLoading
    ? '正在读取 QMT 状态'
    : tradeReady
      ? '真实交易已开启'
      : qmt.data?.reason || 'QMT 未就绪'
  const allocationPreviewState = preview.isFetching
    ? 'loading'
    : preview.isError
      ? 'error'
      : serverPreview?.reason
        ? 'error'
      : serverPreview
        ? 'ready'
        : qmtReady
          ? 'idle'
          : 'unavailable'
  const allocationPreviewMessage = preview.isError
    ? preview.error instanceof Error ? preview.error.message : '委托金额暂时无法计算'
    : !qmtReady
      ? qmt.data?.reason || 'QMT 未就绪，无法读取账户可用金额'
      : serverPreview?.reason
        || serverPreview?.credit_buy_mode_reason
        || (serverPreview?.capped
        ? '目标金额已按账户可用资金或持仓，以及 100 股整手向下调整。'
        : null)
  const allocationModeLabel = qmtAllocationLabel(tradeAction, allocationMode)

  return (
    <Modal
      onClose={() => { if (confirmOpen) setConfirmOpen(false); else onClose() }}
      ariaLabel="QMT 手动交易"
      overlayClassName="fixed inset-0 z-50 flex justify-end bg-black/35"
      panelClassName="flex h-full w-full max-w-md flex-col border-l border-border bg-surface shadow-xl"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div className="min-w-0"><div className="text-sm font-semibold">QMT 手动交易</div><div className="truncate font-mono text-[11px] text-muted">{instrument.name || instrument.symbol} · {instrument.symbol}</div></div>
        <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭交易面板"><X className="h-4 w-4" /></button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <section className="border-y border-border py-3">
          <div className="flex items-center justify-between gap-3"><h3 className="text-xs font-semibold text-secondary">委托参数</h3><span className={cn('text-right text-[10px]', tradeReady ? 'text-warning' : 'text-muted')} title={qmt.data?.reason}>{readiness}</span></div>
          <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-muted">
            <label>方向<select value={tradeAction} disabled={Boolean(riskContext)} onChange={event => setTradeAction(event.target.value as 'BUY' | 'SELL')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px] disabled:opacity-60"><option value="BUY">买入</option><option value="SELL">卖出</option></select></label>
            <div className="min-w-0"><span className="text-[10px] text-muted">账户连接</span><div className="mt-1 h-7 truncate rounded border border-border bg-surface px-2 py-1.5 text-[11px] text-secondary">{qmt.data?.account_id || '未连接 QMT 账户'}</div></div>
            <label>价格方式<select value={tradePriceType} onChange={event => choosePriceType(event.target.value as QmtTradePriceType)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="LIMIT">手动限价</option><option value="LATEST">最新价</option><option value="LIMIT_UP" disabled={limitUp == null}>{priceOptionLabel('涨停价', limitUp)}</option><option value="LIMIT_DOWN" disabled={limitDown == null}>{priceOptionLabel('跌停价', limitDown)}</option></select></label>
            <label className={tradePriceType === 'LATEST' || quickPrice != null ? 'opacity-50' : ''}>限价<input type="number" min="0.001" step="0.001" value={quickPrice != null ? defaultPrice(quickPrice) : tradePrice} disabled={tradePriceType === 'LATEST' || quickPrice != null} onChange={event => setTradePrice(event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px] disabled:cursor-not-allowed" /></label>
          </div>

          <QmtTradeAllocationControls
            action={tradeAction}
            mode={allocationMode}
            value={allocationValue}
            onModeChange={next => { if (next !== 'lot' && next !== 'volume') setAllocationMode(next) }}
            onValueChange={setAllocationValue}
            disabled={tradeMutation.isPending}
            basisLabel={serverPreview?.basis_label ?? cachedBasisLabel}
            basisAmount={serverPreview?.basis_amount ?? cachedBuyingPower}
            accountType={qmt.data?.account_type}
            cashAmount={serverPreview?.cash_amount ?? cachedAccount?.cash}
            financingBuyingPowerAmount={serverPreview?.financing_buying_power_amount ?? null}
            financingBuyingPowerLabel={
              serverPreview?.credit_opvolume?.status === 'ready'
                ? '该股票最大融资可买'
                : undefined
            }
            previewState={allocationPreviewState}
            previewMessage={allocationPreviewMessage}
            disabledModes={{ available: !qmtReady }}
          />
          {creditBuy ? <label className="mt-3 block text-[10px] text-muted">信用账户买入方式
            <select
              value={creditBuyMode}
              disabled={tradeMutation.isPending}
              onChange={event => setCreditBuyMode(event.target.value as QmtCreditBuyMode)}
              className="mt-1 h-8 w-full rounded border border-border bg-surface px-2 text-xs outline-none focus:border-accent disabled:opacity-50"
            >
              <option value="collateral">担保品买入</option>
              <option value="financing">融资买入</option>
            </select>
          </label> : null}
          {(tradePriceType === 'LIMIT_UP' || tradePriceType === 'LIMIT_DOWN') && quickPrice == null ? <p className="mt-2 text-[10px] text-warning">当日{tradePriceType === 'LIMIT_UP' ? '涨停' : '跌停'}价暂不可用，请改用手动限价。</p> : null}
          <button type="button" disabled={!canSubmit || tradeMutation.isPending} onClick={submit} className={cn('mt-3 h-8 w-full rounded-btn text-xs text-white disabled:cursor-not-allowed disabled:opacity-40', tradeAction === 'BUY' ? 'bg-bull' : 'bg-bear')}>
            {tradeMutation.isPending ? '提交中...' : `发送${tradeAction === 'BUY' ? '买入' : '卖出'}委托`}
          </button>
          <p className="mt-2 text-[10px] leading-4 text-muted">金额和股数由 QMT 最新可用资金或可用持仓计算，并向下取 100 股整手。成交结果以券商回报为准。</p>
          {serverPreview?.credit_buy_mode_switched ? <p className="mt-2 text-[10px] leading-4 text-warning">{serverPreview.credit_buy_mode_reason || `首选买入额度不足，实际将自动切换为${effectiveCreditBuyMode === 'financing' ? '融资买入' : '担保品买入'}。`}</p> : null}
        </section>

        {instrumentOrders.length ? <section className="border-b border-border py-3">
          <h3 className="mb-2 text-xs font-semibold text-secondary">当前委托</h3>
          <div className="space-y-1">
            {instrumentOrders.map(order => <div key={order.order_sys_id} className="flex items-center justify-between gap-2 text-[10px] text-muted"><span><span className={order.action === 'SELL' ? 'text-bear' : 'text-bull'}>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '--'}</span> · {qmtOrderStatus(order.status)}</span>{qmtOrderCancelPending(order.status) ? <span className="text-warning">撤单中</span> : qmtOrderCanCancel(order.status) ? <button type="button" disabled={cancelMutation.isPending} onClick={() => cancelMutation.mutate(order.order_sys_id!)} className="h-6 rounded border border-border px-2 hover:bg-elevated disabled:opacity-40">撤单</button> : null}</div>)}
          </div>
        </section> : null}
      </div>
      {confirmOpen ? <Modal
        onClose={() => { if (!tradeMutation.isPending) setConfirmOpen(false) }}
        labelledBy="qmt-trade-confirm-title"
        closeOnBackdrop={!tradeMutation.isPending}
        overlayClassName="fixed inset-0 z-[60] flex items-center justify-center bg-black/45 p-4 backdrop-blur-sm"
        panelClassName="w-[92vw] max-w-md rounded-card border border-border bg-surface shadow-xl"
      >
        <div className="flex items-start gap-3 border-b border-border px-4 py-4">
          <div className="grid h-8 w-8 shrink-0 place-items-center rounded-btn bg-warning/10 text-warning">
            <AlertTriangle className="h-4 w-4" />
          </div>
          <div className="min-w-0">
            <h2 id="qmt-trade-confirm-title" className="text-sm font-semibold">确认发送真实委托</h2>
            <p className="mt-1 text-[11px] leading-4 text-muted">该操作将发送至真实 QMT 账户，请确认以下委托参数。</p>
          </div>
          <button type="button" disabled={tradeMutation.isPending} onClick={() => setConfirmOpen(false)} className="ml-auto grid h-7 w-7 shrink-0 place-items-center rounded-btn text-muted hover:bg-elevated hover:text-foreground disabled:opacity-40" aria-label="取消真实委托确认">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="space-y-3 px-4 py-4 text-xs">
          {creditBuy ? <label className="block text-[10px] text-muted">信用账户买入方式
            <select
              value={creditBuyMode}
              disabled={tradeMutation.isPending}
              onChange={event => setCreditBuyMode(event.target.value as QmtCreditBuyMode)}
              className="mt-1 h-8 w-full rounded border border-border bg-surface px-2 text-xs outline-none focus:border-accent disabled:opacity-50"
            >
              <option value="collateral">担保品买入</option>
              <option value="financing">融资买入</option>
            </select>
          </label> : null}
          <div className="grid grid-cols-2 gap-x-4 gap-y-3">
            <div><div className="text-[10px] text-muted">标的</div><div className="mt-1 font-medium text-foreground">{instrument.name || instrument.symbol}<span className="ml-2 font-mono text-[10px] text-muted">{instrument.symbol}</span></div></div>
            <div><div className="text-[10px] text-muted">方向</div><div className={cn('mt-1 font-medium', tradeAction === 'BUY' ? 'text-bull' : 'text-bear')}>{actionLabel}</div></div>
            <div><div className="text-[10px] text-muted">委托数量</div><div className="mt-1 font-mono text-foreground">{tradeVolume.toLocaleString()} 股</div></div>
            <div><div className="text-[10px] text-muted">预计金额</div><div className="mt-1 font-mono text-foreground">{MONEY.format(actualAmount)} 元</div></div>
            <div><div className="text-[10px] text-muted">账户当前可用</div><div className="mt-1 font-mono text-foreground">{serverPreview ? `${MONEY.format(serverPreview.basis_amount)} 元` : '—'}</div></div>
            <div><div className="text-[10px] text-muted">资金方式</div><div className="mt-1 text-foreground">{allocationModeLabel}</div></div>
            {creditBuy ? <div><div className="text-[10px] text-muted">实际买入方式</div><div className="mt-1 text-foreground">{effectiveCreditBuyMode === 'financing' ? '融资买入' : '担保品买入'}{serverPreview?.credit_buy_mode_switched ? '（自动切换）' : ''}</div></div> : null}
            <div className="col-span-2"><div className="text-[10px] text-muted">价格</div><div className="mt-1 font-mono text-foreground">{priceLabel}</div></div>
          </div>
          <div className="border-y border-warning/25 bg-warning/5 px-3 py-2 text-[10px] leading-4 text-warning">真实交易已开启。成交、排队和撤单结果以 QMT 与券商回报为准。</div>
        </div>
        <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
          <button type="button" disabled={tradeMutation.isPending} onClick={() => setConfirmOpen(false)} className="h-8 rounded-btn border border-border px-3 text-xs text-muted hover:bg-elevated hover:text-foreground disabled:opacity-40">取消</button>
          <button type="button" disabled={tradeMutation.isPending || preview.isFetching || tradeVolume < 100} onClick={() => { setConfirmOpen(false); tradeMutation.mutate() }} className={cn('h-8 rounded-btn px-3 text-xs font-medium text-white disabled:cursor-not-allowed disabled:opacity-50', tradeAction === 'BUY' ? 'bg-bull hover:bg-bull/90' : 'bg-bear hover:bg-bear/90')}>
            {tradeMutation.isPending ? '提交中...' : `确认发送${tradeAction === 'BUY' ? '买入' : '卖出'}`}
          </button>
        </div>
      </Modal> : null}
    </Modal>
  )
}
