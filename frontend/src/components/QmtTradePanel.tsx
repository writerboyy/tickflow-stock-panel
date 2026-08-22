import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { X } from 'lucide-react'
import { api } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { toast } from '@/components/Toast'
import { Modal } from '@/components/Modal'
import { cn } from '@/lib/cn'

export type QmtTradeInstrument = {
  symbol: string
  name?: string
  price?: number | null
}

export type QmtAllocationMode = 'quarter' | 'third' | 'half' | 'fixed'

export const QMT_ALLOCATION_OPTIONS: ReadonlyArray<{ value: QmtAllocationMode; label: string }> = [
  { value: 'quarter', label: '可用金额 1/4' },
  { value: 'third', label: '可用金额 1/3' },
  { value: 'half', label: '可用金额 1/2' },
  { value: 'fixed', label: '固定金额' },
]

export type QmtTradePreset = {
  action?: 'BUY' | 'SELL'
  price?: number | null
  volume?: number | null
  allocationMode?: QmtAllocationMode
  allocationValue?: number | null
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

const ALLOCATION_FRACTION_LABELS: Record<Exclude<QmtAllocationMode, 'fixed'>, string> = {
  quarter: '1/4',
  third: '1/3',
  half: '1/2',
}

const MONEY = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

function allocationLabel(action: 'BUY' | 'SELL', mode: QmtAllocationMode): string {
  if (mode === 'fixed') return '固定金额'
  const basis = action === 'SELL' ? '可用持仓市值' : '可用资金'
  return `${basis} ${ALLOCATION_FRACTION_LABELS[mode]}`
}

function qmtOrderStatus(value?: string) {
  return value ? QMT_ORDER_STATUS[value] ?? `状态 ${value}` : '状态未知'
}

function defaultPrice(value: number | null | undefined): string {
  return value != null && Number.isFinite(value) ? String(value) : ''
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
  const qmt = useQuery({ queryKey: QK.positionRiskQmt, queryFn: api.qmtStatus, refetchInterval: 30_000 })
  const orders = useQuery({
    queryKey: QK.positionRiskQmtOrders,
    queryFn: api.qmtOrders,
    enabled: Boolean(qmt.data?.configured),
    refetchInterval: 15_000,
  })
  const [tradeAction, setTradeAction] = useState<'BUY' | 'SELL'>(preset?.action ?? 'SELL')
  const [tradePrice, setTradePrice] = useState(defaultPrice(preset?.price ?? instrument.price))
  const [tradePriceType, setTradePriceType] = useState<'LIMIT' | 'LATEST'>('LIMIT')
  const [allocationMode, setAllocationMode] = useState<QmtAllocationMode>(preset?.allocationMode ?? 'quarter')
  const [allocationValue, setAllocationValue] = useState(initialAllocationValue(instrument, preset))

  const limitPrice = Number(tradePrice)
  const referencePrice = tradePriceType === 'LIMIT' ? limitPrice : Number(instrument.price)
  const validReferencePrice = Number.isFinite(referencePrice) && referencePrice > 0
  const validAllocation = allocationMode !== 'fixed' || (Number.isFinite(allocationValue) && allocationValue > 0)
  const previewPayload = {
    action: tradeAction,
    symbol: instrument.symbol,
    price: tradePriceType === 'LIMIT' ? limitPrice : null,
    price_type: tradePriceType,
    reference_price: validReferencePrice ? referencePrice : null,
    allocation_mode: allocationMode,
    allocation_value: allocationMode === 'fixed' ? allocationValue : null,
  }
  const preview = useQuery({
    queryKey: QK.positionRiskQmtPreview(
      instrument.symbol,
      tradeAction,
      tradePriceType,
      validReferencePrice ? referencePrice : null,
      allocationMode,
      allocationMode === 'fixed' ? allocationValue : null,
    ),
    queryFn: () => api.qmtPreviewOrder(previewPayload, true),
    enabled: qmt.data?.configured === true && validReferencePrice && validAllocation,
    retry: false,
  })
  const serverPreview = preview.data?.preview
  const tradeVolume = serverPreview?.volume ?? 0
  const actualAmount = serverPreview ? Math.round(tradeVolume * serverPreview.price * 100) / 100 : 0
  const tradeReady = qmt.data?.trade_enabled === true && qmt.data.state === 'ready'
  const canSubmit = tradeReady && tradeVolume >= 100 && validReferencePrice && validAllocation && !preview.isFetching
  const instrumentOrders = useMemo(
    () => (orders.data?.orders ?? []).filter(order => order.symbol === instrument.symbol && order.order_sys_id).slice(0, 5),
    [instrument.symbol, orders.data?.orders],
  )

  const tradeMutation = useMutation({
    mutationFn: () => riskContext
      ? api.qmtConfirmRiskAction({ fingerprint: riskContext.fingerprint, symbol: instrument.symbol, action: tradeAction, volume: tradeVolume })
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
    const actionLabel = tradeAction === 'BUY' ? '买入' : '卖出'
    const priceLabel = tradePriceType === 'LATEST' ? `最新价（参考 ${referencePrice.toFixed(3)}）` : `限价 ${limitPrice.toFixed(3)}`
    if (!window.confirm(`确认${actionLabel} ${instrument.name || instrument.symbol} ${tradeVolume} 股，预计 ${MONEY.format(actualAmount)} 元（${priceLabel}）？该操作将发送至真实 QMT 账户。`)) return
    tradeMutation.mutate()
  }

  const readiness = qmt.isLoading
    ? '正在读取 QMT 状态'
    : tradeReady
      ? '真实交易已开启'
      : qmt.data?.reason || 'QMT 未就绪'

  return (
    <Modal
      onClose={onClose}
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
            <label>金额方式<select value={allocationMode} onChange={event => setAllocationMode(event.target.value as QmtAllocationMode)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]">{QMT_ALLOCATION_OPTIONS.map(option => <option key={option.value} value={option.value}>{allocationLabel(tradeAction, option.value)}</option>)}</select></label>
            <label>价格方式<select value={tradePriceType} onChange={event => setTradePriceType(event.target.value as 'LIMIT' | 'LATEST')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="LIMIT">限价</option><option value="LATEST">最新价</option></select></label>
            <label className={tradePriceType === 'LATEST' ? 'opacity-50' : ''}>限价<input type="number" min="0.001" step="0.001" value={tradePrice} disabled={tradePriceType === 'LATEST'} onChange={event => setTradePrice(event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px] disabled:cursor-not-allowed" /></label>
            {allocationMode === 'fixed' ? <label className="col-span-2">固定金额<input type="number" min="100" step="100" value={allocationValue} onChange={event => setAllocationValue(Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px]" /></label> : null}
          </div>

          <div className="mt-3 border-y border-border py-3 text-[10px]">
            <div className="grid grid-cols-2 gap-x-4 gap-y-2">
              <div className="col-span-2 font-medium text-secondary">金额计算</div>
              <div><span className="text-muted">{serverPreview?.basis_label || (tradeAction === 'BUY' ? '可用资金' : '可用持仓市值')}</span><div className="mt-0.5 font-mono text-foreground">{serverPreview ? `${MONEY.format(serverPreview.basis_amount)} 元` : '—'}</div></div>
              <div><span className="text-muted">{allocationLabel(tradeAction, allocationMode)}</span><div className="mt-0.5 font-mono text-foreground">{serverPreview ? `${MONEY.format(serverPreview.target_amount)} 元` : '—'}</div></div>
              <div className="col-span-2 mt-1 border-t border-border pt-2 font-medium text-secondary">本次委托</div>
              <div><span className="text-muted">委托数量</span><div className="mt-0.5 font-mono text-foreground">{tradeVolume >= 100 ? `${tradeVolume.toLocaleString()} 股` : '—'}</div></div>
              <div><span className="text-muted">预计委托金额</span><div className="mt-0.5 font-mono text-foreground">{actualAmount > 0 ? `${MONEY.format(actualAmount)} 元` : '—'}</div></div>
            </div>
          </div>
          {serverPreview?.capped ? <p className="mt-2 text-[10px] text-muted">目标金额已按可用资金或持仓，以及 100 股整手向下调整。</p> : null}
          {preview.isError ? <p className="mt-2 text-[10px] text-warning">{preview.error instanceof Error ? preview.error.message : '委托金额暂时无法计算'}</p> : null}
          <button type="button" disabled={!canSubmit || tradeMutation.isPending} onClick={submit} className={cn('mt-3 h-8 w-full rounded-btn text-xs text-white disabled:cursor-not-allowed disabled:opacity-40', tradeAction === 'BUY' ? 'bg-bull' : 'bg-bear')}>
            {tradeMutation.isPending ? '提交中...' : `发送${tradeAction === 'BUY' ? '买入' : '卖出'}委托`}
          </button>
          <p className="mt-2 text-[10px] leading-4 text-muted">金额和股数由 QMT 最新可用资金或可用持仓计算，并向下取 100 股整手。成交结果以券商回报为准。</p>
        </section>

        {instrumentOrders.length ? <section className="border-b border-border py-3">
          <h3 className="mb-2 text-xs font-semibold text-secondary">当前委托</h3>
          <div className="space-y-1">
            {instrumentOrders.map(order => <div key={order.order_sys_id} className="flex items-center justify-between gap-2 text-[10px] text-muted"><span><span className={order.action === 'SELL' ? 'text-bear' : 'text-bull'}>{order.action === 'SELL' ? '卖出' : '买入'} {order.volume ?? '--'}</span> · {qmtOrderStatus(order.status)}</span><button type="button" disabled={cancelMutation.isPending} onClick={() => cancelMutation.mutate(order.order_sys_id!)} className="h-6 rounded border border-border px-2 hover:bg-elevated disabled:opacity-40">撤单</button></div>)}
          </div>
        </section> : null}
      </div>
    </Modal>
  )
}
