import { useEffect, useMemo, useState } from 'react'
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

export type QmtTradePreset = {
  action?: 'BUY' | 'SELL'
  price?: number | null
  volume?: number | null
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

function qmtOrderStatus(value?: string) {
  return value ? QMT_ORDER_STATUS[value] ?? `状态 ${value}` : '状态未知'
}

function defaultPrice(value: number | null | undefined): string {
  return value != null && Number.isFinite(value) ? String(value) : ''
}

export function QmtTradePanel({
  instrument,
  preset,
  onClose,
}: {
  instrument: QmtTradeInstrument
  preset?: QmtTradePreset | null
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
  const [tradeVolume, setTradeVolume] = useState(preset?.volume ?? 100)

  useEffect(() => {
    setTradeAction(preset?.action ?? 'SELL')
    setTradePrice(defaultPrice(preset?.price ?? instrument.price))
    setTradePriceType('LIMIT')
    setTradeVolume(preset?.volume ?? 100)
  }, [instrument.symbol, instrument.price, preset?.action, preset?.price, preset?.volume])

  const limitPrice = Number(tradePrice)
  const validVolume = Number.isInteger(tradeVolume) && tradeVolume >= 100 && tradeVolume % 100 === 0
  const validLimitPrice = tradePriceType === 'LATEST' || (Number.isFinite(limitPrice) && limitPrice > 0)
  const tradeReady = qmt.data?.trade_enabled === true && qmt.data.state === 'ready'
  const canSubmit = tradeReady && validVolume && validLimitPrice
  const instrumentOrders = useMemo(
    () => (orders.data?.orders ?? []).filter(order => order.symbol === instrument.symbol && order.order_sys_id).slice(0, 5),
    [instrument.symbol, orders.data?.orders],
  )

  const tradeMutation = useMutation({
    mutationFn: () => api.qmtSubmitOrder({
      action: tradeAction,
      symbol: instrument.symbol,
      volume: tradeVolume,
      price: tradePriceType === 'LIMIT' ? limitPrice : null,
      price_type: tradePriceType,
      idempotency_key: `manual-${instrument.symbol}-${tradeAction}-${Date.now()}`,
    }),
    onSuccess: result => {
      toast(`委托结果：${qmtOrderStatus(result.order.status)}`, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmtOrders })
      queryClient.invalidateQueries({ queryKey: QK.positionRiskQmt })
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
    if (!validVolume) {
      toast('数量必须是至少 100 股的整手')
      return
    }
    if (!validLimitPrice) {
      toast('请输入有效限价')
      return
    }
    if (!tradeReady) {
      toast(qmt.data?.reason || 'QMT 未就绪，无法发送委托')
      return
    }
    const actionLabel = tradeAction === 'BUY' ? '买入' : '卖出'
    const priceLabel = tradePriceType === 'LATEST' ? '最新价' : `限价 ${limitPrice.toFixed(3)}`
    if (!window.confirm(`确认${actionLabel} ${instrument.name || instrument.symbol} ${tradeVolume} 股（${priceLabel}）？该操作将发送至真实 QMT 账户。`)) return
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
            <label>方向<select value={tradeAction} onChange={event => setTradeAction(event.target.value as 'BUY' | 'SELL')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="BUY">买入</option><option value="SELL">卖出</option></select></label>
            <label>数量<input type="number" min="100" step="100" value={tradeVolume} onChange={event => setTradeVolume(Number(event.target.value))} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px]" /></label>
            <label>价格方式<select value={tradePriceType} onChange={event => setTradePriceType(event.target.value as 'LIMIT' | 'LATEST')} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 text-[11px]"><option value="LIMIT">限价</option><option value="LATEST">最新价</option></select></label>
            <label className={tradePriceType === 'LATEST' ? 'opacity-50' : ''}>限价<input type="number" min="0.001" step="0.001" value={tradePrice} disabled={tradePriceType === 'LATEST'} onChange={event => setTradePrice(event.target.value)} className="mt-1 h-7 w-full rounded border border-border bg-surface px-2 font-mono text-[11px] disabled:cursor-not-allowed" /></label>
          </div>
          <button type="button" disabled={!canSubmit || tradeMutation.isPending} onClick={submit} className={cn('mt-2 h-8 w-full rounded-btn text-xs text-white disabled:cursor-not-allowed disabled:opacity-40', tradeAction === 'BUY' ? 'bg-bull' : 'bg-bear')}>
            {tradeMutation.isPending ? '提交中...' : `发送${tradeAction === 'BUY' ? '买入' : '卖出'}委托`}
          </button>
          {!validVolume ? <p className="mt-2 text-[10px] text-warning">数量需为至少 100 股的整手。</p> : null}
          {!validLimitPrice ? <p className="mt-2 text-[10px] text-warning">限价不能为空且必须大于 0。</p> : null}
          <p className="mt-2 text-[10px] leading-4 text-muted">提交前会再次确认。该委托进入真实 QMT 账户，成交结果以券商回报为准。</p>
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
