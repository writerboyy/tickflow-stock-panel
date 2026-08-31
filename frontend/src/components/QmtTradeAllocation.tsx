import { useState } from 'react'
import { Check, Loader2, Pencil } from 'lucide-react'
import { cn } from '@/lib/cn'
import { usePreferences } from '@/lib/useSharedQueries'
import { useUpdateQmtQuickAmountPresets } from '@/lib/useSharedMutations'

export type QmtTradeAllocationMode =
  | 'available'
  | 'sixth'
  | 'fifth'
  | 'quarter'
  | 'third'
  | 'half'
  | 'fixed'
  | 'volume'

export type QmtTradeAllocationAction = 'BUY' | 'SELL'

export const QMT_ALLOCATION_OPTIONS: ReadonlyArray<{ value: QmtTradeAllocationMode; label: string }> = [
  { value: 'available', label: '当前可用金额' },
  { value: 'sixth', label: '可用金额 1/6' },
  { value: 'fifth', label: '可用金额 1/5' },
  { value: 'quarter', label: '可用金额 1/4' },
  { value: 'third', label: '可用金额 1/3' },
  { value: 'half', label: '可用金额 1/2' },
  { value: 'fixed', label: '固定金额' },
]

export const QMT_QUICK_AMOUNT_PRESETS = [10_000, 20_000, 30_000, 40_000] as const

export function minimumQmtQuickAmount(presets?: readonly number[] | null): number {
  const validAmounts = presets?.filter(amount => Number.isFinite(amount) && amount >= 100) ?? []
  return Math.min(...(validAmounts.length ? validAmounts : QMT_QUICK_AMOUNT_PRESETS))
}

const MONEY = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export function qmtAllocationLabel(action: QmtTradeAllocationAction, mode: QmtTradeAllocationMode): string {
  if (mode === 'fixed') return '固定金额'
  if (mode === 'volume') return '固定数量'
  const option = QMT_ALLOCATION_OPTIONS.find(item => item.value === mode)
  if (!option) return mode
  return action === 'SELL' && mode === 'available' ? '当前可用持仓市值' : option.label
}

type PreviewState = 'idle' | 'loading' | 'ready' | 'error' | 'unavailable'

export function QmtTradeAllocationControls({
  action,
  mode,
  value,
  onModeChange,
  onValueChange,
  disabled = false,
  options = QMT_ALLOCATION_OPTIONS,
  disabledModes,
  basisLabel,
  basisAmount,
  accountType,
  cashAmount,
  financingBuyingPowerAmount,
  financingBuyingPowerLabel = '该股票最大融资可买',
  previewState = 'idle',
  previewMessage,
  showQuickPresets = true,
  showSummary = true,
  className,
}: {
  action: QmtTradeAllocationAction
  mode: QmtTradeAllocationMode
  value: number
  onModeChange: (mode: QmtTradeAllocationMode) => void
  onValueChange: (value: number) => void
  disabled?: boolean
  options?: ReadonlyArray<{ value: QmtTradeAllocationMode; label: string }>
  disabledModes?: Partial<Record<QmtTradeAllocationMode, boolean>>
  basisLabel?: string | null
  basisAmount?: number | null
  accountType?: string | null
  cashAmount?: number | null
  financingBuyingPowerAmount?: number | null
  financingBuyingPowerLabel?: string
  previewState?: PreviewState
  previewMessage?: string | null
  showQuickPresets?: boolean
  showSummary?: boolean
  className?: string
}) {
  const hasValueInput = mode === 'fixed' || mode === 'volume'
  const { data: preferences } = usePreferences()
  const saveQuickAmounts = useUpdateQmtQuickAmountPresets()
  const savedQuickAmounts = preferences?.qmt_quick_amount_presets?.length
    ? preferences.qmt_quick_amount_presets
    : QMT_QUICK_AMOUNT_PRESETS
  const [editingQuickAmounts, setEditingQuickAmounts] = useState(false)
  const [draftQuickAmounts, setDraftQuickAmounts] = useState<number[] | null>(null)
  // 编辑中用草稿, 否则用后端保存值 — 让修改后的档位在重开面板、重启后仍然保留。
  const quickAmounts = editingQuickAmounts && draftQuickAmounts
    ? draftQuickAmounts
    : savedQuickAmounts
  const inputLabel = mode === 'volume' ? '确认数量（股）' : '确认金额'
  const creditAccount = String(accountType || '').toUpperCase() === 'CREDIT'
  const displayedBasisLabel = basisLabel === '可用资金'
    ? creditAccount ? '信用账户当前可买额度' : '账户当前可用资金'
    : basisLabel === '可用持仓市值'
      ? creditAccount ? '信用账户当前可用持仓市值' : '账户当前可用持仓市值'
      : basisLabel || (action === 'BUY'
        ? creditAccount ? '信用账户当前可买额度' : '账户当前可用资金'
        : creditAccount ? '信用账户当前可用持仓市值' : '账户当前可用持仓市值')
  const basisText = basisAmount != null && Number.isFinite(basisAmount) ? `${MONEY.format(basisAmount)} 元` : '—'
  const cashText = cashAmount != null && Number.isFinite(cashAmount) ? `${MONEY.format(cashAmount)} 元` : '—'
  const financingBuyingPowerText = financingBuyingPowerAmount != null && Number.isFinite(financingBuyingPowerAmount) ? `${MONEY.format(financingBuyingPowerAmount)} 元` : '—'
  const availableDisabled = disabled || disabledModes?.available === true

  return <div className={cn('space-y-3', className)}>
    <label className="block text-[10px] text-muted">交易数量/金额方式
      <select
        value={mode}
        disabled={disabled}
        onChange={event => onModeChange(event.target.value as QmtTradeAllocationMode)}
        className="mt-1 h-8 w-full rounded border border-border bg-surface px-2 text-xs outline-none focus:border-accent disabled:opacity-50"
      >
        {options.map(option => <option key={option.value} value={option.value} disabled={disabledModes?.[option.value] === true}>{action === 'SELL' && option.value === 'available' ? '当前可用持仓市值' : option.label}</option>)}
      </select>
    </label>

    {showQuickPresets ? <div className="space-y-1.5">
      <div className="grid grid-cols-[80px_repeat(4,minmax(0,1fr))] items-center gap-1.5">
        <div className="flex min-w-0 items-center gap-1">
          <span className="text-[10px] text-muted">快捷金额</span>
          <button
            type="button"
            aria-label={editingQuickAmounts ? '完成快捷金额编辑' : '编辑快捷金额'}
            title={editingQuickAmounts ? '完成并保存快捷金额' : '编辑快捷金额'}
            disabled={disabled}
            onClick={() => {
              if (editingQuickAmounts) {
                const next = quickAmounts.map(amount => Number.isFinite(amount) && amount >= 100 ? amount : 100)
                setDraftQuickAmounts(null)
                if (next.join(',') !== savedQuickAmounts.join(',')) {
                  saveQuickAmounts.mutate(next)
                }
              } else {
                setDraftQuickAmounts([...quickAmounts])
              }
              setEditingQuickAmounts(current => !current)
            }}
            className="grid h-5 w-5 place-items-center rounded text-muted hover:bg-elevated hover:text-accent disabled:opacity-40"
          >{saveQuickAmounts.isPending
            ? <Loader2 className="h-3 w-3 animate-spin" />
            : editingQuickAmounts ? <Check className="h-3 w-3" /> : <Pencil className="h-3 w-3" />}</button>
        </div>
        {editingQuickAmounts ? quickAmounts.map((amount, index) => <input
          key={index}
          type="number"
          min={100}
          step={100}
          value={amount}
          aria-label={`快捷金额 ${index + 1}`}
          disabled={disabled}
          onChange={event => {
            const nextAmount = Number(event.target.value)
            setDraftQuickAmounts(current => (current ?? savedQuickAmounts).map((value, itemIndex) => itemIndex === index ? nextAmount : value))
          }}
          onBlur={() => setDraftQuickAmounts(current => (current ?? savedQuickAmounts).map((value, itemIndex) => itemIndex === index && (!Number.isFinite(value) || value < 100) ? 100 : value))}
          className="h-7 w-full min-w-0 rounded border border-accent/50 bg-accent/5 px-2 text-right font-mono text-[10px] text-foreground outline-none focus:border-accent focus:ring-1 focus:ring-accent/30 disabled:opacity-40"
        />) : quickAmounts.map((amount, index) => <button
          key={index}
          type="button"
          disabled={disabled}
          onClick={() => { onModeChange('fixed'); onValueChange(amount) }}
          className="h-7 w-full min-w-0 rounded border border-border px-2 font-mono text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
        >{amount.toLocaleString('zh-CN')}</button>)}
      </div>
      <div className="grid grid-cols-[80px_repeat(4,minmax(0,1fr))] items-center gap-1.5">
        <span className="min-w-0 text-[10px] text-muted">快捷比例</span>
        {(['sixth', 'fifth', 'quarter'] as const).map(ratio => <button
          key={ratio}
          type="button"
          disabled={disabled || disabledModes?.[ratio] === true}
          onClick={() => onModeChange(ratio)}
          className="h-7 w-full min-w-0 rounded border border-border px-2 font-mono text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
        >{ratio === 'sixth' ? '16.7%' : ratio === 'fifth' ? '20%' : '25%'}</button>)}
        <button
          type="button"
          disabled={availableDisabled}
          onClick={() => onModeChange('available')}
          className="h-7 w-full min-w-0 rounded border border-border px-2 font-mono text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
        >100%</button>
      </div>
    </div> : null}

    {hasValueInput ? <label className="block text-[10px] text-muted">{inputLabel}
      <input
        type="number"
        min={mode === 'volume' ? 100 : 100}
        step={100}
        value={value || ''}
        disabled={disabled}
        onChange={event => onValueChange(Number(event.target.value))}
        className="mt-1 h-8 w-full rounded border border-accent/50 bg-accent/5 px-2 text-right font-mono text-xs font-semibold text-foreground outline-none focus:border-accent focus:ring-1 focus:ring-accent/30 disabled:opacity-50"
      />
    </label> : null}

    {showSummary ? <div className="border-y border-accent/25 bg-accent/5 px-3 py-2 text-[10px] leading-4 text-secondary">
      <div className="grid grid-cols-2 gap-x-4 gap-y-2">
        {!(creditAccount && action === 'BUY') ? <div><span className="text-muted">{displayedBasisLabel}</span><div className="mt-0.5 font-mono text-foreground">{basisText}</div></div> : null}
        {creditAccount && action === 'BUY' ? <>
          <div><span className="text-muted">现金可用</span><div className="mt-0.5 font-mono text-foreground">{cashText}</div></div>
          <div><span className="text-muted">{financingBuyingPowerLabel}</span><div className="mt-0.5 font-mono text-foreground">{financingBuyingPowerText}</div></div>
        </> : null}
      </div>
      {previewState === 'loading' ? <div className="mt-2 border-t border-accent/20 pt-2 text-muted">正在读取账户可用金额并计算委托…</div> : previewMessage ? <div className={cn('mt-2 border-t border-accent/20 pt-2', previewState === 'error' ? 'text-danger' : 'text-muted')}>{previewMessage}</div> : null}
    </div> : null}
  </div>
}
