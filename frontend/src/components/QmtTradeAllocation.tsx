import { cn } from '@/lib/cn'

export type QmtTradeAllocationMode =
  | 'available'
  | 'sixth'
  | 'fifth'
  | 'quarter'
  | 'third'
  | 'half'
  | 'fixed'
  | 'lot'
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

export const QMT_QUICK_AMOUNT_PRESETS = [10_000, 20_000, 30_000] as const

const MONEY = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export function qmtAllocationLabel(action: QmtTradeAllocationAction, mode: QmtTradeAllocationMode): string {
  if (mode === 'fixed') return '固定金额'
  if (mode === 'lot') return '一手（100 股）'
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
  financingAvailableAmount,
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
  financingAvailableAmount?: number | null
  previewState?: PreviewState
  previewMessage?: string | null
  showQuickPresets?: boolean
  showSummary?: boolean
  className?: string
}) {
  const hasValueInput = mode === 'fixed' || mode === 'volume'
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
  const financingText = financingAvailableAmount != null && Number.isFinite(financingAvailableAmount) ? `${MONEY.format(financingAvailableAmount)} 元` : '—'
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

    {showQuickPresets ? <div className="flex flex-wrap items-center gap-1.5">
      <span className="text-[10px] text-muted">快捷金额</span>
      {QMT_QUICK_AMOUNT_PRESETS.map(amount => <button
        key={amount}
        type="button"
        disabled={disabled}
        onClick={() => { onModeChange('fixed'); onValueChange(amount) }}
        className="h-7 rounded border border-border px-2 font-mono text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
      >{amount.toLocaleString('zh-CN')}</button>)}
      {(['sixth', 'fifth', 'quarter'] as const).map(ratio => <button
        key={ratio}
        type="button"
        disabled={disabled || disabledModes?.[ratio] === true}
        onClick={() => onModeChange(ratio)}
        className="h-7 rounded border border-border px-2 font-mono text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
      >{ratio === 'sixth' ? '1/6' : ratio === 'fifth' ? '1/5' : '1/4'}</button>)}
      <button
        type="button"
        disabled={availableDisabled}
        onClick={() => onModeChange('available')}
        className="h-7 rounded border border-border px-2 text-[10px] text-secondary hover:border-accent/50 hover:text-accent disabled:opacity-40"
      >当前可用</button>
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
          <div><span className="text-muted">融资可用</span><div className="mt-0.5 font-mono text-foreground">{financingText}</div></div>
        </> : null}
      </div>
      {previewState === 'loading' ? <div className="mt-2 border-t border-accent/20 pt-2 text-muted">正在读取账户可用金额并计算委托…</div> : previewMessage ? <div className={cn('mt-2 border-t border-accent/20 pt-2', previewState === 'error' ? 'text-warning' : 'text-muted')}>{previewMessage}</div> : null}
    </div> : null}
  </div>
}
