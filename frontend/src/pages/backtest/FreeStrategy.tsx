import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  AlertTriangle,
  ArrowLeft,
  BookOpen,
  CheckCircle2,
  CirclePlay,
  CircleAlert,
  Code2,
  History,
  LoaderCircle,
  Pencil,
  Plus,
  Save,
  Square,
  Trash2,
  WalletCards,
} from 'lucide-react'
import { api, type FreeBacktestConfig, type FreeBacktestResult } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { DatePicker } from '@/components/DatePicker'
import { Modal } from '@/components/Modal'
import { toast } from '@/components/Toast'
import { priceColorClass } from '@/lib/format'
import { QK } from '@/lib/queryKeys'
import { FreeStrategyResult } from './FreeStrategyResult'

const INPUT = 'w-full rounded-input border border-border bg-surface px-2.5 py-1.5 text-xs text-foreground focus:border-accent focus:outline-none'
const DEFAULT_SOURCE = `ETF_POOL = ["510300.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
    context.log("策略初始化")

def on_bar(context, bars):
    for symbol, bar in bars.items():
        context.order_target_percent(symbol, 0.95)
`
const EMPTY_SOURCE = `ETF_POOL = ["510300.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)

def on_bar(context, bars):
    pass`

type WorkspaceView = 'strategy' | 'backtests'
type EditorSnapshot = { name: string; source: string; config: FreeBacktestConfig }
type EditorAction = { type: 'select'; id: string } | { type: 'new' } | { type: 'template'; id: string }
type RenameTarget = { type: 'strategy' | 'backtest'; id: string; name: string }
type DeleteTarget = { type: 'strategy' | 'backtest'; id: string; name: string }

function executionModeLabel(value: unknown) {
  return value === 'scheduled' ? '定时执行' : value === 'full_bar' ? '完整回放' : ''
}

const DEFAULT_CONFIG: FreeBacktestConfig = {
  strategy_id: '', timeframe: '1d', asset_type: 'etf',
  start: `${new Date().getFullYear() - 3}-01-01`, end: new Date().toISOString().slice(0, 10),
  initial_capital: 1_000_000, fees_pct: 0.0002, commission_pct: null, min_commission: 0, stamp_tax_pct: 0.001, transfer_fee_pct: 0,
  slippage_bps: 5, price_tick: null, lot_size: 100, max_exposure_pct: 1, settlement: 't1', fill_policy: 'next_open',
  benchmark_symbol: '510300.SH',
}

function withoutLegacySymbols(value: Record<string, unknown>): Partial<FreeBacktestConfig> {
  const normalized = { ...value }
  delete normalized.symbols
  return normalized as Partial<FreeBacktestConfig>
}

function IconButton({ title, onClick, disabled = false, danger = false, children }: {
  title: string
  onClick: () => void
  disabled?: boolean
  danger?: boolean
  children: ReactNode
}) {
  return <button
    type="button"
    title={title}
    aria-label={title}
    disabled={disabled}
    onClick={onClick}
    className={`inline-flex h-7 w-7 items-center justify-center rounded border border-transparent transition-colors disabled:cursor-not-allowed disabled:opacity-35 ${danger ? 'text-muted hover:border-danger/30 hover:bg-danger/10 hover:text-danger' : 'text-muted hover:border-accent/30 hover:bg-accent/10 hover:text-accent'}`}
  >{children}</button>
}

function RenameDialog({ target, value, pending, error, onValueChange, onClose, onConfirm }: {
  target: RenameTarget
  value: string
  pending: boolean
  error: string
  onValueChange: (value: string) => void
  onClose: () => void
  onConfirm: () => void
}) {
  const titleId = 'quant-strategy-rename-title'
  return <Modal labelledBy={titleId} onClose={onClose} closeOnBackdrop={!pending} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl">
    <form onSubmit={event => { event.preventDefault(); onConfirm() }}>
      <div className="border-b border-border px-4 py-3">
        <h2 id={titleId} className="text-sm font-semibold">重命名{target.type === 'strategy' ? '策略' : '回测记录'}</h2>
      </div>
      <div className="p-4">
        <label className="text-xs text-muted">名称
          <input autoFocus className={`${INPUT} mt-1.5`} maxLength={120} value={value} onChange={event => onValueChange(event.target.value)} />
        </label>
        {error ? <div className="mt-3 rounded border border-danger/30 bg-danger/10 px-3 py-2 text-[11px] text-danger">{error}</div> : null}
      </div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
        <button type="button" disabled={pending} onClick={onClose} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted hover:text-foreground disabled:opacity-50">取消</button>
        <button type="submit" disabled={pending || !value.trim()} className="rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50">{pending ? '保存中…' : '保存名称'}</button>
      </div>
    </form>
  </Modal>
}

function ConfirmDialog({ title, description, confirmLabel, pending, error, danger = true, onClose, onConfirm }: {
  title: string
  description: ReactNode
  confirmLabel: string
  pending: boolean
  error?: string
  danger?: boolean
  onClose: () => void
  onConfirm: () => void
}) {
  const titleId = 'quant-strategy-confirm-title'
  return <Modal labelledBy={titleId} onClose={onClose} closeOnBackdrop={!pending} panelClassName="w-[92vw] max-w-sm rounded-card border border-border bg-surface shadow-xl">
    <div className="p-5">
      <div className="flex items-start gap-3">
        <div className={`mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full ${danger ? 'bg-danger/10 text-danger' : 'bg-warning/10 text-warning'}`}><AlertTriangle className="h-4 w-4" /></div>
        <div className="min-w-0">
          <h2 id={titleId} className="text-sm font-semibold">{title}</h2>
          <div className="mt-1.5 text-xs leading-5 text-muted">{description}</div>
        </div>
      </div>
      {error ? <div className="mt-3 rounded border border-danger/30 bg-danger/10 px-3 py-2 text-[11px] text-danger">{error}</div> : null}
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" disabled={pending} onClick={onClose} className="rounded-btn border border-border px-3 py-1.5 text-xs text-muted hover:text-foreground disabled:opacity-50">取消</button>
        <button type="button" disabled={pending} onClick={onConfirm} className={`rounded-btn px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50 ${danger ? 'bg-danger hover:bg-danger/90' : 'bg-warning hover:bg-warning/90'}`}>{pending ? '处理中…' : confirmLabel}</button>
      </div>
    </div>
  </Modal>
}

export function FreeStrategy() {
  const navigate = useNavigate()
  const [workspaceView, setWorkspaceView] = useState<WorkspaceView>('strategy')
  const [selectedId, setSelectedId] = useState('')
  const strategies = useQuery({ queryKey: ['free-strategies'], queryFn: api.freeStrategies })
  const templates = useQuery({ queryKey: ['free-strategy-templates'], queryFn: api.freeTemplates })
  const savedRuns = useQuery({ queryKey: ['free-backtest-runs'], queryFn: api.freeBacktestRuns })
  const detail = useQuery({ queryKey: ['free-strategy', selectedId], queryFn: () => api.freeStrategy(selectedId), enabled: Boolean(selectedId) })
  const [name, setName] = useState('我的量化策略')
  const [source, setSource] = useState(DEFAULT_SOURCE)
  const [config, setConfig] = useState<FreeBacktestConfig>(DEFAULT_CONFIG)
  const [baseline, setBaseline] = useState<EditorSnapshot | null>(null)
  const [result, setResult] = useState<FreeBacktestResult | null>(null)
  const [progress, setProgress] = useState('')
  const [progressPct, setProgressPct] = useState<number | null>(null)
  const [runningMode, setRunningMode] = useState('')
  const [error, setError] = useState('')
  const [running, setRunning] = useState(false)
  const [saving, setSaving] = useState(false)
  const [selectedRunId, setSelectedRunId] = useState('')
  const [mobileRunDetail, setMobileRunDetail] = useState(false)
  const [pendingEditorAction, setPendingEditorAction] = useState<EditorAction | null>(null)
  const [renameTarget, setRenameTarget] = useState<RenameTarget | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const [renamePending, setRenamePending] = useState(false)
  const [renameError, setRenameError] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null)
  const [deletePending, setDeletePending] = useState(false)
  const [deleteError, setDeleteError] = useState('')
  const sourceRef = useRef<EventSource | null>(null)
  const jobRef = useRef<string | null>(null)
  const didAutoSelectStrategy = useRef(false)

  const list = strategies.data?.strategies ?? []
  const templateList = templates.data?.templates ?? []
  const runs = useMemo(() => savedRuns.data?.runs ?? [], [savedRuns.data?.runs])
  const runCountByStrategy = useMemo(() => {
    const counts = new Map<string, number>()
    runs.forEach(runItem => {
      const strategyId = String(runItem.metadata?.strategy_id ?? '')
      if (strategyId) counts.set(strategyId, (counts.get(strategyId) ?? 0) + 1)
    })
    return counts
  }, [runs])
  const strategyRuns = useMemo(
    () => runs.filter(runItem => String(runItem.metadata?.strategy_id ?? '') === selectedId),
    [runs, selectedId],
  )
  const selected = list.find(item => item.id === selectedId)
  const selectedRun = strategyRuns.find(item => item.job_id === selectedRunId)
  const draft = useMemo<EditorSnapshot>(() => ({ name, source, config }), [name, source, config])
  const dirty = baseline === null || JSON.stringify(draft) !== JSON.stringify(baseline)
  const detailLoading = Boolean(selectedId) && (detail.isFetching || detail.data?.id !== selectedId)
  const dataHealth = useQuery({
    queryKey: QK.freeDataHealth(config.strategy_id, config.start, config.end, config.timeframe),
    queryFn: () => api.freeBacktestDataHealth({
      strategy_id: config.strategy_id,
      asset_type: config.asset_type,
      timeframe: config.timeframe,
      start: config.start,
      end: config.end,
    }),
    enabled: Boolean(config.strategy_id) && config.asset_type === 'etf' && !dirty,
    staleTime: 60_000,
    retry: false,
  })

  useEffect(() => () => sourceRef.current?.close(), [])
  useEffect(() => {
    const first = strategies.data?.strategies[0]
    if (!didAutoSelectStrategy.current && first) {
      didAutoSelectStrategy.current = true
      setSelectedId(first.id)
    }
  }, [strategies.data?.strategies])
  useEffect(() => {
    const saved = detail.data
    if (!saved || saved.id !== selectedId) return
    const nextConfig = { ...DEFAULT_CONFIG, ...withoutLegacySymbols(saved.config ?? {}), strategy_id: saved.id }
    const next = { name: saved.name, source: saved.source ?? '', config: nextConfig }
    setName(next.name)
    setSource(next.source)
    setConfig(next.config)
    setBaseline(next)
  }, [detail.data, selectedId])
  useEffect(() => {
    setSelectedRunId('')
    setResult(null)
    setMobileRunDetail(false)
  }, [selectedId])

  const resetNewStrategy = () => {
    setSelectedId('')
    setName('我的量化策略')
    setSource(EMPTY_SOURCE)
    setConfig({ ...DEFAULT_CONFIG, strategy_id: '' })
    setBaseline(null)
  }

  const applyEditorAction = (action: EditorAction) => {
    if (action.type === 'select') {
      setSelectedId(action.id)
      return
    }
    if (action.type === 'new') {
      resetNewStrategy()
      return
    }
    const template = templateList.find(item => item.id === action.id)
    if (!template) return
    const nextConfig = { ...DEFAULT_CONFIG, ...withoutLegacySymbols(template.config ?? {}), strategy_id: '' }
    setSelectedId('')
    setName(template.name)
    setSource(template.source)
    setConfig(nextConfig)
    setBaseline(null)
  }

  const requestEditorAction = (action: EditorAction) => {
    if (action.type === 'select' && action.id === selectedId) return
    if (dirty) {
      setPendingEditorAction(action)
      return
    }
    applyEditorAction(action)
  }

  const save = async () => {
    setSaving(true)
    setError('')
    try {
      const sourceOrConfigChanged = baseline === null
        || source !== baseline.source
        || JSON.stringify(config) !== JSON.stringify(baseline.config)
      const saved = selectedId
        ? sourceOrConfigChanged
          ? await api.updateFreeStrategy(selectedId, { name, source, config })
          : await api.renameFreeStrategy(selectedId, name)
        : await api.saveFreeStrategy({ name, source, config })
      const nextConfig = { ...config, strategy_id: saved.id }
      setSelectedId(saved.id)
      setConfig(nextConfig)
      setBaseline({ name: saved.name, source: saved.source ?? source, config: nextConfig })
      await strategies.refetch()
      toast('策略已保存', 'success')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  const loadSavedRun = async (jobId: string) => {
    setWorkspaceView('backtests')
    setSelectedRunId(jobId)
    setMobileRunDetail(true)
    if (!jobId) return
    setError('')
    setProgress('正在载入已保存回测...')
    try {
      setResult(await api.freeBacktestResult(jobId))
      setProgress(`已载入回测 ${jobId}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setProgress('')
    }
  }

  const run = async () => {
    if (dirty) {
      setError('请先保存当前修改，再运行历史回测')
      return
    }
    if (!config.strategy_id) {
      setError('请先保存策略，生成源码快照后再运行')
      return
    }
    if (config.asset_type === 'etf') {
      await dataHealth.refetch()
    }
    sourceRef.current?.close()
    jobRef.current = null
    setResult(null); setError(''); setProgress('正在创建回测任务...'); setProgressPct(0); setRunningMode(''); setRunning(true); setSelectedRunId(''); setWorkspaceView('backtests'); setMobileRunDetail(true)
    try {
      const job = await api.startFreeBacktest(config)
      jobRef.current = job.job_id
      setSelectedRunId(job.job_id)
      const events = new EventSource(`/api/free-strategies/backtest/${job.job_id}/stream`)
      sourceRef.current = events
      let finished = false
      events.onmessage = event => {
        const payload = JSON.parse(event.data)
        if (payload.type === 'progress') { setProgress(payload.message); setProgressPct(typeof payload.progress === 'number' ? payload.progress : null); if (payload.execution_mode) setRunningMode(executionModeLabel(payload.execution_mode)) }
        if (payload.type === 'result') { finished = true; jobRef.current = null; setResult(payload.result); setSelectedRunId(job.job_id); setProgress('回测完成'); setProgressPct(1); setRunning(false); void savedRuns.refetch(); events.close() }
        if (payload.type === 'error') { finished = true; jobRef.current = null; setError(payload.error); setProgress('回测失败'); setProgressPct(null); setRunning(false); events.close() }
      }
      events.onerror = () => { if (!finished) { jobRef.current = null; setError('回测连接中断，请查看任务日志'); setProgress('回测连接中断'); setProgressPct(null) } setRunning(false); events.close() }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err)); setProgress('回测启动失败'); setProgressPct(null); setRunning(false)
    }
  }

  const cancel = async () => {
    if (jobRef.current) await api.cancelFreeBacktest(jobRef.current)
    jobRef.current = null
    sourceRef.current?.close(); setRunning(false); setProgress('回测已取消'); setProgressPct(null)
  }

  const createPaper = () => {
    if (!config.strategy_id) { setError('请先保存策略'); return }
    const params = new URLSearchParams({ create: '1', strategy_id: config.strategy_id })
    if (selectedRunId) params.set('backtest_job_id', selectedRunId)
    navigate(`/paper-trading?${params.toString()}`)
  }

  const openDataRepair = () => {
    const query = new URLSearchParams({
      repair: 'etf',
      strategy_id: config.strategy_id,
      start: config.start || '',
      end: config.end || '',
      timeframe: config.timeframe,
    })
    navigate(`/data?${query.toString()}`)
  }

  const openRename = (target: RenameTarget) => {
    setRenameTarget(target)
    setRenameValue(target.name)
    setRenameError('')
  }

  const renameSelected = async () => {
    if (!renameTarget) return
    setRenamePending(true)
    setRenameError('')
    try {
      const nextName = renameValue.trim()
      if (renameTarget.type === 'strategy') {
        await api.renameFreeStrategy(renameTarget.id, nextName)
        if (renameTarget.id === selectedId) {
          setName(nextName)
          setBaseline(previous => previous ? { ...previous, name: nextName } : previous)
        }
        await strategies.refetch()
        toast('策略已重命名', 'success')
      } else {
        await api.renameFreeBacktest(renameTarget.id, nextName)
        await savedRuns.refetch()
        toast('回测记录已重命名', 'success')
      }
      setRenameTarget(null)
    } catch (err) {
      setRenameError(err instanceof Error ? err.message : String(err))
    } finally {
      setRenamePending(false)
    }
  }

  const openDelete = (target: DeleteTarget) => {
    setDeleteTarget(target)
    setDeleteError('')
  }

  const deleteSelected = async () => {
    if (!deleteTarget) return
    setDeletePending(true)
    setDeleteError('')
    try {
      if (deleteTarget.type === 'strategy') {
        await api.deleteFreeStrategy(deleteTarget.id)
        const refreshed = await strategies.refetch()
        const next = refreshed.data?.strategies[0]
        if (next) setSelectedId(next.id)
        else resetNewStrategy()
        toast('策略已删除', 'success')
      } else if (deleteTarget.type === 'backtest') {
        await api.deleteFreeBacktest(deleteTarget.id)
        setSelectedRunId('')
        setResult(null)
        setMobileRunDetail(false)
        await savedRuns.refetch()
        toast('回测记录已删除', 'success')
      }
      setDeleteTarget(null)
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : String(err))
    } finally {
      setDeletePending(false)
    }
  }

  if (strategies.isLoading) return <div className="grid h-full place-items-center text-xs text-muted">加载量化策略...</div>

  return <div className="flex h-full min-h-0 flex-col gap-3 overflow-y-auto pr-1">
    <div className="flex shrink-0 flex-wrap items-center justify-between gap-3">
      <div className="flex items-center gap-2">
        <Code2 className="h-4 w-4 text-accent" />
        <div><div className="text-sm font-semibold">量化策略</div><div className="text-[11px] text-muted">Python 策略 · 自动识别完整回放或定时执行{dirty && workspaceView === 'strategy' ? ' · 未保存' : ''}</div></div>
      </div>
      {workspaceView === 'strategy' ? <div className="flex flex-wrap items-center justify-end gap-1.5">
        <select className={INPUT} value="" onChange={event => requestEditorAction({ type: 'template', id: event.target.value })} aria-label="加载模板">
          <option value="">加载模板</option>{templateList.map(item => <option value={item.id} key={item.id}>{item.name}</option>)}
        </select>
        <button disabled={saving || !dirty || detailLoading} className="inline-flex items-center gap-1 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50" onClick={save}><Save className="h-3.5 w-3.5" />{saving ? '保存中…' : '保存'}</button>
      </div> : <div className="text-[11px] text-muted">{selected ? <><span className="text-foreground">{selected.name}</span><span className="mx-1.5">·</span><span className="tabular-nums">{strategyRuns.length} 条记录</span></> : '未选择策略'}</div>}
    </div>

    <div className="flex shrink-0 border-b border-border" role="tablist" aria-label="量化策略工作区">
      <button type="button" role="tab" aria-selected={workspaceView === 'strategy'} onClick={() => setWorkspaceView('strategy')} className={`inline-flex h-9 items-center gap-1.5 border-b-2 px-3 text-xs transition-colors ${workspaceView === 'strategy' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><Code2 className="h-3.5 w-3.5" />策略开发</button>
      <button type="button" role="tab" aria-selected={workspaceView === 'backtests'} onClick={() => setWorkspaceView('backtests')} className={`inline-flex h-9 items-center gap-1.5 border-b-2 px-3 text-xs transition-colors ${workspaceView === 'backtests' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><History className="h-3.5 w-3.5" />回测记录<span className="tabular-nums text-[10px]">{strategyRuns.length}</span></button>
    </div>

    {workspaceView === 'strategy' ? <div className="grid min-h-[560px] shrink-0 grid-cols-[220px_minmax(0,1fr)_280px] gap-3 max-xl:min-h-[820px] max-xl:grid-cols-[180px_minmax(0,1fr)] max-md:min-h-0 max-md:grid-cols-1">
      <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-2.5 max-md:max-h-64">
        <div className="mb-2 flex h-7 items-center justify-between"><span className="text-xs font-medium">策略列表</span><div className="flex items-center gap-0.5">
          <IconButton title="新建策略" onClick={() => requestEditorAction({ type: 'new' })}><Plus className="h-3.5 w-3.5" /></IconButton>
          {selected ? <><IconButton title="重命名策略" onClick={() => openRename({ type: 'strategy', id: selected.id, name: selected.name })}><Pencil className="h-3.5 w-3.5" /></IconButton><IconButton title="删除策略" danger onClick={() => openDelete({ type: 'strategy', id: selected.id, name: selected.name })}><Trash2 className="h-3.5 w-3.5" /></IconButton></> : null}
        </div></div>
        <div className="space-y-1">{list.map(item => <button key={item.id} type="button" onClick={() => requestEditorAction({ type: 'select', id: item.id })} className={`w-full rounded px-2 py-2 text-left text-xs ${selectedId === item.id ? 'bg-accent/15 text-accent' : 'hover:bg-elevated'}`}><div className="truncate">{item.name}</div></button>)}</div>
        {!list.length ? <EmptyState icon={BookOpen} title="还没有策略" hint="从模板开始，保存后即可运行。" /> : null}
      </section>

      <section className="flex min-h-0 flex-col overflow-hidden rounded-md border border-border bg-surface max-md:h-[460px]">
        <div className="flex h-10 items-center gap-2 border-b border-border px-3">
          <input aria-label="策略名称" className="min-w-0 flex-1 bg-transparent text-sm font-medium outline-none" value={name} onChange={event => setName(event.target.value)} />
          <span className="text-[10px] text-muted">Python</span>
        </div>
        {detailLoading ? <div className="grid min-h-0 flex-1 place-items-center bg-[#101114] text-[11px] text-muted">加载策略源码...</div> : <textarea className="min-h-0 flex-1 resize-none bg-[#101114] p-3 font-mono text-xs leading-5 text-zinc-200 outline-none" spellCheck={false} value={source} onChange={event => setSource(event.target.value)} />}
      </section>

      <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-3 max-xl:col-span-2 max-md:col-span-1">
        <div className="mb-2 text-xs font-medium">运行设置</div>
        <div className="grid grid-cols-2 gap-2 text-[11px]">
          <label>资产<select className={INPUT} value={config.asset_type} onChange={event => setConfig({ ...config, asset_type: event.target.value as FreeBacktestConfig['asset_type'] })}><option value="etf">ETF</option><option value="stock">股票</option></select></label>
          <label>周期<select className={INPUT} value={config.timeframe} onChange={event => setConfig({ ...config, timeframe: event.target.value as FreeBacktestConfig['timeframe'] })}><option value="1d">1d</option><option value="30m">30m</option><option value="5m">5m</option><option value="1m">1m</option></select></label>
          <label className="col-span-2">基准<input className={INPUT} value={config.benchmark_symbol} onChange={event => setConfig({ ...config, benchmark_symbol: event.target.value.trim() })} /></label>
          <div><label className="mb-1 block">开始</label><DatePicker value={config.start ?? ''} onChange={value => setConfig({ ...config, start: value })} max={config.end || undefined} className="w-full" buttonClassName="w-full justify-start" align="left" /></div>
          <div><label className="mb-1 block">结束</label><DatePicker value={config.end ?? ''} onChange={value => setConfig({ ...config, end: value })} min={config.start || undefined} className="w-full" buttonClassName="w-full justify-start" /></div>
          <label>初始资金<input type="number" className={INPUT} value={config.initial_capital} onChange={event => setConfig({ ...config, initial_capital: Number(event.target.value) })} /></label><label>最小单位<input type="number" className={INPUT} value={config.lot_size} onChange={event => setConfig({ ...config, lot_size: Number(event.target.value) })} /></label>
          <label>手续费<input type="number" step="0.0001" className={INPUT} value={config.fees_pct} onChange={event => setConfig({ ...config, fees_pct: Number(event.target.value) })} /></label><label>滑点(bps)<input type="number" className={INPUT} value={config.slippage_bps} onChange={event => setConfig({ ...config, slippage_bps: Number(event.target.value) })} /></label>
          <label>结算<select className={INPUT} value={config.settlement} onChange={event => setConfig({ ...config, settlement: event.target.value as FreeBacktestConfig['settlement'] })}><option value="t1">T+1（默认）</option><option value="t0">T+0</option></select></label><label>成交<select className={INPUT} value={config.fill_policy} onChange={event => setConfig({ ...config, fill_policy: event.target.value as FreeBacktestConfig['fill_policy'] })}><option value="next_open">下一根开盘</option><option value="close">当前收盘</option></select></label>
        </div>
        {config.asset_type === 'etf' && config.strategy_id && !dirty ? <div className="mt-3 border-t border-border pt-3 text-[11px]">
          {dataHealth.isFetching ? <div className="inline-flex items-center gap-1.5 text-muted"><LoaderCircle className="h-3.5 w-3.5 animate-spin" />正在检查回测数据…</div> : dataHealth.isError ? <div className="flex items-center justify-between gap-2 text-muted"><span className="inline-flex items-center gap-1.5"><AlertTriangle className="h-3.5 w-3.5" />数据预检暂不可用，不阻止回测</span><button type="button" onClick={openDataRepair} className="text-accent hover:underline">前往检查</button></div> : dataHealth.data?.status === 'issues' ? <div className="flex items-center justify-between gap-2 text-warning"><span className="inline-flex items-center gap-1.5"><CircleAlert className="h-3.5 w-3.5" />发现 {dataHealth.data.issues.length} 个数据问题，可能影响结果</span><button type="button" onClick={openDataRepair} className="shrink-0 text-accent hover:underline">查看并修复</button></div> : dataHealth.data?.status === 'healthy' ? <div className="inline-flex items-center gap-1.5 text-success"><CheckCircle2 className="h-3.5 w-3.5" />回测数据完整 · 已检查 {dataHealth.data.symbol_count} 只 ETF</div> : null}
        </div> : null}
        {error ? <div className="mt-3 flex gap-2 rounded border border-danger/30 bg-danger/10 p-2 text-[11px] text-danger"><AlertTriangle className="h-3.5 w-3.5 shrink-0" />{error}</div> : null}
        <div className="mt-3 flex gap-2"><button disabled={running || dirty || saving || detailLoading} title={dirty ? '请先保存当前修改' : undefined} onClick={run} className="inline-flex flex-1 items-center justify-center gap-1.5 rounded-btn bg-accent px-3 py-2 text-xs font-medium text-white disabled:opacity-50"><CirclePlay className="h-3.5 w-3.5" />历史回测</button><button disabled={!running} onClick={cancel} className="inline-flex items-center justify-center gap-1.5 rounded-btn border border-border px-3 py-2 text-xs disabled:opacity-50"><Square className="h-3.5 w-3.5" />停止</button></div>
        {progress ? <div className="mt-2 text-[11px] text-muted">{progress}</div> : null}
        <div className="my-4 border-t border-border" />
        <div className="flex items-start gap-2.5">
          <WalletCards className="mt-0.5 h-4 w-4 shrink-0 text-accent" />
          <div className="min-w-0 flex-1"><div className="text-xs font-medium">模拟盘</div><div className="mt-1 text-[11px] leading-4 text-muted">账户、持仓、委托和运行日志已集中到独立工作台。</div></div>
          <button type="button" onClick={createPaper} disabled={!config.strategy_id || dirty} className="shrink-0 rounded-btn border border-border px-2.5 py-1.5 text-[11px] hover:border-accent hover:text-accent disabled:opacity-40">创建模拟账户</button>
        </div>
      </section>
    </div> : <div className="grid min-w-0 shrink-0 grid-cols-[200px_290px_minmax(0,1fr)] items-start gap-3 max-xl:grid-cols-[260px_minmax(0,1fr)] max-lg:grid-cols-1">
      <section className={`max-h-[calc(100vh-180px)] min-h-[420px] overflow-y-auto rounded-md border border-border bg-surface p-2 max-xl:col-span-2 max-xl:min-h-0 max-xl:max-h-36 max-lg:col-span-1 max-md:max-h-44 ${mobileRunDetail ? 'max-md:hidden' : ''}`}>
        <div className="flex h-7 items-center px-2 text-xs font-medium">策略</div>
        <div className="space-y-0.5 max-xl:grid max-xl:grid-cols-2 max-xl:gap-1 max-xl:space-y-0 max-md:grid-cols-1">
          {list.map(item => {
            const count = runCountByStrategy.get(item.id) ?? 0
            return <button key={item.id} type="button" disabled={running} onClick={() => requestEditorAction({ type: 'select', id: item.id })} className={`w-full border-l-2 px-2.5 py-2 text-left transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${selectedId === item.id ? 'border-l-accent bg-accent/10' : 'border-l-transparent hover:bg-elevated'}`}>
              <div className="flex items-center justify-between gap-2"><span className="truncate text-xs font-medium">{item.name}</span><span className={`shrink-0 tabular-nums text-[10px] ${selectedId === item.id ? 'text-accent' : 'text-muted'}`}>{count}</span></div>
              <div className="mt-1 text-[10px] text-muted">r{item.revision} · {count ? `${count} 次回测` : '暂无回测'}</div>
            </button>
          })}
        </div>
        {!list.length ? <EmptyState icon={BookOpen} title="还没有策略" /> : null}
      </section>

      <section className={`max-h-[calc(100vh-180px)] min-h-[420px] overflow-y-auto rounded-md border border-border bg-surface p-2 max-lg:max-h-72 max-lg:min-h-0 max-md:max-h-[calc(100vh-180px)] ${mobileRunDetail ? 'max-md:hidden' : ''}`}>
        <div className="mb-1 flex min-h-9 items-center justify-between gap-2 border-b border-border px-2 pb-1"><span className="min-w-0"><span className="block truncate text-xs font-medium">{selected?.name ?? '回测记录'}</span><span className="mt-0.5 block text-[10px] tabular-nums text-muted">{strategyRuns.length} 条记录</span></span><div className="flex shrink-0 items-center gap-0.5">{selectedRun ? <><IconButton title="重命名回测记录" onClick={() => openRename({ type: 'backtest', id: selectedRun.job_id, name: selectedRun.name })}><Pencil className="h-3.5 w-3.5" /></IconButton><IconButton title="删除回测记录" danger onClick={() => openDelete({ type: 'backtest', id: selectedRun.job_id, name: selectedRun.name })}><Trash2 className="h-3.5 w-3.5" /></IconButton></> : null}</div></div>
        <div>
          {running ? <div className="rounded border border-accent bg-accent/10 px-2.5 py-2.5" aria-current="true">
            <div className="flex items-center justify-between gap-2 text-[11px]"><span className="inline-flex min-w-0 items-center gap-1.5 font-medium text-accent"><LoaderCircle className="h-3.5 w-3.5 shrink-0 animate-spin" /><span className="truncate">{name}</span></span><span className="shrink-0 tabular-nums text-accent">{progressPct == null ? '运行中' : `${Math.round(progressPct * 100)}%`}</span></div>
            <div className="mt-1.5 truncate text-[10px] text-muted">{progress}</div>
          </div> : null}
          {strategyRuns.map(runItem => {
            const metadata = runItem.metadata ?? {}
            const returnPct = Number(runItem.return_pct ?? 0)
            return <button type="button" key={runItem.job_id} onClick={() => void loadSavedRun(runItem.job_id)} aria-label={`查看回测 ${runItem.name}`} className={`w-full border-l-2 border-b px-2.5 py-2.5 text-left transition-colors ${!running && selectedRunId === runItem.job_id ? 'border-l-accent border-b-border bg-accent/10' : 'border-l-transparent border-b-border hover:bg-elevated'}`}>
              <div className="flex items-start justify-between gap-2 text-[11px]"><span className="min-w-0 truncate font-medium">{runItem.name}</span><span className={`shrink-0 tabular-nums ${priceColorClass(returnPct)}`}>{returnPct >= 0 ? '+' : ''}{returnPct.toFixed(2)}%</span></div>
              <div className="mt-1.5 truncate text-[10px] text-muted">{String(metadata.start ?? '—')} 至 {String(metadata.end ?? '—')} · {String(metadata.timeframe ?? '—')}{executionModeLabel(metadata.execution_mode) ? ` · ${executionModeLabel(metadata.execution_mode)}` : ''}</div>
              <div className="mt-1 flex justify-between gap-2 text-[10px] text-muted"><span className="shrink-0">r{String(metadata.source_revision ?? '—')}</span><span className="shrink-0"><span className={priceColorClass(-Math.abs(Number(runItem.max_drawdown_pct ?? 0)))}>回撤 {Number(runItem.max_drawdown_pct ?? 0).toFixed(2)}%</span> · {runItem.fills} 成交</span></div>
            </button>
          })}
        </div>
        {savedRuns.isLoading ? <div className="py-4 text-center text-[11px] text-muted">加载回测记录...</div> : null}
        {!savedRuns.isLoading && selected && !strategyRuns.length && !running ? <EmptyState icon={History} title="该策略暂无回测记录" /> : null}
        {!selected ? <EmptyState icon={History} title="请选择策略" /> : null}
      </section>

      <div className={`min-w-0 max-xl:col-start-2 max-lg:col-start-auto ${mobileRunDetail ? '' : 'max-md:hidden'}`}>
        <button type="button" onClick={() => setMobileRunDetail(false)} className="mb-2 hidden h-8 items-center gap-1 text-xs text-muted hover:text-foreground max-md:inline-flex"><ArrowLeft className="h-3.5 w-3.5" />返回回测记录</button>
        {error ? <div className="mb-3 flex gap-2 rounded border border-danger/30 bg-danger/10 p-2.5 text-[11px] text-danger"><AlertTriangle className="h-3.5 w-3.5 shrink-0" />{error}</div> : null}
        {running ? <section className="min-h-[420px] rounded-md border border-border bg-surface p-4" aria-live="polite">
          <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-4">
            <div><div className="inline-flex items-center gap-2 text-sm font-medium"><LoaderCircle className="h-4 w-4 animate-spin text-accent" />回测运行中{runningMode ? ` · ${runningMode}` : ''}</div><div className="mt-1 text-[11px] text-muted">{name} · {config.asset_type.toUpperCase()} {config.timeframe} · {config.start} 至 {config.end}</div></div>
            <button type="button" onClick={cancel} className="inline-flex items-center gap-1.5 rounded-btn border border-border px-3 py-1.5 text-xs text-muted hover:border-danger hover:text-danger"><Square className="h-3.5 w-3.5" />停止</button>
          </div>
          <div className="mx-auto mt-16 max-w-xl">
            <div className="flex items-center justify-between gap-3 text-xs"><span>{progress}</span><span className="tabular-nums text-accent">{progressPct == null ? '运行中' : `${Math.round(progressPct * 100)}%`}</span></div>
            <div className="mt-3 h-1.5 overflow-hidden rounded bg-elevated"><div className="h-full bg-accent transition-[width] duration-300" style={{ width: `${Math.max(2, Math.round((progressPct ?? 0) * 100))}%` }} /></div>
            <div className="mt-3 break-all font-mono text-[10px] text-muted">任务 {selectedRunId || '正在生成...'}</div>
          </div>
        </section> : result ? <FreeStrategyResult result={result} title={selectedRun?.name} /> : <section className="min-h-[420px] rounded-md border border-border bg-surface"><EmptyState icon={History} title={selected ? `选择「${selected.name}」的回测记录` : '请选择策略'} /></section>}
      </div>
    </div>}

    {renameTarget ? <RenameDialog target={renameTarget} value={renameValue} pending={renamePending} error={renameError} onValueChange={setRenameValue} onClose={() => { if (!renamePending) setRenameTarget(null) }} onConfirm={() => void renameSelected()} /> : null}
    {deleteTarget ? <ConfirmDialog title={`删除「${deleteTarget.name}」？`} description={deleteTarget.type === 'strategy' ? '策略定义将被永久删除。如果存在关联回测或模拟盘账户，系统会阻止此操作。' : '该回测的源码快照、运行配置和结果将被永久删除，不可恢复。'} confirmLabel="确认删除" pending={deletePending} error={deleteError} onClose={() => { if (!deletePending) setDeleteTarget(null) }} onConfirm={() => void deleteSelected()} /> : null}
    {pendingEditorAction ? <ConfirmDialog title="放弃未保存的修改？" description="当前策略的名称、源码或运行设置已修改。继续后这些未保存内容将丢失。" confirmLabel="放弃修改" pending={false} danger={false} onClose={() => setPendingEditorAction(null)} onConfirm={() => { applyEditorAction(pendingEditorAction); setPendingEditorAction(null) }} /> : null}
  </div>
}
