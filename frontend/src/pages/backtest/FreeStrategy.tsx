import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, BookOpen, CirclePause, CirclePlay, Code2, History, Plus, Save, Square, Trash2 } from 'lucide-react'
import { api, type FreeBacktestConfig, type FreeBacktestResult } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { FreeStrategyResult } from './FreeStrategyResult'

const INPUT = 'w-full rounded-input border border-border bg-surface px-2.5 py-1.5 text-xs text-foreground focus:border-accent focus:outline-none'
const DEFAULT_CONFIG: FreeBacktestConfig = {
  strategy_id: '', timeframe: '1d', asset_type: 'etf',
  start: `${new Date().getFullYear() - 3}-01-01`, end: new Date().toISOString().slice(0, 10),
  initial_capital: 1_000_000, fees_pct: 0.0002, commission_pct: null, stamp_tax_pct: 0.001,
  slippage_bps: 5, lot_size: 100, max_exposure_pct: 1, settlement: 't1', fill_policy: 'next_open',
  benchmark_symbol: '510300.SH',
}

function withoutLegacySymbols(value: Record<string, unknown>): Partial<FreeBacktestConfig> {
  const normalized = { ...value }
  delete normalized.symbols
  return normalized as Partial<FreeBacktestConfig>
}

export function FreeStrategy() {
  const [selectedId, setSelectedId] = useState<string>('')
  const strategies = useQuery({ queryKey: ['free-strategies'], queryFn: api.freeStrategies })
  const templates = useQuery({ queryKey: ['free-strategy-templates'], queryFn: api.freeTemplates })
  const savedRuns = useQuery({ queryKey: ['free-backtest-runs'], queryFn: api.freeBacktestRuns })
  const paperAccounts = useQuery({ queryKey: ['free-paper-accounts'], queryFn: api.paperAccounts })
  const detail = useQuery({ queryKey: ['free-strategy', selectedId], queryFn: () => api.freeStrategy(selectedId), enabled: Boolean(selectedId) })
  const [name, setName] = useState('我的自由策略')
  const [source, setSource] = useState(`ETF_POOL = ["510300.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
    context.log("策略初始化")

def on_bar(context, bars):
    for symbol, bar in bars.items():
        context.order_target_percent(symbol, 0.95)
`)
  const [config, setConfig] = useState<FreeBacktestConfig>(DEFAULT_CONFIG)
  const [result, setResult] = useState<FreeBacktestResult | null>(null)
  const [progress, setProgress] = useState('')
  const [error, setError] = useState('')
  const [running, setRunning] = useState(false)
  const [selectedRunId, setSelectedRunId] = useState('')
  const [selectedAccountId, setSelectedAccountId] = useState('')
  const sourceRef = useRef<EventSource | null>(null)
  const jobRef = useRef<string | null>(null)
  const didAutoSelectStrategy = useRef(false)

  const list = strategies.data?.strategies ?? []
  const templateList = templates.data?.templates ?? []
  const runs = savedRuns.data?.runs ?? []
  const selected = list.find(item => item.id === selectedId)
  const accounts = paperAccounts.data?.accounts ?? []
  const paperDetail = useQuery({
    queryKey: ['free-paper-account', selectedAccountId],
    queryFn: () => api.paperAccount(selectedAccountId),
    enabled: Boolean(selectedAccountId),
    refetchInterval: selectedAccountId ? 5_000 : false,
  })
  const account = paperDetail.data ?? accounts.find(item => item.id === selectedAccountId) ?? null
  const accountSnapshot = account?.account ?? account
  const accountPositions = (accountSnapshot?.positions ?? {}) as Record<string, number>
  const activePositions = Object.entries(accountPositions).filter(([, quantity]) => quantity > 0)

  useEffect(() => () => sourceRef.current?.close(), [])
  useEffect(() => {
    const first = strategies.data?.strategies[0]
    if (!didAutoSelectStrategy.current && first) {
      didAutoSelectStrategy.current = true
      setSelectedId(first.id)
    }
  }, [strategies.data?.strategies])
  useEffect(() => {
    const first = paperAccounts.data?.accounts[0]
    if (!selectedAccountId && first) setSelectedAccountId(String(first.id))
  }, [paperAccounts.data?.accounts, selectedAccountId])
  useEffect(() => {
    if (selected) {
      setName(selected.name)
      setSource(detail.data?.source ?? '')
      setConfig(prev => ({ ...prev, ...withoutLegacySymbols(selected.config ?? {}), strategy_id: selected.id }))
    }
  }, [selected, detail.data])

  const save = async () => {
    setError('')
    const saved = selectedId
      ? await api.updateFreeStrategy(selectedId, { name, source, config })
      : await api.saveFreeStrategy({ name, source, config })
    setSelectedId(saved.id)
    setConfig(prev => ({ ...prev, strategy_id: saved.id }))
    await strategies.refetch()
  }

  const loadTemplate = (id: string) => {
    const template = templateList.find(item => item.id === id)
    if (!template) return
    setName(template.name)
    setSource(template.source)
    setSelectedId('')
    setConfig(prev => ({ ...prev, ...withoutLegacySymbols(template.config ?? {}), strategy_id: '' }))
  }

  const loadSavedRun = async (jobId: string) => {
    setSelectedRunId(jobId)
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
    if (!config.strategy_id) {
      setError('请先保存策略，生成源码快照后再运行')
      return
    }
    sourceRef.current?.close()
    setResult(null); setError(''); setProgress('准备回测'); setRunning(true)
    try {
      const job = await api.startFreeBacktest(config)
      jobRef.current = job.job_id
      const events = new EventSource(`/api/free-strategies/backtest/${job.job_id}/stream`)
      sourceRef.current = events
      let finished = false
      events.onmessage = event => {
        const payload = JSON.parse(event.data)
        if (payload.type === 'progress') setProgress(payload.message)
        if (payload.type === 'result') { finished = true; setResult(payload.result); setSelectedRunId(job.job_id); setProgress('回测完成'); setRunning(false); void savedRuns.refetch(); events.close() }
        if (payload.type === 'error') { finished = true; setError(payload.error); setRunning(false); events.close() }
      }
      events.onerror = () => { if (!finished) setError('回测连接中断，请查看任务日志'); setRunning(false); events.close() }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err)); setRunning(false)
    }
  }

  const cancel = async () => {
    if (jobRef.current) await api.cancelFreeBacktest(jobRef.current)
    sourceRef.current?.close(); setRunning(false); setProgress('回测已取消')
  }

  const createPaper = async () => {
    if (!config.strategy_id) { setError('请先保存策略'); return }
    setError('')
    try {
      const created = await api.createPaperAccount({ ...config, name: `${name} · 模拟盘` })
      setSelectedAccountId(String(created.id))
      await paperAccounts.refetch()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const paperAction = async (action: 'start' | 'pause' | 'resume' | 'stop') => {
    if (!selectedAccountId) return
    setError('')
    try {
      await api.paperAction(selectedAccountId, action)
      await Promise.all([paperAccounts.refetch(), paperDetail.refetch()])
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (strategies.isLoading) return <div className="h-full grid place-items-center text-xs text-muted">加载自由策略...</div>

  return <div className="flex h-full min-h-0 flex-col gap-3 overflow-y-auto pr-1">
    <div className="flex shrink-0 flex-wrap items-center justify-between gap-3">
      <div className="flex items-center gap-2">
        <Code2 className="h-4 w-4 text-accent" />
        <div><div className="text-sm font-semibold">自由策略</div><div className="text-[11px] text-muted">独立 Python bar 回放 · 源码快照修订 {selected?.revision ?? '未保存'}</div></div>
      </div>
      <div className="flex flex-wrap items-center justify-end gap-1.5">
        <select className={INPUT} value="" onChange={e => loadTemplate(e.target.value)} aria-label="加载模板">
          <option value="">加载模板</option>{templateList.map(item => <option value={item.id} key={item.id}>{item.name}</option>)}
        </select>
        <button className="inline-flex items-center gap-1 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white" onClick={save}><Save className="h-3.5 w-3.5" />保存</button>
      </div>
    </div>

    <div className="grid min-h-[560px] shrink-0 grid-cols-[220px_minmax(0,1fr)_280px] gap-3 max-xl:min-h-[820px] max-xl:grid-cols-[180px_minmax(0,1fr)] max-md:min-h-0 max-md:grid-cols-1">
      <div className="grid min-h-0 grid-rows-[minmax(160px,0.72fr)_minmax(220px,1.28fr)] gap-3 max-md:grid-rows-none">
        <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-2.5 max-md:max-h-64">
          <div className="mb-2 flex items-center justify-between"><span className="text-xs font-medium">策略列表</span><div className="flex items-center gap-1"><button className="text-muted hover:text-accent" title="新建策略" onClick={() => { setSelectedId(''); setName('我的自由策略'); setSource('ETF_POOL = ["510300.SH"]\n\ndef initialize(context):\n    context.set_universe(ETF_POOL)\n\ndef on_bar(context, bars):\n    pass'); setConfig(prev => ({ ...prev, strategy_id: '' })) }}><Plus className="h-3.5 w-3.5" /></button>{selectedId && <button className="text-muted hover:text-danger" title="删除策略" onClick={async () => { await api.deleteFreeStrategy(selectedId); setSelectedId(''); setConfig(prev => ({ ...prev, strategy_id: '' })); await strategies.refetch() }}><Trash2 className="h-3.5 w-3.5" /></button>}</div></div>
          <div className="space-y-1">{list.map(item => <button key={item.id} onClick={() => setSelectedId(item.id)} className={`w-full rounded px-2 py-2 text-left text-xs ${selectedId === item.id ? 'bg-accent/15 text-accent' : 'hover:bg-elevated'}`}><div className="truncate">{item.name}</div><div className="mt-1 text-[10px] text-muted">修订 {item.revision}</div></button>)}</div>
          {!list.length && <EmptyState icon={BookOpen} title="还没有策略" hint="从模板开始，保存后即可运行。" />}
        </section>

        <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-2.5 max-md:max-h-80">
          <div className="mb-2 flex items-center justify-between gap-2">
            <span className="inline-flex items-center gap-1.5 text-xs font-medium"><History className="h-3.5 w-3.5 text-accent" />回测记录</span>
            <span className="text-[10px] tabular-nums text-muted">{runs.length} 条</span>
          </div>
          <div className="space-y-1.5">
            {runs.map(run => {
              const metadata = run.metadata ?? {}
              const returnPct = Number(run.return_pct ?? 0)
              return <button type="button" key={run.job_id} onClick={() => void loadSavedRun(run.job_id)} aria-label={`查看回测 ${run.job_id}`} className={`w-full rounded border px-2 py-2 text-left transition-colors ${selectedRunId === run.job_id ? 'border-accent bg-accent/10' : 'border-border hover:border-accent/60 hover:bg-elevated'}`}>
                <div className="flex items-start justify-between gap-2 text-[11px]"><span className="min-w-0 truncate font-medium">{String(metadata.strategy_name ?? run.job_id)}</span><span className={`shrink-0 tabular-nums ${returnPct >= 0 ? 'text-success' : 'text-danger'}`}>{returnPct >= 0 ? '+' : ''}{returnPct.toFixed(2)}%</span></div>
                <div className="mt-1 truncate text-[10px] text-muted">{String(metadata.start ?? '—')} 至 {String(metadata.end ?? '—')} · {String(metadata.timeframe ?? '—')}</div>
                <div className="mt-1 flex justify-between gap-2 text-[10px] text-muted"><span className="truncate font-mono">{run.job_id}</span><span className="shrink-0">回撤 {Number(run.max_drawdown_pct ?? 0).toFixed(2)}% · {run.fills} 成交</span></div>
              </button>
            })}
          </div>
          {savedRuns.isLoading ? <div className="py-4 text-center text-[11px] text-muted">加载回测记录...</div> : null}
          {!savedRuns.isLoading && !runs.length ? <EmptyState icon={History} title="还没有回测记录" hint="完成一次历史回测后会保存在这里。" /> : null}
        </section>
      </div>

      <section className="flex min-h-0 flex-col overflow-hidden rounded-md border border-border bg-surface max-md:h-[460px]">
        <div className="flex items-center gap-2 border-b border-border px-3 py-2"><input className="min-w-0 flex-1 bg-transparent text-sm font-medium outline-none" value={name} onChange={e => setName(e.target.value)} /><span className="text-[10px] text-muted">Python</span></div>
        <textarea className="min-h-0 flex-1 resize-none bg-[#101114] p-3 font-mono text-xs leading-5 text-zinc-200 outline-none" spellCheck={false} value={source} onChange={e => setSource(e.target.value)} />
        <div className="flex items-center gap-2 border-t border-border px-3 py-2 text-[10px] text-muted"><span>可信本机执行</span><span>·</span><span>支持任意已安装库</span></div>
      </section>

      <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-3 max-xl:col-span-2 max-md:col-span-1">
        <div className="mb-2 text-xs font-medium">运行设置</div>
        <div className="grid grid-cols-2 gap-2 text-[11px]">
          <label>资产<select className={INPUT} value={config.asset_type} onChange={e => setConfig({ ...config, asset_type: e.target.value as any })}><option value="etf">ETF</option><option value="stock">股票</option></select></label>
          <label>周期<select className={INPUT} value={config.timeframe} onChange={e => setConfig({ ...config, timeframe: e.target.value as any })}><option value="1d">1d</option><option value="30m">30m</option><option value="5m">5m</option><option value="1m">1m</option></select></label>
          <label className="col-span-2">基准<input className={INPUT} value={config.benchmark_symbol} onChange={e => setConfig({ ...config, benchmark_symbol: e.target.value.trim() })} /></label>
          <label>开始<input type="date" className={INPUT} value={config.start} onChange={e => setConfig({ ...config, start: e.target.value })} /></label><label>结束<input type="date" className={INPUT} value={config.end} onChange={e => setConfig({ ...config, end: e.target.value })} /></label>
          <label>初始资金<input type="number" className={INPUT} value={config.initial_capital} onChange={e => setConfig({ ...config, initial_capital: Number(e.target.value) })} /></label><label>最小单位<input type="number" className={INPUT} value={config.lot_size} onChange={e => setConfig({ ...config, lot_size: Number(e.target.value) })} /></label>
          <label>手续费<input type="number" step="0.0001" className={INPUT} value={config.fees_pct} onChange={e => setConfig({ ...config, fees_pct: Number(e.target.value) })} /></label><label>滑点(bps)<input type="number" className={INPUT} value={config.slippage_bps} onChange={e => setConfig({ ...config, slippage_bps: Number(e.target.value) })} /></label>
          <label>结算<select className={INPUT} value={config.settlement} onChange={e => setConfig({ ...config, settlement: e.target.value as any })}><option value="t1">T+1（默认）</option><option value="t0">T+0</option></select></label><label>成交<select className={INPUT} value={config.fill_policy} onChange={e => setConfig({ ...config, fill_policy: e.target.value as any })}><option value="next_open">下一根开盘</option><option value="close">当前收盘</option></select></label>
        </div>
        {error && <div className="mt-3 flex gap-2 rounded border border-danger/30 bg-danger/10 p-2 text-[11px] text-danger"><AlertTriangle className="h-3.5 w-3.5 shrink-0" />{error}</div>}
        <div className="mt-3 flex gap-2"><button disabled={running} onClick={run} className="inline-flex flex-1 items-center justify-center gap-1.5 rounded-btn bg-accent px-3 py-2 text-xs font-medium text-white disabled:opacity-50"><CirclePlay className="h-3.5 w-3.5" />历史回测</button><button disabled={!running} onClick={cancel} className="inline-flex items-center justify-center gap-1.5 rounded-btn border border-border px-3 py-2 text-xs disabled:opacity-50"><Square className="h-3.5 w-3.5" />停止</button></div>
        {progress && <div className="mt-2 text-[11px] text-muted">{progress}</div>}
        <div className="my-4 border-t border-border" />
        <div className="mb-2 flex items-center justify-between"><span className="text-xs font-medium">模拟盘</span><button onClick={createPaper} className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-[11px] hover:border-accent"><CirclePlay className="h-3 w-3" />创建账户</button></div>
        {accounts.length ? <select className={INPUT} value={selectedAccountId} onChange={event => setSelectedAccountId(event.target.value)} aria-label="模拟账户">
          {accounts.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
        </select> : null}
        {account ? <div className="mt-2 rounded border border-border bg-elevated p-2.5 text-[11px]">
          <div className="flex items-center justify-between gap-2"><span className="truncate font-medium">{account.name}</span><span className={account.status === 'running' ? 'text-success' : account.status === 'paused' ? 'text-warning' : 'text-muted'}>{account.status}</span></div>
          <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1.5">
            <div><span className="text-muted">可用资金</span><div className="mt-0.5 tabular-nums">{Number(accountSnapshot?.cash ?? 0).toLocaleString('zh-CN', { maximumFractionDigits: 2 })}</div></div>
            <div><span className="text-muted">持仓标的</span><div className="mt-0.5 tabular-nums">{activePositions.length}</div></div>
            <div className="col-span-2"><span className="text-muted">最新 bar</span><div className="mt-0.5 break-all font-mono text-[10px]">{account.last_bar || '尚未收到行情'}</div></div>
          </div>
          <div className="mt-2 max-h-24 overflow-y-auto border-y border-border py-1">
            {activePositions.map(([symbol, quantity]) => <div key={symbol} className="flex justify-between py-1"><span className="font-mono">{symbol}</span><span className="tabular-nums">{quantity.toLocaleString('zh-CN')}</span></div>)}
            {!activePositions.length && <div className="py-1 text-muted">当前无持仓</div>}
          </div>
          {account.last_error ? <div className="mt-2 break-words text-danger">{account.last_error}</div> : null}
          <div className="mt-2 flex gap-1">
            <button type="button" title={account.status === 'paused' ? '恢复' : '启动'} disabled={account.status === 'running'} onClick={() => void paperAction(account.status === 'paused' ? 'resume' : 'start')} className="inline-flex h-7 w-7 items-center justify-center rounded border border-border text-muted hover:border-accent hover:text-accent disabled:opacity-40"><CirclePlay className="h-3.5 w-3.5" /></button>
            <button type="button" title="暂停" disabled={account.status !== 'running'} onClick={() => void paperAction('pause')} className="inline-flex h-7 w-7 items-center justify-center rounded border border-border text-muted hover:border-warning hover:text-warning disabled:opacity-40"><CirclePause className="h-3.5 w-3.5" /></button>
            <button type="button" title="停止" disabled={account.status === 'stopped'} onClick={() => void paperAction('stop')} className="inline-flex h-7 w-7 items-center justify-center rounded border border-border text-muted hover:border-danger hover:text-danger disabled:opacity-40"><Square className="h-3.5 w-3.5" /></button>
          </div>
        </div> : <div className="mt-2 text-[11px] text-muted">创建账户后可启动持久模拟盘。</div>}
      </section>
    </div>

    {result && <FreeStrategyResult result={result} />}
  </div>
}
