import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, BookOpen, CirclePause, CirclePlay, Code2, Plus, Save, Square, Trash2 } from 'lucide-react'
import { api, type FreeBacktestConfig, type FreeBacktestResult } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { FreeStrategyPerformanceChart } from './charts/FreeStrategyPerformanceChart'

const INPUT = 'w-full rounded-input border border-border bg-surface px-2.5 py-1.5 text-xs text-foreground focus:border-accent focus:outline-none'
const DEFAULT_CONFIG: FreeBacktestConfig = {
  strategy_id: '', symbols: ['510300.SH'], timeframe: '1d', asset_type: 'etf',
  start: `${new Date().getFullYear() - 3}-01-01`, end: new Date().toISOString().slice(0, 10),
  initial_capital: 1_000_000, fees_pct: 0.0002, commission_pct: null, stamp_tax_pct: 0.001,
  slippage_bps: 5, lot_size: 100, max_exposure_pct: 1, settlement: 't1', fill_policy: 'next_open',
  benchmark_symbol: '510300.SH',
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="border-l border-border pl-3"><div className="text-[10px] text-muted">{label}</div><div className="mt-1 text-sm font-semibold tabular-nums">{value}</div></div>
}

export function FreeStrategy() {
  const [selectedId, setSelectedId] = useState<string>('')
  const strategies = useQuery({ queryKey: ['free-strategies'], queryFn: api.freeStrategies })
  const templates = useQuery({ queryKey: ['free-strategy-templates'], queryFn: api.freeTemplates })
  const savedRuns = useQuery({ queryKey: ['free-backtest-runs'], queryFn: api.freeBacktestRuns })
  const detail = useQuery({ queryKey: ['free-strategy', selectedId], queryFn: () => api.freeStrategy(selectedId), enabled: Boolean(selectedId) })
  const [name, setName] = useState('我的自由策略')
  const [source, setSource] = useState(`def initialize(context):
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
  const [account, setAccount] = useState<Record<string, any> | null>(null)
  const sourceRef = useRef<EventSource | null>(null)
  const jobRef = useRef<string | null>(null)

  const list = strategies.data?.strategies ?? []
  const templateList = templates.data?.templates ?? []
  const selected = useMemo(() => list.find(item => item.id === selectedId), [list, selectedId])
  const dailyReports = ((result?.state?.five_fortunes?.daily_reports ?? []) as Array<{ date: string, regime: string, target: string[], candidates: Array<{ symbol: string }> }>)

  useEffect(() => () => sourceRef.current?.close(), [])
  useEffect(() => {
    if (selected) {
      setName(selected.name)
      setSource(detail.data?.source ?? '')
      setConfig(prev => ({ ...prev, ...(selected.config ?? {}), strategy_id: selected.id }))
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
    setConfig(prev => ({ ...prev, ...(template.config ?? {}), strategy_id: '' }))
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
      const job = await api.startFreeBacktest({ ...config, symbols: config.symbols.filter(Boolean) })
      jobRef.current = job.job_id
      const events = new EventSource(`/api/free-strategies/backtest/${job.job_id}/stream`)
      sourceRef.current = events
      events.onmessage = event => {
        const payload = JSON.parse(event.data)
        if (payload.type === 'progress') setProgress(payload.message)
        if (payload.type === 'result') { setResult(payload.result); setSelectedRunId(job.job_id); setProgress('回测完成'); setRunning(false); void savedRuns.refetch(); events.close() }
        if (payload.type === 'error') { setError(payload.error); setRunning(false); events.close() }
      }
      events.onerror = () => { if (running) setError('回测连接中断，请查看任务日志'); setRunning(false); events.close() }
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
    const created = await api.createPaperAccount({ ...config, name: `${name} · 模拟盘` })
    setAccount(created)
  }

  if (strategies.isLoading) return <div className="h-full grid place-items-center text-xs text-muted">加载自由策略...</div>

  return <div className="flex h-full min-h-0 flex-col gap-3 overflow-y-auto pr-1">
    <div className="flex shrink-0 flex-wrap items-center justify-between gap-3">
      <div className="flex items-center gap-2">
        <Code2 className="h-4 w-4 text-accent" />
        <div><div className="text-sm font-semibold">自由策略</div><div className="text-[11px] text-muted">独立 Python bar 回放 · 源码快照修订 {selected?.revision ?? '未保存'}</div></div>
      </div>
      <div className="flex flex-wrap items-center justify-end gap-1.5">
        <select className={INPUT} value={selectedRunId} onChange={e => void loadSavedRun(e.target.value)} aria-label="已保存回测">
          <option value="">已保存回测</option>{(savedRuns.data?.runs ?? []).map(run => <option value={run.job_id} key={run.job_id}>{run.job_id} · {run.return_pct >= 0 ? '+' : ''}{run.return_pct.toFixed(2)}%</option>)}
        </select>
        <select className={INPUT} value="" onChange={e => loadTemplate(e.target.value)} aria-label="加载模板">
          <option value="">加载模板</option>{templateList.map(item => <option value={item.id} key={item.id}>{item.name}</option>)}
        </select>
        <button className="inline-flex items-center gap-1 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white" onClick={save}><Save className="h-3.5 w-3.5" />保存</button>
      </div>
    </div>

    <div className="grid min-h-[560px] shrink-0 grid-cols-[220px_minmax(0,1fr)_280px] gap-3 max-xl:min-h-[820px] max-xl:grid-cols-[180px_minmax(0,1fr)] max-md:min-h-0 max-md:grid-cols-1">
      <section className="min-h-0 overflow-y-auto rounded-md border border-border bg-surface p-2.5">
        <div className="mb-2 flex items-center justify-between"><span className="text-xs font-medium">策略列表</span><div className="flex items-center gap-1"><button className="text-muted hover:text-accent" title="新建策略" onClick={() => { setSelectedId(''); setName('我的自由策略'); setSource('def on_bar(context, bars):\n    pass'); setConfig(prev => ({ ...prev, strategy_id: '' })) }}><Plus className="h-3.5 w-3.5" /></button>{selectedId && <button className="text-muted hover:text-danger" title="删除策略" onClick={async () => { await api.deleteFreeStrategy(selectedId); setSelectedId(''); setConfig(prev => ({ ...prev, strategy_id: '' })); await strategies.refetch() }}><Trash2 className="h-3.5 w-3.5" /></button>}</div></div>
        <div className="space-y-1">{list.map(item => <button key={item.id} onClick={() => setSelectedId(item.id)} className={`w-full rounded px-2 py-2 text-left text-xs ${selectedId === item.id ? 'bg-accent/15 text-accent' : 'hover:bg-elevated'}`}><div className="truncate">{item.name}</div><div className="mt-1 text-[10px] text-muted">修订 {item.revision}</div></button>)}</div>
        {!list.length && <EmptyState icon={BookOpen} title="还没有策略" hint="从模板开始，保存后即可运行。" />}
      </section>

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
          <label className="col-span-2">股票池<input className={INPUT} value={config.symbols.join(',')} onChange={e => setConfig({ ...config, symbols: e.target.value.split(',').map(v => v.trim()).filter(Boolean) })} /></label>
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
        {account && <div className="rounded border border-border bg-elevated p-2 text-[11px]"><div className="flex items-center justify-between"><span>{account.name}</span><span className="text-warning">{account.status}</span></div><div className="mt-2 flex gap-1"><button title="启动" onClick={async () => setAccount(await api.paperAction(account.id, 'start'))}><CirclePlay className="h-3.5 w-3.5" /></button><button title="暂停" onClick={async () => setAccount(await api.paperAction(account.id, 'pause'))}><CirclePause className="h-3.5 w-3.5" /></button><button title="停止" onClick={async () => setAccount(await api.paperAction(account.id, 'stop'))}><Square className="h-3.5 w-3.5" /></button></div></div>}
      </section>
    </div>

    {result && <section className="shrink-0 rounded-md border border-border bg-surface p-3"><div className="mb-3 flex flex-wrap items-center justify-between gap-2"><span className="text-xs font-medium">回测结果</span><span className="text-[10px] text-muted">{result.metadata?.nav_filter === 'skipped_no_data' ? 'NAV/溢价过滤：无数据已跳过' : ''}</span></div><div className="grid grid-cols-4 gap-4 max-lg:grid-cols-2 max-sm:grid-cols-1"><Metric label="期末净值" value={result.final_equity.toLocaleString('zh-CN', { maximumFractionDigits: 2 })} /><Metric label="收益率" value={`${result.return_pct.toFixed(2)}%`} /><Metric label="最大回撤" value={`${result.max_drawdown_pct.toFixed(2)}%`} /><Metric label="成交笔数" value={String(result.fills.length)} /></div>{result.daily_equity_curve?.length ? <div className="mt-4 border-y border-border py-3"><FreeStrategyPerformanceChart result={result} /></div> : null}<div className="mt-3 grid gap-3 lg:grid-cols-2"><div className="overflow-x-auto"><div className="mb-1 text-[11px] font-medium">成交</div><table className="w-full min-w-[520px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-1">时间</th><th className="pb-1">标的</th><th className="pb-1">方向</th><th className="pb-1">数量</th><th className="pb-1">价格</th><th className="pb-1">费用</th></tr></thead><tbody>{result.fills.slice(-20).map((fill, index) => <tr key={`${fill.order_id}-${index}`} className="border-t border-border"><td className="py-1">{fill.timestamp}</td><td>{fill.symbol}</td><td>{fill.side}</td><td>{fill.quantity}</td><td>{Number(fill.price).toFixed(3)}</td><td>{Number(fill.fee).toFixed(2)}</td></tr>)}</tbody></table></div><div className="grid gap-3 sm:grid-cols-2"><div><div className="mb-1 text-[11px] font-medium">当前持仓</div><div className="space-y-1 text-[11px] text-secondary">{Object.entries(result.positions).filter(([, qty]) => qty > 0).map(([symbol, qty]) => <div key={symbol} className="flex justify-between border-b border-border py-1"><span>{symbol}</span><span className="tabular-nums">{qty}</span></div>)}{!Object.values(result.positions).some(qty => qty > 0) && <div className="text-muted">无持仓</div>}</div></div><div><div className="mb-1 text-[11px] font-medium">策略日志</div><div className="max-h-28 space-y-1 overflow-auto text-[10px] text-secondary">{result.logs.slice(-12).map((log, index) => <div key={`${log.timestamp}-${index}`}><span className="text-muted">{log.timestamp.slice(0, 16)}</span> {log.message}</div>)}{!result.logs.length && <div className="text-muted">无策略日志</div>}</div></div></div></div>{dailyReports.length ? <div className="mt-3 overflow-x-auto"><div className="mb-1 text-[11px] font-medium">五福每日决策</div><table className="w-full min-w-[520px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-1">日期</th><th className="pb-1">市场状态</th><th className="pb-1">目标</th><th className="pb-1">候选数</th></tr></thead><tbody>{dailyReports.slice(-12).map(report => <tr key={report.date} className="border-t border-border"><td className="py-1">{report.date}</td><td>{report.regime}</td><td>{report.target.join(',') || '空仓'}</td><td>{report.candidates.length}</td></tr>)}</tbody></table></div> : null}<div className="mt-3 overflow-x-auto"><div className="mb-1 text-[11px] font-medium">逐日资产</div><table className="w-full min-w-[520px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-1">时间</th><th className="pb-1">总资产</th><th className="pb-1">现金</th><th className="pb-1">仓位</th><th className="pb-1">回撤</th></tr></thead><tbody>{(result.daily_equity_curve ?? result.equity_curve).slice(-12).map((row: any) => <tr key={row.timestamp} className="border-t border-border"><td className="py-1">{row.date ?? row.timestamp}</td><td>{row.equity.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}</td><td>{row.cash.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}</td><td>{row.exposure_pct == null ? Object.keys(row.positions).length : `${row.exposure_pct.toFixed(1)}%`}</td><td>{row.drawdown_pct == null ? '—' : `${row.drawdown_pct.toFixed(2)}%`}</td></tr>)}</tbody></table></div></section>}
  </div>
}
