import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  CheckCircle2,
  CircleAlert,
  DatabaseZap,
  Loader2,
  RefreshCw,
  Wrench,
} from 'lucide-react'
import { api, type EtfDataIssue } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { DatePicker } from '@/components/DatePicker'
import { RepairDailyPanel } from './RepairDailyPanel'

type Tab = 'daily' | 'etf' | 'history'
type Scope = 'all' | 'symbols' | 'strategy'

function pad(value: number) { return String(value).padStart(2, '0') }
function dateString(value: Date) {
  return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`
}
function daysAgo(days: number) {
  const value = new Date()
  value.setDate(value.getDate() - days)
  return dateString(value)
}

const TAB_LABELS: Record<Tab, string> = {
  daily: 'A股日K',
  etf: 'ETF回测数据',
  history: '修复记录',
}

const ISSUE_LABELS: Record<EtfDataIssue['type'], string> = {
  daily_missing: '日K缺失',
  minute_gap: '分钟K缺口',
  split_rounding: '拆分比例',
  factor_mismatch: '分红复权',
}

export function RepairDataPanel({
  caps,
  isRunning,
  latestDate,
  initialStrategyId = '',
  initialStart,
  initialEnd,
  initialTimeframe = '1m',
  onDailyStart,
  onJobStart,
}: {
  caps: { label: string; capabilities: Record<string, { rpm: number | null; batch: number | null; subscribe: number | null }> } | undefined
  isRunning: boolean
  latestDate: string | null
  initialStrategyId?: string
  initialStart?: string
  initialEnd?: string
  initialTimeframe?: '1d' | '30m' | '5m' | '1m'
  onDailyStart: () => void
  onJobStart: (jobId: string) => void
}) {
  const qc = useQueryClient()
  const [tab, setTab] = useState<Tab>(initialStrategyId ? 'etf' : 'daily')
  const [scope, setScope] = useState<Scope>(initialStrategyId ? 'strategy' : 'all')
  const [symbolsText, setSymbolsText] = useState('')
  const [start, setStart] = useState(initialStart || daysAgo(30))
  const [end, setEnd] = useState(initialEnd || dateString(new Date()))
  const [requireMinute, setRequireMinute] = useState(initialTimeframe !== '1d')
  const [verifyAxdata, setVerifyAxdata] = useState(false)
  const [selected, setSelected] = useState<string[]>([])
  const [confirmReplace, setConfirmReplace] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

  const history = useQuery({
    queryKey: QK.etfRepairHistory,
    queryFn: api.etfRepairHistory,
    enabled: tab === 'history',
  })

  const parsedSymbols = useMemo(() => (
    symbolsText.split(/[\s,，]+/).map(value => value.trim().toUpperCase()).filter(Boolean)
  ), [symbolsText])

  const check = useMutation({
    mutationFn: () => scope === 'strategy'
      ? api.freeBacktestDataHealth({
          strategy_id: initialStrategyId,
          asset_type: 'etf',
          timeframe: initialTimeframe,
          start,
          end,
          persist_scan: true,
          verify_axdata: false,
        })
      : api.checkEtfData({
          symbols: scope === 'symbols' ? parsedSymbols : [],
          start,
          end,
          require_minute: requireMinute,
          verify_axdata: scope === 'symbols' && verifyAxdata,
          persist_scan: true,
        }),
    onSuccess: data => {
      setSelected(data.issues.map(issue => issue.id))
      setConfirmReplace(false)
      setShowConfirmation(false)
    },
  })

  const repair = useMutation({
    mutationFn: () => api.repairEtfData({
      scan_id: check.data?.scan_id || '',
      issue_ids: selected,
      replace_existing: confirmReplace,
    }),
    onSuccess: data => {
      qc.invalidateQueries({ queryKey: QK.pipelineJobs })
      qc.invalidateQueries({ queryKey: QK.etfRepairHistory })
      onJobStart(data.job_id)
    },
  })

  const selectedIssues = check.data?.issues.filter(issue => selected.includes(issue.id)) ?? []
  const needsReplacement = selectedIssues.some(issue => issue.requires_replace)
  const canCheck = !isRunning && !check.isPending && start <= end
    && (scope !== 'symbols' || parsedSymbols.length > 0)
    && (scope !== 'strategy' || Boolean(initialStrategyId))

  const toggleIssue = (issueId: string) => {
    setSelected(values => values.includes(issueId)
      ? values.filter(value => value !== issueId)
      : [...values, issueId])
    setConfirmReplace(false)
    setShowConfirmation(false)
  }

  const requestRepair = () => {
    if (needsReplacement) {
      setShowConfirmation(true)
      return
    }
    repair.mutate()
  }

  return <div className="min-h-[420px]">
    <div className="flex border-b border-border" role="tablist" aria-label="数据检查与修复">
      {(Object.keys(TAB_LABELS) as Tab[]).map(value => <button
        key={value}
        type="button"
        role="tab"
        aria-selected={tab === value}
        onClick={() => setTab(value)}
        className={`h-9 border-b-2 px-3 text-xs transition-colors ${tab === value ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}
      >{TAB_LABELS[value]}</button>)}
    </div>

    {tab === 'daily' ? <div className="mx-auto mt-5 max-w-md">
      <RepairDailyPanel caps={caps} isRunning={isRunning} latestDate={latestDate} onStart={onDailyStart} />
    </div> : null}

    {tab === 'etf' ? <div className="mt-4 space-y-4">
      <div className="grid gap-x-5 gap-y-3 border-b border-border pb-4 md:grid-cols-2">
        <div>
          <div className="mb-1.5 text-[11px] text-muted">检查范围</div>
          <div className="inline-flex overflow-hidden rounded-btn border border-border">
            {initialStrategyId ? <button type="button" onClick={() => setScope('strategy')} className={`px-3 py-1.5 text-xs ${scope === 'strategy' ? 'bg-accent/15 text-accent' : 'text-muted hover:text-foreground'}`}>当前策略</button> : null}
            <button type="button" onClick={() => setScope('all')} className={`px-3 py-1.5 text-xs ${scope === 'all' ? 'bg-accent/15 text-accent' : 'text-muted hover:text-foreground'}`}>全部ETF</button>
            <button type="button" onClick={() => setScope('symbols')} className={`px-3 py-1.5 text-xs ${scope === 'symbols' ? 'bg-accent/15 text-accent' : 'text-muted hover:text-foreground'}`}>指定标的</button>
          </div>
        </div>
        <div>
          <div className="mb-1.5 text-[11px] text-muted">日期范围</div>
          <div className="flex items-center gap-2">
            <DatePicker value={start} onChange={setStart} max={end} buttonClassName="font-mono text-xs" />
            <span className="text-muted">至</span>
            <DatePicker value={end} onChange={setEnd} min={start} max={dateString(new Date())} align="right" buttonClassName="font-mono text-xs" />
          </div>
        </div>
        {scope === 'symbols' ? <label className="md:col-span-2 text-[11px] text-muted">标的代码
          <input
            value={symbolsText}
            onChange={event => setSymbolsText(event.target.value)}
            placeholder="例如 161226.SZ, 515000.SH"
            className="mt-1.5 w-full rounded-input border border-border bg-base px-2.5 py-2 font-mono text-xs text-foreground outline-none focus:border-accent"
          />
        </label> : null}
        <div className="flex flex-wrap items-center gap-5 md:col-span-2">
          <label className="inline-flex items-center gap-2 text-xs text-secondary"><input type="checkbox" checked={requireMinute} disabled={scope === 'strategy'} onChange={event => setRequireMinute(event.target.checked)} className="accent-accent" />检查分钟K缺口</label>
          <label className={`inline-flex items-center gap-2 text-xs ${scope === 'symbols' ? 'text-secondary' : 'text-muted'}`}><input type="checkbox" checked={verifyAxdata} disabled={scope !== 'symbols'} onChange={event => setVerifyAxdata(event.target.checked)} className="accent-accent" />核对分红与拆分</label>
          <button
            type="button"
            disabled={!canCheck}
            onClick={() => check.mutate()}
            className="ml-auto inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40"
          >{check.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <DatabaseZap className="h-3.5 w-3.5" />}{check.isPending ? '检查中…' : '开始检查'}</button>
        </div>
      </div>

      {check.isError ? <div className="rounded border border-danger/30 bg-danger/8 px-3 py-2 text-xs text-danger">{check.error.message}</div> : null}

      {check.data ? <>
        <div className="flex flex-wrap items-center justify-between gap-3 text-xs">
          <div className="inline-flex items-center gap-2">
            {check.data.status === 'healthy' ? <CheckCircle2 className="h-4 w-4 text-success" /> : <CircleAlert className="h-4 w-4 text-warning" />}
            <span>{check.data.status === 'healthy' ? '所选范围数据完整' : `发现 ${check.data.issues.length} 个问题`}</span>
            <span className="text-muted">检查 {check.data.symbol_count} 只 ETF</span>
          </div>
          <span className={check.data.source?.available ? 'text-success' : 'text-muted'}>{check.data.source?.message}</span>
        </div>

        {check.data.issues.length ? <div className="overflow-x-auto rounded border border-border">
          <table className="w-full min-w-[720px] table-fixed text-left text-[11px]">
            <thead className="bg-elevated text-muted"><tr>
              <th className="w-10 px-3 py-2"><input aria-label="选择全部问题" type="checkbox" checked={selected.length === check.data.issues.length} onChange={event => setSelected(event.target.checked ? check.data!.issues.map(issue => issue.id) : [])} className="accent-accent" /></th>
              <th className="w-28 px-2 py-2 font-medium">标的</th><th className="w-28 px-2 py-2 font-medium">问题</th><th className="w-44 px-2 py-2 font-medium">日期</th><th className="px-2 py-2 font-medium">处理方式</th>
            </tr></thead>
            <tbody className="divide-y divide-border">{check.data.issues.map(issue => <tr key={issue.id} className="hover:bg-elevated/40">
              <td className="px-3 py-2.5"><input aria-label={`选择 ${issue.symbol} ${issue.title}`} type="checkbox" checked={selected.includes(issue.id)} onChange={() => toggleIssue(issue.id)} className="accent-accent" /></td>
              <td className="px-2 py-2.5 font-mono text-foreground">{issue.symbol}</td>
              <td className={`px-2 py-2.5 ${issue.severity === 'error' ? 'text-danger' : 'text-warning'}`}>{ISSUE_LABELS[issue.type]}</td>
              <td className="px-2 py-2.5 font-mono text-muted">{issue.start === issue.end ? issue.start : `${issue.start} 至 ${issue.end}`}</td>
              <td className="px-2 py-2.5"><div className="text-secondary">{issue.action}</div><div className="mt-0.5 text-[10px] text-muted">{issue.detail}</div></td>
            </tr>)}</tbody>
          </table>
        </div> : null}

        {showConfirmation && needsReplacement ? <div className="border-l-2 border-warning bg-warning/8 px-3 py-2.5">
          <div className="flex gap-2 text-xs text-warning"><AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" /><span>所选修复会替换对应标的在 {start} 至 {end} 的已有分钟数据，并使旧回测结果需要重新运行。</span></div>
          <label className="mt-2 inline-flex items-center gap-2 text-xs text-secondary"><input type="checkbox" checked={confirmReplace} onChange={event => setConfirmReplace(event.target.checked)} className="accent-warning" />我确认使用 AxData 替换上述数据</label>
        </div> : null}

        {check.data.issues.length ? <div className="flex items-center justify-between gap-3">
          <button type="button" onClick={() => check.mutate()} disabled={check.isPending} className="inline-flex items-center gap-1.5 text-xs text-muted hover:text-foreground"><RefreshCw className="h-3.5 w-3.5" />重新检查</button>
          <button
            type="button"
            disabled={!selected.length || repair.isPending || isRunning || (showConfirmation && needsReplacement && !confirmReplace)}
            onClick={showConfirmation && needsReplacement ? () => repair.mutate() : requestRepair}
            className="inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40"
          >{repair.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Wrench className="h-3.5 w-3.5" />}{repair.isPending ? '请求中…' : showConfirmation && needsReplacement ? '确认并开始修复' : `修复选中项 (${selected.length})`}</button>
        </div> : null}
        {repair.isError ? <div className="rounded border border-danger/30 bg-danger/8 px-3 py-2 text-xs text-danger">{repair.error.message}</div> : null}
      </> : <div className="py-16 text-center text-xs text-muted">选择范围并开始检查，检查过程不会修改本地行情。</div>}
    </div> : null}

    {tab === 'history' ? <div className="mt-4">
      {history.isLoading ? <div className="py-16 text-center text-xs text-muted">加载修复记录…</div> : null}
      {!history.isLoading && !history.data?.records.length ? <div className="py-16 text-center text-xs text-muted">暂无 ETF 数据修复记录</div> : null}
      <div className="divide-y divide-border rounded border border-border">{history.data?.records.map(record => <div key={record.id} className="flex flex-wrap items-center justify-between gap-3 px-3 py-3 text-xs">
        <div className="min-w-0"><div className="flex items-center gap-2">{record.status === 'succeeded' ? <CheckCircle2 className="h-3.5 w-3.5 text-success" /> : <CircleAlert className="h-3.5 w-3.5 text-danger" />}<span className="font-mono">{record.id}</span><span className="text-muted">{record.source}</span></div><div className="mt-1 truncate text-[10px] text-muted">{record.symbols.join(', ')} · {record.start} 至 {record.end}</div></div>
        <div className="shrink-0 text-right"><div className="text-secondary">{record.status === 'succeeded' ? `修复 ${record.issues_repaired ?? 0} 项 · 分钟K ${record.minute_rows ?? 0} 行` : '修复失败'}</div><div className="mt-1 text-[10px] text-muted">{new Date(record.started_at).toLocaleString('zh-CN')}</div></div>
        {record.error ? <div className="w-full text-[10px] text-danger">{record.error}</div> : null}
      </div>)}</div>
    </div> : null}
  </div>
}
