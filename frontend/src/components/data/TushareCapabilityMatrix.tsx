import { Fragment, useMemo, useState } from 'react'
import {
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  Database,
  FileCheck2,
  RefreshCw,
  Search,
} from 'lucide-react'

import type { TushareCapabilityDataset, TushareCapabilityMatrix } from '@/lib/api'

type MatrixFilter = 'all' | 'available' | 'factor' | 'issues'

const NUMBER = new Intl.NumberFormat('zh-CN')
const READY_STATUSES = new Set(['published', 'valid_empty'])

const FILTERS: Array<{ value: MatrixFilter; label: string }> = [
  { value: 'all', label: '全部' },
  { value: 'available', label: '可用' },
  { value: 'factor', label: '因子输入' },
  { value: 'issues', label: '异常' },
]

const DATASET_LABELS: Record<string, string> = {
  stock_basic: 'A 股标的',
  etf_basic: 'ETF 标的',
  trade_cal: '交易日历',
  namechange: '名称与 ST 变更',
  suspend_d: '停复牌状态',
  daily: 'A 股日线',
  daily_basic: '日度估值与股本',
  adj_factor: 'A 股复权因子',
  index_basic: '指数标的',
  index_daily: '指数日线',
  index_member_all: '指数历史成分',
  index_weight: '指数权重',
  fund_daily: 'ETF 日线',
  fund_adj: 'ETF 复权因子',
  income: '利润表',
  balancesheet: '资产负债表',
  cashflow: '现金流量表',
  fina_indicator: '财务指标',
  dividend: '分红送转',
  moneyflow: '个股资金流',
  margin: '两融汇总',
  margin_detail: '两融明细',
  top_list: '龙虎榜',
  limit_list_d: '涨跌停明细',
  limit_list_ths: '同花顺涨停榜',
  forecast: '业绩预告',
  express: '业绩快报',
  disclosure_date: '财报披露计划',
  stk_holdernumber: '股东户数',
  top10_holders: '十大股东',
  top10_floatholders: '十大流通股东',
  stk_holdertrade: '股东增减持',
  block_trade: '大宗交易',
  repurchase: '股票回购',
  share_float: '限售解禁',
  cyq_perf: '筹码胜率',
  cyq_chips: '筹码分布',
}

const STATUS_META: Record<string, { label: string; className: string }> = {
  published: { label: '已发布', className: 'border-accent/20 bg-accent/8 text-accent' },
  valid_empty: { label: '合法空', className: 'border-border bg-elevated text-secondary' },
  completed: { label: '已采集', className: 'border-accent/20 bg-accent/8 text-accent' },
  running: { label: '运行中', className: 'border-warning/20 bg-warning/8 text-warning' },
  blocked: { label: '已阻断', className: 'border-danger/20 bg-danger/8 text-danger' },
  failed: { label: '失败', className: 'border-danger/20 bg-danger/8 text-danger' },
  unhealthy: { label: '不健康', className: 'border-danger/20 bg-danger/8 text-danger' },
  conflict: { label: '冲突', className: 'border-danger/20 bg-danger/8 text-danger' },
  missing: { label: '缺失', className: 'border-warning/20 bg-warning/8 text-warning' },
}

function formatNumber(value: number | undefined): string {
  return NUMBER.format(value ?? 0)
}

function formatDateTime(value: string | null): string {
  if (!value) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString('zh-CN', { hour12: false })
}

function issueCount(dataset: TushareCapabilityDataset): number {
  return (dataset.failed_batches?.length ?? 0) + (dataset.empty_unconfirmed_batches?.length ?? 0)
}

function hasIssue(dataset: TushareCapabilityDataset): boolean {
  return issueCount(dataset) > 0 || !READY_STATUSES.has(dataset.status)
}

function fieldSummary(dataset: TushareCapabilityDataset): { average: number | null; empty: number } {
  const rates = Object.values(dataset.field_non_null_rate ?? {})
  if (rates.length === 0) return { average: null, empty: 0 }
  return {
    average: rates.reduce((sum, value) => sum + value, 0) / rates.length,
    empty: rates.filter(value => value === 0).length,
  }
}

function StatusBadge({ status }: { status: string }) {
  const meta = STATUS_META[status] ?? {
    label: status || '未知',
    className: 'border-border bg-elevated text-muted',
  }
  return (
    <span className={`inline-flex rounded border px-1.5 py-0.5 text-[10px] font-medium ${meta.className}`}>
      {meta.label}
    </span>
  )
}

function SummaryCard({
  icon: Icon,
  label,
  value,
  hint,
  tone = 'default',
}: {
  icon: typeof Database
  label: string
  value: string
  hint: string
  tone?: 'default' | 'warning'
}) {
  return (
    <div className="rounded-card border border-border bg-surface p-4">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs text-muted">{label}</span>
        <Icon className={`h-4 w-4 ${tone === 'warning' ? 'text-warning' : 'text-secondary'}`} />
      </div>
      <div className={`font-mono text-2xl font-bold tabular-nums ${tone === 'warning' ? 'text-warning' : 'text-foreground'}`}>
        {value}
      </div>
      <div className="mt-1 text-[11px] text-muted">{hint}</div>
    </div>
  )
}

function LoadingState() {
  return (
    <div className="space-y-3 animate-pulse">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="h-28 rounded-card border border-border bg-surface p-4">
            <div className="h-3 w-16 rounded bg-elevated" />
            <div className="mt-5 h-7 w-24 rounded bg-elevated" />
            <div className="mt-2 h-2.5 w-20 rounded bg-elevated" />
          </div>
        ))}
      </div>
      <div className="h-64 rounded-card border border-border bg-surface" />
    </div>
  )
}

export function TushareCapabilityMatrixPanel({
  matrix,
  isLoading,
  isError,
  onRetry,
}: {
  matrix: TushareCapabilityMatrix | undefined
  isLoading: boolean
  isError: boolean
  onRetry: () => void
}) {
  const [filter, setFilter] = useState<MatrixFilter>('all')
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)

  const datasets = useMemo(
    () => Object.entries(matrix?.datasets ?? {}).sort(([left], [right]) => left.localeCompare(right)),
    [matrix?.datasets],
  )

  const summary = useMemo(() => {
    let stagedRows = 0
    let available = 0
    let factorInputs = 0
    let issues = 0
    for (const [, dataset] of datasets) {
      stagedRows += dataset.staged_rows ?? 0
      if (READY_STATUSES.has(dataset.status)) available += 1
      if (dataset.factor_input) factorInputs += 1
      issues += issueCount(dataset)
    }
    return { stagedRows, available, factorInputs, issues }
  }, [datasets])

  const visibleDatasets = useMemo(() => {
    const term = search.trim().toLocaleLowerCase()
    return datasets.filter(([name, dataset]) => {
      const matchesSearch = !term
        || name.toLocaleLowerCase().includes(term)
        || (DATASET_LABELS[name] ?? '').includes(term)
      if (!matchesSearch) return false
      if (filter === 'available') return READY_STATUSES.has(dataset.status)
      if (filter === 'factor') return Boolean(dataset.factor_input)
      if (filter === 'issues') return hasIssue(dataset)
      return true
    })
  }, [datasets, filter, search])

  if (isLoading) return <LoadingState />

  if (isError) {
    return (
      <div className="flex min-h-40 items-center justify-center rounded-card border border-danger/30 bg-danger/[0.03] px-6 py-10">
        <div className="text-center">
          <AlertTriangle className="mx-auto h-6 w-6 text-danger" />
          <div className="mt-3 text-sm font-medium text-foreground">能力矩阵读取失败</div>
          <button
            type="button"
            onClick={onRetry}
            className="mx-auto mt-3 inline-flex h-8 w-8 items-center justify-center rounded-btn text-secondary transition-colors hover:bg-elevated hover:text-accent"
            aria-label="重新读取能力矩阵"
            title="重新读取"
          >
            <RefreshCw className="h-4 w-4" />
          </button>
        </div>
      </div>
    )
  }

  if (!matrix?.available) {
    return (
      <div className="flex min-h-44 items-center justify-center rounded-card border border-border bg-surface px-6 py-10 text-center">
        <div>
          <Database className="mx-auto h-7 w-7 text-muted" />
          <div className="mt-3 text-sm font-medium text-foreground">尚无 Tushare 能力矩阵</div>
          <div className="mt-1 text-xs text-muted">运行时数据源：本地 Parquet</div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <SummaryCard
          icon={Database}
          label="数据集"
          value={formatNumber(datasets.length)}
          hint={`${summary.available} 个已通过发布检查`}
        />
        <SummaryCard
          icon={FileCheck2}
          label="已审计行数"
          value={formatNumber(summary.stagedRows)}
          hint="规范化 staging 口径"
        />
        <SummaryCard
          icon={CheckCircle2}
          label="因子输入"
          value={formatNumber(summary.factorInputs)}
          hint="已通过覆盖率与 PIT 校验"
        />
        <SummaryCard
          icon={AlertTriangle}
          label="异常批次"
          value={formatNumber(summary.issues)}
          hint="失败或未确认空数据"
          tone={summary.issues > 0 ? 'warning' : 'default'}
        />
      </div>

      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-y border-border px-1 py-2.5 text-[11px] text-muted">
        <span>
          RUN <span className="ml-1 font-mono text-secondary">{matrix.run_id ?? '—'}</span>
          {matrix.run_count > 1 ? <span className="ml-1 text-muted">· {matrix.run_count} 次回填汇总</span> : null}
        </span>
        <span>区间 <span className="ml-1 font-mono text-secondary">{matrix.history_start ?? '—'} → {matrix.history_end ?? '—'}</span></span>
        <span>更新 <span className="ml-1 font-mono text-secondary">{formatDateTime(matrix.generated_at)}</span></span>
        <span className="ml-auto inline-flex items-center gap-1.5 text-accent">
          <span className="h-1.5 w-1.5 rounded-full bg-accent" />
          本地 Parquet
        </span>
      </div>

      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="inline-flex w-fit rounded-btn border border-border bg-surface p-0.5" role="group" aria-label="能力矩阵筛选">
          {FILTERS.map(item => (
            <button
              key={item.value}
              type="button"
              onClick={() => setFilter(item.value)}
              className={`h-7 rounded-[4px] px-3 text-[11px] font-medium transition-colors ${
                filter === item.value ? 'bg-elevated text-foreground' : 'text-muted hover:text-secondary'
              }`}
            >
              {item.label}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] text-muted">{visibleDatasets.length} / {datasets.length}</span>
          <label className="relative block">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" />
            <input
              value={search}
              onChange={event => setSearch(event.target.value)}
              placeholder="搜索数据集"
              className="h-8 w-full rounded-input border border-border bg-surface pl-8 pr-3 text-xs text-foreground outline-none transition-colors placeholder:text-muted focus:border-accent sm:w-48"
            />
          </label>
        </div>
      </div>

      <div className="overflow-x-auto rounded-card border border-border bg-surface">
        <table className="w-full min-w-[780px] table-fixed text-left">
          <thead className="border-b border-border bg-elevated/50 text-[10px] font-medium text-muted">
            <tr>
              <th className="w-[24%] px-4 py-2.5">数据集</th>
              <th className="w-[10%] px-3 py-2.5">状态</th>
              <th className="w-[20%] px-3 py-2.5">日期覆盖</th>
              <th className="w-[10%] px-3 py-2.5 text-right">标的</th>
              <th className="w-[14%] px-3 py-2.5 text-right">审计 / 新增</th>
              <th className="w-[12%] px-3 py-2.5 text-right">字段完整度</th>
              <th className="w-[10%] px-4 py-2.5 text-right">批次</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {visibleDatasets.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-4 py-10 text-center text-xs text-muted">无匹配数据集</td>
              </tr>
            ) : visibleDatasets.map(([name, dataset]) => {
              const fields = Object.entries(dataset.field_non_null_rate ?? {}).sort((left, right) => left[1] - right[1])
              const completeness = fieldSummary(dataset)
              const open = expanded === name
              const issues = issueCount(dataset)
              return (
                <Fragment key={name}>
                  <tr className="transition-colors hover:bg-elevated/30">
                    <td className="px-4 py-3">
                      <button
                        type="button"
                        onClick={() => setExpanded(current => current === name ? null : name)}
                        className="flex w-full items-center gap-2 text-left"
                        aria-expanded={open}
                      >
                        <ChevronDown className={`h-3.5 w-3.5 shrink-0 text-muted transition-transform ${open ? 'rotate-180' : ''}`} />
                        <span className="min-w-0">
                          <span className="block truncate text-xs font-medium text-foreground">{DATASET_LABELS[name] ?? name}</span>
                          <span className="mt-0.5 block truncate font-mono text-[9px] text-muted">{name}</span>
                        </span>
                      </button>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex flex-wrap gap-1">
                        <StatusBadge status={dataset.status} />
                        {dataset.factor_input ? (
                          <span className="inline-flex rounded border border-accent/20 bg-accent/8 px-1.5 py-0.5 text-[10px] font-medium text-accent">因子</span>
                        ) : null}
                      </div>
                    </td>
                    <td className="px-3 py-3 font-mono text-[10px] text-secondary">
                      {dataset.min_date ?? '—'} <span className="text-muted">→</span> {dataset.max_date ?? '—'}
                    </td>
                    <td className="px-3 py-3 text-right font-mono text-xs tabular-nums text-secondary">{formatNumber(dataset.symbols)}</td>
                    <td className="px-3 py-3 text-right font-mono text-[11px] tabular-nums text-secondary">
                      {formatNumber(dataset.staged_rows)} <span className="text-muted">/</span> {formatNumber(dataset.published_rows)}
                    </td>
                    <td className="px-3 py-3 text-right font-mono text-[11px] tabular-nums text-secondary">
                      {completeness.average == null ? '—' : `${Math.round(completeness.average * 100)}%`}
                      {completeness.empty > 0 ? <span className="ml-1 text-warning">· {completeness.empty} 空</span> : null}
                    </td>
                    <td className="px-4 py-3 text-right font-mono text-[11px] tabular-nums text-secondary">
                      {formatNumber(dataset.batches)}
                      {issues > 0 ? <span className="ml-1 text-warning">· {issues}</span> : null}
                    </td>
                  </tr>
                  {open ? (
                    <tr className="bg-elevated/20">
                      <td colSpan={7} className="px-8 py-4">
                        <div className="mb-3 flex flex-wrap items-center gap-x-5 gap-y-1 text-[10px] text-muted">
                          <span>逻辑日期 <span className="ml-1 font-mono text-secondary">{dataset.logical_date ?? '—'}</span></span>
                          <span>主键 <span className="ml-1 font-mono text-secondary">{dataset.primary_key?.join(' + ') || '—'}</span></span>
                        </div>
                        {fields.length > 0 ? (
                          <div className="grid grid-cols-1 gap-x-6 gap-y-2 sm:grid-cols-2 xl:grid-cols-3">
                            {fields.map(([field, rate]) => {
                              const percent = Math.max(0, Math.min(100, Math.round(rate * 100)))
                              return (
                                <div key={field} className="grid grid-cols-[minmax(0,1fr)_72px_34px] items-center gap-2 text-[10px]">
                                  <span className="truncate font-mono text-secondary" title={field}>{field}</span>
                                  <span className="h-1.5 overflow-hidden rounded-full bg-border">
                                    <span
                                      className={`block h-full rounded-full ${percent === 0 ? 'bg-danger' : percent < 80 ? 'bg-warning' : 'bg-accent'}`}
                                      style={{ width: `${percent}%` }}
                                    />
                                  </span>
                                  <span className={`text-right font-mono tabular-nums ${percent === 0 ? 'text-danger' : 'text-muted'}`}>{percent}%</span>
                                </div>
                              )
                            })}
                          </div>
                        ) : (
                          <div className="text-[11px] text-muted">暂无字段覆盖统计</div>
                        )}
                        {issues > 0 ? (
                          <div className="mt-4 border-t border-border pt-3 text-[10px] text-warning">
                            {[...(dataset.failed_batches ?? []), ...(dataset.empty_unconfirmed_batches ?? [])].join(' · ')}
                          </div>
                        ) : null}
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-1.5 text-[10px] text-muted">
        <BarChart3 className="h-3.5 w-3.5" />
        字段完整度按非空率统计
      </div>
    </div>
  )
}
