import { useMemo, useState } from 'react'
import { Activity, CalendarDays, ChartNoAxesCombined, ClipboardList, ScrollText } from 'lucide-react'
import type { FreeBacktestResult } from '@/lib/api'
import { FiveFortunesProcessChart, type FiveFortunesDailyReport } from './charts/FiveFortunesProcessChart'
import { FreeStrategyDailyReturnChart } from './charts/FreeStrategyDailyReturnChart'
import { FreeStrategyPerformanceChart } from './charts/FreeStrategyPerformanceChart'

type ResultTab = 'performance' | 'orders' | 'daily' | 'decisions' | 'logs'

const TABS: Array<{ id: ResultTab; label: string; icon: typeof Activity }> = [
  { id: 'performance', label: '绩效', icon: ChartNoAxesCombined },
  { id: 'orders', label: '订单与成交', icon: ClipboardList },
  { id: 'daily', label: '逐日资产', icon: CalendarDays },
  { id: 'decisions', label: '每日决策', icon: Activity },
  { id: 'logs', label: '策略日志', icon: ScrollText },
]

const DECISION_LABELS: Record<string, string> = {
  pending: '待决策',
  ranked_target: '动量排名',
  anti_churn_hold: '防频换持有',
  regime_change_hold: '状态切换日保留',
  low_correlation_switch: '低相关换仓',
  high_correlation_hold: '高相关保留',
  high_pair_overlay: '高相关覆盖拦截',
  correlation_hold_guard: '相关性守卫拦截',
  rebuy_cooldown_fallback: '买回冷却备选',
  no_candidate_defensive: '无候选转防御',
  four_day_filter_fail_defensive: '连续过滤失败转防御',
  drawdown_flat: '回撤清仓',
  drawdown_defensive: '回撤转防御',
}

const SIDE_LABELS: Record<string, string> = {
  buy: '买入',
  sell: '卖出',
  target: '目标仓位',
}

const STATUS_LABELS: Record<string, string> = {
  filled: '已成交',
  rejected: '已拒绝',
  skipped: '已跳过',
  pending: '待成交',
}

function number(value: unknown, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits })
    : '—'
}

function percent(value: unknown, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value) ? `${number(value, digits)}%` : '—'
}

function dateTime(value: unknown) {
  return typeof value === 'string' && value ? value.replace('T', ' ') : '—'
}

function side(value: unknown) {
  const key = String(value ?? '')
  return (SIDE_LABELS[key] ?? key) || '—'
}

function status(value: unknown) {
  const key = String(value ?? '')
  return (STATUS_LABELS[key] ?? key) || '—'
}

function statusTone(value: unknown) {
  return value === 'filled' ? 'text-success' : value === 'rejected' ? 'text-danger' : 'text-warning'
}

function sideTone(value: unknown) {
  return value === 'buy' ? 'text-success' : value === 'sell' ? 'text-danger' : 'text-foreground'
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: 'positive' | 'negative' }) {
  const color = tone === 'positive' ? 'text-success' : tone === 'negative' ? 'text-danger' : 'text-foreground'
  return <div className="min-w-0 border-b border-border px-3 py-3 sm:border-r"><div className="text-[10px] text-muted">{label}</div><div className={`mt-1 truncate text-sm font-semibold tabular-nums ${color}`}>{value}</div></div>
}

function TableWrap({ children }: { children: React.ReactNode }) {
  return <div className="overflow-x-auto">{children}</div>
}

function PerformanceView({ result }: { result: FreeBacktestResult }) {
  const performance = result.performance ?? {}
  const drawdownPeriod = performance.max_drawdown_start && performance.max_drawdown_end
    ? `${performance.max_drawdown_start} 至 ${performance.max_drawdown_end}`
    : '—'
  return <div>
    <div className="grid grid-cols-2 border-l border-t border-border md:grid-cols-4 xl:grid-cols-6">
      <Metric label="期末资产" value={number(result.final_equity)} />
      <Metric label="累计收益" value={percent(result.return_pct)} tone={result.return_pct >= 0 ? 'positive' : 'negative'} />
      <Metric label="年化收益" value={percent(performance.annual_return_pct)} />
      <Metric label="基准收益" value={percent(performance.benchmark_return_pct)} />
      <Metric label="超额收益" value={percent(performance.excess_return_pct)} />
      <Metric label="最大回撤" value={percent(result.max_drawdown_pct)} tone="negative" />
      <Metric label="Alpha" value={percent(performance.alpha_pct)} />
      <Metric label="Beta" value={number(performance.beta, 3)} />
      <Metric label="Sharpe" value={number(performance.sharpe_ratio, 3)} />
      <Metric label="Sortino" value={number(performance.sortino_ratio, 3)} />
      <Metric label="信息比率" value={number(performance.information_ratio, 3)} />
      <Metric label="换手率" value={percent(performance.turnover_pct)} />
    </div>
    {result.daily_equity_curve?.length ? <div className="mt-4 border-y border-border py-3"><FreeStrategyPerformanceChart result={result} /></div> : null}
    <div className="mt-4 grid grid-cols-2 gap-x-6 gap-y-2 text-[11px] lg:grid-cols-4">
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">策略波动率</span><span>{percent(performance.volatility_pct)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">基准波动率</span><span>{percent(performance.benchmark_volatility_pct)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">日胜率</span><span>{percent(performance.positive_day_rate_pct)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">交易胜率</span><span>{percent(performance.trade_win_rate_pct)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">盈亏比</span><span>{number(performance.profit_loss_ratio, 3)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">成交笔数</span><span>{number(performance.trade_count, 0)}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">回撤区间</span><span>{drawdownPeriod}</span></div>
      <div className="flex justify-between border-b border-border py-1.5"><span className="text-muted">交易日</span><span>{result.daily_equity_curve?.length ?? 0}</span></div>
    </div>
  </div>
}

function OrdersView({ result }: { result: FreeBacktestResult }) {
  const transactions = result.transactions ?? result.orders
  return <div className="space-y-5">
    <div>
      <div className="mb-2 text-xs font-medium">订单事务 <span className="font-normal text-muted">{transactions.length}</span></div>
      <div className="divide-y divide-border border-y border-border md:hidden">
        {transactions.map((row, index) => <article key={String(row.transaction_id ?? row.id ?? index)} className="py-3 [contain-intrinsic-size:0_132px] [content-visibility:auto]">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0 text-[10px] tabular-nums text-muted"><span>提交</span><div className="mt-0.5 text-foreground">{dateTime(row.submitted_at)}</div></div>
            <span className={`shrink-0 text-[11px] font-medium ${statusTone(row.status)}`}>{status(row.status)}</span>
          </div>
          <div className="mt-2 flex min-w-0 items-center justify-between gap-3 text-[11px]">
            <span className="truncate font-mono text-xs font-medium">{String(row.symbol ?? '—')}</span>
            <span className="shrink-0 text-muted">{side(row.requested_side ?? row.side)} <span className="px-1">→</span> <span className={sideTone(row.executed_side)}>{side(row.executed_side)}</span></span>
          </div>
          <dl className="mt-2 grid grid-cols-3 gap-2 text-[10px]">
            <div><dt className="text-muted">成交数量</dt><dd className="mt-0.5 tabular-nums">{number(row.filled_quantity, 0)}</dd></div>
            <div><dt className="text-muted">成交均价</dt><dd className="mt-0.5 tabular-nums">{number(row.average_fill_price, 4)}</dd></div>
            <div><dt className="text-muted">费用</dt><dd className="mt-0.5 tabular-nums">{number(row.fee, 2)}</dd></div>
          </dl>
          {row.reason ? <div className="mt-2 break-words text-[10px] text-danger">{String(row.reason)}</div> : null}
        </article>)}
      </div>
      <div className="hidden md:block"><TableWrap><table className="w-full min-w-[980px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">提交时间</th><th>标的</th><th>委托</th><th>成交方向</th><th>成交数量</th><th>均价</th><th>费用</th><th>状态</th><th>原因</th></tr></thead><tbody>{transactions.map((row, index) => <tr key={String(row.transaction_id ?? row.id ?? index)} className="border-t border-border"><td className="whitespace-nowrap py-2">{dateTime(row.submitted_at)}</td><td className="font-mono">{String(row.symbol ?? '')}</td><td>{side(row.requested_side ?? row.side)}</td><td className={sideTone(row.executed_side)}>{side(row.executed_side)}</td><td className="tabular-nums">{number(row.filled_quantity, 0)}</td><td className="tabular-nums">{number(row.average_fill_price, 4)}</td><td className="tabular-nums">{number(row.fee, 2)}</td><td><span className={statusTone(row.status)}>{status(row.status)}</span></td><td className="max-w-64 truncate" title={String(row.reason ?? '')}>{String(row.reason || '—')}</td></tr>)}</tbody></table></TableWrap></div>
    </div>
    <div>
      <div className="mb-2 text-xs font-medium">成交与归因 <span className="font-normal text-muted">{result.fills.length}</span></div>
      <div className="divide-y divide-border border-y border-border md:hidden">
        {result.fills.map((fill, index) => { const attribution = result.attribution?.[index]; const pnl = Number(attribution?.realized_pnl ?? 0); return <article key={`${fill.order_id}-${index}`} className="py-3 [contain-intrinsic-size:0_124px] [content-visibility:auto]">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0 text-[10px] tabular-nums text-muted"><span>成交</span><div className="mt-0.5 text-foreground">{dateTime(fill.timestamp)}</div></div>
            <span className={`shrink-0 text-[11px] font-medium ${sideTone(fill.side)}`}>{side(fill.side)}</span>
          </div>
          <div className="mt-2 font-mono text-xs font-medium">{String(fill.symbol ?? '—')}</div>
          <dl className="mt-2 grid grid-cols-3 gap-x-2 gap-y-2 text-[10px]">
            <div><dt className="text-muted">数量</dt><dd className="mt-0.5 tabular-nums">{number(fill.quantity, 0)}</dd></div>
            <div><dt className="text-muted">价格</dt><dd className="mt-0.5 tabular-nums">{number(fill.price, 4)}</dd></div>
            <div><dt className="text-muted">成交额</dt><dd className="mt-0.5 tabular-nums">{number(fill.value, 2)}</dd></div>
            <div><dt className="text-muted">费用</dt><dd className="mt-0.5 tabular-nums">{number(fill.fee, 2)}</dd></div>
            <div><dt className="text-muted">已实现盈亏</dt><dd className={`mt-0.5 tabular-nums ${pnl > 0 ? 'text-success' : pnl < 0 ? 'text-danger' : ''}`}>{number(pnl, 2)}</dd></div>
            <div><dt className="text-muted">收益率</dt><dd className="mt-0.5 tabular-nums">{percent(attribution?.realized_return_pct)}</dd></div>
          </dl>
        </article> })}
      </div>
      <div className="hidden md:block"><TableWrap><table className="w-full min-w-[860px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">成交时间</th><th>标的</th><th>方向</th><th>数量</th><th>价格</th><th>成交额</th><th>费用</th><th>已实现盈亏</th><th>收益率</th></tr></thead><tbody>{result.fills.map((fill, index) => { const attribution = result.attribution?.[index]; const pnl = Number(attribution?.realized_pnl ?? 0); return <tr key={`${fill.order_id}-${index}`} className="border-t border-border"><td className="whitespace-nowrap py-2">{dateTime(fill.timestamp)}</td><td className="font-mono">{fill.symbol}</td><td className={sideTone(fill.side)}>{side(fill.side)}</td><td>{number(fill.quantity, 0)}</td><td>{number(fill.price, 4)}</td><td>{number(fill.value, 2)}</td><td>{number(fill.fee, 2)}</td><td className={pnl > 0 ? 'text-success' : pnl < 0 ? 'text-danger' : ''}>{number(pnl, 2)}</td><td>{percent(attribution?.realized_return_pct)}</td></tr> })}</tbody></table></TableWrap></div>
    </div>
  </div>
}

function DailyView({ result }: { result: FreeBacktestResult }) {
  const rows = result.daily_equity_curve ?? []
  return <div className="space-y-4"><div className="border-b border-border pb-3"><FreeStrategyDailyReturnChart result={result} /></div><TableWrap><table className="w-full min-w-[1040px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">日期</th><th>总资产</th><th>现金</th><th>仓位</th><th>日收益</th><th>基准日收益</th><th>超额</th><th>回撤</th><th>持仓</th></tr></thead><tbody>{rows.map(row => <tr key={row.date} className="border-t border-border"><td className="whitespace-nowrap py-2">{row.date}</td><td>{number(row.equity)}</td><td>{number(row.cash)}</td><td>{percent(row.exposure_pct, 1)}</td><td>{percent(row.daily_return_pct)}</td><td>{percent(row.benchmark_daily_return_pct)}</td><td>{percent(row.excess_daily_return_pct)}</td><td className="text-danger">{percent(row.drawdown_pct)}</td><td className="max-w-96 font-mono text-[10px]">{Object.entries(row.positions).filter(([, quantity]) => quantity > 0).map(([symbol, quantity]) => `${symbol} ${number(quantity, 0)}`).join(' · ') || '空仓'}</td></tr>)}</tbody></table></TableWrap></div>
}

function DecisionsView({ reports, fills }: { reports: FiveFortunesDailyReport[]; fills: Record<string, any>[] }) {
  const activity = useMemo(() => {
    const byDay = new Map<string, { buy: number; sell: number }>()
    for (const fill of fills) {
      const day = String(fill.timestamp ?? '').slice(0, 10)
      const counts = byDay.get(day) ?? { buy: 0, sell: 0 }
      if (fill.side === 'buy') counts.buy += 1
      if (fill.side === 'sell') counts.sell += 1
      byDay.set(day, counts)
    }
    return byDay
  }, [fills])
  return <div className="space-y-4"><div className="border-b border-border pb-3"><FiveFortunesProcessChart reports={reports} fills={fills} /></div><TableWrap><table className="w-full min-w-[1360px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">日期</th><th>状态</th><th>决策</th><th>目标持仓</th><th>实际持仓</th><th>实际成交</th><th>候选</th><th>过滤</th><th>流动性池</th><th>风控</th></tr></thead><tbody>{reports.map(report => { const trades = activity.get(report.date) ?? { buy: 0, sell: 0 }; return <tr key={report.date} className="border-t border-border align-top"><td className="whitespace-nowrap py-2">{report.date}</td><td><div>{report.regime}</div>{report.raw_regime && report.raw_regime !== report.regime ? <div className="text-[10px] text-muted">原始 {report.raw_regime}</div> : null}</td><td>{DECISION_LABELS[report.decision?.reason ?? ''] ?? report.decision?.reason ?? '—'}</td><td className="font-mono">{report.target.join(', ') || '空仓'}</td><td className="font-mono">{report.holdings?.join(', ') || '空仓'}</td><td className="whitespace-nowrap"><span className="text-success">买 {trades.buy}</span><span className="ml-2 text-danger">卖 {trades.sell}</span></td><td className="max-w-80"><div>{report.candidate_count ?? report.candidates.length} 个</div><div className="mt-1 text-[10px] text-muted">{report.candidates.map(item => `${item.symbol}${item.score == null ? '' : ` ${number(item.score, 2)}`}`).join(' · ') || '—'}</div></td><td className="max-w-64 text-[10px]">{Object.entries(report.filter_rejections ?? {}).map(([key, count]) => `${key} ${count}`).join(' · ') || '—'}</td><td>{report.liquidity_pool_count ?? '—'}</td><td>{report.risk_action ? `${report.risk_action.action ?? ''} ${percent(Number(report.risk_action.drawdown ?? 0) * 100)}` : '—'}</td></tr> })}</tbody></table></TableWrap></div>
}

function LogsView({ result }: { result: FreeBacktestResult }) {
  return <div className="divide-y divide-border font-mono text-[11px]">{result.logs.map((log, index) => <div key={`${log.timestamp}-${index}`} className="grid gap-1 py-2 sm:grid-cols-[150px_54px_1fr]"><span className="text-muted">{log.timestamp}</span><span className={log.level === 'ERROR' ? 'text-danger' : 'text-secondary'}>{log.level}</span><span className="whitespace-pre-wrap break-words">{log.message}</span></div>)}{!result.logs.length && <div className="py-8 text-center font-sans text-muted">无策略日志</div>}</div>
}

export function FreeStrategyResult({ result, title }: { result: FreeBacktestResult; title?: string }) {
  const [tab, setTab] = useState<ResultTab>('performance')
  const reports = useMemo(() => (
    (result.state?.five_fortunes?.daily_reports ?? []) as FiveFortunesDailyReport[]
  ), [result])
  const metadata = result.metadata ?? {}
  const coverage = metadata.data_coverage as Record<string, any> | undefined
  const requestedCount = Array.isArray(coverage?.requested_symbols) ? coverage.requested_symbols.length : Number(metadata.symbol_count ?? 0)
  const seenCount = Array.isArray(coverage?.seen_symbols) ? coverage.seen_symbols.length : requestedCount
  const executionMode = metadata.execution_mode === 'scheduled' ? '定时执行' : metadata.execution_mode === 'full_bar' ? '完整回放' : ''
  return <section className="shrink-0 overflow-hidden rounded-md border border-border bg-surface">
    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5"><div><div className="text-xs font-medium">{title || '回测结果'}</div><div className="mt-0.5 text-[10px] text-muted">{String(metadata.strategy_name ?? '')}{metadata.start ? ` · ${metadata.start} 至 ${metadata.end}` : ''}</div></div><div className="flex flex-wrap gap-1 text-[10px] text-muted"><span>{String(metadata.asset_type ?? '').toUpperCase()} {String(metadata.timeframe ?? '')}</span>{executionMode ? <><span>·</span><span>{executionMode}</span></> : null}<span>·</span><span>{Number(metadata.data_days ?? result.daily_equity_curve?.length ?? 0)} 个交易日</span>{metadata.nav_filter === 'skipped_no_data' ? <><span>·</span><span>NAV 过滤已跳过</span></> : null}</div></div>
    {coverage ? <div className="flex flex-wrap items-center gap-x-2 gap-y-1 border-b border-border px-3 py-2 text-[10px] text-muted"><span>数据源 {String(coverage.configured_provider ?? '—')}</span><span>表 {String(coverage.storage ?? '—')}</span><span>{number(coverage.rows, 0)} {metadata.execution_mode === 'scheduled' ? '次行情读取' : '根 bar'}</span>{metadata.execution_mode === 'scheduled' ? <span>{number(metadata.callbacks_executed, 0)} 次定时回调</span> : null}<span>{String(coverage.first_bar ?? '—')} 至 {String(coverage.last_bar ?? '—')}</span><span className={seenCount === requestedCount ? '' : 'text-danger'}>{seenCount}/{requestedCount} 标的</span>{Array.isArray(coverage.missing_symbols) && coverage.missing_symbols.length ? <span className="text-danger">缺失 {coverage.missing_symbols.join(', ')}</span> : null}</div> : null}
    <div className="flex overflow-x-auto border-b border-border px-2" role="tablist">{TABS.map(item => { const Icon = item.icon; const active = item.id === tab; return <button key={item.id} type="button" role="tab" aria-selected={active} onClick={() => setTab(item.id)} className={`inline-flex h-10 shrink-0 items-center gap-1.5 border-b-2 px-3 text-[11px] transition-colors ${active ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><Icon className="h-3.5 w-3.5" />{item.label}{item.id === 'decisions' && reports.length ? <span className="tabular-nums">{reports.length}</span> : null}</button> })}</div>
    <div className="p-3">{tab === 'performance' && <PerformanceView result={result} />}{tab === 'orders' && <OrdersView result={result} />}{tab === 'daily' && <DailyView result={result} />}{tab === 'decisions' && <DecisionsView reports={reports} fills={result.fills} />}{tab === 'logs' && <LogsView result={result} />}</div>
  </section>
}
