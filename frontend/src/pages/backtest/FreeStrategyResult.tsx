import { useMemo, useState } from 'react'
import { Activity, CalendarDays, ChartNoAxesCombined, ClipboardList, ScrollText } from 'lucide-react'
import type { FreeBacktestResult } from '@/lib/api'
import { FreeStrategyPerformanceChart } from './charts/FreeStrategyPerformanceChart'

type ResultTab = 'performance' | 'orders' | 'daily' | 'decisions' | 'logs'

interface DailyReport {
  date: string
  regime: string
  raw_regime?: string
  regime_changed?: boolean
  target: string[]
  candidates: Array<{ symbol: string; score?: number }>
  filtered_count?: number
  candidate_count?: number
  liquidity_pool_count?: number
  filter_rejections?: Record<string, number>
  decision?: { reason?: string; held?: string; held_rank?: number; filter_fail_symbols?: string[] }
  risk_action?: { action?: string; drawdown?: number } | null
}

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

function number(value: unknown, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits })
    : '—'
}

function percent(value: unknown, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value) ? `${number(value, digits)}%` : '—'
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
    <div><div className="mb-2 text-xs font-medium">订单事务 <span className="font-normal text-muted">{transactions.length}</span></div><TableWrap><table className="w-full min-w-[980px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">提交时间</th><th>标的</th><th>委托</th><th>成交方向</th><th>成交数量</th><th>均价</th><th>费用</th><th>状态</th><th>原因</th></tr></thead><tbody>{transactions.map((row, index) => <tr key={String(row.transaction_id ?? row.id ?? index)} className="border-t border-border"><td className="whitespace-nowrap py-2">{String(row.submitted_at ?? '')}</td><td className="font-mono">{String(row.symbol ?? '')}</td><td>{String(row.requested_side ?? row.side ?? '')}</td><td>{String(row.executed_side ?? '—')}</td><td className="tabular-nums">{number(row.filled_quantity, 0)}</td><td className="tabular-nums">{number(row.average_fill_price, 4)}</td><td className="tabular-nums">{number(row.fee, 2)}</td><td><span className={row.status === 'filled' ? 'text-success' : row.status === 'rejected' ? 'text-danger' : 'text-warning'}>{String(row.status ?? '')}</span></td><td className="max-w-64 truncate" title={String(row.reason ?? '')}>{String(row.reason || '—')}</td></tr>)}</tbody></table></TableWrap></div>
    <div><div className="mb-2 text-xs font-medium">成交与归因 <span className="font-normal text-muted">{result.fills.length}</span></div><TableWrap><table className="w-full min-w-[860px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">成交时间</th><th>标的</th><th>方向</th><th>数量</th><th>价格</th><th>成交额</th><th>费用</th><th>已实现盈亏</th><th>收益率</th></tr></thead><tbody>{result.fills.map((fill, index) => { const attribution = result.attribution?.[index]; const pnl = Number(attribution?.realized_pnl ?? 0); return <tr key={`${fill.order_id}-${index}`} className="border-t border-border"><td className="whitespace-nowrap py-2">{fill.timestamp}</td><td className="font-mono">{fill.symbol}</td><td className={fill.side === 'buy' ? 'text-success' : 'text-danger'}>{fill.side}</td><td>{number(fill.quantity, 0)}</td><td>{number(fill.price, 4)}</td><td>{number(fill.value, 2)}</td><td>{number(fill.fee, 2)}</td><td className={pnl > 0 ? 'text-success' : pnl < 0 ? 'text-danger' : ''}>{number(pnl, 2)}</td><td>{percent(attribution?.realized_return_pct)}</td></tr> })}</tbody></table></TableWrap></div>
  </div>
}

function DailyView({ result }: { result: FreeBacktestResult }) {
  const rows = result.daily_equity_curve ?? []
  return <TableWrap><table className="w-full min-w-[1040px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">日期</th><th>总资产</th><th>现金</th><th>仓位</th><th>日收益</th><th>基准日收益</th><th>超额</th><th>回撤</th><th>持仓</th></tr></thead><tbody>{rows.map(row => <tr key={row.date} className="border-t border-border"><td className="whitespace-nowrap py-2">{row.date}</td><td>{number(row.equity)}</td><td>{number(row.cash)}</td><td>{percent(row.exposure_pct, 1)}</td><td>{percent(row.daily_return_pct)}</td><td>{percent(row.benchmark_daily_return_pct)}</td><td>{percent(row.excess_daily_return_pct)}</td><td className="text-danger">{percent(row.drawdown_pct)}</td><td className="max-w-96 font-mono text-[10px]">{Object.entries(row.positions).filter(([, quantity]) => quantity > 0).map(([symbol, quantity]) => `${symbol} ${number(quantity, 0)}`).join(' · ') || '空仓'}</td></tr>)}</tbody></table></TableWrap>
}

function DecisionsView({ reports }: { reports: DailyReport[] }) {
  return <TableWrap><table className="w-full min-w-[1180px] text-[11px]"><thead className="text-left text-muted"><tr><th className="pb-2">日期</th><th>状态</th><th>决策</th><th>目标</th><th>候选</th><th>过滤</th><th>流动性池</th><th>风控</th></tr></thead><tbody>{reports.map(report => <tr key={report.date} className="border-t border-border align-top"><td className="whitespace-nowrap py-2">{report.date}</td><td><div>{report.regime}</div>{report.raw_regime && report.raw_regime !== report.regime ? <div className="text-[10px] text-muted">原始 {report.raw_regime}</div> : null}</td><td>{DECISION_LABELS[report.decision?.reason ?? ''] ?? report.decision?.reason ?? '—'}</td><td className="font-mono">{report.target.join(', ') || '空仓'}</td><td className="max-w-80"><div>{report.candidate_count ?? report.candidates.length} 个</div><div className="mt-1 text-[10px] text-muted">{report.candidates.map(item => `${item.symbol}${item.score == null ? '' : ` ${number(item.score, 2)}`}`).join(' · ') || '—'}</div></td><td className="max-w-64 text-[10px]">{Object.entries(report.filter_rejections ?? {}).map(([key, count]) => `${key} ${count}`).join(' · ') || '—'}</td><td>{report.liquidity_pool_count ?? '—'}</td><td>{report.risk_action ? `${report.risk_action.action ?? ''} ${percent(Number(report.risk_action.drawdown ?? 0) * 100)}` : '—'}</td></tr>)}</tbody></table></TableWrap>
}

function LogsView({ result }: { result: FreeBacktestResult }) {
  return <div className="divide-y divide-border font-mono text-[11px]">{result.logs.map((log, index) => <div key={`${log.timestamp}-${index}`} className="grid gap-1 py-2 sm:grid-cols-[150px_54px_1fr]"><span className="text-muted">{log.timestamp}</span><span className={log.level === 'ERROR' ? 'text-danger' : 'text-secondary'}>{log.level}</span><span className="whitespace-pre-wrap break-words">{log.message}</span></div>)}{!result.logs.length && <div className="py-8 text-center font-sans text-muted">无策略日志</div>}</div>
}

export function FreeStrategyResult({ result }: { result: FreeBacktestResult }) {
  const [tab, setTab] = useState<ResultTab>('performance')
  const reports = useMemo(() => (
    (result.state?.five_fortunes?.daily_reports ?? []) as DailyReport[]
  ), [result])
  const metadata = result.metadata ?? {}
  return <section className="shrink-0 overflow-hidden rounded-md border border-border bg-surface">
    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2.5"><div><div className="text-xs font-medium">回测结果</div><div className="mt-0.5 text-[10px] text-muted">{String(metadata.strategy_name ?? '')}{metadata.source_revision ? ` · 修订 ${metadata.source_revision}` : ''}{metadata.start ? ` · ${metadata.start} 至 ${metadata.end}` : ''}</div></div><div className="flex flex-wrap gap-1 text-[10px] text-muted"><span>{String(metadata.asset_type ?? '').toUpperCase()} {String(metadata.timeframe ?? '')}</span><span>·</span><span>{Number(metadata.data_days ?? result.daily_equity_curve?.length ?? 0)} 个交易日</span>{metadata.nav_filter === 'skipped_no_data' ? <><span>·</span><span>NAV 过滤已跳过</span></> : null}</div></div>
    <div className="flex overflow-x-auto border-b border-border px-2" role="tablist">{TABS.map(item => { const Icon = item.icon; const active = item.id === tab; return <button key={item.id} type="button" role="tab" aria-selected={active} onClick={() => setTab(item.id)} className={`inline-flex h-10 shrink-0 items-center gap-1.5 border-b-2 px-3 text-[11px] transition-colors ${active ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}><Icon className="h-3.5 w-3.5" />{item.label}{item.id === 'decisions' && reports.length ? <span className="tabular-nums">{reports.length}</span> : null}</button> })}</div>
    <div className="p-3">{tab === 'performance' && <PerformanceView result={result} />}{tab === 'orders' && <OrdersView result={result} />}{tab === 'daily' && <DailyView result={result} />}{tab === 'decisions' && <DecisionsView reports={reports} />}{tab === 'logs' && <LogsView result={result} />}</div>
  </section>
}
