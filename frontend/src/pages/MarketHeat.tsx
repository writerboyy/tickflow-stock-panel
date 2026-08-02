import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { EChartsOption } from 'echarts'
import {
  AlertTriangle,
  ArrowDownUp,
  Flame,
  Info,
  Loader2,
  RefreshCw,
  Sparkles,
  TrendingUp,
} from 'lucide-react'
import { EmptyState } from '@/components/EmptyState'
import { PageHeader } from '@/components/PageHeader'
import { api, type MarketHeatItem, type MarketHeatListKey, type MarketHeatRadar } from '@/lib/api'
import { cn } from '@/lib/cn'
import { QK } from '@/lib/queryKeys'
import { useChartTheme } from '@/lib/theme'
import { useECharts } from './backtest/charts/useECharts'

const VIEW_CONFIG: Array<{
  key: MarketHeatListKey
  label: string
  shortLabel: string
  description: string
  icon: typeof Flame
  tone: string
}> = [
  {
    key: 'hot_day',
    label: '热股榜 · 24小时',
    shortLabel: '热股 24H',
    description: '同花顺当前热股榜，day 表示 24 小时榜。',
    icon: Flame,
    tone: 'from-orange-500/20 to-rose-500/10 text-orange-300',
  },
  {
    key: 'hot_hour',
    label: '热股榜 · 小时',
    shortLabel: '热股 小时',
    description: '同花顺当前热股榜小时周期。',
    icon: Flame,
    tone: 'from-amber-500/20 to-orange-500/10 text-amber-300',
  },
  {
    key: 'skyrocket_day',
    label: '飙升榜 · 24小时',
    shortLabel: '飙升 24H',
    description: '同花顺飙升榜，与热股榜排名逻辑不同。',
    icon: Sparkles,
    tone: 'from-cyan-500/20 to-blue-500/10 text-cyan-300',
  },
  {
    key: 'skyrocket_hour',
    label: '飙升榜 · 小时',
    shortLabel: '飙升 小时',
    description: '同花顺飙升榜小时周期。',
    icon: Sparkles,
    tone: 'from-sky-500/20 to-violet-500/10 text-sky-300',
  },
]

function fmtNumber(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(Number(value))) return '--'
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits })
}

function fmtPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return '--'
  return `${(Number(value) * 100).toFixed(1)}%`
}

function fmtDateTime(value: string | null | undefined) {
  if (!value) return '--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function rankChangeClass(value: number | null | undefined) {
  if (value == null || value === 0) return 'text-muted'
  return value > 0 ? 'text-sky-400' : 'text-amber-400'
}

function fmtRankChange(value: number | null | undefined) {
  if (value == null) return '--'
  if (value > 0) return `+${value}`
  return String(value)
}

function trendLabel(value: string | null | undefined) {
  const text = String(value || '').trim()
  if (!text) return '--'
  const lower = text.toLowerCase()
  if (['up', 'rise', 'rising', 'improving'].includes(lower)) return '上行'
  if (['down', 'fall', 'falling', 'weakening'].includes(lower)) return '下行'
  if (['flat', 'stable'].includes(lower)) return '持平'
  return text
}

function directionMeta(direction: string | null | undefined) {
  if (direction === 'improving') {
    return { label: '名次改善', cls: 'text-sky-300 bg-sky-400/10 border-sky-400/25' }
  }
  if (direction === 'weakening') {
    return { label: '名次回落', cls: 'text-amber-300 bg-amber-400/10 border-amber-400/25' }
  }
  if (direction === 'flat') {
    return { label: '区间持平', cls: 'text-zinc-300 bg-zinc-400/10 border-zinc-400/20' }
  }
  return { label: '样本不足', cls: 'text-muted bg-elevated border-border' }
}

function quantile(values: number[], q: number) {
  if (!values.length) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)))
  return sorted[index]
}

function SummaryCard({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className="rounded-xl border border-border bg-surface/80 px-4 py-3">
      <div className="text-[11px] text-muted">{label}</div>
      <div className="mt-1 text-xl font-semibold text-foreground">{value}</div>
      <div className="mt-1 text-[11px] text-secondary">{hint}</div>
    </div>
  )
}

function RankingTable({ items }: { items: MarketHeatItem[] }) {
  return (
    <div className="overflow-x-auto rounded-xl border border-border bg-surface/80">
      <table className="min-w-[760px] w-full text-sm">
        <thead className="bg-elevated/70 text-xs text-muted">
          <tr>
            <th className="w-16 px-3 py-2 text-left font-medium">排名</th>
            <th className="px-3 py-2 text-left font-medium">股票</th>
            <th className="px-3 py-2 text-right font-medium">热度</th>
            <th className="px-3 py-2 text-right font-medium">排名变化</th>
            <th className="px-3 py-2 text-left font-medium">趋势方向</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {items.map(item => (
            <tr key={item.thscode} className="transition-colors hover:bg-elevated/40">
              <td className="px-3 py-2 font-mono text-foreground">#{item.rank ?? '--'}</td>
              <td className="px-3 py-2">
                <div className="font-medium text-foreground">{item.name || '--'}</div>
                <div className="font-mono text-[11px] text-muted">{item.thscode}</div>
              </td>
              <td className="px-3 py-2 text-right font-mono text-foreground">{fmtNumber(item.heat)}</td>
              <td className={cn('px-3 py-2 text-right font-mono', rankChangeClass(item.rank_change))}>
                {fmtRankChange(item.rank_change)}
              </td>
              <td className="px-3 py-2">
                <span className="inline-flex rounded-full border border-border bg-elevated/60 px-2 py-0.5 text-xs text-secondary">
                  {trendLabel(item.rank_trend)}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TrendChart({ data }: { data: MarketHeatRadar }) {
  const ct = useChartTheme()
  const trends = useMemo(() => Object.values(data.trends), [data.trends])
  const option = useMemo<EChartsOption | null>(() => {
    const dates = Array.from(
      new Set(trends.flatMap(trend => trend.points.map(point => point.date).filter(Boolean))),
    ).sort()
    if (!dates.length || !trends.length) return null
    const ranks = trends
      .flatMap(trend => trend.points.map(point => point.rank))
      .filter((rank): rank is number => rank != null && Number.isFinite(rank))
    if (!ranks.length) return null
    const rawMax = Math.max(...ranks)
    const rawMin = Math.min(...ranks)
    const focusCap = rawMax > 80
      ? Math.max(42, Math.min(80, Math.ceil((quantile(ranks, 0.72) + 8) / 4) * 4))
      : Math.ceil((rawMax + 4) / 4) * 4
    const yMin = Math.max(1, Math.floor(rawMin - 2))
    const yMax = Math.max(focusCap, yMin + 8)
    const folded = rawMax > yMax
    const palette = ['#FDBA4D', '#C06BFF', '#55D6C8']
    return {
      color: palette,
      animationDuration: 360,
      animationEasing: 'cubicOut',
      backgroundColor: 'transparent',
      grid: { left: 54, right: 18, top: 8, bottom: 34 },
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(15,8,28,0.96)',
        borderColor: 'rgba(255,255,255,0.12)',
        borderWidth: 1,
        padding: [10, 12],
        extraCssText: 'border-radius:12px;box-shadow:0 18px 40px rgba(0,0,0,.35);',
        textStyle: { color: '#F8F5FF', fontSize: 12 },
        formatter: (params: any) => {
          const rows = Array.isArray(params) ? params : [params]
          const date = rows[0]?.axisValue ?? ''
          const body = rows.map((row: any) => {
            const rawRank = row?.data?.rawRank ?? row?.value
            const clipped = row?.data?.clipped
            const marker = row?.marker ?? ''
            const suffix = clipped ? ' · 低位折叠' : ''
            return marker + row.seriesName + ': 第 ' + (rawRank ?? '--') + ' 名' + suffix
          }).join('<br/>')
          return '<div style="font-weight:600;margin-bottom:6px">' + date + '</div>' + body
        },
      },
      xAxis: {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLabel: { color: 'rgba(232,224,255,0.62)', fontSize: 11, hideOverlap: true },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        inverse: true,
        min: yMin,
        max: yMax,
        minInterval: 1,
        splitNumber: 4,
        axisLabel: {
          color: 'rgba(232,224,255,0.64)',
          fontSize: 11,
          formatter: (value: number) => folded && value >= yMax ? '#' + Math.round(yMax) + '+' : '#' + Math.round(value),
        },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        axisLine: { show: false },
        axisTick: { show: false },
      },
      dataZoom: [
        { type: 'inside', throttle: 60, zoomOnMouseWheel: true, moveOnMouseMove: true },
      ],
      series: trends.map((trend, index) => {
        const color = palette[index % palette.length]
        const byDate = new Map(trend.points.map(point => [point.date, point.rank]))
        return {
          name: trend.name || trend.ticker,
          type: 'line',
          smooth: 0.25,
          showSymbol: false,
          symbol: 'circle',
          symbolSize: 6,
          connectNulls: false,
          data: dates.map(date => {
            const rank = byDate.get(date)
            if (rank == null) return null
            return {
              value: Math.min(rank, yMax),
              rawRank: rank,
              clipped: rank > yMax,
            }
          }),
          lineStyle: {
            width: 2.6,
            color,
            shadowBlur: 8,
            shadowColor: color + '55',
          },
          itemStyle: { color },
          emphasis: {
            focus: 'series',
            lineStyle: { width: 3.2 },
            itemStyle: { borderWidth: 2, borderColor: '#fff' },
          },
        }
      }),
    }
  }, [ct, trends])
  const chartRef = useECharts(option, [option])

  if (!option) {
    return <EmptyState icon={TrendingUp} title="暂无排名轨迹" hint="热股榜前三名暂未返回可绘制的近 30 日排名数据。" />
  }

  const palette = ['#FDBA4D', '#C06BFF', '#55D6C8']
  return (
    <div className="rounded-2xl border border-violet-400/15 bg-[#12091f] shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]">
      <div className="border-b border-white/10 px-4 py-4">
        <div className="text-base font-semibold tracking-tight text-white">近 30 日排名轨迹</div>
        <div className="mt-1 text-xs text-violet-200/60">悬停查看日期与各标的名次，滚轮或双指可探索区间</div>
        <div className="mt-4 flex flex-wrap gap-x-4 gap-y-2">
          {trends.map((trend, index) => (
            <div key={trend.thscode} className="flex items-center gap-2 text-xs text-violet-100/78">
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: palette[index % palette.length] }}
              />
              <span>{trend.name || trend.ticker}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="px-2 pb-3 pt-4">
        <div ref={chartRef} className="h-72 w-full" />
      </div>
    </div>
  )
}

export function MarketHeat() {
  const [activeKey, setActiveKey] = useState<MarketHeatListKey>('hot_day')
  const query = useQuery({
    queryKey: QK.marketHeatRadar(30),
    queryFn: () => api.marketHeatRadar(30),
    staleTime: 60_000,
    placeholderData: previous => previous,
  })
  const data = query.data
  const activeList = data?.lists[activeKey]
  const activeConfig = VIEW_CONFIG.find(item => item.key === activeKey) ?? VIEW_CONFIG[0]
  const ActiveIcon = activeConfig.icon
  const selectedOverlap = data?.overlaps.find(item => {
    if (activeKey === 'hot_day') return item.key === 'hot_vs_skyrocket_day'
    if (activeKey === 'hot_hour') return item.key === 'hot_vs_skyrocket_hour'
    if (activeKey === 'skyrocket_day') return item.key === 'hot_vs_skyrocket_day'
    return item.key === 'hot_vs_skyrocket_hour'
  }) ?? data?.overlaps[0]
  const trendRows = data ? Object.values(data.trends) : []

  return (
    <div className="min-h-full bg-base">
      <PageHeader
        title="市场热度与飙升雷达"
        subtitle="同花顺特色数据"
        right={(
          <button
            onClick={() => query.refetch()}
            disabled={query.isFetching}
            className="inline-flex items-center gap-2 rounded-btn border border-border bg-surface px-3 py-1.5 text-xs text-secondary transition-colors hover:bg-elevated disabled:opacity-60"
          >
            {query.isFetching ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            刷新
          </button>
        )}
      />

      <main className="space-y-4 p-4 sm:p-5">
        {query.isLoading && (
          <div className="grid min-h-[360px] place-items-center rounded-xl border border-border bg-surface">
            <div className="flex items-center gap-2 text-sm text-muted">
              <Loader2 className="h-4 w-4 animate-spin" />
              正在读取同花顺热股与飙升榜…
            </div>
          </div>
        )}

        {query.isError && (
          <EmptyState
            icon={AlertTriangle}
            title="热榜数据暂不可用"
            hint={query.error instanceof Error ? query.error.message : '请检查同花顺/Fuyao API Key 和网络连接。'}
          />
        )}

        {data && activeList && (
          <>
            <section className="overflow-hidden rounded-2xl border border-border bg-surface">
              <div className={cn('bg-gradient-to-r px-5 py-4', activeConfig.tone)}>
                <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
                  <div>
                    <div className="flex items-center gap-2 text-sm font-medium">
                      <ActiveIcon className="h-4 w-4" />
                      {activeConfig.label}
                    </div>
                    <h2 className="mt-2 text-2xl font-semibold tracking-tight text-foreground">
                      市场热度与飙升雷达
                    </h2>
                    <p className="mt-1 max-w-3xl text-sm text-secondary">{activeConfig.description}</p>
                  </div>
                  <div className="text-xs text-muted lg:text-right">
                    <div>榜单时间：{fmtDateTime(activeList.timestamp_iso)}</div>
                    <div>生成时间：{fmtDateTime(data.generated_at)}</div>
                  </div>
                </div>
              </div>

              <div className="border-t border-border p-3">
                <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
                  {VIEW_CONFIG.map(view => {
                    const Icon = view.icon
                    const list = data.lists[view.key]
                    const active = view.key === activeKey
                    return (
                      <button
                        key={view.key}
                        onClick={() => setActiveKey(view.key)}
                        className={cn(
                          'rounded-xl border px-3 py-3 text-left transition-colors',
                          active ? 'border-sky-400/40 bg-sky-400/10' : 'border-border bg-elevated/40 hover:bg-elevated',
                        )}
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="flex items-center gap-1.5 text-sm font-medium text-foreground">
                            <Icon className="h-4 w-4" />
                            {view.shortLabel}
                          </span>
                          <span className="text-xs text-muted">{list.summary.count} 只</span>
                        </div>
                        <div className="mt-2 text-xs text-secondary">
                          热度峰值 {fmtNumber(list.summary.top_heat)}
                        </div>
                      </button>
                    )
                  })}
                </div>
              </div>
            </section>

            <section className="grid gap-3 md:grid-cols-4">
              <SummaryCard label="榜单样本" value={`${activeList.summary.count} 只`} hint="接口当前返回数量" />
              <SummaryCard label="热度峰值" value={fmtNumber(activeList.summary.top_heat)} hint="当前视图最高热度" />
              <SummaryCard label="平均热度" value={fmtNumber(activeList.summary.avg_heat)} hint="当前榜单算术平均" />
              <SummaryCard
                label="排名变化"
                value={`+${activeList.summary.positive_rank_change_count} / -${activeList.summary.negative_rank_change_count}`}
                hint="仅展示变化方向，不作为交易信号"
              />
            </section>

            <section className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1.55fr)_minmax(340px,0.95fr)]">
              <div className="space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h3 className="text-base font-semibold text-foreground">{activeList.title}</h3>
                    <p className="text-xs text-muted">统计周期：{activeList.period === 'day' ? '24 小时' : '小时'} · 数据源：{data.source_label}</p>
                  </div>
                  <span className="rounded-full border border-border bg-elevated px-2.5 py-1 text-xs text-secondary">
                    {fmtDateTime(activeList.timestamp_iso)}
                  </span>
                </div>
                {activeList.items.length > 0 ? (
                  <RankingTable items={activeList.items} />
                ) : (
                  <EmptyState icon={Info} title="当前榜单为空" hint="同花顺接口返回了空列表，可能与休市、数据就绪状态或权限有关。" />
                )}
              </div>

              <aside className="space-y-4">
                <div className="rounded-xl border border-border bg-surface/80 p-4">
                  <div className="flex items-center gap-2">
                    <ArrowDownUp className="h-4 w-4 text-sky-300" />
                    <h3 className="text-sm font-semibold text-foreground">榜单重合度</h3>
                  </div>
                  {selectedOverlap ? (
                    <div className="mt-3">
                      <div className="flex items-end justify-between">
                        <div>
                          <div className="text-sm text-foreground">{selectedOverlap.label}</div>
                          <div className="mt-1 text-xs text-muted">
                            重合 {selectedOverlap.count} 只 · 覆盖 {fmtPct(selectedOverlap.ratio)}
                          </div>
                        </div>
                      </div>
                      <div className="mt-3 space-y-2">
                        {selectedOverlap.items.slice(0, 6).map(item => (
                          <div key={item.thscode} className="rounded-lg border border-border bg-elevated/40 p-2">
                            <div className="flex items-center justify-between gap-2">
                              <div className="min-w-0">
                                <div className="truncate text-sm font-medium text-foreground">{item.name}</div>
                                <div className="font-mono text-[11px] text-muted">{item.thscode}</div>
                              </div>
                              <div className="text-right text-xs text-secondary">
                                <div>#{item.left.rank ?? '--'} / #{item.right.rank ?? '--'}</div>
                                <div>热度 {fmtNumber(item.left.heat, 1)} / {fmtNumber(item.right.heat, 1)}</div>
                              </div>
                            </div>
                          </div>
                        ))}
                        {selectedOverlap.items.length === 0 && (
                          <div className="rounded-lg border border-dashed border-border p-4 text-center text-xs text-muted">
                            当前两类榜单暂无重合股票。
                          </div>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="mt-3 text-sm text-muted">暂无重合度数据。</div>
                  )}
                </div>

                <div className="space-y-3">
                  <TrendChart data={data} />
                  <div className="rounded-xl border border-border bg-surface/80 p-3 text-xs leading-relaxed text-muted">
                    代表股票取热股榜 24 小时前三。排名数字越小代表榜内位置越靠前；明显低位排名会折叠到图底部，悬停可查看原始名次。
                  </div>
                  <div className="grid gap-2">
                    {trendRows.map(trend => {
                      const meta = directionMeta(trend.analysis.direction)
                      return (
                        <div key={trend.thscode} className="flex items-center justify-between gap-2 rounded-lg bg-elevated/40 px-3 py-2">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium text-foreground">{trend.name}</div>
                            <div className="font-mono text-[11px] text-muted">{trend.thscode}</div>
                          </div>
                          <span className={cn('shrink-0 rounded-full border px-2 py-0.5 text-xs', meta.cls)}>
                            {meta.label}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </aside>
            </section>

            <section className="rounded-xl border border-border bg-surface/80 p-4 text-xs leading-relaxed text-secondary">
              <div>数据源：{data.source_label}。{data.delay_boundary}</div>
              <div>趋势窗口：{data.trend_window.start_date} 至 {data.trend_window.end_date}，自然日 {data.trend_window.natural_days} 天。</div>
              <div className="mt-1 text-warning">{data.disclaimer}</div>
            </section>
          </>
        )}
      </main>
    </div>
  )
}
