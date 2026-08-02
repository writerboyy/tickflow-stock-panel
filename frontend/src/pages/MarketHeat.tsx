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
import { api, type MarketHeatItem, type MarketHeatListKey, type MarketHeatRadar } from '@/lib/api'
import { cn } from '@/lib/cn'
import { QK } from '@/lib/queryKeys'
import { useECharts } from './backtest/charts/useECharts'

const VIEW_CONFIG: Array<{
  key: MarketHeatListKey
  label: string
  shortLabel: string
  description: string
  icon: typeof Flame
  accent: string
}> = [
  {
    key: 'hot_day',
    label: '热股榜 · 24小时',
    shortLabel: '热股 · 24h',
    description: '同花顺当前热股榜，day 表示 24 小时榜。',
    icon: Flame,
    accent: 'text-[#ffbd4a]',
  },
  {
    key: 'hot_hour',
    label: '热股榜 · 小时',
    shortLabel: '热股 · 1h',
    description: '同花顺当前热股榜小时周期。',
    icon: Flame,
    accent: 'text-[#f47bff]',
  },
  {
    key: 'skyrocket_day',
    label: '飙升榜 · 24小时',
    shortLabel: '飙升 · 24h',
    description: '同花顺飙升榜，与热股榜排名逻辑不同。',
    icon: Sparkles,
    accent: 'text-[#55d6c8]',
  },
  {
    key: 'skyrocket_hour',
    label: '飙升榜 · 小时',
    shortLabel: '飙升 · 1h',
    description: '同花顺飙升榜小时周期。',
    icon: Sparkles,
    accent: 'text-[#c06bff]',
  },
]

const PANEL = 'rounded-[24px] border border-[#34224d] bg-[#130a21]/88 shadow-[inset_0_1px_0_rgba(255,255,255,0.06)] backdrop-blur'
const ROW_PANEL = 'rounded-[18px] border border-[#2c1e40] bg-[#160c25]/78 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]'

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
    return { label: '名次改善', cls: 'text-[#55d6c8] bg-[#55d6c8]/10 border-[#55d6c8]/30' }
  }
  if (direction === 'weakening') {
    return { label: '名次回落', cls: 'text-[#ffbd4a] bg-[#ffbd4a]/10 border-[#ffbd4a]/30' }
  }
  if (direction === 'flat') {
    return { label: '区间持平', cls: 'text-[#d7c8f5] bg-white/5 border-white/10' }
  }
  return { label: '样本不足', cls: 'text-[#aa9ac7] bg-white/5 border-white/10' }
}

function rankChangeTone(value: number | null | undefined) {
  if (value == null || value === 0) return 'text-[#aa9ac7]'
  return value > 0 ? 'text-[#55d6c8]' : 'text-[#ffbd4a]'
}

function quantile(values: number[], q: number) {
  if (!values.length) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)))
  return sorted[index]
}

function MetricCard({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className={cn(PANEL, 'relative overflow-hidden px-5 py-5')}>
      <div className="absolute -right-6 -top-10 h-24 w-24 rounded-full bg-[#a855f7]/18" />
      <div className="relative text-sm font-medium text-[#b8a9d4]">{label}</div>
      <div className="relative mt-3 text-4xl font-semibold leading-none tracking-tight text-white sm:text-5xl">
        {value}
      </div>
      <div className="relative mt-3 text-sm text-[#a899c1]">{hint}</div>
    </div>
  )
}

function RankingTable({ items }: { items: MarketHeatItem[] }) {
  const maxHeat = Math.max(...items.map(item => Number(item.heat) || 0), 1)

  return (
    <div className="space-y-3">
      {items.map(item => {
        const heat = Number(item.heat) || 0
        const width = Math.max(4, Math.min(100, (heat / maxHeat) * 100))
        return (
          <div key={item.thscode} className={cn(ROW_PANEL, 'px-5 py-4 transition-colors hover:border-[#c06bff]/45 hover:bg-[#1b0f2d]')}>
            <div className="grid grid-cols-[52px_minmax(0,1fr)_auto] items-center gap-4">
              <div className="font-mono text-3xl font-semibold leading-none text-[#d36aff]">
                #{item.rank ?? '--'}
              </div>
              <div className="min-w-0">
                <div className="truncate text-base font-semibold text-white">{item.name || '--'}</div>
                <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-[#aa9ac7]">
                  <span className="font-mono">{item.thscode}</span>
                  <span>·</span>
                  <span>{trendLabel(item.rank_trend)}</span>
                  <span className={cn('font-mono', rankChangeTone(item.rank_change))}>
                    {fmtRankChange(item.rank_change)}
                  </span>
                </div>
              </div>
              <div className="text-right">
                <div className="font-mono text-sm font-semibold text-white">{fmtNumber(item.heat, 0)}</div>
                <div className="mt-1 text-[11px] text-[#8f7ba9]">热度</div>
              </div>
            </div>
            <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-[#261a36]">
              <div
                className="h-full rounded-full bg-gradient-to-r from-[#c06bff] via-[#f47bff] to-[#ffbd4a]"
                style={{ width: `${width}%` }}
              />
            </div>
          </div>
        )
      })}
    </div>
  )
}

function TrendChart({ data }: { data: MarketHeatRadar }) {
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
  }, [trends])
  const chartRef = useECharts(option, [option])

  if (!option) {
    return <EmptyState icon={TrendingUp} title="暂无排名轨迹" hint="热股榜前三名暂未返回可绘制的近 30 日排名数据。" />
  }

  const palette = ['#FDBA4D', '#C06BFF', '#55D6C8']
  return (
    <div className={cn(PANEL, 'overflow-hidden')}>
      <div className="border-b border-white/10 px-5 py-5">
        <div className="text-xl font-semibold tracking-tight text-white">近 30 日排名轨迹</div>
        <div className="mt-1 text-sm text-[#aa9ac7]">悬停查看日期与各标的名次</div>
        <div className="mt-5 flex flex-wrap gap-x-4 gap-y-2">
          {trends.map((trend, index) => (
            <div key={trend.thscode} className="flex items-center gap-2 text-sm text-[#c8bce0]">
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: palette[index % palette.length] }}
              />
              <span>{trend.name || trend.ticker}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="px-3 pb-4 pt-5">
        <div ref={chartRef} className="h-[420px] w-full xl:h-[520px]" />
      </div>
    </div>
  )
}

function OverlapPanel({ overlap }: { overlap: MarketHeatRadar['overlaps'][number] | undefined }) {
  return (
    <div className={cn(PANEL, 'p-5')}>
      <div className="flex items-center gap-2">
        <ArrowDownUp className="h-4 w-4 text-[#c06bff]" />
        <h3 className="text-base font-semibold text-white">榜单重合度</h3>
      </div>
      {overlap ? (
        <div className="mt-4">
          <div className="flex items-end justify-between gap-3">
            <div>
              <div className="text-sm text-[#d9cff0]">{overlap.label}</div>
              <div className="mt-1 text-sm text-[#9f8bbd]">重合 {overlap.count} 只 · 覆盖 {fmtPct(overlap.ratio)}</div>
            </div>
            <div className="font-mono text-3xl font-semibold text-white">{overlap.count}</div>
          </div>
          <div className="mt-4 space-y-2">
            {overlap.items.slice(0, 5).map(item => (
              <div key={item.thscode} className="flex items-center justify-between gap-3 rounded-2xl border border-[#2b1e3f] bg-[#170d26]/80 px-3 py-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-white">{item.name}</div>
                  <div className="font-mono text-xs text-[#9f8bbd]">{item.thscode}</div>
                </div>
                <div className="text-right text-xs text-[#c8bce0]">
                  <div>#{item.left.rank ?? '--'} / #{item.right.rank ?? '--'}</div>
                  <div>热度 {fmtNumber(item.left.heat, 0)} / {fmtNumber(item.right.heat, 0)}</div>
                </div>
              </div>
            ))}
            {overlap.items.length === 0 && (
              <div className="rounded-2xl border border-dashed border-[#34224d] px-4 py-5 text-center text-sm text-[#9f8bbd]">
                当前两类榜单暂无重合股票。
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="mt-4 text-sm text-[#9f8bbd]">暂无重合度数据。</div>
      )}
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
  const dayOverlap = data?.overlaps.find(item => item.key === 'hot_vs_skyrocket_day')

  return (
    <div className="min-h-full overflow-hidden bg-[#07040d] text-[#f7f1ff]">
      <main className="relative px-4 py-6 sm:px-6 lg:px-8">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_78%_6%,rgba(126,34,206,0.34),transparent_32%),radial-gradient(circle_at_16%_0%,rgba(236,72,153,0.16),transparent_26%),linear-gradient(180deg,#160823_0%,#07040d_34%,#090510_100%)]" />
        <div className="relative mx-auto max-w-[1680px] space-y-6">
          <section className="pt-2 sm:pt-4">
            <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="inline-flex w-fit items-center gap-2 rounded-full border border-[#34224d] bg-[#130a21]/86 px-3 py-1.5 text-sm text-[#c8bce0]">
                <span className="h-2 w-2 rounded-full bg-[#ef4444] shadow-[0_0_18px_rgba(239,68,68,0.9)]" />
                同花顺/Fuyao · 当前快照
              </div>
              <button
                onClick={() => query.refetch()}
                disabled={query.isFetching}
                className="inline-flex w-fit items-center gap-2 rounded-full border border-[#5b3b7a] bg-[#171024]/80 px-4 py-2 text-sm font-medium text-[#f4edff] transition-colors hover:border-[#c06bff] hover:bg-[#211333] disabled:opacity-60"
              >
                {query.isFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                刷新
              </button>
            </div>

            <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_320px] lg:items-end">
              <div>
                <div className={cn('mb-3 flex items-center gap-2 text-sm font-semibold', activeConfig.accent)}>
                  <ActiveIcon className="h-4 w-4" />
                  {activeConfig.label}
                </div>
                <h1 className="max-w-5xl text-5xl font-semibold leading-[0.96] tracking-tight text-white sm:text-6xl xl:text-7xl">
                  市场热度与<span className="text-[#d36aff]">飙升雷达</span>
                </h1>
                <p className="mt-6 max-w-4xl text-lg font-medium leading-relaxed text-[#b7a9cf] sm:text-xl">
                  用排名速度、榜单重合和多标的轨迹观察关注度变化，不把热度与飙升合成投资评分。
                </p>
              </div>
              <div className="border-l-4 border-[#c06bff] px-6 py-3 text-[#b7a9cf]">
                <div className="font-semibold text-[#efe8ff]">排名越小越靠前</div>
                <div className="mt-2 leading-relaxed">榜单热度口径彼此独立</div>
                <div className="mt-3 text-sm text-[#8f7ba9]">榜单时间：{fmtDateTime(activeList?.timestamp_iso)}</div>
              </div>
            </div>
          </section>

          {query.isLoading && (
            <div className={cn(PANEL, 'grid min-h-[360px] place-items-center')}>
              <div className="flex items-center gap-2 text-sm text-[#b7a9cf]">
                <Loader2 className="h-4 w-4 animate-spin" />
                正在读取同花顺热股与飙升榜…
              </div>
            </div>
          )}

          {query.isError && (
            <div className={cn(PANEL, 'min-h-[360px]')}>
              <EmptyState
                icon={AlertTriangle}
                title="热榜数据暂不可用"
                hint={query.error instanceof Error ? query.error.message : '请检查同花顺/Fuyao API Key 和网络连接。'}
              />
            </div>
          )}

          {data && activeList && (
            <>
              <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <MetricCard label="24 小时热股" value={fmtNumber(data.lists.hot_day.summary.count, 0)} hint="热股榜样本" />
                <MetricCard label="24 小时飙升" value={fmtNumber(data.lists.skyrocket_day.summary.count, 0)} hint="排名跃迁样本" />
                <MetricCard label="双榜重合" value={fmtNumber(dayOverlap?.count, 0)} hint="前 30 名交集" />
                <MetricCard label="趋势跟踪" value={fmtNumber(trendRows.length, 0)} hint="代表股票 · 近 30 日" />
              </section>

              <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(380px,0.85fr)]">
                <div className={cn(PANEL, 'overflow-hidden')}>
                  <div className="flex flex-col gap-4 border-b border-white/10 px-5 py-5 lg:flex-row lg:items-center lg:justify-between">
                    <div>
                      <h2 className="text-xl font-semibold text-white">热度雷达</h2>
                      <p className="mt-2 text-sm text-[#aa9ac7]">按榜单与周期切换；排名条按当前视图热度归一。</p>
                    </div>
                    <div className="grid grid-cols-2 gap-2 sm:flex">
                      {VIEW_CONFIG.map(view => {
                        const active = view.key === activeKey
                        return (
                          <button
                            key={view.key}
                            onClick={() => setActiveKey(view.key)}
                            className={cn(
                              'rounded-xl border px-4 py-2 text-sm font-semibold transition-colors',
                              active
                                ? 'border-[#d36aff] bg-[#c06bff]/16 text-white shadow-[0_0_24px_rgba(192,107,255,0.18)]'
                                : 'border-[#34224d] bg-[#171024]/80 text-[#c8bce0] hover:border-[#7e4fad] hover:text-white',
                            )}
                          >
                            {view.shortLabel}
                          </button>
                        )
                      })}
                    </div>
                  </div>
                  <div className="px-5 py-5">
                    <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                      <div>
                        <h3 className="text-lg font-semibold text-white">{activeList.title}</h3>
                        <p className="mt-1 text-sm text-[#9f8bbd]">{activeConfig.description}</p>
                      </div>
                      <div className="text-sm text-[#8f7ba9]">
                        统计周期：{activeList.period === 'day' ? '24 小时' : '小时'} · {fmtDateTime(activeList.timestamp_iso)}
                      </div>
                    </div>
                    {activeList.items.length > 0 ? (
                      <RankingTable items={activeList.items} />
                    ) : (
                      <div className={cn(ROW_PANEL, 'min-h-[260px]')}>
                        <EmptyState icon={Info} title="当前榜单为空" hint="同花顺接口返回了空列表，可能与休市、数据就绪状态或权限有关。" />
                      </div>
                    )}
                  </div>
                </div>

                <aside className="space-y-5">
                  <TrendChart data={data} />
                  <div className={cn(PANEL, 'p-5')}>
                    <div className="flex items-start gap-3">
                      <Info className="mt-0.5 h-4 w-4 shrink-0 text-[#d36aff]" />
                      <div className="text-sm leading-relaxed text-[#aa9ac7]">
                        代表股票取热股榜 24 小时前三。排名数字越小代表榜内位置越靠前；明显低位排名会折叠到图底部，悬停可查看原始名次。
                      </div>
                    </div>
                  </div>
                  <div className="grid gap-2">
                    {trendRows.map(trend => {
                      const meta = directionMeta(trend.analysis.direction)
                      return (
                        <div key={trend.thscode} className="flex items-center justify-between gap-3 rounded-2xl border border-[#2b1e3f] bg-[#170d26]/80 px-4 py-3">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-semibold text-white">{trend.name}</div>
                            <div className="font-mono text-xs text-[#9f8bbd]">{trend.thscode}</div>
                          </div>
                          <span className={cn('shrink-0 rounded-full border px-2.5 py-1 text-xs font-medium', meta.cls)}>
                            {meta.label}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                  <OverlapPanel overlap={selectedOverlap} />
                </aside>
              </section>

              <section className={cn(PANEL, 'p-5 text-sm leading-relaxed text-[#aa9ac7]')}>
                <div>数据源：{data.source_label}。{data.delay_boundary}</div>
                <div>趋势窗口：{data.trend_window.start_date} 至 {data.trend_window.end_date}，自然日 {data.trend_window.natural_days} 天。</div>
                <div className="mt-1 text-[#ffbd4a]">{data.disclaimer}</div>
              </section>
            </>
          )}
        </div>
      </main>
    </div>
  )
}
