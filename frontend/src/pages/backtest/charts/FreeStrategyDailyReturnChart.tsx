import { useMemo } from 'react'
import type { EChartsOption } from 'echarts'
import type { FreeBacktestResult } from '@/lib/api'
import { useChartTheme } from '@/lib/theme'
import { useECharts } from './useECharts'

export function FreeStrategyDailyReturnChart({ result }: { result: FreeBacktestResult }) {
  const ct = useChartTheme()
  const option = useMemo<EChartsOption | null>(() => {
    const rows = result.daily_equity_curve ?? []
    if (!rows.length) return null
    const dates = rows.map(row => row.date)
    return {
      animation: false,
      grid: { left: 52, right: 20, top: 18, bottom: 48 },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { color: ct.text, fontSize: 10, hideOverlap: true, interval: Math.max(0, Math.floor(dates.length / 6)), formatter: (value: string) => value.slice(5) },
        axisTick: { show: false }, axisLine: { lineStyle: { color: ct.border } },
      },
      yAxis: {
        type: 'value', scale: true,
        axisLabel: { color: ct.text, fontSize: 10, formatter: (value: number) => `${value.toFixed(1)}%` },
        splitLine: { lineStyle: { color: ct.grid } },
      },
      dataZoom: [
        { type: 'inside', filterMode: 'filter' },
        { type: 'slider', height: 14, bottom: 8, borderColor: ct.border, backgroundColor: ct.zoomFill, fillerColor: 'rgba(59,130,246,0.18)', textStyle: { color: ct.text, fontSize: 10 }, brushSelect: false },
      ],
      tooltip: {
        trigger: 'axis', backgroundColor: ct.tooltipBg, borderColor: ct.tooltipBorder,
        textStyle: { color: ct.tooltipText, fontSize: 11 },
        formatter: (params: any) => {
          const row = rows[params[0]?.dataIndex ?? 0]
          const fmt = (value: number | null | undefined) => value == null ? '—' : `${value.toFixed(2)}%`
          return `<div>${row.date}</div><div>策略 ${fmt(row.daily_return_pct)}</div><div>基准 ${fmt(row.benchmark_daily_return_pct)}</div><div>超额 ${fmt(row.excess_daily_return_pct)}</div>`
        },
      },
      series: [
        { name: '策略日收益', type: 'bar', data: rows.map(row => row.daily_return_pct ?? 0), itemStyle: { color: '#2563eb' }, barMaxWidth: 8 },
        { name: '基准日收益', type: 'line', data: rows.map(row => row.benchmark_daily_return_pct), symbol: 'none', lineStyle: { color: '#64748b', width: 1.1, type: 'dashed' }, itemStyle: { color: '#64748b' } },
        { name: '超额日收益', type: 'line', data: rows.map(row => row.excess_daily_return_pct), symbol: 'none', lineStyle: { color: '#0f766e', width: 1.4 }, itemStyle: { color: '#0f766e' }, markLine: { silent: true, symbol: 'none', label: { show: false }, lineStyle: { color: ct.border, width: 1 }, data: [{ yAxis: 0 }] } },
      ],
    } as any
  }, [ct, result.daily_equity_curve])
  const chartRef = useECharts(option, [option])

  return <div>
    <div className="flex flex-wrap items-center gap-3 px-1 pb-2 text-[10px] text-muted">
      <span className="inline-flex items-center gap-1"><i className="h-2 w-2 bg-blue-600" />策略日收益</span>
      <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 border-t border-dashed border-slate-500" />基准日收益</span>
      <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 bg-teal-700" />超额日收益</span>
    </div>
    <div ref={chartRef} className="h-[260px] sm:h-[300px]" />
  </div>
}
