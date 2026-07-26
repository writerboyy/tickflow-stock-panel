import { useMemo } from 'react'
import type { EChartsOption } from 'echarts'
import { useECharts } from './useECharts'
import type { FreeBacktestResult } from '@/lib/api'
import { useChartTheme } from '@/lib/theme'

export function FreeStrategyPerformanceChart({ result }: { result: FreeBacktestResult }) {
  const ct = useChartTheme()
  const option = useMemo<EChartsOption | null>(() => {
    const rows = result.daily_equity_curve ?? []
    if (!rows.length) return null
    const dates = rows.map(row => row.date)
    const nav = rows.map(row => row.strategy_nav)
    const benchmark = rows.map(row => row.benchmark_nav)
    const drawdown = rows.map(row => -row.drawdown_pct)
    const exposure = rows.map(row => row.exposure_pct)
    const navByDate = new Map(rows.map(row => [row.date, row.strategy_nav]))
    const tradeMarks = result.fills.reduce<Array<{ name: string, coord: [string, number], value: string, itemStyle: { color: string } }>>((marks, fill) => {
      const date = String(fill.timestamp).slice(0, 10)
      const value = navByDate.get(date)
      if (value != null) marks.push({
        name: fill.side === 'buy' ? '买入' : '卖出',
        coord: [date, value],
        value: fill.symbol,
        itemStyle: { color: fill.side === 'buy' ? '#16a34a' : '#dc2626' },
      })
      return marks
    }, [])
    const hasBenchmark = benchmark.some(value => value != null)
    return {
      animation: false,
      grid: [
        { left: 52, right: 54, top: 20, bottom: '37%' },
        { left: 52, right: 54, top: '72%', bottom: 42 },
      ],
      xAxis: [
        { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { lineStyle: { color: ct.border } } },
        { type: 'category', data: dates, gridIndex: 1, axisLabel: { color: ct.text, fontSize: 10, hideOverlap: true, interval: Math.max(0, Math.floor(dates.length / 6)), formatter: (value: string) => value.slice(5) }, axisTick: { show: false }, axisLine: { lineStyle: { color: ct.border } } },
      ],
      yAxis: [
        { type: 'value', gridIndex: 0, scale: true, axisLabel: { color: ct.text, fontSize: 10, formatter: (value: number) => value.toFixed(2) }, splitLine: { lineStyle: { color: ct.grid } } },
        { type: 'value', gridIndex: 0, min: 0, max: 100, position: 'right', axisLabel: { color: ct.text, fontSize: 10, formatter: (value: number) => `${value}%` }, splitLine: { show: false } },
        { type: 'value', gridIndex: 1, max: 0, axisLabel: { color: ct.text, fontSize: 10, formatter: (value: number) => `${value.toFixed(1)}%` }, splitLine: { lineStyle: { color: ct.grid } } },
      ],
      dataZoom: [
        { type: 'inside', xAxisIndex: [0, 1], filterMode: 'filter' },
        { type: 'slider', xAxisIndex: [0, 1], height: 14, bottom: 8, borderColor: ct.border, backgroundColor: ct.zoomFill, fillerColor: 'rgba(59,130,246,0.18)', textStyle: { color: ct.text, fontSize: 10 }, brushSelect: false },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: ct.tooltipBg,
        borderColor: ct.tooltipBorder,
        textStyle: { color: ct.tooltipText, fontSize: 11 },
        formatter: (params: any) => {
          const index = params[0]?.dataIndex ?? 0
          const row = rows[index]
          return `<div>${row.date}</div><div>策略净值 ${row.strategy_nav.toFixed(4)}</div>${row.benchmark_nav == null ? '' : `<div>基准净值 ${row.benchmark_nav.toFixed(4)}</div>`}<div>超额净值 ${row.excess_nav?.toFixed(4) ?? '—'}</div><div>回撤 ${row.drawdown_pct.toFixed(2)}%</div><div>仓位 ${row.exposure_pct.toFixed(1)}%</div>`
        },
      },
      series: [
        {
          name: '策略净值', type: 'line', data: nav, symbol: 'none', yAxisIndex: 0,
          lineStyle: { color: '#2563eb', width: 2 }, itemStyle: { color: '#2563eb' },
          areaStyle: { color: 'rgba(37,99,235,0.12)' },
          markPoint: tradeMarks.length ? { symbolSize: 8, label: { show: false }, data: tradeMarks } : undefined,
        },
        ...(hasBenchmark ? [{ name: result.benchmark_symbol ?? '基准', type: 'line', data: benchmark, symbol: 'none', yAxisIndex: 0, lineStyle: { color: '#64748b', width: 1.2, type: 'dashed' }, itemStyle: { color: '#64748b' } }] : []),
        { name: '仓位', type: 'line', data: exposure, symbol: 'none', yAxisIndex: 1, lineStyle: { color: '#d97706', width: 1.1 }, itemStyle: { color: '#d97706' }, areaStyle: { color: 'rgba(217,119,6,0.08)' } },
        { name: '回撤', type: 'line', data: drawdown, xAxisIndex: 1, yAxisIndex: 2, symbol: 'none', lineStyle: { color: '#dc2626', width: 1 }, itemStyle: { color: '#dc2626' }, areaStyle: { color: 'rgba(220,38,38,0.12)' } },
      ],
    } as any
  }, [ct, result])
  const chartRef = useECharts(option, [result.daily_equity_curve, result.fills, ct])

  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 px-1 pb-2 text-[10px] text-muted">
        <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 bg-blue-600" />策略净值</span>
        <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 border-t border-dashed border-slate-500" />基准</span>
        <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 bg-amber-600" />仓位</span>
        <span className="inline-flex items-center gap-1"><i className="h-0.5 w-3 bg-red-600" />回撤</span>
      </div>
      <div ref={chartRef} className="h-[330px]" />
    </div>
  )
}
