import { useMemo } from 'react'
import type { EChartsOption } from 'echarts'
import { useChartTheme } from '@/lib/theme'
import { useECharts } from './useECharts'

export interface FiveFortunesDailyReport {
  date: string
  regime: string
  raw_regime?: string
  regime_changed?: boolean
  target: string[]
  holdings?: string[]
  candidates: Array<{ symbol: string; score?: number }>
  filtered_count?: number
  candidate_count?: number
  liquidity_pool_count?: number
  filter_rejections?: Record<string, number>
  decision?: { reason?: string; held?: string; held_rank?: number; filter_fail_symbols?: string[] }
  risk_action?: { action?: string; drawdown?: number } | null
}

const REGIME_COLORS: Record<string, string> = {
  正常期: '#16a34a',
  震荡期: '#d97706',
  走弱期: '#dc2626',
}

export function FiveFortunesProcessChart({ reports, fills }: { reports: FiveFortunesDailyReport[]; fills: Record<string, any>[] }) {
  const ct = useChartTheme()
  const option = useMemo<EChartsOption | null>(() => {
    if (!reports.length) return null
    const dates = reports.map(report => report.date)
    const trades = new Map<string, { buy: number; sell: number }>()
    for (const fill of fills) {
      const day = String(fill.timestamp ?? '').slice(0, 10)
      const count = trades.get(day) ?? { buy: 0, sell: 0 }
      if (fill.side === 'buy') count.buy += 1
      if (fill.side === 'sell') count.sell += 1
      trades.set(day, count)
    }
    return {
      animation: false,
      grid: [
        { left: 52, right: 42, top: 18, height: 34 },
        { left: 52, right: 42, top: 88, bottom: 48 },
      ],
      xAxis: [
        { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { lineStyle: { color: ct.border } } },
        { type: 'category', data: dates, gridIndex: 1, axisLabel: { color: ct.text, fontSize: 10, hideOverlap: true, interval: Math.max(0, Math.floor(dates.length / 6)), formatter: (value: string) => value.slice(5) }, axisTick: { show: false }, axisLine: { lineStyle: { color: ct.border } } },
      ],
      yAxis: [
        { type: 'value', gridIndex: 0, min: 0, max: 1, show: false },
        { type: 'value', gridIndex: 1, min: 0, axisLabel: { color: ct.text, fontSize: 10 }, splitLine: { lineStyle: { color: ct.grid } } },
        { type: 'value', gridIndex: 1, position: 'right', axisLabel: { color: ct.text, fontSize: 10 }, splitLine: { show: false } },
      ],
      dataZoom: [
        { type: 'inside', xAxisIndex: [0, 1], filterMode: 'filter' },
        { type: 'slider', xAxisIndex: [0, 1], height: 14, bottom: 8, borderColor: ct.border, backgroundColor: ct.zoomFill, fillerColor: 'rgba(59,130,246,0.18)', textStyle: { color: ct.text, fontSize: 10 }, brushSelect: false },
      ],
      tooltip: {
        trigger: 'axis', backgroundColor: ct.tooltipBg, borderColor: ct.tooltipBorder,
        textStyle: { color: ct.tooltipText, fontSize: 11 },
        formatter: (params: any) => {
          const report = reports[params[0]?.dataIndex ?? 0]
          const activity = trades.get(report.date) ?? { buy: 0, sell: 0 }
          return `<div>${report.date} · ${report.regime}</div><div>过滤后 ${report.filtered_count ?? 0} · 候选 ${report.candidate_count ?? report.candidates.length} · 流动性池 ${report.liquidity_pool_count ?? 0}</div><div>目标 ${report.target.join(', ') || '空仓'}</div><div>实际持仓 ${report.holdings?.join(', ') || '空仓'}</div><div>实际成交 买入 ${activity.buy} · 卖出 ${activity.sell}</div>${report.regime_changed ? '<div>状态切换</div>' : ''}${report.risk_action ? `<div>风控 ${report.risk_action.action ?? ''}</div>` : ''}`
        },
      },
      series: [
        { name: '市场状态', type: 'bar', xAxisIndex: 0, yAxisIndex: 0, barWidth: '82%', data: reports.map(report => ({ value: 1, itemStyle: { color: REGIME_COLORS[report.regime] ?? '#64748b' } })) },
        { name: '状态切换', type: 'scatter', xAxisIndex: 0, yAxisIndex: 0, symbol: 'diamond', symbolSize: 9, data: reports.filter(report => report.regime_changed).map(report => [report.date, 0.5]), itemStyle: { color: '#ffffff', borderColor: '#111827', borderWidth: 1.5 } },
        { name: '风控', type: 'scatter', xAxisIndex: 0, yAxisIndex: 0, symbol: 'triangle', symbolSize: 10, data: reports.filter(report => report.risk_action).map(report => [report.date, 0.5]), itemStyle: { color: '#7f1d1d' } },
        { name: '过滤后', type: 'line', xAxisIndex: 1, yAxisIndex: 1, data: reports.map(report => report.filtered_count ?? 0), symbol: 'none', lineStyle: { color: '#2563eb', width: 1.4 }, itemStyle: { color: '#2563eb' } },
        { name: '候选', type: 'line', xAxisIndex: 1, yAxisIndex: 1, data: reports.map(report => report.candidate_count ?? report.candidates.length), symbol: 'none', lineStyle: { color: '#7c3aed', width: 1.2 }, itemStyle: { color: '#7c3aed' } },
        { name: '流动性池', type: 'line', xAxisIndex: 1, yAxisIndex: 1, data: reports.map(report => report.liquidity_pool_count ?? 0), symbol: 'none', lineStyle: { color: '#64748b', width: 1, type: 'dashed' }, itemStyle: { color: '#64748b' } },
        { name: '买入', type: 'bar', xAxisIndex: 1, yAxisIndex: 2, stack: 'trade', data: dates.map(day => trades.get(day)?.buy ?? 0), itemStyle: { color: '#16a34a' }, barMaxWidth: 8 },
        { name: '卖出', type: 'bar', xAxisIndex: 1, yAxisIndex: 2, stack: 'trade', data: dates.map(day => -(trades.get(day)?.sell ?? 0)), itemStyle: { color: '#dc2626' }, barMaxWidth: 8 },
      ],
    } as any
  }, [ct, fills, reports])
  const chartRef = useECharts(option, [option])

  return <div>
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-1 pb-2 text-[10px] text-muted">
      <span className="inline-flex items-center gap-1"><i className="h-2 w-2 bg-green-600" />正常期</span>
      <span className="inline-flex items-center gap-1"><i className="h-2 w-2 bg-amber-600" />震荡期</span>
      <span className="inline-flex items-center gap-1"><i className="h-2 w-2 bg-red-600" />走弱期</span>
      <span className="text-blue-600">过滤后</span><span className="text-violet-600">候选</span><span>流动性池</span>
      <span className="text-success">买入</span><span className="text-danger">卖出</span>
    </div>
    <div ref={chartRef} className="h-[300px] sm:h-[340px]" />
  </div>
}
