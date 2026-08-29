import { AlertTriangle, CheckCircle } from 'lucide-react'

export interface ScoreDetailRow {
  label: string
  value: string
  tone?: string
}

export interface ComprehensiveScoreDetails {
  history?: ScoreDetailRow[]
  sentiment?: ScoreDetailRow[]
  health?: ScoreDetailRow[]
}

interface ComprehensiveScoreProps {
  score: number
  maxScore: number
  grade: string
  gradeLabel: string
  dimensions: {
    history: DimensionScore
    sentiment: DimensionScore
    health: DimensionScore
  }
  warnings?: string[]
  strengths?: string[]
  compact?: boolean
  details?: ComprehensiveScoreDetails
  dataCompleteness?: number
}

interface DimensionScore {
  score: number
  maxScore: number
  fullMaxScore?: number
  percentage: number
  components: Record<string, number>
  unavailableComponents?: string[]
  label: string
  displayValue?: string
}

const GRADE_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  'S': { bg: 'bg-bull/10', text: 'text-bull', border: 'border-bull/30' },
  'A+': { bg: 'bg-bull/10', text: 'text-bull', border: 'border-bull/30' },
  'A': { bg: 'bg-bull/10', text: 'text-bull', border: 'border-bull/30' },
  'B+': { bg: 'bg-accent/10', text: 'text-accent', border: 'border-accent/30' },
  'B': { bg: 'bg-warning/10', text: 'text-warning', border: 'border-warning/30' },
  'C': { bg: 'bg-warning/10', text: 'text-warning', border: 'border-warning/30' },
  'D': { bg: 'bg-danger/10', text: 'text-danger', border: 'border-danger/30' },
}

const COMPONENT_LABELS: Record<string, string> = {
  // 历史涨停基因
  next_day_red: '次日收红',
  seal_success: '封板成功',
  consecutive_ability: '连板能力',
  // 板块强度
  relative_momentum: '相对动量',
  trend: '趋势',
  persistence: '持续性',
  stability: '稳定性',
  breadth: '上涨广度',
  money_flow: '资金流',
  leadership: '龙头带动',
  liquidity: '流动性',
  // 旧缓存兼容字段
  sector_pattern: '板块形态',
  overheat_risk: '过热风险',
  sector_current: '当日表现',
  // 拉升健康度
  sector_position: '板块地位',
  pullup_form: '拉升形态',
  intraday_volume_price: '分钟量价',
  capital_flow: '资金强度',
  daily_k_pattern: '日K位置',
}

function ProgressBar({ value, max, color = 'bg-accent' }: { value: number; max: number; color?: string }) {
  const percentage = Math.min(100, Math.max(0, (value / max) * 100))
  return (
    <div className="relative h-1.5 overflow-hidden rounded-full bg-elevated">
      <div
        className={`h-full transition-all duration-300 ${color}`}
        style={{ width: `${percentage}%` }}
      />
    </div>
  )
}

function DimensionCard({ dimension, compact, detailRows = [] }: { dimension: DimensionScore; compact?: boolean; detailRows?: ScoreDetailRow[] }) {
  const percentage = dimension.percentage
  const color = percentage >= 80 ? 'bg-bull' : percentage >= 60 ? 'bg-accent' : 'bg-warning'
  const unavailable = dimension.unavailableComponents ?? []
  const noData = dimension.maxScore <= 0

  return (
    <div className="min-w-0 rounded border border-border bg-surface p-2.5">
      <div className="mb-1.5 flex items-center justify-between">
        <span className="min-w-0 truncate text-[10px] font-medium text-secondary">{dimension.label}</span>
        {noData
          ? <span className="shrink-0 text-[10px] text-warning">数据不足</span>
          : (
            <span className="shrink-0 font-mono text-xs text-foreground">
              {dimension.displayValue ?? `${dimension.score.toFixed(1)}/${dimension.maxScore.toFixed(0)}`}
              {!dimension.displayValue && dimension.fullMaxScore != null && dimension.fullMaxScore > dimension.maxScore
                ? <span className="ml-0.5 text-[9px] text-muted">/{dimension.fullMaxScore.toFixed(0)}</span>
                : null}
            </span>
          )}
      </div>
      <ProgressBar value={dimension.score} max={dimension.maxScore} color={noData ? 'bg-elevated' : color} />
      {!compact && (
        <>
          <div className="mt-2 space-y-1 border-t border-border/70 pt-2">
            {Object.entries(dimension.components).map(([key, value]) => (
              <div key={key} className="flex items-center justify-between gap-2 text-[9px]">
                <span className="min-w-0 truncate text-muted">{COMPONENT_LABELS[key] || key}</span>
                <span className="shrink-0 font-mono text-secondary">{value.toFixed(1)} 分</span>
              </div>
            ))}
            {unavailable.map(key => (
              <div key={key} className="flex items-center justify-between gap-2 text-[9px]">
                <span className="min-w-0 truncate text-muted">{COMPONENT_LABELS[key] || key}</span>
                <span className="shrink-0 text-warning">数据不足</span>
              </div>
            ))}
            {noData ? <div className="text-[9px] text-muted">缺项不计分，等数据返回后自动刷新</div> : null}
          </div>
          {detailRows.length > 0 ? <div className="mt-2 space-y-1 border-t border-border/70 pt-2">
            {detailRows.map(row => <div key={row.label} className="flex items-start justify-between gap-2 text-[9px]">
              <span className="min-w-0 max-w-[42%] shrink-0 truncate text-muted">{row.label}</span>
              <span className={`min-w-0 flex-1 break-words text-right font-mono ${row.tone || 'text-secondary'}`}>{row.value}</span>
            </div>)}
          </div> : null}
        </>
      )}
    </div>
  )
}

export function ComprehensiveScore({
  score,
  maxScore,
  grade,
  gradeLabel,
  dimensions,
  warnings = [],
  strengths = [],
  compact = false,
  details,
  dataCompleteness,
}: ComprehensiveScoreProps) {
  const gradeColor = GRADE_COLORS[grade] || GRADE_COLORS['B']

  return (
    <div className="space-y-3">
      {/* 综合评分头部 */}
      <div className={`rounded-lg border p-3 ${gradeColor.border} ${gradeColor.bg}`}>
        <div className="flex items-center justify-between">
          <div>
            <div className="text-[10px] text-muted">综合评分</div>
            <div className="mt-0.5 flex items-baseline gap-2">
              <span className={`font-mono text-2xl font-bold ${gradeColor.text}`}>
                {score.toFixed(1)}
              </span>
              <span className="text-xs text-muted">/ {maxScore}</span>
            </div>
          </div>
          <div className="text-right">
            <div className={`text-2xl font-bold ${gradeColor.text}`}>{grade}</div>
            <div className="text-[10px] text-muted">{gradeLabel}</div>
          </div>
        </div>
        <div className="mt-2">
          <ProgressBar value={score} max={maxScore} color={gradeColor.text.replace('text-', 'bg-')} />
        </div>
        {dataCompleteness != null && dataCompleteness < 1 ? (
          <div className="mt-2 border-t border-border/50 pt-1.5 text-[9px] text-muted">
            数据完整度 {Math.round(dataCompleteness * 100)}% · 缺项不计分{dataCompleteness < 1 ? '，评级封顶 B' : ''}
          </div>
        ) : null}
      </div>

      {/* 优势和警示 */}
      {(strengths.length > 0 || warnings.length > 0) && (
        <div className="space-y-1.5">
          {strengths.length > 0 && (
            <div className="flex items-start gap-1.5 text-[10px]">
              <CheckCircle className="mt-0.5 h-3 w-3 shrink-0 text-bull" />
              <div className="min-w-0 flex-1 text-bull">{strengths.join('、')}</div>
            </div>
          )}
          {warnings.length > 0 && (
            <div className="flex items-start gap-1.5 text-[10px]">
              <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0 text-warning" />
              <div className="min-w-0 flex-1 text-warning">{warnings.join('、')}</div>
            </div>
          )}
        </div>
      )}

      {/* 三个维度 */}
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
        <DimensionCard dimension={dimensions.history} compact={compact} detailRows={details?.history} />
        <DimensionCard dimension={dimensions.sentiment} compact={compact} detailRows={details?.sentiment} />
        <DimensionCard dimension={dimensions.health} compact={compact} detailRows={details?.health} />
      </div>
    </div>
  )
}
