import { AlertTriangle, CheckCircle } from 'lucide-react'

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
}

interface DimensionScore {
  score: number
  maxScore: number
  percentage: number
  components: Record<string, number>
  label: string
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
  // 板块情绪周期
  sector_pattern: '板块形态',
  overheat_risk: '过热风险',
  sector_current: '当日表现',
  // 拉升健康度
  sector_position: '板块地位',
  intraday_volume_price: '分钟量价',
  capital_flow: '资金流向',
  daily_k_pattern: '日K形态',
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

function DimensionCard({ dimension, compact }: { dimension: DimensionScore; compact?: boolean }) {
  const percentage = dimension.percentage
  const color = percentage >= 80 ? 'bg-bull' : percentage >= 60 ? 'bg-accent' : 'bg-warning'

  return (
    <div className="rounded border border-border bg-surface p-2.5">
      <div className="mb-1.5 flex items-center justify-between">
        <span className="text-[10px] font-medium text-secondary">{dimension.label}</span>
        <span className="font-mono text-xs text-foreground">
          {dimension.score.toFixed(1)}/{dimension.maxScore.toFixed(0)}
        </span>
      </div>
      <ProgressBar value={dimension.score} max={dimension.maxScore} color={color} />
      {!compact && (
        <div className="mt-2 space-y-1">
          {Object.entries(dimension.components).map(([key, value]) => (
            <div key={key} className="flex items-center justify-between text-[9px]">
              <span className="text-muted">{COMPONENT_LABELS[key] || key}</span>
              <span className="font-mono text-secondary">{value.toFixed(1)}</span>
            </div>
          ))}
        </div>
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
      <div className="grid grid-cols-3 gap-2">
        <DimensionCard dimension={dimensions.history} compact={compact} />
        <DimensionCard dimension={dimensions.sentiment} compact={compact} />
        <DimensionCard dimension={dimensions.health} compact={compact} />
      </div>
    </div>
  )
}
