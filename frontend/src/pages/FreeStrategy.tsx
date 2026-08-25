import { ArrowLeft } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { FreeStrategy } from './backtest/FreeStrategy'

export function FreeStrategyPage() {
  const navigate = useNavigate()
  return <div className="h-full max-md:fixed max-md:inset-0 max-md:z-[10000] max-md:bg-base max-md:p-3">
    <div className="hidden h-9 items-center border-b border-border max-md:flex">
      <button type="button" title="返回" onClick={() => navigate(-1)} className="inline-flex h-8 w-8 items-center justify-center text-muted hover:text-foreground">
        <ArrowLeft className="h-4 w-4" />
      </button>
    </div>
    <div className="h-full max-md:h-[calc(100%-2.25rem)] max-md:pt-3">
      <FreeStrategy />
    </div>
  </div>
}
