/**
 * 共享 mutation hooks — 消除多页面重复的 useMutation 调用。
 */
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, type Preferences } from './api'
import { QK } from './queryKeys'

/** 切换实时行情 — Layout / Data 共用 */
export function useToggleRealtimeQuotes() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (enabled: boolean) => api.updateRealtimeQuotes(enabled),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.preferences })
      qc.invalidateQueries({ queryKey: QK.quoteStatus })
    },
  })
}

/** 更新行情轮询间隔 — Layout / Data 共用 */
export function useUpdateQuoteInterval() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (v: number) => api.updateQuoteInterval(v),
    onSuccess: (data) => {
      qc.setQueryData(QK.quoteInterval, data)
      qc.invalidateQueries({ queryKey: QK.quoteStatus })
    },
  })
}

/** 保存 QMT 交易面板的快捷金额预设 — QmtTradePanel / LimitBoard 共用 */
export function useUpdateQmtQuickAmountPresets() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (presets: number[]) => api.updateQmtQuickAmountPresets(presets),
    // 乐观更新: 点完成后立刻显示新档位, 不等请求返回 (否则会闪回旧值)
    onMutate: async (presets) => {
      await qc.cancelQueries({ queryKey: QK.preferences })
      const previous = qc.getQueryData<Preferences>(QK.preferences)
      qc.setQueryData<Preferences>(QK.preferences, old =>
        old ? { ...old, qmt_quick_amount_presets: presets } : old)
      return { previous }
    },
    onError: (_error, _presets, context) => {
      if (context?.previous) qc.setQueryData(QK.preferences, context.previous)
    },
    onSettled: () => {
      qc.invalidateQueries({ queryKey: QK.preferences })
    },
  })
}

interface WatchlistBatchAddInput {
  symbols: string[]
  groupId?: string | null
}

/** 批量添加自选 — Screener / 截图导入共用 */
export function useWatchlistBatchAdd() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ symbols, groupId }: WatchlistBatchAddInput) =>
      api.watchlistBatchAdd(symbols, '', groupId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.watchlist })
      // 前缀匹配: 实际 key 为 ['watchlist-enriched', extColumnsParam],
      // 不能用 QK.watchlistEnriched()(= undefined) 精确匹配, 否则列表不刷新。
      qc.invalidateQueries({ queryKey: ['watchlist-enriched'] })
    },
  })
}
