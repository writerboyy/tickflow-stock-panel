import { useEffect, useMemo, useRef, useState } from 'react'
import { AlertTriangle, CheckCircle2, ImagePlus, Loader2, RefreshCw, Upload, X } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Modal } from '@/components/Modal'
import { toast } from '@/components/Toast'
import {
  api,
  type PositionRiskOcrResult,
  type PositionRiskOcrRow,
  type PositionRiskPortfolio,
  type PositionRiskPreview,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'

interface Props {
  open: boolean
  portfolio: PositionRiskPortfolio
  onClose: () => void
}

type EditableRow = PositionRiskOcrRow & {
  alternatives?: PositionRiskOcrRow[]
}

const MAX_IMAGES = 10

function sameHolding(a: PositionRiskOcrRow, b: PositionRiskOcrRow) {
  return a.quantity === b.quantity
    && a.available === b.available
    && a.cost_price === b.cost_price
    && a.current_price === b.current_price
}

/** 重叠行数值一致时去重；数值冲突时保留全部候选并要求人工选择。 */
export function mergePositionRows(lists: PositionRiskOcrRow[][]): EditableRow[] {
  const byCode = new Map<string, EditableRow>()
  for (const list of lists) {
    for (const row of list) {
      const key = row.symbol || row.code
      const previous = byCode.get(key)
      if (!previous) {
        byCode.set(key, { ...row })
      } else if (!sameHolding(previous, row)) {
        const { alternatives: existingAlternatives, ...base } = previous
        const alternatives: PositionRiskOcrRow[] = existingAlternatives ?? [base]
        if (!alternatives.some(item => sameHolding(item, row))) alternatives.push(row)
        byCode.set(key, { ...previous, alternatives })
      }
    }
  }
  return [...byCode.values()]
}

function numberOrNull(value: string): number | null {
  if (value.trim() === '') return null
  const parsed = Number(value.replaceAll(',', ''))
  return Number.isFinite(parsed) ? parsed : null
}

function fmtMoney(value: number | null | undefined) {
  return value == null ? '—' : value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

export function PositionRiskImportDialog({ open, portfolio, onClose }: Props) {
  const inputRef = useRef<HTMLInputElement>(null)
  const abortRef = useRef<AbortController | null>(null)
  const queryClient = useQueryClient()
  const [busy, setBusy] = useState(false)
  const [progress, setProgress] = useState('')
  const [previews, setPreviews] = useState<string[]>([])
  const [rows, setRows] = useState<EditableRow[]>([])
  const [account, setAccount] = useState({
    name: portfolio.account.name || '手工账户',
    cash: portfolio.account.cash == null ? '' : String(portfolio.account.cash),
    total_asset: portfolio.account.total_asset == null ? '' : String(portfolio.account.total_asset),
    previous_close_total_asset: portfolio.account.previous_close_total_asset == null ? '' : String(portfolio.account.previous_close_total_asset),
  })
  const [preview, setPreview] = useState<PositionRiskPreview | null>(null)

  const previewMutation = useMutation({
    mutationFn: () => api.positionRiskPreview({
      revision: portfolio.revision,
      account: {
        name: account.name,
        cash: numberOrNull(account.cash),
        total_asset: numberOrNull(account.total_asset),
        previous_close_total_asset: numberOrNull(account.previous_close_total_asset),
      },
      positions: rows.map(({ alternatives: _alternatives, ...row }) => row),
    }),
    onSuccess: setPreview,
  })
  const replaceMutation = useMutation({
    mutationFn: () => api.positionRiskReplace({
      revision: portfolio.revision,
      account: preview?.account ?? {},
      positions: preview?.positions ?? [],
    }),
    onSuccess: data => {
      toast(data.message, 'success')
      queryClient.invalidateQueries({ queryKey: QK.positionRisk })
      onClose()
    },
  })

  useEffect(() => {
    if (open) return
    abortRef.current?.abort()
    setBusy(false)
    setRows([])
    setPreview(null)
    setProgress('')
    setPreviews(previous => {
      previous.forEach(URL.revokeObjectURL)
      return []
    })
  }, [open])

  const unresolved = rows.filter(row => row.alternatives?.length).length
  const lowConfidence = rows.filter(row => row.requires_review).length

  const applyAccountCandidates = (results: PositionRiskOcrResult[]) => {
    const found: Record<string, string> = {}
    for (const result of results) {
      for (const [key, candidate] of Object.entries(result.account_candidates)) {
        if (!(key in found)) found[key] = String(candidate.value)
      }
    }
    setAccount(previous => ({
      ...previous,
      name: found.account_name || previous.name,
      cash: found.cash || previous.cash,
      total_asset: found.total_asset || previous.total_asset,
      previous_close_total_asset: found.previous_close_total_asset || previous.previous_close_total_asset,
    }))
  }

  const runOcr = async (files: File[]) => {
    const images = files.filter(file => file.type.startsWith('image/') || /\.(jpe?g|png|webp|bmp|gif)$/i.test(file.name))
    if (!images.length) {
      toast('请选择同花顺持仓截图', 'error')
      return
    }
    const selected = images.slice(0, MAX_IMAGES)
    if (images.length > MAX_IMAGES) toast(`一次最多识别 ${MAX_IMAGES} 张`, 'error')
    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller
    setPreviews(previous => {
      previous.forEach(URL.revokeObjectURL)
      return selected.map(URL.createObjectURL)
    })
    setBusy(true)
    setRows([])
    setPreview(null)
    const results: PositionRiskOcrResult[] = []
    let failures = 0
    try {
      for (let index = 0; index < selected.length; index += 1) {
        setProgress(`OCR ${index + 1}/${selected.length}`)
        try {
          results.push(await api.positionRiskImportImage(selected[index], controller.signal, true))
        } catch {
          if (!controller.signal.aborted) failures += 1
        }
      }
      if (controller.signal.aborted) return
      const merged = mergePositionRows(results.map(result => result.positions))
      setRows(merged)
      applyAccountCandidates(results)
      if (!merged.length) toast('未识别到持仓行，请使用同花顺手机持仓页清晰截图', 'error')
      else if (failures) toast(`${failures} 张识别失败，已保留其余结果`, 'error')
    } finally {
      if (!controller.signal.aborted) {
        setBusy(false)
        setProgress('')
      }
    }
  }

  const updateRow = (index: number, patch: Partial<EditableRow>) => {
    setRows(previous => previous.map((row, rowIndex) => rowIndex === index
      ? { ...row, ...patch, requires_review: false, issues: [] }
      : row))
    setPreview(null)
  }

  const reconciliationTone = preview?.can_confirm ? 'text-bull' : 'text-danger'
  const replacementSummary = useMemo(() => preview
    ? `新增 ${preview.replacement.added.length} · 删除 ${preview.replacement.removed.length} · 变更 ${preview.replacement.changed.length}`
    : '', [preview])

  if (!open) return null
  return (
    <Modal
      onClose={onClose}
      labelledBy="position-risk-import-title"
      panelClassName="flex max-h-[92vh] w-[96vw] max-w-6xl flex-col overflow-hidden rounded-card border border-border bg-surface shadow-xl"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 id="position-risk-import-title" className="text-sm font-semibold">导入同花顺持仓</h2>
          <p className="mt-0.5 text-[11px] text-muted">识别后可逐格校正；确认将全量替换当前手工账户</p>
        </div>
        <button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-btn hover:bg-elevated" aria-label="关闭">
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <input
          ref={inputRef}
          type="file"
          multiple
          accept="image/jpeg,image/png,image/webp,image/bmp,image/gif"
          className="hidden"
          onChange={event => {
            void runOcr(Array.from(event.target.files ?? []))
            event.target.value = ''
          }}
        />
        <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_280px]">
          <div className="min-w-0 space-y-5">
            <section>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold text-secondary">1. 截图与 OCR</h3>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => inputRef.current?.click()}
                  className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border px-3 text-xs hover:bg-elevated disabled:opacity-50"
                >
                  {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ImagePlus className="h-3.5 w-3.5" />}
                  {progress || '选择图片'}
                </button>
              </div>
              {previews.length > 0 ? (
                <div className="flex gap-2 overflow-x-auto border-y border-border py-2">
                  {previews.map((url, index) => (
                    <img key={url} src={url} alt={`持仓截图 ${index + 1}`} className="h-24 w-20 shrink-0 object-contain" />
                  ))}
                </div>
              ) : (
                <button
                  type="button"
                  onClick={() => inputRef.current?.click()}
                  onDragOver={event => event.preventDefault()}
                  onDrop={event => { event.preventDefault(); void runOcr(Array.from(event.dataTransfer.files)) }}
                  className="grid h-28 w-full place-items-center border-y border-dashed border-border text-xs text-muted hover:bg-elevated/30"
                >
                  <span className="inline-flex items-center gap-2"><ImagePlus className="h-5 w-5" />最多 10 张深浅色滚动截图</span>
                </button>
              )}
            </section>

            <section>
              <div className="mb-2 flex items-center justify-between gap-3">
                <h3 className="text-xs font-semibold text-secondary">2. 校正持仓</h3>
                <span className="text-[11px] text-muted">{rows.length} 只 · {unresolved} 个冲突 · {lowConfidence} 行低置信度</span>
              </div>
              <div className="overflow-x-auto border-y border-border">
                <table className="w-full min-w-[860px] text-xs">
                  <thead className="bg-elevated/50 text-muted">
                    <tr>
                      <th className="px-2 py-2 text-left">证券</th>
                      <th className="px-2 py-2 text-left">代码</th>
                      <th className="px-2 py-2 text-right">持仓</th>
                      <th className="px-2 py-2 text-right">可用</th>
                      <th className="px-2 py-2 text-right">成本价</th>
                      <th className="px-2 py-2 text-right">截图现价</th>
                      <th className="px-2 py-2 text-left">核对</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/70">
                    {rows.map((row, index) => (
                      <tr key={`${row.code}-${index}`} className={row.alternatives?.length || row.requires_review ? 'bg-warning/5' : ''}>
                        <td className="px-2 py-2"><input value={row.name ?? ''} onChange={event => updateRow(index, { name: event.target.value })} className="h-7 w-28 rounded border border-border bg-transparent px-2" /></td>
                        <td className="px-2 py-2"><input value={row.symbol ?? ''} onChange={event => updateRow(index, { symbol: event.target.value.toUpperCase() })} className="h-7 w-28 rounded border border-border bg-transparent px-2 font-mono" /></td>
                        {(['quantity', 'available', 'cost_price', 'current_price'] as const).map(field => (
                          <td key={field} className="px-2 py-2 text-right">
                            <input
                              type="number"
                              step={field.includes('price') ? '0.001' : '1'}
                              value={row[field] ?? ''}
                              onChange={event => updateRow(index, { [field]: numberOrNull(event.target.value) })}
                              className="h-7 w-24 rounded border border-border bg-transparent px-2 text-right font-mono"
                            />
                          </td>
                        ))}
                        <td className="px-2 py-2">
                          {row.alternatives?.length ? (
                            <select
                              defaultValue=""
                              onChange={event => {
                                const choice = row.alternatives?.[Number(event.target.value)]
                                if (choice) updateRow(index, { ...choice, alternatives: undefined })
                              }}
                              className="h-7 rounded border border-warning/50 bg-surface px-2 text-warning"
                            >
                              <option value="" disabled>选择冲突记录</option>
                              {row.alternatives.map((choice, choiceIndex) => (
                                <option key={choiceIndex} value={choiceIndex}>数量 {choice.quantity} / 成本 {choice.cost_price}</option>
                              ))}
                            </select>
                          ) : row.requires_review ? (
                            <span className="inline-flex items-center gap-1 text-warning"><AlertTriangle className="h-3.5 w-3.5" />需人工确认</span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-bull"><CheckCircle2 className="h-3.5 w-3.5" />已核对</span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {!rows.length && <tr><td colSpan={7} className="px-3 py-10 text-center text-muted">选择截图后在这里校正识别结果</td></tr>}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <aside className="space-y-5 border-l-0 border-border lg:border-l lg:pl-5">
            <section>
              <h3 className="mb-2 text-xs font-semibold text-secondary">3. 账户字段</h3>
              <div className="space-y-2">
                {([
                  ['name', '账户名'],
                  ['cash', '可用资金'],
                  ['total_asset', '总资产'],
                  ['previous_close_total_asset', '上日收盘总资产'],
                ] as const).map(([field, label]) => (
                  <label key={field} className="block">
                    <span className="mb-1 block text-[11px] text-muted">{label}</span>
                    <input
                      type={field === 'name' ? 'text' : 'number'}
                      value={account[field]}
                      onChange={event => { setAccount(previous => ({ ...previous, [field]: event.target.value })); setPreview(null) }}
                      className="h-8 w-full rounded border border-border bg-transparent px-2 text-xs"
                    />
                  </label>
                ))}
              </div>
            </section>

            <section>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold text-secondary">4. 资产核对与替换差异</h3>
                <button
                  type="button"
                  disabled={!rows.length || unresolved > 0 || lowConfidence > 0 || previewMutation.isPending}
                  onClick={() => previewMutation.mutate()}
                  className="grid h-8 w-8 place-items-center rounded-btn border border-border hover:bg-elevated disabled:opacity-40"
                  title="重新核对"
                >
                  <RefreshCw className={`h-3.5 w-3.5 ${previewMutation.isPending ? 'animate-spin' : ''}`} />
                </button>
              </div>
              {preview ? (
                <div className="space-y-2 border-y border-border py-2 text-xs">
                  <div className="flex justify-between"><span className="text-muted">现金 + 持仓</span><span className="font-mono">{fmtMoney(preview.reconciliation.computed_total)}</span></div>
                  <div className="flex justify-between"><span className="text-muted">截图总资产</span><span className="font-mono">{fmtMoney(preview.reconciliation.reported_total)}</span></div>
                  <div className={`flex justify-between ${reconciliationTone}`}><span>差异</span><span className="font-mono">{fmtMoney(preview.reconciliation.difference)}</span></div>
                  <div className="pt-1 text-[11px] text-muted">{replacementSummary}</div>
                  {preview.issues.map((issue, index) => <div key={index} className="text-[11px] text-danger">{issue.message}</div>)}
                </div>
              ) : (
                <p className="border-y border-border py-4 text-[11px] leading-5 text-muted">完成校正后点击核对。负差异超过总资产 1% 将禁止确认。</p>
              )}
            </section>
          </aside>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-3">
        <span className="text-[11px] text-muted">确认后重置风险高水位；截图和 OCR 全文不会保存</span>
        <div className="flex gap-2">
          <button type="button" onClick={onClose} className="h-8 rounded-btn px-3 text-xs hover:bg-elevated">取消</button>
          <button
            type="button"
            disabled={!preview?.can_confirm || replaceMutation.isPending}
            onClick={() => replaceMutation.mutate()}
            className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs text-white disabled:opacity-40"
          >
            {replaceMutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
            确认替换持仓
          </button>
        </div>
      </div>
    </Modal>
  )
}
