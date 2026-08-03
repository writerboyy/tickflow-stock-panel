import { useId } from 'react'
import { motion } from 'framer-motion'
import { X } from 'lucide-react'
import { useDialogBackdrop } from '@/lib/useDialogBackdrop'

export function SettingsModal({
  title,
  onClose,
  children,
  panelClassName = 'max-w-md',
}: {
  title: string
  onClose: () => void
  children: React.ReactNode
  panelClassName?: string
}) {
  const titleId = useId()
  const backdrop = useDialogBackdrop(onClose)
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" {...backdrop} />
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 12 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.97, y: 8 }}
        transition={{ duration: 0.2, ease: [0.16, 1, 0.3, 1] }}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className={`relative mx-4 max-h-[90vh] w-full overflow-hidden rounded-card border border-border bg-surface shadow-2xl ${panelClassName}`}
      >
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h3 id={titleId} className="text-sm font-medium text-foreground">{title}</h3>
          <button aria-label="关闭" onClick={onClose} className="p-0.5 rounded hover:bg-elevated text-secondary">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="max-h-[calc(90vh-49px)] overflow-y-auto p-5">
          {children}
        </div>
      </motion.div>
    </div>
  )
}
