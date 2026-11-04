'use client'

/**
 * 确认对话框——替代 window.confirm，用于停止节点、删除资源等破坏性操作。
 */

import { useState } from 'react'
import { LoaderCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

export interface ConfirmRequest {
  title: string
  description: React.ReactNode
  confirmLabel?: string
  destructive?: boolean
  onConfirm: () => Promise<void> | void
}

export function ConfirmDialog({
  request,
  onClose,
}: {
  request: ConfirmRequest | null
  onClose: () => void
}) {
  const [busy, setBusy] = useState(false)

  async function handleConfirm() {
    if (!request) return
    setBusy(true)
    try {
      await request.onConfirm()
    } finally {
      setBusy(false)
      onClose()
    }
  }

  return (
    <Dialog open={request !== null} onOpenChange={(open) => !open && !busy && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{request?.title}</DialogTitle>
          <DialogDescription>{request?.description}</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button variant={request?.destructive ? 'destructive' : 'default'} onClick={handleConfirm} disabled={busy}>
            {busy && <LoaderCircle className="size-4 animate-spin" />}
            {request?.confirmLabel ?? '确认'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
