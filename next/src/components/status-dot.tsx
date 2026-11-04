/**
 * 节点状态圆点——颜色来自状态 token（--status-*），启动中带呼吸动画。
 */

import { cn } from '@/lib/utils'
import { STATUS_LABELS } from '@/lib/mq-registry'
import type { NodeStatus } from '@/types/cluster'

const DOT_CLASS: Record<NodeStatus, string> = {
  RUNNING: 'bg-status-running',
  STOPPED: 'bg-status-stopped',
  FAILED: 'bg-status-failed',
  DEGRADED: 'bg-status-degraded',
  STARTING: 'bg-status-starting mq-pulse',
}

const TEXT_CLASS: Record<NodeStatus, string> = {
  RUNNING: 'text-status-running',
  STOPPED: 'text-status-stopped',
  FAILED: 'text-status-failed',
  DEGRADED: 'text-status-degraded',
  STARTING: 'text-status-starting',
}

export function StatusDot({ status, className }: { status: NodeStatus; className?: string }) {
  return (
    <span
      aria-label={STATUS_LABELS[status]}
      className={cn('inline-block size-2 shrink-0 rounded-full', DOT_CLASS[status], className)}
    />
  )
}

/** 圆点 + 中文状态文字 */
export function StatusBadge({ status, className }: { status: NodeStatus; className?: string }) {
  return (
    <span className={cn('inline-flex items-center gap-1.5 text-xs font-medium', TEXT_CLASS[status], className)}>
      <StatusDot status={status} />
      {STATUS_LABELS[status]}
    </span>
  )
}

export function statusTextClass(status: NodeStatus): string {
  return TEXT_CLASS[status]
}
