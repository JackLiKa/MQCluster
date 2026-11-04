'use client'

/**
 * 节点操作 hook——单节点 启动/停止/重启 与 集群一键启停。
 *
 * - pending 记录进行中的操作，界面据此显示 loading，并把 START/RESTART 中的节点叠加为 STARTING
 * - 结果统一用 toast 提示，完成后立即刷新拓扑
 * - 嵌入式 Kafka / RocketMQ 启动耗时较长（数秒到数十秒），接口超时已在 api.ts 放宽
 */

import { useCallback, useState } from 'react'
import { toast } from 'sonner'
import { operateCluster, operateNode } from '@/lib/api'
import type { ClusterSelection, ClusterTopology, OperationType } from '@/types/cluster'

export const OP_LABELS: Record<OperationType, string> = {
  START: '启动',
  STOP: '停止',
  RESTART: '重启',
}

/** 集群级操作在 pending 中使用的 key */
export const CLUSTER_KEY = '__cluster__'

export interface NodeOperations {
  pending: Record<string, OperationType>
  operate: (nodeId: string, op: OperationType, label?: string) => Promise<void>
  operateAll: (op: OperationType) => Promise<void>
  clusterBusy: boolean
}

export function useNodeOperations(selection: ClusterSelection, refresh: () => Promise<void>): NodeOperations {
  const [pending, setPending] = useState<Record<string, OperationType>>({})

  const mark = useCallback((key: string, op: OperationType | null) => {
    setPending((prev) => {
      const next = { ...prev }
      if (op) next[key] = op
      else delete next[key]
      return next
    })
  }, [])

  const operate = useCallback(
    async (nodeId: string, op: OperationType, label?: string) => {
      mark(nodeId, op)
      const name = label ?? nodeId
      try {
        const result = await operateNode(selection, nodeId, op)
        if (result.success) toast.success(`${name} ${OP_LABELS[op]}成功`, { description: result.message })
        else toast.error(`${name} ${OP_LABELS[op]}失败`, { description: result.message })
      } catch (err) {
        toast.error(`${name} ${OP_LABELS[op]}失败`, { description: err instanceof Error ? err.message : String(err) })
      } finally {
        mark(nodeId, null)
        await refresh()
      }
    },
    [selection, refresh, mark],
  )

  const operateAll = useCallback(
    async (op: OperationType) => {
      mark(CLUSTER_KEY, op)
      const id = toast.loading(`正在${OP_LABELS[op]}整个集群…`, { description: '按 provider 规定的顺序逐个执行' })
      try {
        const results = await operateCluster(selection, op)
        const failed = results.filter((r) => !r.success)
        if (failed.length === 0) {
          toast.success(`集群${OP_LABELS[op]}完成`, { id, description: `共 ${results.length} 个节点` })
        } else {
          toast.error(`集群${OP_LABELS[op]}部分失败`, {
            id,
            description: failed.map((r) => `${r.targetId}：${r.message}`).join('；'),
          })
        }
      } catch (err) {
        toast.error(`集群${OP_LABELS[op]}失败`, { id, description: err instanceof Error ? err.message : String(err) })
      } finally {
        mark(CLUSTER_KEY, null)
        await refresh()
      }
    },
    [selection, refresh, mark],
  )

  return { pending, operate, operateAll, clusterBusy: CLUSTER_KEY in pending }
}

/** 把进行中的启动 / 重启操作叠加到拓扑上，节点显示为 STARTING */
export function overlayPending(
  topology: ClusterTopology | null,
  pending: Record<string, OperationType>,
): ClusterTopology | null {
  if (!topology) return topology
  const clusterOp = pending[CLUSTER_KEY]
  if (Object.keys(pending).length === 0) return topology
  return {
    ...topology,
    nodes: topology.nodes.map((node) => {
      const op = pending[node.nodeId] ?? clusterOp
      if ((op === 'START' && node.status !== 'RUNNING') || op === 'RESTART') {
        return { ...node, status: 'STARTING' }
      }
      return node
    }),
  }
}
