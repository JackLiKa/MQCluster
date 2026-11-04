'use client'

/**
 * 节点管理卡片——节点表 + 单节点 启动/停止/重启 + 集群一键启停 + 服务登记（RocketMQ）。
 *
 * - 停止 / 重启 / 删除前弹出确认框
 * - 操作进行中按钮显示 loading，节点显示为“启动中”
 * - 按钮禁用时通过 tooltip 说明原因
 */

import { useState } from 'react'
import { LoaderCircle, Play, Plus, Power, RotateCw, Square, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { HintButton } from '@/components/hint-button'
import { StatusBadge } from '@/components/status-dot'
import type { ConfirmRequest } from '@/components/confirm-dialog'
import { CLUSTER_KEY, OP_LABELS, type NodeOperations } from '@/hooks/use-node-operations'
import { deleteService, registerService } from '@/lib/api'
import { roleMeta, type MqMeta } from '@/lib/mq-registry'
import type { ClusterNode, ClusterSelection, ClusterTopology, OperationType, ProviderDescriptor } from '@/types/cluster'

interface NodeOperationsCardProps {
  meta: MqMeta
  provider: ProviderDescriptor
  selection: ClusterSelection
  topology: ClusterTopology | null
  ops: NodeOperations
  onConfirm: (req: ConfirmRequest) => void
  onChanged: () => Promise<void>
}

/** 节点在当前状态下某操作不可用的原因 */
function disabledReason(node: ClusterNode, op: OperationType, busy: boolean): string | null {
  if (busy) return '有操作正在进行，请稍候'
  if (op === 'START' && (node.status === 'RUNNING' || node.status === 'STARTING')) return '节点已在运行'
  if (op === 'STOP' && node.status === 'STOPPED') return '节点已停止'
  if (op === 'RESTART' && node.status === 'STOPPED') return '节点未运行，请直接启动'
  return null
}

export function NodeOperationsCard({
  meta,
  provider,
  selection,
  topology,
  ops,
  onConfirm,
  onChanged,
}: NodeOperationsCardProps) {
  const nodes = topology?.nodes ?? []
  const caps = new Set(provider.capabilities)
  const canCluster = caps.has('CLUSTER_OPERATIONS')
  const canRegister = caps.has('SERVICE_REGISTRATION')
  const [regOpen, setRegOpen] = useState(false)

  const running = nodes.filter((n) => n.status === 'RUNNING').length
  const allRunning = nodes.length > 0 && running === nodes.length
  const noneRunning = running === 0 && !nodes.some((n) => n.status === 'STARTING')
  const clusterOp = ops.pending[CLUSTER_KEY]
  const anyBusy = Object.keys(ops.pending).length > 0

  function requestNodeOp(node: ClusterNode, op: OperationType) {
    const name = node.displayName || node.nodeId
    if (op === 'START') {
      void ops.operate(node.nodeId, op, name)
      return
    }
    const role = roleMeta(meta, node.labels?.role)
    onConfirm({
      title: `${OP_LABELS[op]} ${name}？`,
      description:
        op === 'STOP'
          ? `${role.label} 将被停止，依赖它的客户端会感知到故障——这正是观察故障转移的好时机。`
          : `${role.label} 将先停止再启动，期间短暂不可用。`,
      confirmLabel: OP_LABELS[op],
      destructive: op === 'STOP',
      onConfirm: () => ops.operate(node.nodeId, op, name),
    })
  }

  function requestClusterStop() {
    onConfirm({
      title: `一键停止 ${meta.name} 集群？`,
      description: `将按顺序停止全部 ${running} 个运行中的节点，未持久化的演示数据可能丢失。`,
      confirmLabel: '全部停止',
      destructive: true,
      onConfirm: () => ops.operateAll('STOP'),
    })
  }

  function requestDelete(node: ClusterNode) {
    onConfirm({
      title: `删除节点 ${node.displayName || node.nodeId}？`,
      description: '仅从登记表中移除该节点，不会影响真实进程。',
      confirmLabel: '删除',
      destructive: true,
      onConfirm: async () => {
        try {
          await deleteService(selection, node.nodeId)
          toast.success('节点已删除')
        } catch (err) {
          toast.error('删除失败', { description: err instanceof Error ? err.message : String(err) })
        }
        await onChanged()
      },
    })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>节点管理</CardTitle>
        <CardDescription>
          {running}/{nodes.length} 个节点运行中
          {provider.mode === 'REAL' && ' · 真实集群只读状态，由外部运维管理'}
        </CardDescription>
        <CardAction className="flex flex-wrap gap-2">
          {canCluster && (
            <>
              <HintButton
                size="sm"
                onClick={() => void ops.operateAll('START')}
                disabledReason={anyBusy ? '有操作正在进行，请稍候' : allRunning ? '所有节点已在运行' : null}
              >
                {clusterOp === 'START' ? <LoaderCircle className="size-4 animate-spin" /> : <Power className="size-4" />}
                一键启动
              </HintButton>
              <HintButton
                size="sm"
                variant="outline"
                onClick={requestClusterStop}
                disabledReason={anyBusy ? '有操作正在进行，请稍候' : noneRunning ? '没有运行中的节点' : null}
              >
                {clusterOp === 'STOP' ? <LoaderCircle className="size-4 animate-spin" /> : <Square className="size-4" />}
                一键停止
              </HintButton>
            </>
          )}
          {canRegister && (
            <Button size="sm" variant="outline" onClick={() => setRegOpen(true)}>
              <Plus className="size-4" /> 登记节点
            </Button>
          )}
        </CardAction>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>节点</TableHead>
              <TableHead>角色</TableHead>
              <TableHead>地址</TableHead>
              <TableHead>状态</TableHead>
              <TableHead className="text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {nodes.map((node) => {
              const role = roleMeta(meta, node.labels?.role)
              const pendingOp = ops.pending[node.nodeId]
              const busy = anyBusy
              return (
                <TableRow key={node.nodeId}>
                  <TableCell>
                    <div className="font-medium">{node.displayName || node.nodeId}</div>
                    <div className="font-mono text-xs text-muted-foreground">{node.nodeId}</div>
                  </TableCell>
                  <TableCell className="text-sm">{role.label}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {node.hostName}
                    {node.labels?.port && node.labels.port !== '0' ? `:${node.labels.port}` : ''}
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={node.status} />
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-1">
                      {(['START', 'STOP', 'RESTART'] as OperationType[]).map((op) => {
                        const Icon = op === 'START' ? Play : op === 'STOP' ? Square : RotateCw
                        return (
                          <HintButton
                            key={op}
                            size="icon-sm"
                            variant="ghost"
                            aria-label={`${OP_LABELS[op]} ${node.nodeId}`}
                            title={OP_LABELS[op]}
                            onClick={() => requestNodeOp(node, op)}
                            disabledReason={disabledReason(node, op, busy)}
                          >
                            {pendingOp === op ? <LoaderCircle className="size-4 animate-spin" /> : <Icon className="size-4" />}
                          </HintButton>
                        )
                      })}
                      {canRegister && (
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          aria-label={`删除 ${node.nodeId}`}
                          title="删除登记"
                          onClick={() => requestDelete(node)}
                        >
                          <Trash2 className="size-4 text-destructive" />
                        </Button>
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              )
            })}
            {nodes.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} className="py-8 text-center whitespace-normal text-muted-foreground">
                  暂无节点
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </CardContent>
      {canRegister && (
        <RegisterDialog
          open={regOpen}
          onOpenChange={setRegOpen}
          meta={meta}
          selection={selection}
          onRegistered={onChanged}
        />
      )}
    </Card>
  )
}

const EMPTY_FORM = { nodeId: '', displayName: '', role: 'broker-master', hostName: '', address: '', port: 10911 }

function RegisterDialog({
  open,
  onOpenChange,
  meta,
  selection,
  onRegistered,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  meta: MqMeta
  selection: ClusterSelection
  onRegistered: () => Promise<void>
}) {
  const [form, setForm] = useState(EMPTY_FORM)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit() {
    setError(null)
    if (!form.nodeId.trim() || !form.displayName.trim() || !form.address.trim()) {
      setError('节点 ID、显示名称、地址不能为空')
      return
    }
    setBusy(true)
    try {
      await registerService(selection, {
        nodeId: form.nodeId.trim(),
        displayName: form.displayName.trim(),
        role: form.role,
        hostName: form.hostName.trim() || form.nodeId.trim(),
        address: form.address.trim(),
        port: form.port,
      })
      toast.success(`节点 ${form.nodeId} 已登记`)
      setForm(EMPTY_FORM)
      onOpenChange(false)
      await onRegistered()
    } catch (err) {
      setError(err instanceof Error ? err.message : '登记失败')
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>登记新节点</DialogTitle>
          <DialogDescription>把一个外部节点登记到拓扑中（仅登记元数据，不会启动进程）。</DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-1.5">
            <Label htmlFor="reg-id">节点 ID</Label>
            <Input id="reg-id" value={form.nodeId} onChange={(e) => setForm({ ...form, nodeId: e.target.value })} placeholder="broker-c" />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="reg-name">显示名称</Label>
            <Input id="reg-name" value={form.displayName} onChange={(e) => setForm({ ...form, displayName: e.target.value })} placeholder="Broker C" />
          </div>
          <div className="space-y-1.5">
            <Label>角色</Label>
            <Select value={form.role} onValueChange={(v) => v && setForm({ ...form, role: v })}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {Object.entries(meta.roles).map(([key, role]) => (
                  <SelectItem key={key} value={key}>
                    {role.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="reg-host">主机名（可选）</Label>
            <Input id="reg-host" value={form.hostName} onChange={(e) => setForm({ ...form, hostName: e.target.value })} placeholder="默认同节点 ID" />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="reg-addr">地址</Label>
            <Input id="reg-addr" value={form.address} onChange={(e) => setForm({ ...form, address: e.target.value })} placeholder="127.0.0.1" />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="reg-port">端口</Label>
            <Input
              id="reg-port"
              type="number"
              value={form.port}
              onChange={(e) => setForm({ ...form, port: Number(e.target.value) || 0 })}
            />
          </div>
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={busy}>
            取消
          </Button>
          <Button onClick={submit} disabled={busy}>
            {busy && <LoaderCircle className="size-4 animate-spin" />}
            登记
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
