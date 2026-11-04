'use client'

/**
 * Kafka「Topic 与分区」标签页。
 *
 * - Topic 列表 + 创建 / 删除
 * - 分区矩阵：行 = 分区，列 = broker；单元格区分 Leader / Follower（ISR 内）/ Follower（掉出 ISR）
 *   停止某个 broker 后，leader 会迁移到 ISR 中的其他副本，矩阵每 3 秒刷新
 * - leader 切换时间线（后端在 topic 描述变化时记录）
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { ArrowRight, Crown, LoaderCircle, Plus, RefreshCw, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { HintButton } from '@/components/hint-button'
import { StatusDot } from '@/components/status-dot'
import type { ConfirmRequest } from '@/components/confirm-dialog'
import { createKafkaTopic, deleteKafkaTopic, fetchKafkaLeaderChanges, fetchKafkaTopics } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { ClusterNode, ClusterSelection, ClusterTopology } from '@/types/cluster'
import type { KafkaLeaderChange, KafkaPartitionDetail, KafkaTopicDetail } from '@/types/kafka'

const POLL_MS = 3000

export interface KafkaBroker {
  id: number
  node: ClusterNode
}

/** 从拓扑中取出 broker 列表（按 node.id 排序） */
export function kafkaBrokers(topology: ClusterTopology | null): KafkaBroker[] {
  return (topology?.nodes ?? [])
    .filter((n) => n.labels?.role === 'kafka-broker')
    .map((node) => ({ id: Number(node.labels.kafkaNodeId), node }))
    .sort((a, b) => a.id - b.id)
}

/** 集群能否响应 Admin 请求 */
export function kafkaUnavailable(topology: ClusterTopology | null): string | null {
  const nodes = topology?.nodes ?? []
  if (!nodes.some((n) => n.labels?.role === 'kafka-controller' && n.status === 'RUNNING'))
    return 'KRaft Controller 未运行'
  if (!nodes.some((n) => n.labels?.role === 'kafka-broker' && n.status === 'RUNNING')) return '没有运行中的 Broker'
  return null
}

export function KafkaTopicsPanel({
  selection,
  topology,
  onConfirm,
}: {
  selection: ClusterSelection
  topology: ClusterTopology | null
  onConfirm: (req: ConfirmRequest) => void
}) {
  const brokers = kafkaBrokers(topology)
  const unavailable = kafkaUnavailable(topology)
  const [topics, setTopics] = useState<KafkaTopicDetail[]>([])
  const [changes, setChanges] = useState<KafkaLeaderChange[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [showInternal, setShowInternal] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [createOpen, setCreateOpen] = useState(false)

  const load = useCallback(async () => {
    try {
      const [t, c] = await Promise.all([fetchKafkaTopics(selection, showInternal), fetchKafkaLeaderChanges(selection, 30)])
      setTopics(t)
      setChanges(c)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [selection, showInternal])

  useEffect(() => {
    if (unavailable) return
    const first = setTimeout(() => void load(), 0)
    const timer = setInterval(() => void load(), POLL_MS)
    return () => {
      clearTimeout(first)
      clearInterval(timer)
    }
  }, [load, unavailable])

  const current = topics.find((t) => t.name === selected) ?? topics.find((t) => !t.internal) ?? topics[0] ?? null

  function requestDelete(topic: KafkaTopicDetail) {
    onConfirm({
      title: `删除 Topic ${topic.name}？`,
      description: '删除后该 topic 的全部分区与消息都会被清除，消费者组的 offset 也随之失效。',
      confirmLabel: '删除',
      destructive: true,
      onConfirm: async () => {
        try {
          await deleteKafkaTopic(selection, topic.name)
          toast.success(`Topic ${topic.name} 已删除`)
          if (selected === topic.name) setSelected(null)
        } catch (err) {
          toast.error('删除失败', { description: err instanceof Error ? err.message : String(err) })
        }
        await load()
      },
    })
  }

  if (unavailable) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          {unavailable}，暂时无法查询 Topic。请先在「概览」中启动 Controller 与至少一个 Broker。
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,420px)_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Topic 列表</CardTitle>
            <CardDescription>点击一行查看分区副本分布</CardDescription>
            <CardAction className="flex gap-1">
              <Button size="icon-sm" variant="ghost" aria-label="刷新" onClick={() => void load()}>
                <RefreshCw className="size-4" />
              </Button>
              <Button size="sm" onClick={() => setCreateOpen(true)}>
                <Plus className="size-4" /> 创建
              </Button>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-3">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Switch checked={showInternal} onCheckedChange={(v) => setShowInternal(v)} size="sm" />
              显示内部 topic（__consumer_offsets 等）
            </label>
            {error && <div className="rounded-md bg-destructive/10 p-2 text-xs text-destructive">{error}</div>}
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>名称</TableHead>
                  <TableHead className="text-right">分区</TableHead>
                  <TableHead className="text-right">副本</TableHead>
                  <TableHead>健康</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {topics.map((t) => {
                  const offline = t.partitions.filter((p) => p.leader < 0).length
                  const under = t.partitions.filter((p) => p.leader >= 0 && p.isr.length < p.replicas.length).length
                  return (
                    <TableRow
                      key={t.name}
                      data-state={current?.name === t.name ? 'selected' : undefined}
                      className="cursor-pointer"
                      onClick={() => setSelected(t.name)}
                    >
                      <TableCell className="max-w-40 truncate font-mono text-xs" title={t.name}>
                        {t.name}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{t.partitionCount}</TableCell>
                      <TableCell className="text-right tabular-nums">{t.replicationFactor}</TableCell>
                      <TableCell>
                        {offline > 0 ? (
                          <Badge variant="destructive">{offline} 离线</Badge>
                        ) : under > 0 ? (
                          <span className="text-xs text-status-degraded">{under} 个副本不足</span>
                        ) : (
                          <span className="text-xs text-status-running">正常</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        {!t.internal && (
                          <Button
                            size="icon-sm"
                            variant="ghost"
                            aria-label={`删除 ${t.name}`}
                            onClick={(e) => {
                              e.stopPropagation()
                              requestDelete(t)
                            }}
                          >
                            <Trash2 className="size-4 text-destructive" />
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  )
                })}
                {topics.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="py-8 text-center whitespace-normal text-muted-foreground">
                      {loading ? '加载中…' : '暂无 topic，点击“创建”或在消息工作台直接发送（自动创建）'}
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <PartitionMatrix topic={current} brokers={brokers} />
      </div>

      <LeaderTimeline changes={changes} brokers={brokers} />

      <CreateTopicDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        brokerCount={brokers.length}
        runningBrokers={brokers.filter((b) => b.node.status === 'RUNNING').length}
        onCreate={async (req) => {
          const created = await createKafkaTopic(selection, req)
          toast.success(`Topic ${created.name} 已创建`, {
            description: `${created.partitionCount} 个分区 × ${created.replicationFactor} 副本`,
          })
          setSelected(created.name)
          await load()
        }}
      />
    </div>
  )
}

type CellKind = 'leader' | 'isr' | 'out-of-sync' | 'none'

function cellKind(p: KafkaPartitionDetail, brokerId: number): CellKind {
  if (p.leader === brokerId) return 'leader'
  if (!p.replicas.includes(brokerId)) return 'none'
  return p.isr.includes(brokerId) ? 'isr' : 'out-of-sync'
}

function PartitionMatrix({ topic, brokers }: { topic: KafkaTopicDetail | null; brokers: KafkaBroker[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>分区矩阵{topic ? ` · ${topic.name}` : ''}</CardTitle>
        <CardDescription>
          行是分区，列是 broker。停止某个 broker 后观察 leader 迁移与 ISR 收缩；重新启动后副本追上进度会重新加入 ISR。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {!topic ? (
          <div className="py-12 text-center text-sm text-muted-foreground">选择左侧的 topic</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-separate border-spacing-1.5 text-sm">
              <thead>
                <tr>
                  <th className="w-20 text-left text-xs font-medium text-muted-foreground">分区</th>
                  {brokers.map((b) => (
                    <th key={b.id} className="min-w-28 text-center text-xs font-medium">
                      <div className={cn('inline-flex items-center gap-1.5', b.node.status !== 'RUNNING' && 'text-muted-foreground')}>
                        <StatusDot status={b.node.status} />
                        {b.node.displayName}
                      </div>
                      <div className="font-mono text-[10px] text-muted-foreground">node.id={b.id}</div>
                    </th>
                  ))}
                  <th className="text-right text-xs font-medium text-muted-foreground">ISR</th>
                  <th className="text-right text-xs font-medium text-muted-foreground">LEO</th>
                </tr>
              </thead>
              <tbody>
                {topic.partitions.map((p) => (
                  <tr key={p.partition}>
                    <td className="font-mono text-xs">
                      P{p.partition}
                      {p.leader < 0 && <span className="ml-1 text-status-failed">离线</span>}
                    </td>
                    {brokers.map((b) => {
                      const kind = cellKind(p, b.id)
                      const down = b.node.status !== 'RUNNING'
                      return (
                        <td key={b.id} className="p-0">
                          <div
                            className={cn(
                              'flex h-10 items-center justify-center gap-1 rounded-md border text-xs font-medium transition-colors duration-500',
                              kind === 'leader' && 'border-mq-accent bg-mq-accent text-mq-accent-foreground',
                              kind === 'isr' && 'border-mq-accent/50 bg-mq-accent-soft text-mq-accent',
                              kind === 'out-of-sync' && 'border-dashed border-status-failed/60 bg-status-failed/10 text-status-failed',
                              kind === 'none' && 'border-dashed text-muted-foreground/50',
                              down && kind !== 'none' && 'opacity-80',
                            )}
                            title={
                              kind === 'leader'
                                ? 'Leader 副本：负责该分区全部读写'
                                : kind === 'isr'
                                  ? 'Follower（ISR 内）：已与 leader 同步，可被选为新 leader'
                                  : kind === 'out-of-sync'
                                    ? 'Follower（已掉出 ISR）：broker 停止或复制落后'
                                    : '该 broker 不持有此分区副本'
                            }
                          >
                            {kind === 'leader' && <Crown className="size-3.5" />}
                            {kind === 'leader' ? 'Leader' : kind === 'isr' ? 'Follower' : kind === 'out-of-sync' ? '不同步' : '—'}
                          </div>
                        </td>
                      )
                    })}
                    <td className="text-right font-mono text-xs tabular-nums">
                      <span className={cn(p.isr.length < p.replicas.length && 'text-status-degraded')}>
                        {p.isr.length}/{p.replicas.length}
                      </span>
                    </td>
                    <td className="text-right font-mono text-xs tabular-nums">{p.logEndOffset}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="flex flex-wrap gap-4 text-xs text-muted-foreground">
          <Legend className="border-mq-accent bg-mq-accent" label="Leader" />
          <Legend className="border-mq-accent/50 bg-mq-accent-soft" label="Follower（ISR 内）" />
          <Legend className="border-dashed border-status-failed/60 bg-status-failed/10" label="Follower（掉出 ISR）" />
          <Legend className="border-dashed" label="无副本" />
        </div>
      </CardContent>
    </Card>
  )
}

function Legend({ className, label }: { className: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={cn('size-3 rounded-sm border', className)} />
      {label}
    </span>
  )
}

function LeaderTimeline({ changes, brokers }: { changes: KafkaLeaderChange[]; brokers: KafkaBroker[] }) {
  const nameOf = useMemo(() => {
    const map = new Map(brokers.map((b) => [b.id, b.node.displayName]))
    return (id: number) => (id < 0 ? '无 leader' : (map.get(id) ?? `broker ${id}`))
  }, [brokers])

  return (
    <Card>
      <CardHeader>
        <CardTitle>Leader 切换时间线</CardTitle>
        <CardDescription>每次分区 leader 变化都会记录在这里——试试在「概览」里停掉一个 broker</CardDescription>
      </CardHeader>
      <CardContent>
        {changes.length === 0 ? (
          <div className="py-8 text-center text-sm text-muted-foreground">暂无 leader 切换记录</div>
        ) : (
          <div className="max-h-64 overflow-y-auto">
            <ol className="relative ml-2 space-y-3 border-l pl-5">
              {changes.map((c, i) => (
                <li key={`${c.timestamp}-${c.topic}-${c.partition}-${i}`} className="relative">
                  <span
                    className={cn(
                      'absolute top-1.5 -left-[26px] size-2.5 rounded-full ring-4 ring-card',
                      c.newLeader < 0 ? 'bg-status-failed' : 'bg-mq-accent',
                    )}
                  />
                  <div className="flex flex-wrap items-center gap-2 text-sm">
                    <span className="font-mono text-xs text-muted-foreground tabular-nums">
                      {new Date(c.timestamp).toLocaleTimeString('zh-CN', { hour12: false })}
                    </span>
                    <span className="font-mono text-xs">
                      {c.topic}-P{c.partition}
                    </span>
                    <span className="text-muted-foreground">{nameOf(c.previousLeader)}</span>
                    <ArrowRight className="size-3.5 text-muted-foreground" />
                    <span className={cn('font-medium', c.newLeader < 0 ? 'text-status-failed' : 'text-mq-accent')}>
                      {nameOf(c.newLeader)}
                    </span>
                  </div>
                </li>
              ))}
            </ol>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function CreateTopicDialog({
  open,
  onOpenChange,
  brokerCount,
  runningBrokers,
  onCreate,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  brokerCount: number
  runningBrokers: number
  onCreate: (req: { name: string; partitions: number; replicationFactor: number }) => Promise<void>
}) {
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState(3)
  const [rf, setRf] = useState(Math.min(3, Math.max(1, brokerCount)))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  let reason: string | null = null
  if (!/^[a-zA-Z0-9._-]{1,200}$/.test(name)) reason = '名称只能包含字母、数字、点、下划线和短横线'
  else if (partitions < 1 || partitions > 64) reason = '分区数范围 1–64'
  else if (rf < 1 || rf > brokerCount) reason = `副本因子范围 1–${brokerCount}`
  else if (rf > runningBrokers) reason = `只有 ${runningBrokers} 个 broker 在运行，副本因子不能超过它`

  async function submit() {
    setBusy(true)
    setError(null)
    try {
      await onCreate({ name: name.trim(), partitions, replicationFactor: rf })
      setName('')
      onOpenChange(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>创建 Topic</DialogTitle>
          <DialogDescription>分区决定并行度，副本因子决定容错能力（RF=3 可容忍 2 个 broker 故障）。</DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="kt-name">名称</Label>
            <Input id="kt-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="例如 lab-payments" />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-2">
              <Label htmlFor="kt-partitions">分区数</Label>
              <Input
                id="kt-partitions"
                type="number"
                min={1}
                max={64}
                value={partitions}
                onChange={(e) => setPartitions(Number(e.target.value) || 0)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="kt-rf">副本因子</Label>
              <Input
                id="kt-rf"
                type="number"
                min={1}
                max={brokerCount}
                value={rf}
                onChange={(e) => setRf(Number(e.target.value) || 0)}
              />
            </div>
          </div>
          {error && <div className="rounded-md bg-destructive/10 p-2 text-sm text-destructive">{error}</div>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <HintButton onClick={() => void submit()} disabledReason={busy ? '正在创建…' : name ? reason : '请输入名称'}>
            {busy && <LoaderCircle className="size-4 animate-spin" />}
            创建
          </HintButton>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
