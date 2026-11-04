'use client'

/**
 * RabbitMQ「确认与死信」标签页。
 *
 * 预置实验：手动 ack、nack 后重新入队、nack 进入死信、超出 prefetch、TTL 过期。
 * 每个实验一句话说明 + 一键运行，并以 ready → unacked → acked / 重新入队 / 死信 / 过期 的状态流转展示结果。
 * Qpid 不支持的特性（TTL 过期转死信）禁用并注明原因。
 */

import { useEffect, useRef, useState } from 'react'
import { ArrowRight, Ban, CheckCircle2, Clock, Gauge, LoaderCircle, Play, RotateCcw, Skull, Square } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { HintButton } from '@/components/hint-button'
import { OutcomeBadge, RABBIT_OUTCOMES } from '@/components/dashboard/message-workbench'
import { addRabbitConsumer, purgeRabbitQueue, removeRabbitConsumer, simulateMessages } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { ClusterSelection, ClusterTopology, MessageDeliveryResult } from '@/types/cluster'
import type { RabbitQueue } from '@/types/rabbit'
import { rabbitUnavailable, useRabbitRouting } from './routing-panel'

type ScenarioId = 'manual-ack' | 'nack-requeue' | 'nack-dlx' | 'prefetch' | 'ttl'

interface Scenario {
  id: ScenarioId
  title: string
  icon: React.ComponentType<{ className?: string }>
  summary: string
  /** 结果中重点观察的队列 */
  queues: string[]
  defaultCount: number
}

const SCENARIOS: Scenario[] = [
  {
    id: 'manual-ack',
    title: '手动 ack',
    icon: CheckCircle2,
    summary: '消费者处理完才发送 basic.ack，broker 收到 ack 后才把消息从队列删除；ack 之前消息处于 unacked 状态。',
    queues: ['orders.created'],
    defaultCount: 10,
  },
  {
    id: 'nack-requeue',
    title: 'nack 后重新入队',
    icon: RotateCcw,
    summary: '约 30% 的消息被 nack(requeue=true) 退回队列头部，再次投递时 redelivered=true，第二次处理成功后 ack。',
    queues: ['orders.created'],
    defaultCount: 10,
  },
  {
    id: 'nack-dlx',
    title: 'nack 进入死信',
    icon: Skull,
    summary: 'work.tasks 配置了死信交换机 lab.dlx。一半消息被 nack(requeue=false)，经 lab.dlx 路由进 dead.letters。',
    queues: ['work.tasks', 'dead.letters'],
    defaultCount: 10,
  },
  {
    id: 'prefetch',
    title: '超出 prefetch',
    icon: Gauge,
    summary: '常驻消费者 prefetch=2、每条处理 1 秒。一次写入多条后，最多只有 2 条处于 unacked，其余在 ready 中排队。',
    queues: ['work.tasks'],
    defaultCount: 12,
  },
  {
    id: 'ttl',
    title: 'TTL 过期',
    icon: Clock,
    summary: '消息带 1.5 秒 expiration，消费者 2.5 秒后才开始消费——消息在队列中过期，消费者收不到。',
    queues: ['orders.paid'],
    defaultCount: 5,
  },
]

const PREFETCH = 2
const PREFETCH_DELAY_MS = 1000

interface RunResult {
  scenario: ScenarioId
  at: string
  deliveries: MessageDeliveryResult[]
}

export function RabbitAckLab({ selection, topology }: { selection: ClusterSelection; topology: ClusterTopology | null }) {
  const unavailable = rabbitUnavailable(topology)
  const { routing, load } = useRabbitRouting(selection, !unavailable, 1000)
  const [active, setActive] = useState<ScenarioId>('manual-ack')
  const [counts, setCounts] = useState<Record<ScenarioId, number>>(
    () => Object.fromEntries(SCENARIOS.map((s) => [s.id, s.defaultCount])) as Record<ScenarioId, number>,
  )
  const [running, setRunning] = useState<ScenarioId | null>(null)
  const [result, setResult] = useState<RunResult | null>(null)
  const [prefetchConsumer, setPrefetchConsumer] = useState<string | null>(null)

  // 卸载时清理 prefetch 实验创建的常驻消费者
  const consumerRef = useRef<string | null>(null)
  useEffect(() => {
    consumerRef.current = prefetchConsumer
  }, [prefetchConsumer])
  useEffect(
    () => () => {
      if (consumerRef.current) void removeRabbitConsumer(selection, consumerRef.current).catch(() => undefined)
    },
    [selection],
  )

  // prefetch 实验：队列排空后自动移除常驻消费者
  const workTasks = routing?.queues.find((q) => q.name === 'work.tasks')
  const drained = prefetchConsumer !== null && workTasks !== undefined && workTasks.ready === 0 && workTasks.unacked === 0
  useEffect(() => {
    if (!drained || !prefetchConsumer) return
    const timer = setTimeout(() => {
      void removeRabbitConsumer(selection, prefetchConsumer)
        .then(() => toast.success('prefetch 实验完成', { description: '队列已排空，常驻消费者已移除' }))
        .catch(() => undefined)
        .finally(() => {
          setPrefetchConsumer(null)
          void load()
        })
    }, 1500)
    return () => clearTimeout(timer)
  }, [drained, prefetchConsumer, selection, load])

  const scenario = SCENARIOS.find((s) => s.id === active)!

  async function stopPrefetch() {
    if (!prefetchConsumer) return
    try {
      await removeRabbitConsumer(selection, prefetchConsumer)
      toast.success('已停止常驻消费者', { description: '未处理完的 unacked 消息会回到 ready' })
    } catch (err) {
      toast.error('停止失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setPrefetchConsumer(null)
      void load()
    }
  }

  async function run(id: ScenarioId) {
    const count = counts[id]
    setRunning(id)
    setResult(null)
    try {
      if (id === 'prefetch') {
        if (prefetchConsumer) await removeRabbitConsumer(selection, prefetchConsumer).catch(() => undefined)
        await purgeRabbitQueue(selection, 'work.tasks')
        const consumer = await addRabbitConsumer(selection, {
          queue: 'work.tasks',
          prefetch: PREFETCH,
          ackMode: 'manual',
          processingDelayMs: PREFETCH_DELAY_MS,
          nackRatio: 0,
          requeue: false,
        })
        setPrefetchConsumer(consumer.consumerId)
      }
      if (id === 'nack-dlx') await purgeRabbitQueue(selection, 'dead.letters').catch(() => undefined)

      const options = optionsOf(id)
      const res = await simulateMessages(selection, {
        topic: options.routingKey,
        consumerGroup: 'lab-ack',
        messageCount: count,
        producerNodeId: 'rabbit-producer',
        payloadTemplate: '{"lab":"' + id + '","index":{index},"at":"{timestamp}"}',
        options,
      })
      setResult({ scenario: id, at: new Date().toLocaleTimeString('zh-CN', { hour12: false }), deliveries: res.deliveries })
      toast.success(`「${SCENARIOS.find((s) => s.id === id)!.title}」已运行`, {
        description: id === 'prefetch' ? '观察下方 ready / unacked 的实时变化' : `${res.deliveries.length} 条结果`,
      })
      await load()
    } catch (err) {
      toast.error('实验运行失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setRunning(null)
    }
  }

  if (unavailable) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          {unavailable}。请先在「概览」中启动 rabbit-node-01，再回来运行确认与死信实验。
        </CardContent>
      </Card>
    )
  }

  const shownResult = result?.scenario === active ? result : null
  const queues = scenario.queues
    .map((name) => routing?.queues.find((q) => q.name === name))
    .filter((q): q is RabbitQueue => q !== undefined)

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,320px)_minmax(0,1fr)]">
      <div className="space-y-2" role="tablist" aria-label="实验场景">
        {SCENARIOS.map((s) => {
          const Icon = s.icon
          const selected = s.id === active
          return (
            <button
              key={s.id}
              type="button"
              role="tab"
              aria-selected={selected}
              onClick={() => setActive(s.id)}
              className={cn(
                'flex w-full items-start gap-3 rounded-xl border p-3 text-left transition-colors',
                selected ? 'border-mq-accent bg-mq-accent-soft' : 'hover:bg-muted/60',
              )}
            >
              <span
                className={cn(
                  'mt-0.5 flex size-8 shrink-0 items-center justify-center rounded-lg',
                  selected ? 'bg-mq-accent text-mq-accent-foreground' : 'bg-muted text-muted-foreground',
                )}
              >
                <Icon className="size-4" />
              </span>
              <span className="min-w-0">
                <span className="flex items-center gap-2 text-sm font-medium">
                  {s.title}
                  {running === s.id && <LoaderCircle className="size-3.5 animate-spin" />}
                  {s.id === 'prefetch' && prefetchConsumer && (
                    <Badge variant="outline" className="text-[10px]">
                      运行中
                    </Badge>
                  )}
                </span>
                <span className="mt-0.5 line-clamp-2 text-xs text-muted-foreground">{s.summary}</span>
              </span>
            </button>
          )
        })}
      </div>

      <div className="min-w-0 space-y-4">
        <Card>
          <CardHeader>
            <CardTitle>{scenario.title}</CardTitle>
            <CardDescription>{scenario.summary}</CardDescription>
            <CardAction>
              {active === 'prefetch' && prefetchConsumer && (
                <Button variant="outline" size="sm" onClick={() => void stopPrefetch()}>
                  <Square className="size-3.5" /> 停止消费者
                </Button>
              )}
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-4">
            <ScenarioParams id={active} />
            <div className="flex flex-wrap items-end gap-3">
              <div className="space-y-2">
                <Label htmlFor="ack-count">消息条数</Label>
                <Input
                  id="ack-count"
                  type="number"
                  className="w-28"
                  min={1}
                  max={50}
                  value={counts[active]}
                  onChange={(e) =>
                    setCounts((c) => ({ ...c, [active]: Math.max(1, Math.min(50, Number(e.target.value) || 1)) }))
                  }
                />
              </div>
              <HintButton
                onClick={() => void run(active)}
                disabledReason={running ? '有实验正在运行…' : null}
              >
                {running === active ? <LoaderCircle className="size-4 animate-spin" /> : <Play className="size-4" />}
                一键运行
              </HintButton>
            </div>
            {active === 'ttl' && <TtlLimitation />}
          </CardContent>
        </Card>

        {active === 'prefetch' ? (
          <PrefetchLive queue={workTasks} consumer={routing?.consumers.find((c) => c.consumerId === prefetchConsumer)} />
        ) : (
          <FlowCard result={shownResult} running={running === active} />
        )}

        <Card size="sm">
          <CardHeader>
            <CardTitle className="text-sm">相关队列实况</CardTitle>
            <CardDescription>每秒刷新，数据直接读取 Qpid 进程内对象模型</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>队列</TableHead>
                  <TableHead className="text-right">ready</TableHead>
                  <TableHead className="text-right">unacked</TableHead>
                  <TableHead className="text-right">消费者</TableHead>
                  <TableHead>参数</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {queues.map((q) => (
                  <TableRow key={q.name}>
                    <TableCell className="font-mono text-xs">{q.name}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.ready}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.unacked}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.consumers}</TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {[
                        q.deadLetterExchange ? `DLX=${q.deadLetterExchange}` : null,
                        q.messageTtlMs ? `TTL=${q.messageTtlMs}ms` : null,
                      ]
                        .filter(Boolean)
                        .join(' · ') || '—'}
                    </TableCell>
                  </TableRow>
                ))}
                {queues.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="py-6 text-center text-xs whitespace-normal text-muted-foreground">
                      演示队列不存在（可能已被删除）。重启 rabbit-node-01 会重新声明演示拓扑。
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function optionsOf(id: ScenarioId): Record<string, string> & { routingKey: string } {
  switch (id) {
    case 'manual-ack':
      return { exchange: 'lab.direct', routingKey: 'order.created', ackMode: 'manual', prefetch: '10', confirm: 'true' }
    case 'nack-requeue':
      return {
        exchange: 'lab.direct',
        routingKey: 'order.created',
        ackMode: 'manual',
        nackRatio: '0.3',
        requeue: 'true',
        prefetch: '10',
        confirm: 'true',
      }
    case 'nack-dlx':
      return {
        exchange: 'lab.direct',
        routingKey: 'task',
        ackMode: 'manual',
        nackRatio: '0.5',
        requeue: 'false',
        prefetch: '10',
        confirm: 'true',
      }
    case 'prefetch':
      return { exchange: 'lab.direct', routingKey: 'task', consume: 'false', confirm: 'true' }
    case 'ttl':
      return {
        exchange: 'lab.direct',
        routingKey: 'order.paid',
        ackMode: 'manual',
        ttlMs: '1500',
        consumeDelayMs: '2500',
        confirm: 'true',
      }
  }
}

function ScenarioParams({ id }: { id: ScenarioId }) {
  const o = optionsOf(id)
  const entries = Object.entries(o).filter(([k]) => k !== 'confirm')
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map(([k, v]) => (
        <span key={k} className="rounded-md border bg-muted/40 px-2 py-0.5 font-mono text-xs">
          {k}=<span className="text-mq-accent">{v}</span>
        </span>
      ))}
      {id === 'prefetch' && (
        <>
          <span className="rounded-md border bg-muted/40 px-2 py-0.5 font-mono text-xs">
            consumer.prefetch=<span className="text-mq-accent">{PREFETCH}</span>
          </span>
          <span className="rounded-md border bg-muted/40 px-2 py-0.5 font-mono text-xs">
            consumer.delay=<span className="text-mq-accent">{PREFETCH_DELAY_MS}ms</span>
          </span>
        </>
      )}
    </div>
  )
}

function TtlLimitation() {
  return (
    <div className="flex items-start justify-between gap-4 rounded-lg border border-dashed p-3">
      <div className="space-y-1">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Ban className="size-4 text-muted-foreground" /> 过期消息转入死信队列
        </div>
        <p className="text-xs text-muted-foreground">
          Qpid 不支持（真实 RabbitMQ 支持）：在 RabbitMQ 中，配置了 x-dead-letter-exchange 的队列里过期的消息会以
          reason=expired 进入死信队列；嵌入的 Qpid Broker-J 会直接丢弃过期消息。
        </p>
      </div>
      <Switch checked={false} disabled aria-label="过期消息转入死信队列（Qpid 不支持）" />
    </div>
  )
}

/** 终态 → 状态流转图中的最后一站 */
const TERMINALS = [
  { outcome: 'ACKED', label: 'acked', icon: CheckCircle2, tone: 'text-status-running' },
  { outcome: 'REQUEUED_THEN_ACKED', label: '重新入队 → acked', icon: RotateCcw, tone: 'text-status-running' },
  { outcome: 'DEAD_LETTERED', label: '死信', icon: Skull, tone: 'text-status-degraded' },
  { outcome: 'DISCARDED', label: '丢弃', icon: Ban, tone: 'text-status-failed' },
  { outcome: 'EXPIRED', label: '过期', icon: Clock, tone: 'text-status-failed' },
  { outcome: 'UNROUTABLE', label: '不可路由', icon: Ban, tone: 'text-status-failed' },
  { outcome: 'PENDING', label: '未收到', icon: Clock, tone: 'text-muted-foreground' },
] as const

function FlowCard({ result, running }: { result: RunResult | null; running: boolean }) {
  if (!result) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          {running ? (
            <span className="inline-flex items-center gap-2">
              <LoaderCircle className="size-4 animate-spin" /> 正在发布并消费…
            </span>
          ) : (
            '点击「一键运行」，这里会展示每条消息的状态流转'
          )}
        </CardContent>
      </Card>
    )
  }
  const rows = result.deliveries
  const tally: Record<string, number> = {}
  for (const d of rows) {
    const o = d.attributes?.outcome ?? 'PENDING'
    tally[o] = (tally[o] ?? 0) + 1
  }
  const redelivered = rows.filter((d) => d.attributes?.redelivered === 'true').length
  const delivered = rows.filter((d) => !['EXPIRED', 'UNROUTABLE', 'PENDING', 'BROKER_NACK'].includes(d.attributes?.outcome ?? '')).length

  return (
    <Card className="mq-fade-in">
      <CardHeader>
        <CardTitle>状态流转</CardTitle>
        <CardDescription>{result.at} 运行 · 共 {rows.length} 条</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="flex flex-wrap items-center gap-2">
          <Stage label="发布" value={rows.length} />
          <ArrowRight className="size-4 text-muted-foreground" />
          <Stage label="ready" value={rows.length} />
          <ArrowRight className="size-4 text-muted-foreground" />
          <Stage label="unacked（已投递）" value={delivered} hint={redelivered > 0 ? `其中 ${redelivered} 条重投` : undefined} />
          <ArrowRight className="size-4 text-muted-foreground" />
          <div className="flex flex-wrap gap-2">
            {TERMINALS.filter((t) => tally[t.outcome]).map((t) => {
              const Icon = t.icon
              return (
                <div key={t.outcome} className="flex items-center gap-2 rounded-lg border px-3 py-2">
                  <Icon className={cn('size-4', t.tone)} />
                  <div>
                    <div className="text-xs text-muted-foreground">{t.label}</div>
                    <div className="text-lg font-semibold tabular-nums">{tally[t.outcome]}</div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        <div className="max-h-80 overflow-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>消息</TableHead>
                <TableHead>队列</TableHead>
                <TableHead>结果</TableHead>
                <TableHead>重投</TableHead>
                <TableHead>说明</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((d, i) => (
                <TableRow key={`${d.messageKey}-${d.attributes?.queue}-${i}`}>
                  <TableCell className="font-mono text-xs">{d.messageKey.split('-').slice(0, 2).join('-')}</TableCell>
                  <TableCell className="font-mono text-xs">{d.attributes?.queue ?? '—'}</TableCell>
                  <TableCell>
                    <OutcomeBadge outcome={d.attributes?.outcome ?? 'PENDING'} />
                  </TableCell>
                  <TableCell className="text-xs">{d.attributes?.redelivered === 'true' ? '是' : ''}</TableCell>
                  <TableCell className="max-w-md truncate text-xs text-muted-foreground" title={d.detail}>
                    {d.detail}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="text-xs text-muted-foreground">
          图例：
          {Object.entries(RABBIT_OUTCOMES)
            .filter(([k]) => tally[k])
            .map(([k, v]) => `${v.label}（${k}）`)
            .join('、')}
        </p>
      </CardContent>
    </Card>
  )
}

function Stage({ label, value, hint }: { label: string; value: number; hint?: string }) {
  return (
    <div className="rounded-lg border bg-muted/30 px-3 py-2">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg font-semibold tabular-nums">{value}</div>
      {hint && <div className="text-[10px] text-muted-foreground">{hint}</div>}
    </div>
  )
}

function PrefetchLive({
  queue,
  consumer,
}: {
  queue: RabbitQueue | undefined
  consumer: { consumerId: string; acked: number; inFlight: number; prefetch: number } | undefined
}) {
  const ready = queue?.ready ?? 0
  const unacked = queue?.unacked ?? 0
  const acked = consumer?.acked ?? 0
  const total = Math.max(ready + unacked + acked, 1)
  return (
    <Card>
      <CardHeader>
        <CardTitle>实时：ready → unacked → acked</CardTitle>
        <CardDescription>
          {consumer
            ? `常驻消费者 ${consumer.consumerId}：prefetch=${consumer.prefetch}，unacked 永远不超过 prefetch`
            : '运行实验后，这里每秒刷新 work.tasks 的消息分布'}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-3 gap-3">
          <LiveStat label="ready（排队）" value={ready} className="text-status-degraded" />
          <LiveStat
            label={`unacked（≤ ${consumer?.prefetch ?? PREFETCH}）`}
            value={unacked}
            className={cn('text-mq-accent', consumer && unacked >= consumer.prefetch && 'font-bold')}
          />
          <LiveStat label="acked（已确认）" value={acked} className="text-status-running" />
        </div>
        <div className="flex h-4 overflow-hidden rounded-full bg-muted" aria-hidden>
          <div className="bg-status-degraded transition-[width] duration-500" style={{ width: `${(ready / total) * 100}%` }} />
          <div className="bg-mq-accent transition-[width] duration-500" style={{ width: `${(unacked / total) * 100}%` }} />
          <div className="bg-status-running transition-[width] duration-500" style={{ width: `${(acked / total) * 100}%` }} />
        </div>
        <div className="flex flex-wrap gap-2">
          {Array.from({ length: Math.min(unacked + ready, 50) }).map((_, i) => (
            <span
              key={i}
              className={cn(
                'size-5 rounded transition-colors duration-500',
                i < unacked ? 'mq-pulse bg-mq-accent' : 'bg-status-degraded/60',
              )}
              title={i < unacked ? '已推送给消费者，等待 ack' : '在队列中等待（受 prefetch 限制未推送）'}
            />
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

function LiveStat({ label, value, className }: { label: string; value: number; className?: string }) {
  return (
    <div className="rounded-lg border p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={cn('mt-1 text-3xl font-semibold tabular-nums', className)}>{value}</div>
    </div>
  )
}
