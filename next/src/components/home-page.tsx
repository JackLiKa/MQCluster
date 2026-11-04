'use client'

/**
 * 首页——三种 MQ 的入口卡片、推荐学习路径与资源占用提示。
 *
 * - 卡片显示实时运行状态（来自全局 ClusterStatusProvider，10 秒轮询），可直接一键启动 / 停止或进入实验室
 * - 资源提示：JVM 堆占用 + 各嵌入式集群的大致堆开销（来自阶段 0 实测）
 */

import Link from 'next/link'
import { useMemo } from 'react'
import { ArrowRight, BookOpen, Cpu, GitCompareArrows, LoaderCircle, MemoryStick, Play, ServerCrash, Square } from 'lucide-react'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { HintButton } from '@/components/hint-button'
import { StatusDot } from '@/components/status-dot'
import { selectionOf, useClusterStatus, type MqStatusSummary } from '@/components/cluster-status-provider'
import { useNodeOperations } from '@/hooks/use-node-operations'
import { MQ_LIST, type MqMeta } from '@/lib/mq-registry'
import { cn } from '@/lib/utils'
import type { ClusterSelection } from '@/types/cluster'

/** 各嵌入式集群全部运行时的大致堆占用（docs/MQ_FEATURE_MATRIX.md 实测） */
const HEAP_ESTIMATE: Record<string, string> = {
  rocketmq: '堆占用较小（commitlog 走 mmap，堆外）',
  kafka: '约 70 MB（1 controller + 3 broker）',
  rabbitmq: '约 30 MB（Qpid Broker-J）',
}

const LEARNING_PATH = [
  { title: '启动集群', desc: '在任一实验室点击「一键启动集群」，观察拓扑中节点依次变为运行中。', href: '/mq/rocketmq?tab=overview' },
  { title: '收发消息', desc: '在「消息工作台」真实生产并消费消息，查看逐条投递结果。', href: '/mq/rocketmq?tab=workbench' },
  { title: '制造故障', desc: '停止 Kafka 的 leader broker，看分区 leader 如何切换、ISR 如何收缩。', href: '/mq/kafka?tab=topics' },
  { title: '理解消费', desc: '增减 Kafka 消费者触发 rebalance；在 RabbitMQ 中体验 ack、prefetch 与死信。', href: '/mq/rabbitmq?tab=ack' },
  { title: '横向对比', desc: '对照存储、路由、顺序性与高可用，总结三种 MQ 的设计取舍。', href: '/compare' },
]

export function HomePage() {
  const { summaries, backendDown, loaded, systemProfile, totalRunning, refresh } = useClusterStatus()

  return (
    <div className="mx-auto w-full max-w-[1600px] space-y-8 px-4 py-8 lg:px-6">
      <section className="space-y-2">
        <h1 className="text-3xl font-semibold tracking-tight">在一台电脑上，动手学会三种消息队列</h1>
        <p className="max-w-3xl text-muted-foreground">
          平台在后端进程内运行真实的 RocketMQ 5、Kafka（KRaft）与 AMQP 0-9-1 broker，并用各自的官方客户端收发消息。
          启停节点、制造故障、观察路由与消费——所有现象都来自真实运行时，而不是动画模拟。
        </p>
      </section>

      {loaded && backendDown && (
        <Card className="border-status-failed/40">
          <CardContent className="flex flex-wrap items-center gap-3">
            <ServerCrash className="size-5 text-status-failed" />
            <div className="flex-1 text-sm">
              <div className="font-medium">无法连接后端服务</div>
              <div className="text-muted-foreground">
                请在 java 目录执行 ./mvnw spring-boot:run（端口 8088）。页面每 10 秒自动重试。
              </div>
            </div>
            <Button variant="outline" size="sm" onClick={refresh}>
              立即重试
            </Button>
          </CardContent>
        </Card>
      )}

      <section className="grid gap-4 lg:grid-cols-3" aria-label="MQ 实验室入口">
        {MQ_LIST.map((meta) => (
          <MqCard key={meta.slug} meta={meta} summary={summaries[meta.slug]} loaded={loaded} onChanged={refresh} />
        ))}
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_420px]">
        <Card>
          <CardHeader>
            <CardTitle>推荐学习路径</CardTitle>
            <CardDescription>按顺序完成 5 个小实验，大约 30 分钟</CardDescription>
          </CardHeader>
          <CardContent>
            <ol className="grid gap-3 md:grid-cols-5">
              {LEARNING_PATH.map((step, i) => (
                <li key={step.title}>
                  <Link
                    href={step.href}
                    className="group flex h-full flex-col gap-1.5 rounded-xl border p-3 transition-colors hover:bg-muted/60"
                  >
                    <span className="flex size-6 items-center justify-center rounded-full bg-foreground text-xs text-background">
                      {i + 1}
                    </span>
                    <span className="text-sm font-medium">{step.title}</span>
                    <span className="text-xs leading-relaxed text-muted-foreground">{step.desc}</span>
                  </Link>
                </li>
              ))}
            </ol>
            <div className="mt-4 flex flex-wrap gap-2">
              <Button variant="outline" size="sm" nativeButton={false} render={<Link href="/guide" />}>
                <BookOpen className="size-4" /> 分步学习指南
              </Button>
              <Button variant="outline" size="sm" nativeButton={false} render={<Link href="/compare" />}>
                <GitCompareArrows className="size-4" /> 三种 MQ 对比
              </Button>
            </div>
          </CardContent>
        </Card>

        <ResourceCard
          totalRunning={totalRunning}
          maxHeapMb={systemProfile?.maxHeapMb ?? null}
          availableHeapMb={systemProfile?.availableHeapMb ?? null}
          cores={systemProfile?.logicalCores ?? null}
          runningBySlug={Object.fromEntries(MQ_LIST.map((m) => [m.slug, summaries[m.slug].running]))}
        />
      </section>
    </div>
  )
}

function MqCard({
  meta,
  summary,
  loaded,
  onChanged,
}: {
  meta: MqMeta
  summary: MqStatusSummary
  loaded: boolean
  onChanged: () => void
}) {
  const Icon = meta.icon
  const provider = summary.provider
  const canOperate = !!provider && provider.mode === 'PSEUDO' && provider.capabilities.includes('CLUSTER_OPERATIONS')
  const status = summary.starting > 0 ? 'STARTING' : summary.running > 0 ? 'RUNNING' : 'STOPPED'

  return (
    <Card data-mq={meta.slug} className="relative overflow-hidden">
      <div className="absolute inset-x-0 top-0 h-1 bg-mq-accent" aria-hidden />
      <CardHeader>
        <div className="flex items-center gap-3">
          <span className="flex size-11 items-center justify-center rounded-xl bg-mq-accent text-mq-accent-foreground shadow-sm">
            <Icon className="size-6" />
          </span>
          <div className="min-w-0">
            <CardTitle className="text-lg">{meta.name}</CardTitle>
            <CardDescription>{meta.tagline}</CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="flex flex-1 flex-col gap-4">
        <div className="flex flex-1 flex-wrap content-start gap-1.5">
          {meta.highlights.map((h) => (
            <span key={h} className="rounded-full bg-mq-accent-soft px-2.5 py-0.5 text-xs text-mq-accent">
              {h}
            </span>
          ))}
        </div>
        <div className="flex items-center justify-between rounded-lg border bg-muted/30 px-3 py-2 text-sm">
          {!loaded ? (
            <span className="text-muted-foreground">读取状态中…</span>
          ) : !provider ? (
            <span className="text-muted-foreground">后端未启用</span>
          ) : (
            <>
              <span className="inline-flex items-center gap-2">
                <StatusDot status={status} />
                {summary.total === 0
                  ? '无节点'
                  : summary.running === 0
                    ? '集群未运行'
                    : `${summary.running}/${summary.total} 个节点运行中`}
              </span>
              <span className="flex gap-1" aria-hidden>
                {summary.statuses.map((s, i) => (
                  <StatusDot key={i} status={s} />
                ))}
              </span>
            </>
          )}
        </div>
      </CardContent>
      <CardFooter className="gap-2">
        <Button className="flex-1" nativeButton={false} render={<Link href={`/mq/${meta.slug}`} />}>
          进入实验室 <ArrowRight className="size-4" />
        </Button>
        {canOperate && provider && (
          <QuickClusterButton selection={selectionOf(provider)} summary={summary} onChanged={onChanged} />
        )}
      </CardFooter>
    </Card>
  )
}

function QuickClusterButton({
  selection,
  summary,
  onChanged,
}: {
  selection: ClusterSelection
  summary: MqStatusSummary
  onChanged: () => void
}) {
  const refresh = useMemo(() => async () => onChanged(), [onChanged])
  const ops = useNodeOperations(selection, refresh)
  const allRunning = summary.total > 0 && summary.running === summary.total
  if (ops.clusterBusy) {
    return (
      <Button variant="outline" disabled>
        <LoaderCircle className="size-4 animate-spin" /> 处理中
      </Button>
    )
  }
  return allRunning ? (
    <HintButton variant="outline" onClick={() => void ops.operateAll('STOP')} disabledReason={null}>
      <Square className="size-4" /> 全部停止
    </HintButton>
  ) : (
    <HintButton variant="outline" onClick={() => void ops.operateAll('START')} disabledReason={summary.starting > 0 ? '有节点正在启动' : null}>
      <Play className="size-4" /> 一键启动
    </HintButton>
  )
}

function ResourceCard({
  totalRunning,
  maxHeapMb,
  availableHeapMb,
  cores,
  runningBySlug,
}: {
  totalRunning: number
  maxHeapMb: number | null
  availableHeapMb: number | null
  cores: number | null
  runningBySlug: Record<string, number>
}) {
  const used = maxHeapMb !== null && availableHeapMb !== null ? Math.max(0, maxHeapMb - availableHeapMb) : null
  const percent = used !== null && maxHeapMb ? Math.round((used / maxHeapMb) * 100) : null
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <MemoryStick className="size-4" /> 资源占用
        </CardTitle>
        <CardDescription>三种 MQ 可以同时运行；全部启动约增加 100 MB 堆</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-1.5">
          <div className="flex justify-between text-sm">
            <span>JVM 堆</span>
            <span className="tabular-nums text-muted-foreground">
              {used !== null && maxHeapMb !== null ? `${used} / ${maxHeapMb} MB` : '—'}
            </span>
          </div>
          <Progress value={percent ?? 0} aria-label="JVM 堆占用" />
          {percent !== null && percent > 80 && (
            <p className="text-xs text-status-degraded">堆占用较高，建议停止暂不使用的集群</p>
          )}
        </div>
        <div className="flex items-center justify-between text-sm">
          <span className="inline-flex items-center gap-1.5">
            <Cpu className="size-4 text-muted-foreground" /> 运行中节点
          </span>
          <span className="tabular-nums">
            {totalRunning}
            {cores !== null && <span className="text-muted-foreground"> · {cores} 核</span>}
          </span>
        </div>
        <ul className="space-y-1.5 text-xs">
          {MQ_LIST.map((m) => (
            <li key={m.slug} data-mq={m.slug} className="flex items-center justify-between gap-2">
              <span className={cn('inline-flex items-center gap-1.5', runningBySlug[m.slug] > 0 ? 'text-foreground' : 'text-muted-foreground')}>
                <span className="size-2 rounded-full bg-mq-accent" /> {m.name}
              </span>
              <span className="text-muted-foreground">{HEAP_ESTIMATE[m.slug]}</span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}
