'use client'

/**
 * 「压力实验」标签页——用真实客户端把 broker 打到过载，看它怎么被冲垮。
 *
 * 三件事保证"真实"：
 * 1. 发送用的是各 MQ 的官方客户端（KafkaProducer / DefaultMQProducer / RabbitMQ Channel），
 *    后端刻意关掉了会掩盖失败的重试；
 * 2. 失败原因来自客户端真正抛出的异常（TimeoutException / SYSTEM_BUSY 等），原样聚合展示；
 * 3. 判定文案由后端根据真实成功/失败计数生成，前端不改写。
 *
 * 压测是后端后台任务：启动后按秒轮询状态，曲线随采样点增长；跑完或手动停止后停止轮询。
 */

import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Ban,
  CircleAlert,
  Gauge,
  LoaderCircle,
  Play,
  ShieldCheck,
  Square,
  TriangleAlert,
  Zap,
} from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { EChart } from '@/components/echart'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { HintButton } from '@/components/hint-button'
import { useChartPalette, type ChartPalette } from '@/hooks/use-chart-palette'
import { fetchStressStatus, startStress, stopStress } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { ClusterSelection } from '@/types/cluster'
import type { StressStartRequest, StressStatus } from '@/types/lab'
import type { MqMeta, MqSlug } from '@/lib/mq-registry'

/** 预设：一键切换成三种典型压力形态 */
const PRESETS = [
  {
    id: 'warmup',
    label: '温和热身',
    targetRate: 500,
    threads: 4,
    durationSeconds: 20,
    payloadBytes: 256,
    description: '远低于 broker 极限，用来确认链路通畅、建立 p99 基线',
  },
  {
    id: 'flood',
    label: '洪峰冲垮',
    targetRate: 50_000,
    threads: 32,
    durationSeconds: 30,
    payloadBytes: 4096,
    description: '满速率 + 4 KB 负载，正常会在数秒内出现拒绝、超时或流控',
  },
  {
    id: 'sustained',
    label: '持续高压',
    targetRate: 5_000,
    threads: 8,
    durationSeconds: 120,
    payloadBytes: 1024,
    description: '贴着临界点长时间运行，观察队列堆积、时延爬升与恢复过程',
  },
] as const

type PresetId = (typeof PRESETS)[number]['id']

/**
 * 「洪峰冲垮」档额外收紧各客户端能忍耐的时间。
 *
 * <p>只把速率拉满是不够的：Kafka 默认 16 MB 缓冲区 + 2 s 阻塞、RabbitMQ 3 s confirm 超时，
 * 客户端会默默把流量挡住而不报错，看起来「没被冲垮」。把这些忍耐值调小，
 * broker 一慢客户端就立刻抛异常，过载才看得见。
 */
const FLOOD_KNOBS: Record<MqSlug, Partial<Form>> = {
  kafka: { bufferMemoryMb: 8, maxBlockMs: 500 },
  rocketmq: { sendTimeoutMs: 1000 },
  // Qpid 处理 4 KB 小消息非常快（实测 46k/s 不报错），要压垮它得把负载提到 64 KB：
  // 32 条同步 confirm 通道会被大包传输拖死，实测吞吐塌到目标的 10%
  rabbitmq: { confirmTimeoutMs: 300, payloadBytes: 65_536 },
}

interface Form {
  topic: string
  targetRate: number
  threads: number
  durationSeconds: number
  payloadBytes: number
  acks: string
  /** RabbitMQ */
  exchange: string
  confirmTimeoutMs: number
  /** Kafka */
  bufferMemoryMb: number
  maxBlockMs: number
  /** RocketMQ */
  sendTimeoutMs: number
}

const LIMITS = {
  targetRate: [1, 50_000],
  threads: [1, 32],
  durationSeconds: [3, 300],
  payloadBytes: [16, 65_536],
} as const

/** 各 MQ 的初值：RabbitMQ 默认投到 lab.direct → order.created，保证消息真的落到队列里 */
function defaultsOf(slug: MqSlug): Form {
  if (slug === 'rabbitmq') {
    return {
      topic: 'order.created',
      targetRate: 500,
      threads: 4,
      durationSeconds: 20,
      payloadBytes: 256,
      acks: 'all',
      exchange: 'lab.direct',
      confirmTimeoutMs: 3000,
      bufferMemoryMb: 16,
      maxBlockMs: 2000,
      sendTimeoutMs: 3000,
    }
  }
  return {
    topic: 'lab-stress',
    targetRate: 500,
    threads: 4,
    durationSeconds: 20,
    payloadBytes: 256,
    acks: 'all',
    exchange: 'lab.direct',
    confirmTimeoutMs: 3000,
    bufferMemoryMb: 16,
    maxBlockMs: 2000,
    sendTimeoutMs: 3000,
  }
}

export function StressPanel({ meta, selection }: { meta: MqMeta; selection: ClusterSelection }) {
  const ref = useRef<HTMLDivElement>(null)
  const palette = useChartPalette(ref)
  const [form, setForm] = useState<Form>(() => defaultsOf(meta.slug))
  const [preset, setPreset] = useState<PresetId | null>(null)
  const [status, setStatus] = useState<StressStatus | null>(null)
  const [starting, setStarting] = useState(false)
  const [stopping, setStopping] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const running = status?.running ?? false

  // 进入标签页时同步一次：可能已有实验在别处跑着
  useEffect(() => {
    let alive = true
    void fetchStressStatus(selection)
      .then((next) => {
        if (!alive) return
        setStatus(next)
        if (next.running) setError(null)
      })
      .catch(() => undefined)
    return () => {
      alive = false
    }
  }, [selection])

  // 运行期间按秒轮询；结束后自动停止
  useEffect(() => {
    if (!running) return
    const timer = setInterval(() => {
      void fetchStressStatus(selection)
        .then((next) => {
          setStatus(next)
          if (!next.running) setError(null)
        })
        .catch((err) => setError(err instanceof Error ? err.message : String(err)))
    }, 1000)
    return () => clearInterval(timer)
  }, [running, selection])

  function applyPreset(next: (typeof PRESETS)[number]) {
    setPreset(next.id)
    const defaults = defaultsOf(meta.slug)
    setForm((prev) => ({
      ...prev,
      targetRate: next.targetRate,
      threads: next.threads,
      durationSeconds: next.durationSeconds,
      payloadBytes: next.payloadBytes,
      // 先把客户端忍耐值复原成默认，再让「洪峰冲垮」收紧它们
      bufferMemoryMb: defaults.bufferMemoryMb,
      maxBlockMs: defaults.maxBlockMs,
      sendTimeoutMs: defaults.sendTimeoutMs,
      confirmTimeoutMs: defaults.confirmTimeoutMs,
      ...(next.id === 'flood' ? FLOOD_KNOBS[meta.slug] : {}),
    }))
  }

  async function start() {
    setStarting(true)
    setError(null)
    try {
      const next = await startStress(selection, buildRequest(meta.slug, form))
      setStatus(next)
      toast.success('压力实验已启动', {
        description: `${form.targetRate.toLocaleString()} 条/秒 × ${form.threads} 线程 × ${form.durationSeconds} 秒`,
      })
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      setError(message)
      toast.error('无法启动压力实验', { description: message })
    } finally {
      setStarting(false)
    }
  }

  async function stop() {
    setStopping(true)
    try {
      setStatus(await stopStress(selection))
    } catch (err) {
      toast.error('停止失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setStopping(false)
    }
  }

  const samples = useMemo(() => status?.samples ?? [], [status])
  const chartOption = useMemo(() => chartOptions(samples, status?.targetRate ?? 0, palette), [samples, status?.targetRate, palette])
  const active = status !== null && status.phase !== 'IDLE'

  return (
    <div ref={ref} className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex flex-wrap items-center gap-2">
            <Zap className="size-4 text-mq-accent" /> 压力实验
            {running && (
              <Badge className="bg-mq-accent text-mq-accent-foreground">
                <LoaderCircle className="size-3 animate-spin" /> 正在压测
              </Badge>
            )}
            {status?.phase === 'DONE' && <Badge variant="outline">已结束</Badge>}
          </CardTitle>
          <CardDescription>
            用真实 {meta.name} 客户端按目标速率持续投递，直到把 broker 打到拒绝、超时或流控。失败原因原样取自客户端异常，不做任何修饰。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="grid gap-2 sm:grid-cols-3">
            {PRESETS.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => applyPreset(item)}
                disabled={running}
                className={cn(
                  'rounded-xl border p-3 text-left transition-colors disabled:opacity-60',
                  preset === item.id ? 'border-mq-accent bg-mq-accent-soft' : 'hover:bg-muted/60',
                )}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium">{item.label}</span>
                  <span className="font-mono text-[11px] text-muted-foreground tabular-nums">
                    {item.targetRate.toLocaleString()}/s
                  </span>
                </div>
                <div className="mt-1 text-[11px] leading-snug text-muted-foreground">{item.description}</div>
              </button>
            ))}
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <NumberField
              id="stress-rate"
              label="目标速率（条/秒）"
              value={form.targetRate}
              min={LIMITS.targetRate[0]}
              max={LIMITS.targetRate[1]}
              step={100}
              onChange={(v) => setForm((f) => ({ ...f, targetRate: v }))}
              disabled={running}
            />
            <NumberField
              id="stress-threads"
              label="生产者线程"
              value={form.threads}
              min={LIMITS.threads[0]}
              max={LIMITS.threads[1]}
              onChange={(v) => setForm((f) => ({ ...f, threads: v }))}
              disabled={running}
            />
            <NumberField
              id="stress-duration"
              label="持续时长（秒）"
              value={form.durationSeconds}
              min={LIMITS.durationSeconds[0]}
              max={LIMITS.durationSeconds[1]}
              onChange={(v) => setForm((f) => ({ ...f, durationSeconds: v }))}
              disabled={running}
            />
            <NumberField
              id="stress-payload"
              label="单条负载（字节）"
              value={form.payloadBytes}
              min={LIMITS.payloadBytes[0]}
              max={LIMITS.payloadBytes[1]}
              step={64}
              onChange={(v) => setForm((f) => ({ ...f, payloadBytes: v }))}
              disabled={running}
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <div className="space-y-1.5 xl:col-span-2">
              <Label htmlFor="stress-topic">{meta.slug === 'rabbitmq' ? 'routingKey' : 'topic'}</Label>
              <Input
                id="stress-topic"
                value={form.topic}
                disabled={running}
                onChange={(e) => setForm((f) => ({ ...f, topic: e.target.value }))}
              />
            </div>
            {meta.slug === 'rabbitmq' ? (
              <>
                <div className="space-y-1.5">
                  <Label>exchange</Label>
                  <Select
                    value={form.exchange}
                    onValueChange={(v) => v && setForm((f) => ({ ...f, exchange: v }))}
                    disabled={running}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {['lab.direct', 'lab.topic', 'lab.fanout', 'lab.headers'].map((name) => (
                        <SelectItem key={name} value={name}>
                          {name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <NumberField
                  id="stress-confirm"
                  label="confirm 超时（毫秒）"
                  value={form.confirmTimeoutMs}
                  min={200}
                  max={60_000}
                  step={500}
                  onChange={(v) => setForm((f) => ({ ...f, confirmTimeoutMs: v }))}
                  disabled={running}
                />
              </>
            ) : meta.slug === 'kafka' ? (
              <>
                <div className="space-y-1.5">
                  <Label>acks</Label>
                  <Select
                    value={form.acks}
                    onValueChange={(v) => v && setForm((f) => ({ ...f, acks: v }))}
                    disabled={running}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">all（等全部 ISR 落盘）</SelectItem>
                      <SelectItem value="1">1（等 leader 落盘）</SelectItem>
                      <SelectItem value="0">0（不等确认）</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <NumberField
                  id="stress-timeout"
                  label="max.block.ms（毫秒）"
                  value={form.maxBlockMs}
                  min={100}
                  max={60_000}
                  step={100}
                  onChange={(v) => setForm((f) => ({ ...f, maxBlockMs: v }))}
                  disabled={running}
                />
              </>
            ) : (
              <NumberField
                id="stress-timeout"
                label="发送超时（毫秒）"
                value={form.sendTimeoutMs}
                min={200}
                max={60_000}
                step={500}
                onChange={(v) => setForm((f) => ({ ...f, sendTimeoutMs: v }))}
                disabled={running}
              />
            )}
          </div>

          {meta.slug === 'kafka' && (
            <div className="flex flex-wrap items-end gap-3">
              <NumberField
                id="stress-buffer"
                label="生产者缓冲区（MB）"
                value={form.bufferMemoryMb}
                min={1}
                max={512}
                onChange={(v) => setForm((f) => ({ ...f, bufferMemoryMb: v }))}
                disabled={running}
              />
              <p className="max-w-xl text-[11px] leading-snug text-muted-foreground">
                缓冲区越小越容易被冲垮：broker 消化不及时，缓冲区分配超过 max.block.ms 就会抛 TimeoutException。默认 16MB 是刻意贴近“会疼”的配置。
              </p>
            </div>
          )}

          <div className="flex flex-wrap items-center gap-3">
            <HintButton
              onClick={() => void start()}
              disabledReason={running ? '已有实验在运行，先停止再开始新的实验' : null}
            >
              {starting ? <LoaderCircle className="size-4 animate-spin" /> : <Play className="size-4" />}
              开始实验
            </HintButton>
            {running && (
              <Button variant="outline" onClick={() => void stop()} disabled={stopping}>
                {stopping ? <LoaderCircle className="size-4 animate-spin" /> : <Square className="size-4" />}
                停止
              </Button>
            )}
            <p className="text-xs text-muted-foreground">
              实验由后端后台线程执行，离开本页也会继续；节点未启动时会被拒绝（409）。
            </p>
          </div>

          {error && (
            <p className="flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/10 p-3 text-xs text-destructive">
              <CircleAlert className="mt-0.5 size-3.5 shrink-0" /> {error}
            </p>
          )}
        </CardContent>
      </Card>

      {!active ? (
        <Card className="border-dashed">
          <CardContent className="space-y-2 py-8 text-center text-sm text-muted-foreground">
            <p>点击「开始实验」后，这里会实时显示发送速率、失败率与 p99 耗时。</p>
            <p className="text-xs">
              想看到“被冲垮”的样子：选「洪峰冲垮」→ 开始实验 → 曲线里失败速率抬头、p99 飙升，下方会列出客户端真实抛出的异常。
              随后切到「内部机制」标签页，能看到写指针、日志段、队列深度在同一时刻被灌满的过程。
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
            <Kpi label="目标速率" value={`${(status?.targetRate ?? 0).toLocaleString()}/s`} />
            <Kpi label="累计成功" value={(status?.produced ?? 0).toLocaleString()} tone="text-status-running" />
            <Kpi
              label="累计失败"
              value={(status?.failed ?? 0).toLocaleString()}
              tone={(status?.failed ?? 0) > 0 ? 'text-status-failed' : undefined}
            />
            <Kpi
              label="失败率"
              value={`${failureRate(status).toFixed(1)}%`}
              tone={failureRate(status) > 0 ? 'text-status-failed' : undefined}
            />
            <Kpi label="p99 单次耗时" value={formatMs(status?.p99LatencyMs ?? 0)} />
          </div>

          <VerdictCard status={status} />

          <div className="grid gap-4 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Gauge className="size-4 text-mq-accent" /> 速率与失败
                </CardTitle>
                <CardDescription>
                  每秒一个采样点，共 {samples.length} 个 · 目标 {status?.targetRate.toLocaleString()}/s（虚线）
                </CardDescription>
              </CardHeader>
              <CardContent>
                {samples.length < 2 ? (
                  <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
                    <LoaderCircle className="mr-2 size-4 animate-spin" /> 正在采集第一个采样点…
                  </div>
                ) : (
                  <EChart option={chartOption} className="h-64" ariaLabel="发送速率与失败速率曲线" />
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Ban className="size-4 text-status-failed" /> 失败原因
                </CardTitle>
                <CardDescription>客户端真实抛出的异常，按出现次数倒序</CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                {(status?.errors.length ?? 0) === 0 ? (
                  <p className="py-6 text-center text-xs text-muted-foreground">
                    {running
                      ? '暂未出现失败——继续观察'
                      : '全程没有失败：要么 broker 真的扛住了，要么它靠背压把速率压了下来（看上面的结论）'}
                  </p>
                ) : (
                  status?.errors.map((item) => (
                    <div key={item.message} className="rounded-lg border p-2.5">
                      <div className="flex items-start justify-between gap-3">
                        <code className="break-all font-mono text-[11px] leading-snug">{item.message}</code>
                        <span className="shrink-0 font-mono text-xs tabular-nums text-status-failed">×{item.count}</span>
                      </div>
                    </div>
                  ))
                )}
                <p className="pt-1 text-[11px] leading-snug text-muted-foreground">
                  这些异常是真实过载的直接证据：Kafka 卡在缓冲区分配，RocketMQ 出现 TIMEOUT_CLEAN_QUEUE，
                  RabbitMQ 卡在 publisher confirm——都是 broker 拒绝接收的信号。
                </p>
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  )
}

/** 组装请求体：只把当前 MQ 用得到的特有参数发给后端 */
function buildRequest(slug: MqSlug, form: Form): StressStartRequest {
  const options: Record<string, string> = {}
  if (slug === 'kafka') {
    options.bufferMemoryBytes = String(form.bufferMemoryMb * 1024 * 1024)
    options.maxBlockMs = String(form.maxBlockMs)
  } else if (slug === 'rocketmq') {
    options.sendTimeoutMs = String(form.sendTimeoutMs)
  } else {
    options.exchange = form.exchange
    options.confirmTimeoutMs = String(form.confirmTimeoutMs)
  }
  return {
    topic: form.topic.trim(),
    targetRate: form.targetRate,
    threads: form.threads,
    durationSeconds: form.durationSeconds,
    payloadBytes: form.payloadBytes,
    acks: slug === 'rabbitmq' ? undefined : form.acks,
    options,
  }
}

function failureRate(status: StressStatus | null): number {
  if (!status) return 0
  const total = status.produced + status.failed
  return total === 0 ? 0 : (status.failed / total) * 100
}

function formatMs(ms: number): string {
  if (!ms) return '0 ms'
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`
}

function VerdictCard({ status }: { status: StressStatus | null }) {
  if (!status) return null
  const outcome = outcomeOf(status)
  const Icon =
    outcome === 'running'
      ? LoaderCircle
      : outcome === 'collapsed'
        ? TriangleAlert
        : outcome === 'throttled'
          ? Gauge
          : outcome === 'healthy'
            ? ShieldCheck
            : CircleAlert
  return (
    <div className={cn('flex items-start gap-3 rounded-xl border p-4', outcomeTone(outcome))}>
      <Icon className={cn('mt-0.5 size-5 shrink-0', outcome === 'running' && 'animate-spin')} />
      <div className="min-w-0 space-y-1">
        <div className="text-sm font-medium">
          {outcome === 'running'
            ? '实验进行中'
            : outcome === 'collapsed'
              ? '结论：已被冲垮'
              : outcome === 'throttled'
                ? '结论：被限流——吞吐塌了'
                : outcome === 'healthy'
                  ? '结论：未被冲垮'
                  : '实验结束'}
        </div>
        <p className="text-xs leading-relaxed">{status.verdict || '等待采样…'}</p>
        <p className="text-[11px] text-muted-foreground">
          已运行 {(status.elapsedMs / 1000).toFixed(1)} 秒 · 成功 {status.produced.toLocaleString()} 条 · 失败{' '}
          {status.failed.toLocaleString()} 条
        </p>
      </div>
    </div>
  )
}

type Outcome = 'idle' | 'running' | 'collapsed' | 'throttled' | 'healthy'

/**
 * 结论由后端按真实计数与达标率生成，前端只按结论前缀分档：
 * 失败率 ≥1% = 冲垮，速率远低于目标 = 被背压限流，其余 = 扛住了。
 */
function outcomeOf(status: StressStatus): Outcome {
  if (status.running) return 'running'
  if (status.phase !== 'DONE') return 'idle'
  if (status.verdict.startsWith('已被冲垮')) return 'collapsed'
  if (status.verdict.startsWith('被限流')) return 'throttled'
  return 'healthy'
}

function outcomeTone(outcome: Outcome): string {
  if (outcome === 'collapsed') return 'border-destructive/40 bg-destructive/10 text-destructive'
  if (outcome === 'throttled') return 'border-status-degraded/40 bg-status-degraded/10 text-status-degraded'
  if (outcome === 'healthy') return 'border-status-running/40 bg-status-running/10 text-status-running'
  if (outcome === 'running') return 'border-mq-accent/40 bg-mq-accent-soft text-foreground'
  return 'border-border bg-muted/40'
}

function Kpi({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className="rounded-lg border p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={cn('mt-1 text-2xl font-semibold tabular-nums', tone)}>{value}</div>
    </div>
  )
}

function NumberField({
  id,
  label,
  value,
  min,
  max,
  step = 1,
  onChange,
  disabled,
}: {
  id: string
  label: string
  value: number
  min: number
  max: number
  step?: number
  onChange: (value: number) => void
  disabled?: boolean
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="number"
        min={min}
        max={max}
        step={step}
        value={value}
        disabled={disabled}
        onChange={(e) => {
          const next = Number(e.target.value)
          onChange(Number.isFinite(next) ? Math.max(min, Math.min(max, next)) : min)
        }}
      />
    </div>
  )
}

function axisStyle(palette: ChartPalette) {
  return {
    axisLine: { lineStyle: { color: palette.border } },
    axisLabel: { color: palette.muted, fontSize: 11 },
    splitLine: { lineStyle: { color: palette.border, type: 'dashed' as const } },
  }
}

function chartOptions(samples: StressStatus['samples'], targetRate: number, palette: ChartPalette) {
  return {
    color: [palette.series[0], palette.status.FAILED],
    tooltip: {
      trigger: 'axis',
      backgroundColor: palette.card,
      borderColor: palette.border,
      textStyle: { color: palette.foreground },
    },
    legend: { data: ['发送成功', '失败'], textStyle: { color: palette.muted }, top: 0 },
    grid: { left: 56, right: 20, top: 36, bottom: 28 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: samples.map((s) => `${Math.round(s.elapsedMs / 1000)}s`),
      ...axisStyle(palette),
      splitLine: { show: false },
    },
    yAxis: { type: 'value', name: '条/秒', nameTextStyle: { color: palette.muted }, ...axisStyle(palette) },
    series: [
      {
        name: '发送成功',
        type: 'line',
        smooth: true,
        showSymbol: false,
        areaStyle: { opacity: 0.14 },
        data: samples.map((s) => +s.produceTps.toFixed(1)),
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { type: 'dashed', color: palette.muted, width: 1 },
          label: { formatter: '目标速率', color: palette.muted, fontSize: 10, position: 'insideEndTop' },
          data: [{ yAxis: targetRate }],
        },
      },
      {
        name: '失败',
        type: 'line',
        smooth: true,
        showSymbol: false,
        areaStyle: { opacity: 0.14 },
        data: samples.map((s) => +s.failTps.toFixed(1)),
      },
    ],
  }
}
