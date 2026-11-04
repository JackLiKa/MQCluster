'use client'

/**
 * 消息工作台——三种 MQ 共用的发送 / 消费验证表单。
 *
 * 通用字段：数量、消息模板、payload；各 MQ 特有参数通过 options 传给后端：
 * - RocketMQ：topic、消费组、生产者节点、消费者节点
 * - Kafka：topic、消费组、key、指定分区、acks、压缩方式；结果多出 partition / offset 列
 * - RabbitMQ：交换机、routing key、headers、ack 模式、nack 比例、requeue、prefetch、TTL、confirm
 * 集群不可用时发送按钮禁用并说明原因；数量超出本机安全上限时拦截。
 */

import { useEffect, useMemo, useState } from 'react'
import { LoaderCircle, Send } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { HintButton } from '@/components/hint-button'
import {
  fetchKafkaTopics,
  fetchMessageTemplates,
  fetchRabbitRouting,
  fetchRateLimit,
  simulateMessages,
} from '@/lib/api'
import type { MqMeta } from '@/lib/mq-registry'
import { cn } from '@/lib/utils'
import type {
  ClusterSelection,
  ClusterTopology,
  MessageDeliveryResult,
  MessageTemplate,
  RateLimitResult,
} from '@/types/cluster'
import type { RabbitExchange } from '@/types/rabbit'

const EMBEDDED_LIMIT = 500

/** RabbitMQ 投递结果中文说明与配色 */
export const RABBIT_OUTCOMES: Record<string, { label: string; tone: 'ok' | 'warn' | 'bad' | 'info' }> = {
  ACKED: { label: '已确认', tone: 'ok' },
  REQUEUED_THEN_ACKED: { label: '重新入队后确认', tone: 'ok' },
  DEAD_LETTERED: { label: '进入死信', tone: 'warn' },
  DISCARDED: { label: '被丢弃', tone: 'bad' },
  EXPIRED: { label: 'TTL 过期', tone: 'warn' },
  UNROUTABLE: { label: '不可路由', tone: 'bad' },
  BROKER_NACK: { label: 'Broker 拒绝', tone: 'bad' },
  QUEUED: { label: '已入队', tone: 'info' },
  PENDING: { label: '未投递', tone: 'info' },
}

export const TONE_CLASS = {
  ok: 'border-status-running/40 bg-status-running/10 text-status-running',
  warn: 'border-status-degraded/40 bg-status-degraded/10 text-status-degraded',
  bad: 'border-status-failed/40 bg-status-failed/10 text-status-failed',
  info: 'border-status-starting/40 bg-status-starting/10 text-status-starting',
} as const

export function OutcomeBadge({ outcome }: { outcome: string }) {
  const meta = RABBIT_OUTCOMES[outcome] ?? { label: outcome, tone: 'info' as const }
  return (
    <span className={cn('inline-flex items-center rounded-md border px-1.5 py-0.5 text-xs font-medium', TONE_CLASS[meta.tone])}>
      {meta.label}
    </span>
  )
}

interface WorkbenchProps {
  meta: MqMeta
  selection: ClusterSelection
  topology: ClusterTopology | null
}

/** 判断集群当前能否收发消息，返回不可用原因 */
function unavailableReason(meta: MqMeta, topology: ClusterTopology | null): string | null {
  const nodes = topology?.nodes ?? []
  const running = (role: string) => nodes.some((n) => n.labels?.role === role && n.status === 'RUNNING')
  if (meta.slug === 'rocketmq') {
    if (!running('nameserver')) return 'NameServer 未运行，生产者无法获取路由'
    if (!running('broker-master')) return '没有运行中的 Broker Master，消息无法写入'
    return null
  }
  if (meta.slug === 'kafka') {
    if (!running('kafka-controller')) return 'KRaft Controller 未运行，集群没有元数据仲裁'
    if (!running('kafka-broker')) return '没有运行中的 Broker'
    return null
  }
  if (!running('rabbit-broker')) return 'AMQP Broker 未运行'
  return null
}

export function MessageWorkbench({ meta, selection, topology }: WorkbenchProps) {
  const [templates, setTemplates] = useState<MessageTemplate[]>([])
  const [rateLimit, setRateLimit] = useState<RateLimitResult | null>(null)
  const [templateId, setTemplateId] = useState('custom')
  const [payload, setPayload] = useState('{"orderId":"{uuid}","index":{index}}')
  const [count, setCount] = useState(10)
  const [topic, setTopic] = useState(meta.defaultTopic)
  const [group, setGroup] = useState(meta.defaultGroup)
  const [options, setOptions] = useState<Record<string, string>>(defaultOptions(meta))
  const [producer, setProducer] = useState('')
  const [consumers, setConsumers] = useState<string[]>([])
  const [sending, setSending] = useState(false)
  const [results, setResults] = useState<MessageDeliveryResult[] | null>(null)
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    fetchMessageTemplates().then(setTemplates).catch(() => {})
    fetchRateLimit().then(setRateLimit).catch(() => {})
  }, [])

  const nodes = useMemo(() => topology?.nodes ?? [], [topology])
  const brokers = nodes.filter((n) => n.labels?.role === 'broker-master' || n.labels?.role === 'broker-slave')
  const effectiveProducer =
    meta.slug === 'rocketmq'
      ? producer || brokers.find((b) => b.status === 'RUNNING' && b.labels?.role === 'broker-master')?.nodeId || ''
      : meta.slug === 'kafka'
        ? 'kafka-producer'
        : 'rabbit-producer'

  const limit = Math.min(rateLimit?.maxMessages ?? EMBEDDED_LIMIT, meta.slug === 'rocketmq' ? Infinity : EMBEDDED_LIMIT)
  const unavailable = unavailableReason(meta, topology)
  const overLimit = count > limit
  const setOpt = (key: string, value: string) => setOptions((prev) => ({ ...prev, [key]: value }))

  const effectiveTopic =
    meta.slug === 'rabbitmq' ? options.routingKey || options.exchange || 'amq.default' : topic.trim()

  let blockReason = unavailable
  if (!blockReason && !effectiveTopic) blockReason = 'Topic 不能为空'
  if (!blockReason && !group.trim()) blockReason = '消费组不能为空'
  if (!blockReason && count < 1) blockReason = '消息数量至少为 1'
  if (!blockReason && overLimit) blockReason = `超过本机安全上限 ${limit} 条`
  if (!blockReason && meta.slug === 'rocketmq' && !effectiveProducer) blockReason = '请选择生产者节点'

  async function send() {
    setSending(true)
    const started = performance.now()
    try {
      const sendOptions = Object.fromEntries(Object.entries(options).filter(([, v]) => v !== ''))
      const res = await simulateMessages(selection, {
        topic: effectiveTopic,
        consumerGroup: group.trim(),
        messageCount: count,
        payloadTemplate: payload || undefined,
        producerNodeId: effectiveProducer,
        consumerNodeIds: meta.slug === 'rocketmq' && consumers.length > 0 ? consumers : undefined,
        options: Object.keys(sendOptions).length > 0 ? sendOptions : undefined,
      })
      setElapsed(performance.now() - started)
      setResults(res.deliveries)
      const ok = res.deliveries.filter((d) => d.success).length
      if (ok === res.deliveries.length) toast.success(`发送完成：${ok}/${res.deliveries.length} 条成功`)
      else toast.warning(`发送完成：${ok}/${res.deliveries.length} 条成功`, { description: '失败原因见结果表' })
    } catch (err) {
      toast.error('发送失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setSending(false)
    }
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,420px)_minmax(0,1fr)]">
      <Card>
        <CardHeader>
          <CardTitle>发送消息</CardTitle>
          <CardDescription>使用官方客户端向真实 broker 发送，并由消费者验证投递结果</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {meta.slug !== 'rabbitmq' && (
            <div className="grid grid-cols-2 gap-3">
              <Field label="Topic" htmlFor="wb-topic">
                <Input id="wb-topic" value={topic} onChange={(e) => setTopic(e.target.value)} list="wb-topic-list" />
                {meta.slug === 'kafka' && <KafkaTopicList selection={selection} enabled={!unavailable} />}
              </Field>
              <Field label="消费组" htmlFor="wb-group">
                <Input id="wb-group" value={group} onChange={(e) => setGroup(e.target.value)} />
              </Field>
            </div>
          )}

          {meta.slug === 'rocketmq' && (
            <RocketMqFields
              brokers={brokers}
              producer={producer}
              setProducer={setProducer}
              consumers={consumers}
              setConsumers={setConsumers}
            />
          )}
          {meta.slug === 'kafka' && <KafkaFields options={options} setOpt={setOpt} />}
          {meta.slug === 'rabbitmq' && (
            <RabbitFields selection={selection} options={options} setOpt={setOpt} enabled={!unavailable} />
          )}

          <Field
            label={`消息数量（上限 ${Number.isFinite(limit) ? limit : '-'}）`}
            htmlFor="wb-count"
            hint={
              rateLimit
                ? `上限按本机 CPU ${rateLimit.systemProfile.logicalCores} 核 / 可用堆 ${rateLimit.systemProfile.availableHeapMb} MB 计算${meta.slug === 'rocketmq' ? '' : '，嵌入式集群单次最多 500 条'}`
                : undefined
            }
          >
            <Input
              id="wb-count"
              type="number"
              min={1}
              value={count}
              aria-invalid={overLimit}
              onChange={(e) => setCount(Math.max(0, Number(e.target.value) || 0))}
            />
          </Field>

          <Field label="消息模板" hint="支持占位符 {index}、{timestamp}、{uuid}、{random}、{topic}">
            <Select
              value={templateId}
              onValueChange={(v) => {
                if (!v) return
                setTemplateId(v)
                const t = templates.find((x) => x.id === v)
                if (t) setPayload(t.template)
              }}
            >
              <SelectTrigger className="w-full">
                <SelectValue>{templateId === 'custom' ? '自定义' : templates.find((t) => t.id === templateId)?.name}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="custom">自定义</SelectItem>
                {templates.map((t) => (
                  <SelectItem key={t.id} value={t.id}>
                    {t.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Input
              aria-label="Payload"
              value={payload}
              onChange={(e) => {
                setPayload(e.target.value)
                setTemplateId('custom')
              }}
              className="mt-2 font-mono text-xs"
            />
          </Field>

          <HintButton className="w-full" onClick={send} disabled={sending} disabledReason={sending ? null : blockReason}>
            {sending ? <LoaderCircle className="size-4 animate-spin" /> : <Send className="size-4" />}
            {sending ? '发送中…' : `发送 ${count} 条消息`}
          </HintButton>
          {blockReason && !sending && <p className="text-xs text-muted-foreground">无法发送：{blockReason}</p>}
        </CardContent>
      </Card>

      <DeliveryResults meta={meta} results={results} elapsed={elapsed} />
    </div>
  )
}

function defaultOptions(meta: MqMeta): Record<string, string> {
  if (meta.slug === 'kafka') return { key: '', partition: '', acks: 'all', compression: 'none' }
  if (meta.slug === 'rabbitmq')
    return {
      exchange: 'lab.direct',
      routingKey: 'order.created',
      ackMode: 'manual',
      nackRatio: '0',
      requeue: 'false',
      prefetch: '10',
      ttlMs: '',
      confirm: 'true',
      consume: 'true',
      consumeDelayMs: '0',
    }
  return {}
}

function Field({
  label,
  htmlFor,
  hint,
  children,
  className,
}: {
  label: string
  htmlFor?: string
  hint?: string
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={cn('space-y-1.5', className)}>
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  )
}

function RocketMqFields({
  brokers,
  producer,
  setProducer,
  consumers,
  setConsumers,
}: {
  brokers: ClusterTopology['nodes']
  producer: string
  setProducer: (v: string) => void
  consumers: string[]
  setConsumers: (v: string[]) => void
}) {
  return (
    <>
      <Field label="生产者节点" hint="消息从该 Broker 写入；留空自动选择运行中的 Master">
        <Select value={producer || 'auto'} onValueChange={(v) => setProducer(!v || v === 'auto' ? '' : v)}>
          <SelectTrigger className="w-full">
            <SelectValue>{producer || '自动选择'}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="auto">自动选择</SelectItem>
            {brokers.map((b) => (
              <SelectItem key={b.nodeId} value={b.nodeId}>
                {b.displayName}（{b.status === 'RUNNING' ? '运行中' : '未运行'}）
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
      <Field label="消费者节点" hint="不勾选时自动使用所有 Broker">
        <div className="flex flex-wrap gap-2">
          {brokers.map((b) => {
            const checked = consumers.includes(b.nodeId)
            return (
              <button
                key={b.nodeId}
                type="button"
                aria-pressed={checked}
                onClick={() => setConsumers(checked ? consumers.filter((c) => c !== b.nodeId) : [...consumers, b.nodeId])}
                className={cn(
                  'rounded-md border px-2 py-1 font-mono text-xs transition-colors',
                  checked ? 'border-primary bg-primary text-primary-foreground' : 'hover:bg-muted',
                )}
              >
                {b.nodeId}
              </button>
            )
          })}
        </div>
      </Field>
    </>
  )
}

function KafkaTopicList({ selection, enabled }: { selection: ClusterSelection; enabled: boolean }) {
  const [topics, setTopics] = useState<string[]>([])
  useEffect(() => {
    if (!enabled) return
    fetchKafkaTopics(selection)
      .then((list) => setTopics(list.map((t) => t.name)))
      .catch(() => {})
  }, [selection, enabled])
  return (
    <datalist id="wb-topic-list">
      {topics.map((t) => (
        <option key={t} value={t} />
      ))}
    </datalist>
  )
}

function KafkaFields({ options, setOpt }: { options: Record<string, string>; setOpt: (k: string, v: string) => void }) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <Field label="消息 Key" htmlFor="wb-key" hint="相同 key 总是进入同一分区，保证分区内有序">
        <Input id="wb-key" value={options.key} placeholder="留空则轮询分区" onChange={(e) => setOpt('key', e.target.value)} />
      </Field>
      <Field label="指定分区" htmlFor="wb-partition" hint="留空由分区器决定">
        <Input
          id="wb-partition"
          type="number"
          min={0}
          value={options.partition}
          placeholder="自动"
          onChange={(e) => setOpt('partition', e.target.value)}
        />
      </Field>
      <Field label="acks" hint="all：等待全部 ISR 副本确认，最可靠">
        <Select value={options.acks} onValueChange={(v) => v && setOpt('acks', v)}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="0">0（不等待确认）</SelectItem>
            <SelectItem value="1">1（仅 leader 确认）</SelectItem>
            <SelectItem value="all">all（全部 ISR 确认）</SelectItem>
          </SelectContent>
        </Select>
      </Field>
      <Field label="压缩方式">
        <Select value={options.compression} onValueChange={(v) => v && setOpt('compression', v)}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {['none', 'gzip', 'snappy', 'lz4', 'zstd'].map((c) => (
              <SelectItem key={c} value={c}>
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
    </div>
  )
}

function RabbitFields({
  selection,
  options,
  setOpt,
  enabled,
}: {
  selection: ClusterSelection
  options: Record<string, string>
  setOpt: (k: string, v: string) => void
  enabled: boolean
}) {
  const [exchanges, setExchanges] = useState<RabbitExchange[]>([])
  useEffect(() => {
    if (!enabled) return
    fetchRabbitRouting(selection)
      .then((t) => setExchanges(t.exchanges))
      .catch(() => {})
  }, [selection, enabled])

  const current = exchanges.find((e) => e.name === options.exchange)
  const isHeaders = current?.type === 'headers'
  const headerText = Object.entries(options)
    .filter(([k]) => k.startsWith('headers.'))
    .map(([k, v]) => `${k.slice(8)}=${v}`)
    .join(', ')
  const manual = options.ackMode === 'manual'
  const nack = Number(options.nackRatio) || 0

  function setHeaders(text: string) {
    for (const key of Object.keys(options)) if (key.startsWith('headers.')) setOpt(key, '')
    text
      .split(',')
      .map((pair) => pair.split('='))
      .forEach(([k, v]) => {
        if (k?.trim() && v !== undefined) setOpt(`headers.${k.trim()}`, v.trim())
      })
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <Field label="交换机">
          <Select value={options.exchange || '__default'} onValueChange={(v) => v && setOpt('exchange', v === '__default' ? '' : v)}>
            <SelectTrigger className="w-full">
              <SelectValue>{options.exchange || '(默认交换机)'}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__default">(默认交换机)</SelectItem>
              {exchanges
                .filter((e) => e.name)
                .map((e) => (
                  <SelectItem key={e.name} value={e.name}>
                    {e.name}（{e.type}）
                  </SelectItem>
                ))}
            </SelectContent>
          </Select>
        </Field>
        <Field label="Routing Key" htmlFor="wb-rk" hint={current?.type === 'fanout' ? 'fanout 忽略 routing key' : undefined}>
          <Input id="wb-rk" value={options.routingKey} onChange={(e) => setOpt('routingKey', e.target.value)} />
        </Field>
      </div>
      {isHeaders && (
        <Field label="Headers" htmlFor="wb-headers" hint="格式 key=value，多个用逗号分隔，如 type=invoice, format=pdf">
          <Input id="wb-headers" defaultValue={headerText} onBlur={(e) => setHeaders(e.target.value)} />
        </Field>
      )}
      <div className="grid grid-cols-2 gap-3">
        <Field label="确认模式">
          <Select
            value={options.ackMode}
            onValueChange={(v) => {
              if (!v) return
              setOpt('ackMode', v)
              if (v === 'auto') setOpt('nackRatio', '0')
            }}
          >
            <SelectTrigger className="w-full">
              <SelectValue>{manual ? '手动 ack' : '自动 ack'}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="manual">手动 ack</SelectItem>
              <SelectItem value="auto">自动 ack</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <Field label="prefetch" htmlFor="wb-prefetch" hint="0 表示不限制">
          <Input
            id="wb-prefetch"
            type="number"
            min={0}
            max={1000}
            value={options.prefetch}
            onChange={(e) => setOpt('prefetch', e.target.value)}
          />
        </Field>
      </div>
      <Field label={`nack 比例：${Math.round(nack * 100)}%`} hint={manual ? '按比例对消息执行 basic.nack' : '自动 ack 模式下无法 nack'}>
        <Slider
          min={0}
          max={100}
          step={5}
          disabled={!manual}
          value={[Math.round(nack * 100)]}
          onValueChange={(v) => setOpt('nackRatio', String((Array.isArray(v) ? v[0] : v) / 100))}
        />
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <ToggleRow
          label="nack 后重新入队"
          checked={options.requeue === 'true'}
          disabled={!manual || nack === 0}
          onChange={(c) => setOpt('requeue', String(c))}
        />
        <ToggleRow label="Publisher Confirm" checked={options.confirm === 'true'} onChange={(c) => setOpt('confirm', String(c))} />
        <ToggleRow label="发送后立即消费" checked={options.consume === 'true'} onChange={(c) => setOpt('consume', String(c))} />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Field label="消息 TTL（毫秒）" htmlFor="wb-ttl" hint="留空不过期">
          <Input id="wb-ttl" type="number" min={0} value={options.ttlMs} onChange={(e) => setOpt('ttlMs', e.target.value)} />
        </Field>
        <Field label="消费前等待（毫秒）" htmlFor="wb-delay" hint="配合 TTL 观察过期">
          <Input
            id="wb-delay"
            type="number"
            min={0}
            max={10000}
            value={options.consumeDelayMs}
            onChange={(e) => setOpt('consumeDelayMs', e.target.value)}
          />
        </Field>
      </div>
    </div>
  )
}

function ToggleRow({
  label,
  checked,
  disabled,
  onChange,
}: {
  label: string
  checked: boolean
  disabled?: boolean
  onChange: (checked: boolean) => void
}) {
  return (
    <label className={cn('flex items-center justify-between gap-2 rounded-md border px-3 py-2 text-sm', disabled && 'opacity-50')}>
      {label}
      <Switch checked={checked} disabled={disabled} onCheckedChange={(c) => onChange(Boolean(c))} />
    </label>
  )
}

/** 各 MQ 在结果表中额外展示的 attributes 列 */
const EXTRA_COLUMNS: Record<MqMeta['slug'], { key: string; label: string }[]> = {
  rocketmq: [],
  kafka: [
    { key: 'partition', label: '分区' },
    { key: 'offset', label: 'Offset' },
    { key: 'leader', label: 'Leader' },
  ],
  rabbitmq: [
    { key: 'queue', label: '队列' },
    { key: 'outcome', label: '结果' },
    { key: 'redelivered', label: '重投' },
  ],
}

const PAGE_SIZE = 10

function DeliveryResults({
  meta,
  results,
  elapsed,
}: {
  meta: MqMeta
  results: MessageDeliveryResult[] | null
  elapsed: number
}) {
  const [page, setPage] = useState(0)
  const [onlyFailed, setOnlyFailed] = useState(false)
  const [prevResults, setPrevResults] = useState(results)
  if (prevResults !== results) {
    // 新结果到达时回到第一页
    setPrevResults(results)
    setPage(0)
  }

  if (!results) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>投递结果</CardTitle>
          <CardDescription>发送后在这里查看每条消息的去向</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex h-64 flex-col items-center justify-center gap-2 rounded-lg border border-dashed text-sm text-muted-foreground">
            <Send className="size-6" />
            填写左侧表单并发送消息
          </div>
        </CardContent>
      </Card>
    )
  }

  const ok = results.filter((r) => r.success).length
  const list = onlyFailed ? results.filter((r) => !r.success) : results
  const pages = Math.max(1, Math.ceil(list.length / PAGE_SIZE))
  const current = list.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE)
  const columns = EXTRA_COLUMNS[meta.slug]
  const outcomeCounts =
    meta.slug === 'rabbitmq'
      ? results.reduce<Record<string, number>>((acc, r) => {
          const o = r.attributes?.outcome ?? '-'
          acc[o] = (acc[o] ?? 0) + 1
          return acc
        }, {})
      : null
  const partitionCounts =
    meta.slug === 'kafka'
      ? results.reduce<Record<string, number>>((acc, r) => {
          const p = r.attributes?.partition ?? '-'
          acc[p] = (acc[p] ?? 0) + 1
          return acc
        }, {})
      : null

  return (
    <Card>
      <CardHeader>
        <CardTitle>投递结果</CardTitle>
        <CardDescription>
          共 {results.length} 条 · 成功 {ok} · 失败 {results.length - ok} · 成功率{' '}
          {results.length ? ((ok / results.length) * 100).toFixed(1) : 0}% · 耗时 {(elapsed / 1000).toFixed(2)} 秒
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {outcomeCounts && (
          <div className="flex flex-wrap gap-2">
            {Object.entries(outcomeCounts).map(([o, n]) => (
              <span key={o} className="inline-flex items-center gap-1 text-xs">
                <OutcomeBadge outcome={o} /> × {n}
              </span>
            ))}
          </div>
        )}
        {partitionCounts && (
          <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
            分区分布：
            {Object.entries(partitionCounts)
              .sort(([a], [b]) => Number(a) - Number(b))
              .map(([p, n]) => (
                <Badge key={p} variant="secondary">
                  P{p} × {n}
                </Badge>
              ))}
          </div>
        )}
        <div className="flex items-center justify-between">
          <label className="flex items-center gap-2 text-sm">
            <Switch
              checked={onlyFailed}
              onCheckedChange={(c) => {
                setOnlyFailed(Boolean(c))
                setPage(0)
              }}
            />
            只看失败
          </label>
          <div className="flex items-center gap-2 text-sm">
            <Button size="sm" variant="outline" disabled={page === 0} onClick={() => setPage(page - 1)}>
              上一页
            </Button>
            <span className="tabular-nums text-muted-foreground">
              {page + 1} / {pages}
            </span>
            <Button size="sm" variant="outline" disabled={page >= pages - 1} onClick={() => setPage(page + 1)}>
              下一页
            </Button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-16">状态</TableHead>
                <TableHead>消息 Key</TableHead>
                {columns.map((c) => (
                  <TableHead key={c.key}>{c.label}</TableHead>
                ))}
                {meta.slug === 'rocketmq' && <TableHead>消费者</TableHead>}
                <TableHead>详情</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {current.map((r, i) => (
                <TableRow key={`${r.messageKey}-${i}`}>
                  <TableCell>
                    <span className={cn('text-xs font-medium', r.success ? 'text-status-running' : 'text-status-failed')}>
                      {r.success ? '成功' : '失败'}
                    </span>
                  </TableCell>
                  <TableCell className="max-w-48 truncate font-mono text-xs" title={r.messageKey}>
                    {r.messageKey}
                  </TableCell>
                  {columns.map((c) => (
                    <TableCell key={c.key} className="font-mono text-xs">
                      {c.key === 'outcome' && r.attributes?.outcome ? (
                        <OutcomeBadge outcome={r.attributes.outcome} />
                      ) : c.key === 'redelivered' ? (
                        r.attributes?.redelivered === 'true' ? '是' : '否'
                      ) : (
                        (r.attributes?.[c.key] ?? '-')
                      )}
                    </TableCell>
                  ))}
                  {meta.slug === 'rocketmq' && <TableCell className="font-mono text-xs">{r.consumerNodeId ?? '-'}</TableCell>}
                  <TableCell className="max-w-96 text-xs whitespace-normal text-muted-foreground">{r.detail}</TableCell>
                </TableRow>
              ))}
              {current.length === 0 && (
                <TableRow>
                  <TableCell colSpan={columns.length + 4} className="py-6 text-center text-muted-foreground">
                    没有符合条件的记录
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}
