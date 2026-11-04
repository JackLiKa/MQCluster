'use client'

/**
 * RabbitMQ「交换机与队列」标签页。
 *
 * - ECharts 分层图：Producer → Exchange → Queue → Consumer，边上标注 binding key；队列到死信交换机画虚线
 * - 输入 exchange / routing key / headers 后调用 /routing/preview（纯计算，不发消息），高亮命中的路径
 * - 声明 / 删除交换机、队列、绑定；清空队列
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Eraser, LoaderCircle, Plus, Route, Send, Trash2, Unlink } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { EChart } from '@/components/echart'
import { HintButton } from '@/components/hint-button'
import { OutcomeBadge } from '@/components/dashboard/message-workbench'
import type { ConfirmRequest } from '@/components/confirm-dialog'
import { useChartPalette } from '@/hooks/use-chart-palette'
import {
  bindRabbit,
  declareRabbitExchange,
  declareRabbitQueue,
  deleteRabbitExchange,
  deleteRabbitQueue,
  fetchRabbitRouting,
  previewRabbitRoute,
  purgeRabbitQueue,
  simulateMessages,
  unbindRabbit,
} from '@/lib/api'
import type { ClusterSelection, ClusterTopology } from '@/types/cluster'
import type { RabbitBinding, RabbitExchange, RabbitExchangeType, RabbitRoutePreview, RabbitRoutingTopology } from '@/types/rabbit'

const POLL_MS = 3000

export const EXCHANGE_TYPE_LABELS: Record<string, string> = {
  direct: 'direct · 精确匹配',
  fanout: 'fanout · 广播',
  topic: 'topic · 通配符',
  headers: 'headers · 消息头',
}

/** "a=1, b=2" → { a: '1', b: '2' } */
export function parseHeaders(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const part of text.split(',')) {
    const [k, ...rest] = part.split('=')
    if (k?.trim()) out[k.trim()] = rest.join('=').trim()
  }
  return out
}

function bindingLabel(b: RabbitBinding, type: string | undefined): string {
  if (type === 'headers') {
    return Object.entries(b.headers ?? {})
      .map(([k, v]) => `${k}=${v}`)
      .join(', ')
  }
  if (type === 'fanout') return '（忽略 key）'
  return b.routingKey || '""'
}

const bindingId = (b: RabbitBinding) => `${b.source}|${b.destination}|${b.routingKey}|${JSON.stringify(b.headers ?? {})}`

export function rabbitUnavailable(topology: ClusterTopology | null): string | null {
  const ok = (topology?.nodes ?? []).some((n) => n.labels?.role === 'rabbit-broker' && n.status === 'RUNNING')
  return ok ? null : 'AMQP Broker 未运行'
}

export function useRabbitRouting(selection: ClusterSelection, enabled: boolean, intervalMs = POLL_MS) {
  const [routing, setRouting] = useState<RabbitRoutingTopology | null>(null)
  const [error, setError] = useState<string | null>(null)
  const load = useCallback(async () => {
    try {
      setRouting(await fetchRabbitRouting(selection))
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }, [selection])
  useEffect(() => {
    if (!enabled) return
    const first = setTimeout(() => void load(), 0)
    const timer = setInterval(() => void load(), intervalMs)
    return () => {
      clearTimeout(first)
      clearInterval(timer)
    }
  }, [load, enabled, intervalMs])
  return { routing, error, load }
}

export function RabbitRoutingPanel({
  selection,
  topology,
  onConfirm,
}: {
  selection: ClusterSelection
  topology: ClusterTopology | null
  onConfirm: (req: ConfirmRequest) => void
}) {
  const unavailable = rabbitUnavailable(topology)
  const { routing, error, load } = useRabbitRouting(selection, !unavailable)
  const [showBuiltIn, setShowBuiltIn] = useState(false)
  const [exchange, setExchange] = useState('lab.topic')
  const [routingKey, setRoutingKey] = useState('app.order.error')
  const [headersText, setHeadersText] = useState('type=invoice, format=pdf')
  const [preview, setPreview] = useState<RabbitRoutePreview | null>(null)
  const [sending, setSending] = useState(false)

  const exchanges = (routing?.exchanges ?? []).filter((e) => showBuiltIn || !e.builtIn)
  const currentExchange = routing?.exchanges.find((e) => e.name === exchange)

  // 输入变化 300ms 后请求路由预览
  useEffect(() => {
    if (unavailable || !routing?.exchanges.some((e) => e.name === exchange)) return
    const type = routing.exchanges.find((e) => e.name === exchange)?.type
    const timer = setTimeout(() => {
      previewRabbitRoute(selection, {
        exchange,
        routingKey,
        headers: type === 'headers' ? parseHeaders(headersText) : undefined,
      })
        .then(setPreview)
        .catch(() => setPreview(null))
    }, 300)
    return () => clearTimeout(timer)
  }, [selection, exchange, routingKey, headersText, unavailable, routing])

  async function sendOne() {
    setSending(true)
    try {
      const headers = currentExchange?.type === 'headers' ? parseHeaders(headersText) : {}
      const options: Record<string, string> = { exchange, routingKey, ackMode: 'auto', confirm: 'true' }
      for (const [k, v] of Object.entries(headers)) options[`headers.${k}`] = v
      const res = await simulateMessages(selection, {
        topic: routingKey || exchange || 'amq.default',
        consumerGroup: 'lab-routing',
        messageCount: 1,
        producerNodeId: 'rabbit-producer',
        payloadTemplate: '{"routingKey":"' + routingKey + '","at":"{timestamp}"}',
        options,
      })
      const queues = res.deliveries.map((d) => d.attributes?.queue).filter((q): q is string => !!q && q !== '-')
      const notify = res.deliveries.some((d) => d.success) ? toast.success : toast.warning
      notify('已按此路由发送 1 条消息', {
        description:
          queues.length > 1
            ? `复制投递到 ${queues.length} 个队列：${queues.join('、')}`
            : res.deliveries[0]?.detail,
      })
      await load()
    } catch (err) {
      toast.error('发送失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setSending(false)
    }
  }

  if (unavailable) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          {unavailable}。请先在「概览」中启动 rabbit-node-01，演示用的交换机与队列会自动声明。
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,360px)_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Route className="size-4" /> 路由试算
            </CardTitle>
            <CardDescription>只计算不发送：右图实时高亮消息会经过的绑定与队列</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>交换机</Label>
              <Select value={exchange} onValueChange={(v) => v !== null && setExchange(v)}>
                <SelectTrigger className="w-full">
                  <SelectValue>
                    {exchange === '' ? '(默认交换机)' : exchange}
                    {currentExchange && <span className="ml-2 text-xs text-muted-foreground">{currentExchange.type}</span>}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {(routing?.exchanges ?? []).map((e) => (
                    <SelectItem key={e.name || '(default)'} value={e.name}>
                      {e.name || '(默认交换机)'} · {e.type}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {currentExchange?.type === 'headers' ? (
              <div className="space-y-2">
                <Label htmlFor="rp-headers">消息头</Label>
                <Input
                  id="rp-headers"
                  value={headersText}
                  onChange={(e) => setHeadersText(e.target.value)}
                  placeholder="type=invoice, format=pdf"
                  className="font-mono text-xs"
                />
              </div>
            ) : (
              <div className="space-y-2">
                <Label htmlFor="rp-key">Routing key</Label>
                <Input
                  id="rp-key"
                  value={routingKey}
                  onChange={(e) => setRoutingKey(e.target.value)}
                  placeholder="如 order.created / app.order.error"
                  className="font-mono text-xs"
                  disabled={currentExchange?.type === 'fanout'}
                />
                {currentExchange?.type === 'topic' && (
                  <p className="text-xs text-muted-foreground">topic 通配：* 匹配一个单词，# 匹配零或多个单词（以 . 分隔）</p>
                )}
                {currentExchange?.type === 'fanout' && (
                  <p className="text-xs text-muted-foreground">fanout 忽略 routing key，广播到所有绑定队列</p>
                )}
              </div>
            )}
            <div className="space-y-2 rounded-lg border bg-muted/30 p-3">
              <div className="text-xs font-medium text-muted-foreground">试算结果</div>
              {preview ? (
                <>
                  <div className="flex flex-wrap gap-1.5">
                    {preview.matchedQueues.length === 0 ? (
                      <OutcomeBadge outcome="UNROUTABLE" />
                    ) : (
                      preview.matchedQueues.map((q) => (
                        <Badge key={q} className="bg-mq-accent font-mono text-mq-accent-foreground">
                          {q}
                        </Badge>
                      ))
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground">{preview.explanation}</p>
                </>
              ) : (
                <p className="text-xs text-muted-foreground">选择交换机后显示</p>
              )}
            </div>
            <HintButton
              className="w-full"
              variant="outline"
              onClick={() => void sendOne()}
              disabledReason={sending ? '发送中…' : !currentExchange ? '请选择交换机' : null}
            >
              {sending ? <LoaderCircle className="size-4 animate-spin" /> : <Send className="size-4" />}
              按此路由真实发送 1 条
            </HintButton>
          </CardContent>
        </Card>

        <RoutingGraph
          routing={routing}
          exchanges={exchanges}
          preview={preview}
          onPickExchange={(name) => setExchange(name)}
          action={
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Switch size="sm" checked={showBuiltIn} onCheckedChange={(v) => setShowBuiltIn(v)} />
              显示内置交换机
            </label>
          }
        />
      </div>
      {error && <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{error}</div>}
      <ManageCard selection={selection} routing={routing} onChanged={load} onConfirm={onConfirm} />
    </div>
  )
}

function RoutingGraph({
  routing,
  exchanges,
  preview,
  onPickExchange,
  action,
}: {
  routing: RabbitRoutingTopology | null
  exchanges: RabbitExchange[]
  preview: RabbitRoutePreview | null
  onPickExchange: (name: string) => void
  action: React.ReactNode
}) {
  const ref = useRef<HTMLDivElement>(null)
  const palette = useChartPalette(ref)
  const queues = useMemo(() => routing?.queues ?? [], [routing])
  const consumers = useMemo(() => routing?.consumers ?? [], [routing])
  const rows = Math.max(exchanges.length, queues.length, consumers.length, 3)
  const height = Math.max(360, rows * 52 + 60)

  const option = useMemo(() => {
    const typeOf = new Map((routing?.exchanges ?? []).map((e) => [e.name, e.type]))
    const shown = new Set(exchanges.map((e) => e.name))
    const hlQueues = new Set(preview?.matchedQueues ?? [])
    const hlBindings = new Set((preview?.matchedBindings ?? []).map(bindingId))
    const active = preview != null
    const dim = (on: boolean) => (active && !on ? 0.18 : 1)
    const colX = [40, 300, 600, 880]
    const yOf = (i: number, n: number) => 30 + ((i + 0.5) * (height - 60)) / Math.max(n, 1)

    const nodes: Record<string, unknown>[] = []
    nodes.push({
      id: 'producer',
      name: 'Producer',
      x: colX[0],
      y: height / 2,
      symbol: 'roundRect',
      symbolSize: [86, 34],
      itemStyle: { color: palette.accent },
      label: { show: true, color: '#fff', fontWeight: 600 },
    })
    exchanges.forEach((e, i) => {
      const on = preview?.exchange === e.name
      nodes.push({
        id: `x:${e.name}`,
        name: e.name || '(默认)',
        x: colX[1],
        y: yOf(i, exchanges.length),
        symbol: 'diamond',
        symbolSize: 26,
        itemStyle: { color: on ? palette.accent : palette.series[1], opacity: dim(on), borderColor: palette.card, borderWidth: 2 },
        label: { show: true, position: 'left', color: palette.foreground, opacity: dim(on), formatter: `{b|${e.name || '(默认)'}}\n{t|${e.type}}`, rich: { b: { fontWeight: 600, fontSize: 12 }, t: { color: palette.muted, fontSize: 10 } } },
        tooltip: { formatter: `交换机 ${e.name || '(默认)'}<br/>类型：${EXCHANGE_TYPE_LABELS[e.type] ?? e.type}<br/>点击设为试算交换机` },
        exchangeName: e.name,
      })
    })
    queues.forEach((q, i) => {
      const on = hlQueues.has(q.name)
      const isDead = q.name.includes('dead')
      nodes.push({
        id: `q:${q.name}`,
        name: q.name,
        x: colX[2],
        y: yOf(i, queues.length),
        symbol: 'rect',
        symbolSize: [18, 18],
        itemStyle: { color: on ? palette.accent : isDead ? palette.status.FAILED : palette.series[0], opacity: dim(on) },
        label: {
          show: true,
          position: 'right',
          opacity: dim(on),
          formatter: `{b|${q.name}}\n{n|${q.ready} ready · ${q.unacked} unacked}`,
          rich: {
            b: { color: palette.foreground, fontWeight: 600, fontSize: 12, lineHeight: 16 },
            n: { color: palette.muted, fontSize: 10, lineHeight: 14 },
          },
        },
        tooltip: {
          formatter: [
            `队列 ${q.name}`,
            `Ready：${q.ready}　Unacked：${q.unacked}　消费者：${q.consumers}`,
            q.deadLetterExchange ? `死信交换机：${q.deadLetterExchange}` : '',
            q.messageTtlMs ? `TTL：${q.messageTtlMs} ms` : '',
          ]
            .filter(Boolean)
            .join('<br/>'),
        },
      })
    })
    consumers.forEach((c, i) => {
      const on = hlQueues.has(c.queue)
      nodes.push({
        id: `c:${c.consumerId}`,
        name: c.consumerId,
        x: colX[3],
        y: yOf(i, consumers.length),
        symbol: 'circle',
        symbolSize: 18,
        itemStyle: { color: c.active ? palette.status.RUNNING : palette.status.STOPPED, opacity: dim(on) },
        label: { show: true, position: 'right', color: palette.foreground, fontSize: 11, opacity: dim(on) },
        tooltip: { formatter: `消费者 ${c.consumerId}<br/>队列：${c.queue}<br/>prefetch ${c.prefetch} · ${c.ackMode} ack<br/>已确认 ${c.acked} · 拒绝 ${c.nacked} · 处理中 ${c.inFlight}` },
      })
    })

    // layout:'none' 会把节点包围盒拉伸铺满画布：放两个不可见锚点固定四列与上下边界，
    // 否则没有消费者时队列列会被推到最右侧，标签被裁掉
    const anchor = (id: string, x: number, y: number) => ({
      id,
      name: '',
      x,
      y,
      symbolSize: 0,
      itemStyle: { opacity: 0 },
      label: { show: false },
      tooltip: { show: false },
    })
    nodes.push(anchor('__anchor-tl', colX[0], 0), anchor('__anchor-br', colX[3], height))

    const links: Record<string, unknown>[] = []
    exchanges.forEach((e) => {
      const on = preview?.exchange === e.name
      links.push({
        source: 'producer',
        target: `x:${e.name}`,
        lineStyle: { color: on ? palette.accent : palette.border, width: on ? 3 : 1, opacity: active ? (on ? 1 : 0.15) : 0.5, curveness: 0.1 },
      })
    })
    for (const b of routing?.bindings ?? []) {
      if (!shown.has(b.source)) continue
      const on = hlBindings.has(bindingId(b))
      links.push({
        source: `x:${b.source}`,
        target: `q:${b.destination}`,
        label: {
          show: true,
          formatter: bindingLabel(b, typeOf.get(b.source)),
          color: on ? palette.accent : palette.muted,
          fontSize: 10,
          fontWeight: on ? 700 : 400,
          backgroundColor: palette.card,
          padding: [1, 3],
          opacity: dim(on),
        },
        lineStyle: { color: on ? palette.accent : palette.series[1], width: on ? 3 : 1.2, opacity: active ? (on ? 1 : 0.15) : 0.7, curveness: 0.05 },
      })
    }
    for (const q of queues) {
      if (q.deadLetterExchange && shown.has(q.deadLetterExchange)) {
        links.push({
          source: `q:${q.name}`,
          target: `x:${q.deadLetterExchange}`,
          label: { show: true, formatter: q.messageTtlMs ? `DLX · TTL ${q.messageTtlMs}ms` : 'DLX', color: palette.status.FAILED, fontSize: 10 },
          lineStyle: { color: palette.status.FAILED, type: 'dashed', width: 1, opacity: active ? 0.25 : 0.7, curveness: -0.35 },
        })
      }
    }
    consumers.forEach((c) => {
      const on = hlQueues.has(c.queue)
      links.push({
        source: `q:${c.queue}`,
        target: `c:${c.consumerId}`,
        lineStyle: { color: on ? palette.accent : palette.border, width: on ? 2 : 1, opacity: active ? (on ? 1 : 0.15) : 0.6 },
      })
    })

    return {
      tooltip: { backgroundColor: palette.card, borderColor: palette.border, textStyle: { color: palette.foreground } },
      animationDurationUpdate: 400,
      series: [
        {
          type: 'graph',
          layout: 'none',
          roam: false,
          left: 110,
          right: 200,
          top: 20,
          bottom: 20,
          edgeSymbol: ['none', 'arrow'],
          edgeSymbolSize: 7,
          data: nodes,
          links,
          emphasis: { disabled: true },
        },
      ],
    }
  }, [routing, exchanges, queues, consumers, preview, palette, height])

  return (
    <Card ref={ref}>
      <CardHeader>
        <CardTitle>路由拓扑</CardTitle>
        <CardDescription>Producer → Exchange → Queue → Consumer，边上是 binding key；红色虚线为死信路径</CardDescription>
        <CardAction>{action}</CardAction>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <div style={{ height, minWidth: 900 }}>
            <EChart
              option={option}
              className="h-full"
              ariaLabel="RabbitMQ 路由拓扑图"
              onEvents={{
                click: (p) => {
                  const data = (p as { data?: { exchangeName?: string } }).data
                  if (data?.exchangeName !== undefined) onPickExchange(data.exchangeName)
                },
              }}
            />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function ManageCard({
  selection,
  routing,
  onChanged,
  onConfirm,
}: {
  selection: ClusterSelection
  routing: RabbitRoutingTopology | null
  onChanged: () => Promise<void>
  onConfirm: (req: ConfirmRequest) => void
}) {
  const exchanges = routing?.exchanges ?? []
  const userExchanges = exchanges.filter((e) => !e.builtIn)
  const queues = routing?.queues ?? []
  const bindings = routing?.bindings ?? []
  const typeOf = new Map(exchanges.map((e) => [e.name, e.type]))

  async function run(action: () => Promise<unknown>, success: string) {
    try {
      await action()
      toast.success(success)
    } catch (err) {
      toast.error('操作失败', { description: err instanceof Error ? err.message : String(err) })
      throw err
    } finally {
      await onChanged()
    }
  }

  const confirmDelete = (title: string, description: string, action: () => Promise<unknown>, success: string) =>
    onConfirm({
      title,
      description,
      confirmLabel: '确认',
      destructive: true,
      onConfirm: () => run(action, success).catch(() => {}),
    })

  return (
    <Card>
      <CardHeader>
        <CardTitle>管理交换机、队列与绑定</CardTitle>
        <CardDescription>声明后立即生效，路由拓扑与试算会同步更新</CardDescription>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="queues">
          <TabsList>
            <TabsTrigger value="queues">队列（{queues.length}）</TabsTrigger>
            <TabsTrigger value="exchanges">交换机（{userExchanges.length}）</TabsTrigger>
            <TabsTrigger value="bindings">绑定（{bindings.length}）</TabsTrigger>
          </TabsList>

          <TabsContent value="queues" className="mt-3 space-y-4">
            <DeclareQueueForm exchanges={userExchanges} onSubmit={(req) => run(() => declareRabbitQueue(selection, req), `队列 ${req.name} 已声明`)} />
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>队列</TableHead>
                  <TableHead className="text-right">Ready</TableHead>
                  <TableHead className="text-right">Unacked</TableHead>
                  <TableHead className="text-right">消费者</TableHead>
                  <TableHead>参数</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {queues.map((q) => (
                  <TableRow key={q.name}>
                    <TableCell className="font-mono text-xs">{q.name}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.ready}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.unacked}</TableCell>
                    <TableCell className="text-right tabular-nums">{q.consumers}</TableCell>
                    <TableCell className="space-x-1 text-xs">
                      {q.deadLetterExchange && <Badge variant="outline">DLX={q.deadLetterExchange}</Badge>}
                      {q.messageTtlMs != null && <Badge variant="outline">TTL={q.messageTtlMs}ms</Badge>}
                    </TableCell>
                    <TableCell>
                      <div className="flex justify-end gap-1">
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          title="清空队列"
                          aria-label={`清空 ${q.name}`}
                          disabled={q.ready === 0}
                          onClick={() =>
                            confirmDelete(`清空队列 ${q.name}？`, `将丢弃 ${q.ready} 条就绪消息（未确认的消息不受影响）。`, () => purgeRabbitQueue(selection, q.name), `队列 ${q.name} 已清空`)
                          }
                        >
                          <Eraser className="size-4" />
                        </Button>
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          title="删除队列"
                          aria-label={`删除 ${q.name}`}
                          onClick={() =>
                            confirmDelete(`删除队列 ${q.name}？`, '队列及其绑定、消息会一并删除。', () => deleteRabbitQueue(selection, q.name), `队列 ${q.name} 已删除`)
                          }
                        >
                          <Trash2 className="size-4 text-destructive" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TabsContent>

          <TabsContent value="exchanges" className="mt-3 space-y-4">
            <DeclareExchangeForm onSubmit={(req) => run(() => declareRabbitExchange(selection, req), `交换机 ${req.name} 已声明`)} />
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>交换机</TableHead>
                  <TableHead>类型</TableHead>
                  <TableHead className="text-right">绑定数</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {userExchanges.map((e) => (
                  <TableRow key={e.name}>
                    <TableCell className="font-mono text-xs">{e.name}</TableCell>
                    <TableCell className="text-xs">{EXCHANGE_TYPE_LABELS[e.type] ?? e.type}</TableCell>
                    <TableCell className="text-right tabular-nums">{bindings.filter((b) => b.source === e.name).length}</TableCell>
                    <TableCell className="text-right">
                      <Button
                        size="icon-sm"
                        variant="ghost"
                        aria-label={`删除 ${e.name}`}
                        onClick={() =>
                          confirmDelete(`删除交换机 ${e.name}？`, '其上的绑定会一并删除，已投递到队列的消息不受影响。', () => deleteRabbitExchange(selection, e.name), `交换机 ${e.name} 已删除`)
                        }
                      >
                        <Trash2 className="size-4 text-destructive" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TabsContent>

          <TabsContent value="bindings" className="mt-3 space-y-4">
            <BindForm
              exchanges={userExchanges}
              queues={queues.map((q) => q.name)}
              onSubmit={(req) => run(() => bindRabbit(selection, req), `已绑定 ${req.exchange} → ${req.queue}`)}
            />
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>交换机</TableHead>
                  <TableHead>队列</TableHead>
                  <TableHead>Binding key / 参数</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {bindings.map((b) => (
                  <TableRow key={bindingId(b)}>
                    <TableCell className="font-mono text-xs">{b.source}</TableCell>
                    <TableCell className="font-mono text-xs">{b.destination}</TableCell>
                    <TableCell className="font-mono text-xs">{bindingLabel(b, typeOf.get(b.source))}</TableCell>
                    <TableCell className="text-right">
                      <Button
                        size="icon-sm"
                        variant="ghost"
                        aria-label="解除绑定"
                        onClick={() =>
                          confirmDelete(
                            '解除绑定？',
                            `${b.source} → ${b.destination}（${bindingLabel(b, typeOf.get(b.source))}）`,
                            () =>
                              unbindRabbit(selection, {
                                exchange: b.source,
                                queue: b.destination,
                                routingKey: b.routingKey,
                                headers: b.headers,
                              }),
                            '绑定已解除',
                          )
                        }
                      >
                        <Unlink className="size-4 text-destructive" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  )
}

const NAME_RE = /^[a-zA-Z0-9._-]{1,120}$/

function SubmitButton({ busy, reason, onClick, children }: { busy: boolean; reason: string | null; onClick: () => void; children: React.ReactNode }) {
  return (
    <HintButton onClick={onClick} disabledReason={busy ? '提交中…' : reason}>
      {busy ? <LoaderCircle className="size-4 animate-spin" /> : <Plus className="size-4" />}
      {children}
    </HintButton>
  )
}

function useSubmit(fn: () => Promise<void>, reset: () => void) {
  const [busy, setBusy] = useState(false)
  return {
    busy,
    submit: async () => {
      setBusy(true)
      try {
        await fn()
        reset()
      } catch {
        // toast 已提示
      } finally {
        setBusy(false)
      }
    },
  }
}

function DeclareExchangeForm({ onSubmit }: { onSubmit: (req: { name: string; type: RabbitExchangeType }) => Promise<void> }) {
  const [name, setName] = useState('')
  const [type, setType] = useState<RabbitExchangeType>('direct')
  const { busy, submit } = useSubmit(() => onSubmit({ name: name.trim(), type }), () => setName(''))
  return (
    <div className="flex flex-wrap items-end gap-3">
      <div className="space-y-2">
        <Label htmlFor="rx-name">名称</Label>
        <Input id="rx-name" className="w-56" value={name} onChange={(e) => setName(e.target.value)} placeholder="lab.my-exchange" />
      </div>
      <div className="space-y-2">
        <Label>类型</Label>
        <Select value={type} onValueChange={(v) => v && setType(v as RabbitExchangeType)}>
          <SelectTrigger className="w-44">
            <SelectValue>{EXCHANGE_TYPE_LABELS[type]}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            {Object.entries(EXCHANGE_TYPE_LABELS).map(([k, v]) => (
              <SelectItem key={k} value={k}>
                {v}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <SubmitButton busy={busy} reason={NAME_RE.test(name) ? (name.startsWith('amq.') ? 'amq. 前缀为保留名称' : null) : '请输入合法名称（字母、数字、. _ -）'} onClick={() => void submit()}>
        声明交换机
      </SubmitButton>
    </div>
  )
}

function DeclareQueueForm({
  exchanges,
  onSubmit,
}: {
  exchanges: RabbitExchange[]
  onSubmit: (req: { name: string; deadLetterExchange?: string | null; messageTtlMs?: number | null }) => Promise<void>
}) {
  const [name, setName] = useState('')
  const [dlx, setDlx] = useState('__none__')
  const [ttl, setTtl] = useState('')
  const { busy, submit } = useSubmit(
    () =>
      onSubmit({
        name: name.trim(),
        deadLetterExchange: dlx === '__none__' ? null : dlx,
        messageTtlMs: ttl ? Number(ttl) : null,
      }),
    () => {
      setName('')
      setTtl('')
    },
  )
  const ttlInvalid = ttl !== '' && (!Number.isInteger(Number(ttl)) || Number(ttl) < 1 || Number(ttl) > 3_600_000)
  return (
    <div className="flex flex-wrap items-end gap-3">
      <div className="space-y-2">
        <Label htmlFor="rq-name">名称</Label>
        <Input id="rq-name" className="w-56" value={name} onChange={(e) => setName(e.target.value)} placeholder="lab.my-queue" />
      </div>
      <div className="space-y-2">
        <Label>死信交换机（x-dead-letter-exchange）</Label>
        <Select value={dlx} onValueChange={(v) => v && setDlx(v)}>
          <SelectTrigger className="w-52">
            <SelectValue>{dlx === '__none__' ? '不设置' : dlx}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="__none__">不设置</SelectItem>
            {exchanges.map((e) => (
              <SelectItem key={e.name} value={e.name}>
                {e.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-2">
        <Label htmlFor="rq-ttl">消息 TTL（ms，x-message-ttl）</Label>
        <Input id="rq-ttl" className="w-40" type="number" value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="不设置" aria-invalid={ttlInvalid} />
      </div>
      <SubmitButton busy={busy} reason={!NAME_RE.test(name) ? '请输入合法名称（字母、数字、. _ -）' : ttlInvalid ? 'TTL 范围 1–3600000 ms' : null} onClick={() => void submit()}>
        声明队列
      </SubmitButton>
    </div>
  )
}

function BindForm({
  exchanges,
  queues,
  onSubmit,
}: {
  exchanges: RabbitExchange[]
  queues: string[]
  onSubmit: (req: { exchange: string; queue: string; routingKey?: string; headers?: Record<string, string> }) => Promise<void>
}) {
  const [exchange, setExchange] = useState('')
  const [queue, setQueue] = useState('')
  const [key, setKey] = useState('')
  const [headers, setHeaders] = useState('x-match=all, type=invoice')
  const type = exchanges.find((e) => e.name === exchange)?.type
  const { busy, submit } = useSubmit(
    () =>
      onSubmit({
        exchange,
        queue,
        routingKey: type === 'headers' ? '' : key,
        headers: type === 'headers' ? parseHeaders(headers) : undefined,
      }),
    () => setKey(''),
  )
  return (
    <div className="flex flex-wrap items-end gap-3">
      <div className="space-y-2">
        <Label>交换机</Label>
        <Select value={exchange} onValueChange={(v) => v && setExchange(v)}>
          <SelectTrigger className="w-48">
            <SelectValue>{exchange || '选择交换机'}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            {exchanges.map((e) => (
              <SelectItem key={e.name} value={e.name}>
                {e.name} · {e.type}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-2">
        <Label>队列</Label>
        <Select value={queue} onValueChange={(v) => v && setQueue(v)}>
          <SelectTrigger className="w-48">
            <SelectValue>{queue || '选择队列'}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            {queues.map((q) => (
              <SelectItem key={q} value={q}>
                {q}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      {type === 'headers' ? (
        <div className="space-y-2">
          <Label htmlFor="rb-headers">匹配参数（含 x-match=all/any）</Label>
          <Input id="rb-headers" className="w-72 font-mono text-xs" value={headers} onChange={(e) => setHeaders(e.target.value)} />
        </div>
      ) : (
        <div className="space-y-2">
          <Label htmlFor="rb-key">Binding key</Label>
          <Input
            id="rb-key"
            className="w-56 font-mono text-xs"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder={type === 'topic' ? '如 *.order.# ' : type === 'fanout' ? 'fanout 无需填写' : '如 order.created'}
            disabled={type === 'fanout'}
          />
        </div>
      )}
      <SubmitButton busy={busy} reason={!exchange ? '请选择交换机' : !queue ? '请选择队列' : null} onClick={() => void submit()}>
        创建绑定
      </SubmitButton>
    </div>
  )
}
