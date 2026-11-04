'use client'

/**
 * 监控面板——KPI、吞吐趋势、各节点资源，RabbitMQ 额外展示队列深度与速率。
 *
 * 指标口径（后端 MonitoringSnapshot）：
 * - cpuUsage / memoryUsage：百分比；嵌入式 Kafka / RabbitMQ 所有节点同处一个 JVM，只能取进程值
 * - networkIn / networkOut：消息速率（msg/s）。Kafka 用“leader 分区 LEO 增量 ÷ 间隔”估算
 */

import { useEffect, useMemo, useRef, useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { EChart } from '@/components/echart'
import { StatusBadge } from '@/components/status-dot'
import { useChartPalette, type ChartPalette } from '@/hooks/use-chart-palette'
import { fetchRabbitRouting } from '@/lib/api'
import type { MqMeta } from '@/lib/mq-registry'
import type { ClusterSelection, ClusterTopology, MonitoringSnapshot } from '@/types/cluster'
import type { RabbitQueue } from '@/types/rabbit'

const MAX_POINTS = 30

interface Sample {
  time: string
  inRate: number
  outRate: number
  cpu: number
}

export function formatRate(v: number): string {
  if (v < 1) return v.toFixed(2)
  if (v < 100) return v.toFixed(1)
  if (v < 10000) return v.toFixed(0)
  return `${(v / 1000).toFixed(1)}K`
}

function axisStyle(p: ChartPalette) {
  return {
    axisLine: { lineStyle: { color: p.border } },
    axisLabel: { color: p.muted, fontSize: 11 },
    splitLine: { lineStyle: { color: p.border, type: 'dashed' as const } },
  }
}

export function MonitoringPanel({
  meta,
  selection,
  topology,
  metrics,
}: {
  meta: MqMeta
  selection: ClusterSelection
  topology: ClusterTopology | null
  metrics: MonitoringSnapshot | null
}) {
  const ref = useRef<HTMLDivElement>(null)
  const palette = useChartPalette(ref)
  const [history, setHistory] = useState<Sample[]>([])
  const [lastSnapshot, setLastSnapshot] = useState<MonitoringSnapshot | null>(null)

  const nodes = useMemo(() => topology?.nodes ?? [], [topology])
  const runningIds = new Set(nodes.filter((n) => n.status === 'RUNNING').map((n) => n.nodeId))
  const running = (metrics?.nodes ?? []).filter((m) => runningIds.has(m.nodeId))
  const avgCpu = running.length ? running.reduce((s, n) => s + n.cpuUsage, 0) / running.length : 0
  const avgMem = running.length ? running.reduce((s, n) => s + n.memoryUsage, 0) / running.length : 0
  const totalIn = running.reduce((s, n) => s + n.networkInBytesPerSecond, 0)
  const totalOut = running.reduce((s, n) => s + n.networkOutBytesPerSecond, 0)

  // 新快照到达时追加到趋势序列（渲染期间根据 props 派生状态，避免 effect 内 setState）
  if (metrics && metrics !== lastSnapshot) {
    setLastSnapshot(metrics)
    const time = new Date(metrics.capturedAt).toLocaleTimeString('zh-CN', { hour12: false })
    setHistory((prev) => [...prev, { time, inRate: totalIn, outRate: totalOut, cpu: avgCpu }].slice(-MAX_POINTS))
  }

  const trendOption = useMemo(
    () => ({
      color: [palette.series[0], palette.series[1], palette.series[2]],
      tooltip: { trigger: 'axis', backgroundColor: palette.card, borderColor: palette.border, textStyle: { color: palette.foreground } },
      legend: { data: ['写入速率', '投递速率', '平均 CPU'], textStyle: { color: palette.muted }, top: 0 },
      grid: { left: 48, right: 48, top: 36, bottom: 28 },
      xAxis: { type: 'category', data: history.map((h) => h.time), boundaryGap: false, ...axisStyle(palette), splitLine: { show: false } },
      yAxis: [
        { type: 'value', name: 'msg/s', nameTextStyle: { color: palette.muted }, ...axisStyle(palette) },
        { type: 'value', name: 'CPU %', max: 100, nameTextStyle: { color: palette.muted }, ...axisStyle(palette), splitLine: { show: false } },
      ],
      series: [
        { name: '写入速率', type: 'line', smooth: true, showSymbol: false, areaStyle: { opacity: 0.12 }, data: history.map((h) => +h.inRate.toFixed(2)) },
        { name: '投递速率', type: 'line', smooth: true, showSymbol: false, areaStyle: { opacity: 0.12 }, data: history.map((h) => +h.outRate.toFixed(2)) },
        { name: '平均 CPU', type: 'line', smooth: true, showSymbol: false, yAxisIndex: 1, lineStyle: { type: 'dashed' }, data: history.map((h) => +h.cpu.toFixed(1)) },
      ],
    }),
    [history, palette],
  )

  const nodeOption = useMemo(() => {
    const list = metrics?.nodes ?? []
    const names = list.map((m) => nodes.find((n) => n.nodeId === m.nodeId)?.displayName ?? m.nodeId)
    return {
      color: [palette.series[0], palette.series[1], palette.series[2], palette.series[3]],
      tooltip: { trigger: 'axis', backgroundColor: palette.card, borderColor: palette.border, textStyle: { color: palette.foreground } },
      legend: { data: ['写入 msg/s', '投递 msg/s'], textStyle: { color: palette.muted }, top: 0 },
      grid: { left: 48, right: 16, top: 36, bottom: 40 },
      xAxis: { type: 'category', data: names, ...axisStyle(palette), axisLabel: { color: palette.muted, fontSize: 11, interval: 0, rotate: names.length > 4 ? 20 : 0 } },
      yAxis: { type: 'value', ...axisStyle(palette) },
      series: [
        { name: '写入 msg/s', type: 'bar', barMaxWidth: 28, data: list.map((m) => +m.networkInBytesPerSecond.toFixed(2)) },
        { name: '投递 msg/s', type: 'bar', barMaxWidth: 28, data: list.map((m) => +m.networkOutBytesPerSecond.toFixed(2)) },
      ],
    }
  }, [metrics, nodes, palette])

  return (
    <div ref={ref} className="space-y-4">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Kpi label="平均 CPU" value={`${avgCpu.toFixed(1)}%`} />
        <Kpi label="平均内存" value={`${avgMem.toFixed(1)}%`} />
        <Kpi label="写入速率" value={`${formatRate(totalIn)} msg/s`} />
        <Kpi label="投递速率" value={`${formatRate(totalOut)} msg/s`} />
      </div>
      {meta.slug !== 'rocketmq' && (
        <p className="text-xs text-muted-foreground">
          {meta.slug === 'kafka'
            ? '说明：嵌入式 Kafka 的所有节点运行在同一个 JVM 中，CPU / 内存为进程值；各 broker 的写入速率按“该 broker 作为 leader 的分区 LEO 增量 ÷ 采样间隔”计算，投递速率按消费者组 committed offset 增量计算。'
            : '说明：嵌入式 Qpid 与平台同进程，CPU / 内存为进程值；队列级的入队 / 出队速率见下方队列监控。'}
        </p>
      )}
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>吞吐趋势</CardTitle>
            <CardDescription>最近 {MAX_POINTS} 个采样点（约每 5 秒一次）</CardDescription>
          </CardHeader>
          <CardContent>
            {history.length < 2 ? (
              <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">正在采集数据…</div>
            ) : (
              <EChart option={trendOption} className="h-72" ariaLabel="吞吐趋势折线图" />
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>各节点速率</CardTitle>
            <CardDescription>当前快照</CardDescription>
          </CardHeader>
          <CardContent>
            <EChart option={nodeOption} className="h-72" ariaLabel="各节点速率柱状图" />
          </CardContent>
        </Card>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>节点资源</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {nodes.map((node) => {
            const m = metrics?.nodes.find((x) => x.nodeId === node.nodeId)
            return (
              <div key={node.nodeId} className="space-y-2 rounded-lg border p-3">
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate text-sm font-medium">{node.displayName}</span>
                  <StatusBadge status={node.status} />
                </div>
                <UsageBar label="CPU" value={node.status === 'RUNNING' ? (m?.cpuUsage ?? 0) : 0} />
                <UsageBar label="内存" value={node.status === 'RUNNING' ? (m?.memoryUsage ?? 0) : 0} />
                <div className="flex justify-between text-xs text-muted-foreground tabular-nums">
                  <span>写入 {formatRate(m?.networkInBytesPerSecond ?? 0)}/s</span>
                  <span>投递 {formatRate(m?.networkOutBytesPerSecond ?? 0)}/s</span>
                </div>
              </div>
            )
          })}
        </CardContent>
      </Card>
      {meta.slug === 'rabbitmq' && <RabbitQueueMonitor selection={selection} palette={palette} />}
    </div>
  )
}

function Kpi({ label, value }: { label: string; value: string }) {
  return (
    <Card size="sm">
      <CardContent>
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="mt-1 text-2xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  )
}

function UsageBar({ label, value }: { label: string; value: number }) {
  const v = Math.max(0, Math.min(100, value))
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-mono">{v.toFixed(1)}%</span>
      </div>
      <Progress value={v} />
    </div>
  )
}

/** RabbitMQ 队列监控：ready / unacked 堆叠柱 + 速率表，每 3 秒刷新 */
function RabbitQueueMonitor({ selection, palette }: { selection: ClusterSelection; palette: ChartPalette }) {
  const [queues, setQueues] = useState<RabbitQueue[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const t = await fetchRabbitRouting(selection)
        if (!cancelled) {
          setQueues(t.queues)
          setError(null)
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err))
      }
    }
    void load()
    const timer = setInterval(() => void load(), 3000)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [selection])

  const option = useMemo(
    () => ({
      color: [palette.series[0], palette.series[2]],
      tooltip: { trigger: 'axis', backgroundColor: palette.card, borderColor: palette.border, textStyle: { color: palette.foreground } },
      legend: { data: ['Ready', 'Unacked'], textStyle: { color: palette.muted }, top: 0 },
      grid: { left: 40, right: 16, top: 36, bottom: 60 },
      xAxis: { type: 'category', data: queues.map((q) => q.name), ...axisStyle(palette), axisLabel: { color: palette.muted, fontSize: 11, interval: 0, rotate: 30 } },
      yAxis: { type: 'value', minInterval: 1, ...axisStyle(palette) },
      series: [
        { name: 'Ready', type: 'bar', stack: 'depth', barMaxWidth: 28, data: queues.map((q) => q.ready) },
        { name: 'Unacked', type: 'bar', stack: 'depth', barMaxWidth: 28, data: queues.map((q) => q.unacked) },
      ],
    }),
    [queues, palette],
  )

  return (
    <Card>
      <CardHeader>
        <CardTitle>队列监控</CardTitle>
        <CardDescription>Ready：等待投递；Unacked：已投递未确认。每 3 秒刷新</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {error ? (
          <p className="text-sm text-muted-foreground">{error}</p>
        ) : (
          <>
            <EChart option={option} className="h-64" ariaLabel="队列深度柱状图" />
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>队列</TableHead>
                    <TableHead className="text-right">Ready</TableHead>
                    <TableHead className="text-right">Unacked</TableHead>
                    <TableHead className="text-right">消费者</TableHead>
                    <TableHead className="text-right">入队 msg/s</TableHead>
                    <TableHead className="text-right">出队 msg/s</TableHead>
                    <TableHead className="text-right">累计入队 / 出队</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {queues.map((q) => (
                    <TableRow key={q.name}>
                      <TableCell className="font-mono text-xs">{q.name}</TableCell>
                      <TableCell className="text-right tabular-nums">{q.ready}</TableCell>
                      <TableCell className="text-right tabular-nums">{q.unacked}</TableCell>
                      <TableCell className="text-right tabular-nums">{q.consumers}</TableCell>
                      <TableCell className="text-right tabular-nums">{formatRate(q.publishRate)}</TableCell>
                      <TableCell className="text-right tabular-nums">{formatRate(q.deliverRate)}</TableCell>
                      <TableCell className="text-right text-xs tabular-nums text-muted-foreground">
                        {q.totalEnqueued} / {q.totalDequeued}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  )
}
