'use client'

/**
 * 「内部机制」标签页——把 broker 此刻的真实内部状态画出来。
 *
 * 数据全部来自后端 IInternalsInspector（RocketMQ 读进程内 CommitLog 写指针与分发落后字节、
 * Kafka 读日志目录与 Admin API、Qpid 读对象模型），前端只负责展示，不做二次推算。
 * 每 2 秒刷新一次：跑压力实验时切到这里，能看到写指针、日志段、队列深度随流量增长。
 */

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { AlertCircle, ArrowRight, Database, Gauge, RefreshCw, Timer } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import { fetchInternals } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { ClusterSelection } from '@/types/cluster'
import type { InternalsSegment, InternalsSnapshot } from '@/types/lab'

const POLL_MS = 2000

export function InternalsPanel({ selection }: { selection: ClusterSelection }) {
  const [snapshot, setSnapshot] = useState<InternalsSnapshot | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [at, setAt] = useState<string | null>(null)
  const inFlight = useRef(false)

  const load = useCallback(async () => {
    if (inFlight.current) return
    inFlight.current = true
    try {
      const next = await fetchInternals(selection)
      setSnapshot(next)
      setError(null)
      setAt(new Date().toLocaleTimeString('zh-CN', { hour12: false }))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      inFlight.current = false
    }
  }, [selection])

  // 首次加载 + 自动刷新：用 setTimeout 打头，避免在 effect 内同步 setState
  useEffect(() => {
    if (!autoRefresh) return
    const first = setTimeout(() => void load(), 0)
    const timer = setInterval(() => void load(), POLL_MS)
    return () => {
      clearTimeout(first)
      clearInterval(timer)
    }
  }, [autoRefresh, load])

  const groups = useMemo(() => groupSegments(snapshot?.segments ?? []), [snapshot])

  if (error && !snapshot) {
    return (
      <Card>
        <CardContent className="space-y-3 py-12 text-center">
          <AlertCircle className="mx-auto size-8 text-status-failed" />
          <p className="text-sm text-muted-foreground">{error}</p>
          <Button variant="outline" size="sm" onClick={() => void load()}>
            <RefreshCw className="size-4" /> 重试
          </Button>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex flex-wrap items-center gap-2">
            broker 内部正在做什么
            {snapshot && (
              <Badge variant="outline" className="font-mono text-[10px]">
                {at} 快照
              </Badge>
            )}
          </CardTitle>
          <CardDescription>
            每 {POLL_MS / 1000} 秒读取一次真实运行时状态：节点内的存储写指针、磁盘段文件、broker 对象模型。数值全部来自进程与磁盘，不做模拟。
          </CardDescription>
          <CardAction className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              自动刷新
              <Switch checked={autoRefresh} onCheckedChange={(c) => setAutoRefresh(Boolean(c))} />
            </label>
            <Button variant="outline" size="sm" onClick={() => void load()}>
              <RefreshCw className="size-3.5" /> 刷新
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-sm leading-relaxed">{snapshot?.headline ?? '正在读取 broker 内部状态…'}</p>
          {error && (
            <p className="flex items-center gap-2 text-xs text-status-failed">
              <AlertCircle className="size-3.5" /> 本次刷新失败（下方为上一次成功的数据）：{error}
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            想看到这些数字剧烈变化？切到「压力实验」制造洪峰，再回到这里观察。
          </p>
        </CardContent>
      </Card>

      {snapshot && snapshot.pipeline.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Timer className="size-4 text-mq-accent" /> 消息在 broker 内部走的四步
            </CardTitle>
            <CardDescription>每一步的数值都是当前真实计数——卡在哪一步，就是吞吐瓶颈在哪一步</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-stretch gap-2">
              {snapshot.pipeline.map((stage, index) => (
                <Fragment key={stage.name}>
                  {index > 0 && <ArrowRight className="size-4 shrink-0 self-center text-muted-foreground" />}
                  <div className="min-w-40 flex-1 rounded-lg border bg-muted/30 p-3">
                    <div className="text-xs text-muted-foreground">{stage.name}</div>
                    <div className="mt-1 text-lg font-semibold tabular-nums">{stage.value}</div>
                    <div className="mt-1 text-[11px] leading-snug text-muted-foreground">{stage.detail}</div>
                  </div>
                </Fragment>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="size-4 text-mq-accent" /> 底层存储单元
            </CardTitle>
            <CardDescription>
              {groups.length > 0
                ? `共 ${snapshot?.segments.length ?? 0} 个单元——条越长表示该单元越大`
                : '当前没有可读的存储单元（节点未运行或尚未写入数据）'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {/* 分段文件可能很多（Kafka 每个 broker 都有若干段），限高滚动避免页面被拉长 */}
            <div className="max-h-[560px] space-y-5 overflow-y-auto pr-1">
              {groups.map((group) => (
                <SegmentGroup key={group.name} name={group.name} segments={group.segments} total={group.total} />
              ))}
              {groups.length === 0 && (
                <p className="py-6 text-center text-xs text-muted-foreground">先在「概览」启动节点，或发送一批消息后再来查看。</p>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Gauge className="size-4 text-mq-accent" /> 关键内部计数器
            </CardTitle>
            <CardDescription>直接读取 broker 自己的统计对象</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 sm:grid-cols-2">
            {(snapshot?.metrics ?? []).map((metric) => (
              <div key={metric.label} className="rounded-lg border p-3">
                <div className="text-xs text-muted-foreground">{metric.label}</div>
                <div className="mt-1 text-xl font-semibold tabular-nums">{metric.value}</div>
                <div className="mt-1 text-[11px] leading-snug text-muted-foreground">{metric.hint}</div>
              </div>
            ))}
            {(snapshot?.metrics.length ?? 0) === 0 && (
              <p className="text-xs text-muted-foreground">暂无计数器数据。</p>
            )}
          </CardContent>
        </Card>
      </div>

      {(snapshot?.notes.length ?? 0) > 0 && (
        <Card size="sm">
          <CardHeader>
            <CardTitle className="text-sm">机制说明</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-1.5 text-xs leading-relaxed text-muted-foreground">
              {snapshot?.notes.map((note) => (
                <li key={note} className="flex gap-2">
                  <span className="mt-1.5 size-1 shrink-0 rounded-full bg-mq-accent" aria-hidden />
                  <span>{note}</span>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

interface SegmentGroupData {
  name: string
  segments: InternalsSegment[]
  total: string
}

/** 按 group 聚合并算出组内合计（字节与条数分别累加） */
function groupSegments(segments: InternalsSegment[]): SegmentGroupData[] {
  const map = new Map<string, InternalsSegment[]>()
  for (const segment of segments) {
    const list = map.get(segment.group)
    if (list) list.push(segment)
    else map.set(segment.group, [segment])
  }
  return [...map.entries()].map(([name, list]) => {
    const bytes = list.filter((s) => s.unit === 'B').reduce((sum, s) => sum + s.size, 0)
    const items = list.filter((s) => s.unit !== 'B').reduce((sum, s) => sum + s.size, 0)
    const parts = [bytes > 0 ? formatBytes(bytes) : null, items > 0 ? `${items.toLocaleString()} 条` : null].filter(Boolean)
    return { name, segments: list, total: parts.join(' + ') }
  })
}

function SegmentGroup({ name, segments, total }: SegmentGroupData) {
  const max = Math.max(...segments.map((s) => s.size), 1)
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-baseline justify-between gap-2 border-b pb-1.5">
        <span className="font-mono text-xs font-medium">{name}</span>
        <span className="text-[11px] text-muted-foreground tabular-nums">
          {segments.length} 个单元 · 合计 {total}
        </span>
      </div>
      {segments.map((segment) => (
        <div key={`${segment.name}-${segment.detail}`} className="space-y-1">
          <div className="flex items-baseline justify-between gap-3">
            <span className="truncate font-mono text-[11px]" title={segment.name}>
              {segment.name}
            </span>
            <span className="shrink-0 text-[11px] tabular-nums text-muted-foreground">{formatSize(segment)}</span>
          </div>
          <div className="h-1.5 overflow-hidden rounded-full bg-muted">
            <div
              className={cn('h-full rounded-full bg-mq-accent transition-[width] duration-500')}
              style={{ width: `${Math.max(2, (segment.size / max) * 100)}%` }}
            />
          </div>
          <div className="truncate text-[11px] text-muted-foreground" title={segment.detail}>
            {segment.detail}
          </div>
        </div>
      ))}
    </div>
  )
}

function formatSize(segment: InternalsSegment): string {
  if (segment.unit === 'B') return formatBytes(segment.size)
  return `${segment.size.toLocaleString()} ${segment.unit}`.trim()
}

function formatBytes(bytes: number): string {
  if (bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** exponent
  return `${value >= 100 || exponent === 0 ? Math.round(value) : value.toFixed(1)} ${units[exponent]}`
}
