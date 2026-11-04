'use client'

/**
 * 日志面板——集群活动日志（初次 REST 拉取 + STOMP 实时追加）。
 *
 * 支持按节点 / 级别 / 关键字筛选，清空本地视图（不影响后端日志）。
 */

import { useMemo, useState } from 'react'
import { Eraser, Radio } from 'lucide-react'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'
import type { ClusterTopology, LogEntry } from '@/types/cluster'

const LEVELS = ['ALL', 'INFO', 'WARN', 'ERROR'] as const
const LEVEL_LABELS: Record<string, string> = { ALL: '全部级别', INFO: 'INFO', WARN: 'WARN', ERROR: 'ERROR' }

const LEVEL_CLASS: Record<string, string> = {
  INFO: 'text-status-starting',
  WARN: 'text-status-degraded',
  ERROR: 'text-status-failed',
  DEBUG: 'text-muted-foreground',
}

export function LogsPanel({
  topology,
  logs,
  live,
  onClear,
}: {
  topology: ClusterTopology | null
  logs: LogEntry[]
  live: boolean
  onClear: () => void
}) {
  const [node, setNode] = useState('ALL')
  const [level, setLevel] = useState<string>('ALL')
  const [keyword, setKeyword] = useState('')

  const nodeIds = useMemo(() => {
    const ids = new Set((topology?.nodes ?? []).map((n) => n.nodeId))
    for (const l of logs) ids.add(l.nodeId)
    return [...ids].sort()
  }, [topology, logs])

  const filtered = logs.filter(
    (l) =>
      (node === 'ALL' || l.nodeId === node) &&
      (level === 'ALL' || l.level?.toUpperCase() === level) &&
      (!keyword.trim() || l.message.toLowerCase().includes(keyword.trim().toLowerCase())),
  )

  return (
    <Card>
      <CardHeader>
        <CardTitle>活动日志</CardTitle>
        <CardDescription className="flex items-center gap-1.5">
          <Radio className={cn('size-3.5', live ? 'text-status-running' : 'text-muted-foreground')} />
          {live ? '实时推送已连接' : '实时推送未连接，正在轮询'} · 显示 {filtered.length}/{logs.length} 条
        </CardDescription>
        <CardAction>
          <Button size="sm" variant="outline" onClick={onClear} disabled={logs.length === 0}>
            <Eraser className="size-4" /> 清空视图
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap gap-2">
          <Select value={node} onValueChange={(v) => v && setNode(v)}>
            <SelectTrigger className="w-48" aria-label="按节点筛选">
              <SelectValue>{node === 'ALL' ? '全部节点' : node}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">全部节点</SelectItem>
              {nodeIds.map((id) => (
                <SelectItem key={id} value={id}>
                  {id}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={level} onValueChange={(v) => v && setLevel(v)}>
            <SelectTrigger className="w-36" aria-label="按级别筛选">
              <SelectValue>{LEVEL_LABELS[level]}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              {LEVELS.map((lv) => (
                <SelectItem key={lv} value={lv}>
                  {LEVEL_LABELS[lv]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            className="w-64"
            placeholder="搜索日志内容…"
            aria-label="搜索日志内容"
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
          />
        </div>
        <ScrollArea className="h-[480px] rounded-lg border bg-muted/30">
          <div className="divide-y font-mono text-xs">
            {filtered.map((l, i) => (
              <div key={`${l.timestamp}-${l.nodeId}-${i}`} className="grid grid-cols-[150px_140px_52px_1fr] gap-3 px-3 py-1.5">
                <span className="text-muted-foreground tabular-nums">
                  {new Date(l.timestamp).toLocaleString('zh-CN', { hour12: false })}
                </span>
                <span className="truncate" title={l.nodeId}>
                  {l.nodeId}
                </span>
                <span className={cn('font-semibold', LEVEL_CLASS[l.level?.toUpperCase()] ?? '')}>{l.level}</span>
                <span className="break-all">{l.message}</span>
              </div>
            ))}
            {filtered.length === 0 && (
              <div className="py-16 text-center font-sans text-sm text-muted-foreground">
                {logs.length === 0 ? '暂无日志——启动节点或发送消息后这里会出现记录' : '没有符合筛选条件的日志'}
              </div>
            )}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  )
}
