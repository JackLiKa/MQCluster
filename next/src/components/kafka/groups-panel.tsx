'use client'

/**
 * Kafka「消费者组」标签页。
 *
 * - 后端常驻消费者（KafkaConsumerGroupManager）：增减成员即触发 rebalance，可调节每秒处理条数人为制造 lag
 * - 分区分配按成员上色；每次分配变化记录为一次 rebalance，并以“之前 / 之后”对比 + 颜色过渡动画展示
 * - 每个分区的 lag 柱状图（LEO − committed offset）
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { LoaderCircle, Plus, Send, UserMinus } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Slider } from '@/components/ui/slider'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { EChart } from '@/components/echart'
import { HintButton } from '@/components/hint-button'
import { useChartPalette, type ChartPalette } from '@/hooks/use-chart-palette'
import {
  addKafkaMember,
  fetchKafkaGroups,
  fetchKafkaTopics,
  removeKafkaMember,
  simulateMessages,
  updateKafkaMemberRate,
} from '@/lib/api'
import { cn } from '@/lib/utils'
import type { ClusterSelection, ClusterTopology } from '@/types/cluster'
import type { KafkaConsumerGroupDetail, KafkaGroupMember, KafkaTopicDetail } from '@/types/kafka'
import { kafkaUnavailable } from './topics-panel'

const POLL_MS = 2000
const DEFAULT_GROUP = 'lab-consumers'

const GROUP_STATES: Record<string, { label: string; tone: string }> = {
  STABLE: { label: '稳定', tone: 'text-status-running' },
  PREPARINGREBALANCE: { label: '准备 rebalance', tone: 'text-status-degraded' },
  PREPARING_REBALANCE: { label: '准备 rebalance', tone: 'text-status-degraded' },
  COMPLETINGREBALANCE: { label: '完成 rebalance 中', tone: 'text-status-degraded' },
  COMPLETING_REBALANCE: { label: '完成 rebalance 中', tone: 'text-status-degraded' },
  EMPTY: { label: '空组', tone: 'text-muted-foreground' },
  DEAD: { label: '已失效', tone: 'text-status-failed' },
  UNKNOWN: { label: '未知', tone: 'text-muted-foreground' },
}

function stateMeta(state: string) {
  return GROUP_STATES[state.toUpperCase()] ?? { label: state, tone: 'text-muted-foreground' }
}

export function KafkaGroupsPanel({ selection, topology }: { selection: ClusterSelection; topology: ClusterTopology | null }) {
  const unavailable = kafkaUnavailable(topology)
  const [groups, setGroups] = useState<KafkaConsumerGroupDetail[]>([])
  const [topics, setTopics] = useState<KafkaTopicDetail[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const [g, t] = await Promise.all([fetchKafkaGroups(selection), fetchKafkaTopics(selection)])
      setGroups(g)
      setTopics(t)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }, [selection])

  useEffect(() => {
    if (unavailable) return
    const first = setTimeout(() => void load(), 0)
    const timer = setInterval(() => void load(), POLL_MS)
    return () => {
      clearTimeout(first)
      clearInterval(timer)
    }
  }, [load, unavailable])

  if (unavailable) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          {unavailable}，暂时无法查询消费者组。请先在「概览」中启动集群。
        </CardContent>
      </Card>
    )
  }

  const visible = groups.filter((g) => !g.groupId.startsWith('mqcluster-'))
  const current = visible.find((g) => g.groupId === selected) ?? visible.find((g) => g.groupId === DEFAULT_GROUP) ?? visible[0] ?? null

  return (
    <div className="space-y-4">
      <AddMemberCard
        selection={selection}
        topics={topics}
        groupIds={visible.map((g) => g.groupId)}
        onAdded={async (groupId) => {
          setSelected(groupId)
          await load()
        }}
      />
      {error && <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{error}</div>}
      {visible.length > 0 && (
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm text-muted-foreground">消费者组：</span>
          {visible.map((g) => (
            <button
              key={g.groupId}
              type="button"
              onClick={() => setSelected(g.groupId)}
              className={cn(
                'rounded-full border px-3 py-1 font-mono text-xs transition-colors',
                current?.groupId === g.groupId
                  ? 'border-mq-accent bg-mq-accent text-mq-accent-foreground'
                  : 'hover:bg-muted',
              )}
            >
              {g.groupId} · {g.members.length} 成员
            </button>
          ))}
        </div>
      )}
      {current ? (
        <GroupDetail key={current.groupId} selection={selection} group={current} onChanged={load} />
      ) : (
        <Card>
          <CardContent className="py-12 text-center text-sm text-muted-foreground">
            还没有消费者组。用上方表单添加第一个常驻消费者，然后再添加第二个，观察 rebalance 如何重新分配分区。
          </CardContent>
        </Card>
      )}
    </div>
  )
}

function AddMemberCard({
  selection,
  topics,
  groupIds,
  onAdded,
}: {
  selection: ClusterSelection
  topics: KafkaTopicDetail[]
  groupIds: string[]
  onAdded: (groupId: string) => Promise<void>
}) {
  const userTopics = topics.filter((t) => !t.internal)
  const [groupId, setGroupId] = useState(DEFAULT_GROUP)
  const [topic, setTopic] = useState('lab-orders')
  const [rate, setRate] = useState(5)
  const [busy, setBusy] = useState(false)
  const [producing, setProducing] = useState(false)
  const [produceCount, setProduceCount] = useState(300)

  const effectiveTopic = userTopics.some((t) => t.name === topic) ? topic : (userTopics[0]?.name ?? '')

  async function add() {
    setBusy(true)
    try {
      const m = await addKafkaMember(selection, groupId.trim(), effectiveTopic, rate)
      toast.success(`成员 ${m.memberId} 已加入 ${groupId}`, { description: '协调者将触发 rebalance，几秒后分区重新分配' })
      await onAdded(groupId.trim())
    } catch (err) {
      toast.error('添加成员失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setBusy(false)
    }
  }

  async function produce() {
    setProducing(true)
    try {
      const res = await simulateMessages(selection, {
        topic: effectiveTopic,
        consumerGroup: 'lab-workbench',
        messageCount: produceCount,
        producerNodeId: 'kafka-producer',
        payloadTemplate: '{"orderId":"{uuid}","index":{index}}',
      })
      const ok = res.deliveries.filter((d) => d.success).length
      toast.success(`已写入 ${ok} 条消息到 ${effectiveTopic}`, { description: '常驻消费者按设定速率消费，观察 lag 变化' })
    } catch (err) {
      toast.error('写入失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setProducing(false)
    }
  }

  const reason = !groupId.trim()
    ? '请输入消费者组 ID'
    : !effectiveTopic
      ? '还没有 topic，请先在「Topic 与分区」中创建'
      : null

  return (
    <Card>
      <CardHeader>
        <CardTitle>添加常驻消费者</CardTitle>
        <CardDescription>
          消费者在后端常驻运行。同一组的成员数 ≤ 分区数时每个成员都能分到分区；超出的成员会空闲——自己动手验证。
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid items-end gap-3 md:grid-cols-2 xl:grid-cols-[1fr_1fr_200px_auto_auto_auto]">
          <div className="space-y-2">
            <Label htmlFor="kg-group">消费者组</Label>
            <Input id="kg-group" value={groupId} onChange={(e) => setGroupId(e.target.value)} list="kg-group-list" />
            <datalist id="kg-group-list">
              {groupIds.map((g) => (
                <option key={g} value={g} />
              ))}
            </datalist>
          </div>
          <div className="space-y-2">
            <Label>订阅 Topic</Label>
            <Select value={effectiveTopic} onValueChange={(v) => v && setTopic(v)}>
              <SelectTrigger className="w-full">
                <SelectValue>{effectiveTopic || '暂无 topic'}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {userTopics.map((t) => (
                  <SelectItem key={t.name} value={t.name}>
                    {t.name}（{t.partitionCount} 分区）
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>消费速率：{rate} 条/秒</Label>
            <Slider
              value={[rate]}
              min={0}
              max={100}
              onValueChange={(v) => setRate(Array.isArray(v) ? v[0] : (v as number))}
              aria-label="消费速率"
            />
          </div>
          <HintButton onClick={() => void add()} disabledReason={busy ? '正在添加…' : reason}>
            {busy ? <LoaderCircle className="size-4 animate-spin" /> : <Plus className="size-4" />}
            加入消费者
          </HintButton>
          <Input
            type="number"
            aria-label="写入条数"
            className="w-24"
            min={1}
            max={500}
            value={produceCount}
            onChange={(e) => setProduceCount(Math.max(1, Math.min(500, Number(e.target.value) || 1)))}
          />
          <HintButton
            variant="outline"
            onClick={() => void produce()}
            disabledReason={producing ? '正在写入…' : effectiveTopic ? null : '还没有 topic'}
          >
            {producing ? <LoaderCircle className="size-4 animate-spin" /> : <Send className="size-4" />}
            写入消息
          </HintButton>
        </div>
      </CardContent>
    </Card>
  )
}

/** partition key（topic-N）→ memberId */
type Assignment = Record<string, string | null>

interface RebalanceEvent {
  time: string
  before: Assignment
  after: Assignment
}

function assignmentOf(group: KafkaConsumerGroupDetail): Assignment {
  const result: Assignment = {}
  for (const lag of group.lagPerPartition) result[`${lag.topic}-${lag.partition}`] = lag.ownerMemberId
  return result
}

function memberColors(members: KafkaGroupMember[], palette: ChartPalette): Record<string, string> {
  const ids = [...members.map((m) => m.memberId)].sort()
  return Object.fromEntries(ids.map((id, i) => [id, palette.series[i % palette.series.length]]))
}

function GroupDetail({
  selection,
  group,
  onChanged,
}: {
  selection: ClusterSelection
  group: KafkaConsumerGroupDetail
  onChanged: () => Promise<void>
}) {
  const ref = useRef<HTMLDivElement>(null)
  const palette = useChartPalette(ref)
  const colors = memberColors(group.members, palette)
  const colorOf = (id: string | null | undefined) => (id ? (colors[id] ?? palette.muted) : 'transparent')

  // 分配变化即视为一次 rebalance（渲染期间按 props 派生，避免 effect 内 setState）
  const assignment = assignmentOf(group)
  const signature = JSON.stringify(assignment)
  const [prevSignature, setPrevSignature] = useState(signature)
  const [prevAssignment, setPrevAssignment] = useState(assignment)
  const [history, setHistory] = useState<RebalanceEvent[]>([])
  if (signature !== prevSignature) {
    setPrevSignature(signature)
    setPrevAssignment(assignment)
    setHistory((h) =>
      [{ time: new Date().toLocaleTimeString('zh-CN', { hour12: false }), before: prevAssignment, after: assignment }, ...h].slice(0, 6),
    )
  }

  const partitions = group.lagPerPartition
  const state = stateMeta(group.state)

  const lagOption = useMemo(
    () => ({
      tooltip: {
        trigger: 'axis',
        backgroundColor: palette.card,
        borderColor: palette.border,
        textStyle: { color: palette.foreground },
      },
      grid: { left: 48, right: 16, top: 16, bottom: 32 },
      xAxis: {
        type: 'category',
        data: partitions.map((p) => `${p.topic}-P${p.partition}`),
        axisLine: { lineStyle: { color: palette.border } },
        axisLabel: { color: palette.muted, fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        minInterval: 1,
        axisLabel: { color: palette.muted, fontSize: 11 },
        splitLine: { lineStyle: { color: palette.border, type: 'dashed' as const } },
      },
      series: [
        {
          name: 'Lag',
          type: 'bar',
          barMaxWidth: 36,
          data: partitions.map((p) => ({
            value: p.lag,
            itemStyle: { color: p.ownerMemberId ? colorOf(p.ownerMemberId) : palette.muted, borderRadius: [4, 4, 0, 0] },
          })),
          label: { show: true, position: 'top', color: palette.muted, fontSize: 11 },
        },
      ],
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [partitions, palette, signature],
  )

  return (
    <div ref={ref} className="space-y-4">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat label="组状态" value={<span className={state.tone}>{state.label}</span>} />
        <Stat label="成员数" value={group.members.length} />
        <Stat label="分区数" value={partitions.length} />
        <Stat
          label="总 Lag"
          value={<span className={cn(group.totalLag > 0 && 'text-status-degraded')}>{group.totalLag}</span>}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>成员</CardTitle>
            <CardDescription>拖动滑块调节处理速率；速率低于写入速率时 lag 会持续增长</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {group.members.length === 0 && (
              <div className="py-6 text-center text-sm text-muted-foreground">该组当前没有活跃成员</div>
            )}
            {group.members.map((m) => (
              <MemberRow
                key={m.memberId}
                member={m}
                color={colorOf(m.memberId)}
                selection={selection}
                groupId={group.groupId}
                onChanged={onChanged}
              />
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>分区分配</CardTitle>
            <CardDescription>颜色代表所属成员；灰色虚线框表示暂未分配</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <AssignmentRow assignment={assignment} colorOf={colorOf} />
            <div className="space-y-2">
              <div className="text-sm font-medium">Rebalance 记录</div>
              {history.length === 0 ? (
                <p className="text-xs text-muted-foreground">
                  尚未观察到 rebalance。添加或移除一个成员，几秒后这里会显示分配前后的对比。
                </p>
              ) : (
                <div className="space-y-3">
                  {history.map((h, i) => (
                    <div key={`${h.time}-${i}`} className={cn('space-y-1.5 rounded-lg border p-2.5', i === 0 && 'mq-fade-in')}>
                      <div className="text-xs text-muted-foreground tabular-nums">{h.time}</div>
                      <div className="grid grid-cols-[40px_1fr] items-center gap-2 text-xs">
                        <span className="text-muted-foreground">之前</span>
                        <AssignmentRow assignment={h.before} colorOf={colorOf} compact />
                        <span className="text-muted-foreground">之后</span>
                        <AssignmentRow assignment={h.after} colorOf={colorOf} compact changedFrom={h.before} />
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>各分区 Lag</CardTitle>
          <CardDescription>lag = 分区末端 offset（LEO）− 组内已提交 offset；柱子颜色对应负责该分区的成员</CardDescription>
        </CardHeader>
        <CardContent>
          {partitions.length === 0 ? (
            <div className="py-8 text-center text-sm text-muted-foreground">组尚未订阅任何分区</div>
          ) : (
            <EChart option={lagOption} className="h-64" ariaLabel="各分区 lag 柱状图" />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Card size="sm">
      <CardContent>
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="mt-1 text-2xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  )
}

function AssignmentRow({
  assignment,
  colorOf,
  compact,
  changedFrom,
}: {
  assignment: Assignment
  colorOf: (id: string | null) => string
  compact?: boolean
  changedFrom?: Assignment
}) {
  const keys = Object.keys(assignment).sort((a, b) => {
    const [ta, pa] = [a.slice(0, a.lastIndexOf('-')), Number(a.slice(a.lastIndexOf('-') + 1))]
    const [tb, pb] = [b.slice(0, b.lastIndexOf('-')), Number(b.slice(b.lastIndexOf('-') + 1))]
    return ta === tb ? pa - pb : ta.localeCompare(tb)
  })
  if (keys.length === 0) return <span className="text-xs text-muted-foreground">无分区</span>
  return (
    <div className="flex flex-wrap gap-1.5">
      {keys.map((k) => {
        const owner = assignment[k]
        const moved = changedFrom && changedFrom[k] !== owner
        const label = `P${k.slice(k.lastIndexOf('-') + 1)}`
        return (
          <div
            key={k}
            title={`${k} → ${owner ?? '未分配'}`}
            className={cn(
              'flex flex-col items-center justify-center rounded-md border font-mono text-white transition-[background-color,box-shadow] duration-700',
              compact ? 'h-7 min-w-9 px-1 text-[10px]' : 'h-12 min-w-14 px-2 text-xs',
              !owner && 'border-dashed text-muted-foreground',
              moved && 'ring-2 ring-foreground/60 ring-offset-1 ring-offset-card',
            )}
            style={{ backgroundColor: colorOf(owner) }}
          >
            <span className="font-semibold">{label}</span>
            {!compact && <span className="text-[10px] opacity-90">{owner ?? '—'}</span>}
          </div>
        )
      })}
    </div>
  )
}

function MemberRow({
  member,
  color,
  selection,
  groupId,
  onChanged,
}: {
  member: KafkaGroupMember
  color: string
  selection: ClusterSelection
  groupId: string
  onChanged: () => Promise<void>
}) {
  const [rate, setRate] = useState(member.ratePerSecond)
  const [busy, setBusy] = useState(false)
  const [syncedRate, setSyncedRate] = useState(member.ratePerSecond)
  if (member.ratePerSecond !== syncedRate) {
    setSyncedRate(member.ratePerSecond)
    setRate(member.ratePerSecond)
  }

  async function commitRate(value: number) {
    try {
      await updateKafkaMemberRate(selection, groupId, member.memberId, value)
      toast.success(`${member.memberId} 速率调整为 ${value} 条/秒`)
      await onChanged()
    } catch (err) {
      toast.error('调整失败', { description: err instanceof Error ? err.message : String(err) })
    }
  }

  async function remove() {
    setBusy(true)
    try {
      await removeKafkaMember(selection, groupId, member.memberId)
      toast.success(`${member.memberId} 已离开消费者组`, { description: '剩余成员将接管其分区' })
      await onChanged()
    } catch (err) {
      toast.error('移除失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-2 rounded-lg border p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="size-3 shrink-0 rounded-full" style={{ backgroundColor: color }} />
          <span className="font-mono text-sm font-medium">{member.memberId}</span>
          {!member.managed && <Badge variant="outline">外部成员</Badge>}
          <span className="truncate text-xs text-muted-foreground">
            {member.assignedPartitions.length > 0 ? member.assignedPartitions.join(', ') : '暂未分到分区'}
          </span>
        </div>
        {member.managed && (
          <Button size="sm" variant="ghost" onClick={() => void remove()} disabled={busy} aria-label={`移除 ${member.memberId}`}>
            {busy ? <LoaderCircle className="size-4 animate-spin" /> : <UserMinus className="size-4" />}
            移除
          </Button>
        )}
      </div>
      {member.managed && (
        <div className="grid grid-cols-[1fr_auto] items-center gap-3">
          <Slider
            value={[rate]}
            min={0}
            max={100}
            aria-label={`${member.memberId} 消费速率`}
            onValueChange={(v) => setRate(Array.isArray(v) ? v[0] : (v as number))}
            onValueCommitted={(v) => void commitRate(Array.isArray(v) ? v[0] : (v as number))}
          />
          <span className="w-36 text-right text-xs text-muted-foreground tabular-nums">
            {rate === 0 ? '已暂停' : `${rate} 条/秒`} · 已处理 {member.processed}
          </span>
        </div>
      )}
    </div>
  )
}
