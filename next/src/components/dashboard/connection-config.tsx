'use client'

/**
 * RocketMQ 连接配置——自定义 NameServer 地址等客户端参数，并展示本机动态限流因子。
 *
 * - GET / PUT /api/clusters/settings/rocketmq：保存后立即生效并持久化到后端文件
 * - GET /api/clusters/rate-limit：单批消息量上限及计算依据（随本机硬件变化）
 */

import { useEffect, useState } from 'react'
import { LoaderCircle, Save } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { fetchRateLimit, fetchRocketMqConfig, updateRocketMqConfig } from '@/lib/api'
import type { RateLimitResult, RocketMqConnectionConfig } from '@/types/cluster'

const PRESETS = ['127.0.0.1:9876', 'localhost:9876']

export function ConnectionConfigPanel() {
  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,420px)]">
      <ConnectionConfigCard />
      <RateLimitCard />
    </div>
  )
}

function ConnectionConfigCard() {
  const [config, setConfig] = useState<RocketMqConnectionConfig | null>(null)
  const [nameServers, setNameServers] = useState('')
  const [sendTimeout, setSendTimeout] = useState(10000)
  const [consumeTimeout, setConsumeTimeout] = useState(15)
  const [prefix, setPrefix] = useState('mqcluster')
  const [loadError, setLoadError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  function apply(data: RocketMqConnectionConfig) {
    setConfig(data)
    setNameServers(data.nameServers.join(', '))
    setSendTimeout(data.sendMsgTimeoutMs)
    setConsumeTimeout(data.consumeTimeoutSeconds)
    setPrefix(data.consumerGroupPrefix)
  }

  useEffect(() => {
    let cancelled = false
    fetchRocketMqConfig()
      .then((data) => {
        if (!cancelled) apply(data)
      })
      .catch((err) => {
        if (!cancelled) setLoadError(err instanceof Error ? err.message : '加载配置失败')
      })
    return () => {
      cancelled = true
    }
  }, [])

  async function save() {
    setSaving(true)
    try {
      const updated = await updateRocketMqConfig({
        nameServers: nameServers
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean),
        sendMsgTimeoutMs: sendTimeout,
        consumeTimeoutSeconds: consumeTimeout,
        consumerGroupPrefix: prefix,
      })
      apply(updated)
      toast.success('配置已保存并立即生效')
    } catch (err) {
      toast.error('保存配置失败', { description: err instanceof Error ? err.message : String(err) })
    } finally {
      setSaving(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>客户端连接配置</CardTitle>
        <CardDescription>真实集群模式（REAL）下，生产者 / 消费者通过这里的 NameServer 地址连接外部 RocketMQ</CardDescription>
        {config && (
          <CardAction>
            <Badge variant={config.nameServers.length > 0 ? 'default' : 'secondary'}>
              {config.nameServers.length > 0 ? `已配置 ${config.nameServers.length} 个 NameServer` : '未配置'}
            </Badge>
          </CardAction>
        )}
      </CardHeader>
      <CardContent className="space-y-4">
        {loadError && <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{loadError}</div>}
        {!config && !loadError ? (
          <div className="space-y-3">
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-2/3" />
            <Skeleton className="h-9 w-1/2" />
          </div>
        ) : (
          <>
            <div className="space-y-2">
              <Label htmlFor="cfg-ns">
                NameServer 地址 <span className="text-xs font-normal text-muted-foreground">多个地址用英文逗号分隔</span>
              </Label>
              <Input
                id="cfg-ns"
                value={nameServers}
                onChange={(e) => setNameServers(e.target.value)}
                placeholder="例如 127.0.0.1:9876 或 192.168.1.100:9876,192.168.1.101:9876"
              />
              <div className="flex flex-wrap gap-2">
                {PRESETS.map((p) => (
                  <Button key={p} type="button" size="sm" variant="outline" onClick={() => setNameServers(p)}>
                    {p}
                  </Button>
                ))}
                <Button type="button" size="sm" variant="ghost" onClick={() => setNameServers('')}>
                  清空
                </Button>
              </div>
            </div>
            <div className="grid gap-4 sm:grid-cols-3">
              <div className="space-y-2">
                <Label htmlFor="cfg-send">发送超时（ms）</Label>
                <Input
                  id="cfg-send"
                  type="number"
                  min={1000}
                  max={600000}
                  value={sendTimeout}
                  onChange={(e) => setSendTimeout(Number(e.target.value) || 10000)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cfg-consume">消费超时（秒）</Label>
                <Input
                  id="cfg-consume"
                  type="number"
                  min={1}
                  max={300}
                  value={consumeTimeout}
                  onChange={(e) => setConsumeTimeout(Number(e.target.value) || 15)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cfg-prefix">消费组前缀</Label>
                <Input id="cfg-prefix" value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="mqcluster" />
              </div>
            </div>
            <Button onClick={() => void save()} disabled={saving}>
              {saving ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}
              保存并生效
            </Button>
            <ul className="list-inside list-disc space-y-1 rounded-lg bg-muted/50 p-3 text-xs text-muted-foreground">
              <li>保存后立即生效，下一次收发消息使用新配置</li>
              <li>配置持久化到后端文件，重启后保留</li>
              <li>地址为空时，嵌入式集群（PSEUDO）使用自身 NameServer</li>
            </ul>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function RateLimitCard() {
  const [limit, setLimit] = useState<RateLimitResult | null>(null)
  const [failed, setFailed] = useState(false)

  useEffect(() => {
    let cancelled = false
    fetchRateLimit()
      .then((data) => {
        if (!cancelled) setLimit(data)
      })
      .catch(() => {
        if (!cancelled) setFailed(true)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const p = limit?.systemProfile
  return (
    <Card>
      <CardHeader>
        <CardTitle>动态限流</CardTitle>
        <CardDescription>单批消息量上限按本机硬件计算，防止本地机器被压垮</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {failed && <div className="text-sm text-muted-foreground">无法获取限流信息</div>}
        {!limit || !p ? (
          !failed && <Skeleton className="h-40 w-full" />
        ) : (
          <>
            <div className="rounded-lg border bg-mq-accent-soft p-4 text-center">
              <div className="text-3xl font-bold text-mq-accent tabular-nums">{limit.maxMessages}</div>
              <div className="mt-1 text-xs text-muted-foreground">单批最大安全消息量</div>
            </div>
            <div className="grid grid-cols-3 gap-2 text-center text-xs">
              <Factor value={p.logicalCores} label="CPU 核数" sub={`×${limit.messagesPerCore}/核`} />
              <Factor value={p.availableHeapMb} label="可用堆（MB）" sub={`×${limit.messagesPerMb}/MB`} />
              <Factor value={p.availableDiskGb.toFixed(1)} label="可用磁盘（GB）" sub={`×${limit.messagesPerGb}/GB`} />
            </div>
            <dl className="space-y-1 text-xs">
              <Row label="物理内存" value={`${(p.freePhysicalMb / 1024).toFixed(1)} / ${(p.totalPhysicalMb / 1024).toFixed(1)} GB 可用`} />
              <Row label="JVM 最大堆" value={`${p.maxHeapMb} MB`} />
              <Row label="系统 CPU 使用率" value={`${p.systemCpuLoad.toFixed(1)}%`} />
              <Row label="安全系数" value={`${limit.safetyCoefficient}（预留余量）`} />
              <Row label="绝对上限 / 最低保底" value={`${limit.baselineCeiling} / ${limit.minFloor}`} />
            </dl>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function Factor({ value, label, sub }: { value: string | number; label: string; sub: string }) {
  return (
    <div className="rounded-lg border p-2">
      <div className="text-lg font-semibold tabular-nums">{value}</div>
      <div className="text-muted-foreground">{label}</div>
      <div className="text-muted-foreground">{sub}</div>
    </div>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-2">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="font-mono">{value}</dd>
    </div>
  )
}
