'use client'

/**
 * MQ 仪表盘——/mq/[middleware] 的客户端主体，三种 MQ 共用。
 *
 * - provider 来自全局 ClusterStatusProvider；RocketMQ 可在嵌入式（PSEUDO）与真实集群（REAL）间切换（?mode=）
 * - 当前标签页保存在 ?tab=，刷新不丢失；标签页由 mq-registry 结合后端 capabilities 决定
 * - 以 provider 作为 key 渲染 DashboardBody，切换 provider 时自动重置所有状态
 * - 集群未运行时显示引导卡片（一键启动）；后端不可达时显示重试
 */

import Link from 'next/link'
import { useMemo, useState } from 'react'
import { usePathname, useSearchParams } from 'next/navigation'
import { BookOpen, LoaderCircle, Power, Radio, RefreshCw, ServerCrash } from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ConfirmDialog, type ConfirmRequest } from '@/components/confirm-dialog'
import { selectionOf, useClusterStatus } from '@/components/cluster-status-provider'
import { TopologyCard } from '@/components/dashboard/topology-card'
import { NodeOperationsCard } from '@/components/dashboard/node-operations-card'
import { MessageWorkbench } from '@/components/dashboard/message-workbench'
import { MonitoringPanel } from '@/components/dashboard/monitoring-panel'
import { LogsPanel } from '@/components/dashboard/logs-panel'
import { ConnectionConfigPanel } from '@/components/dashboard/connection-config'
import { KafkaTopicsPanel } from '@/components/kafka/topics-panel'
import { KafkaGroupsPanel } from '@/components/kafka/groups-panel'
import { RabbitRoutingPanel } from '@/components/rabbit/routing-panel'
import { RabbitAckLab } from '@/components/rabbit/ack-lab-panel'
import { StressPanel } from '@/components/lab/stress-panel'
import { InternalsPanel } from '@/components/lab/internals-panel'
import { useClusterData } from '@/hooks/use-cluster-data'
import { overlayPending, useNodeOperations } from '@/hooks/use-node-operations'
import { availableTabs, MQ_REGISTRY, roleMeta, type DashboardTab, type MqMeta, type MqSlug } from '@/lib/mq-registry'
import { cn } from '@/lib/utils'
import type { ProviderDescriptor } from '@/types/cluster'

export function MqDashboard({ slug }: { slug: MqSlug }) {
  const meta = MQ_REGISTRY[slug]
  const Icon = meta.icon
  const { providers, loaded, backendDown, refresh } = useClusterStatus()
  const pathname = usePathname()
  const params = useSearchParams()
  const [modeParamRaw, setMode] = useUrlParam('mode', pathname, params)
  const [tabParam, setTab] = useUrlParam('tab', pathname, params)

  const candidates = providers.filter((p) => p.middleware === meta.middleware)
  const modeParam = modeParamRaw?.toUpperCase()
  const provider =
    candidates.find((p) => p.mode === modeParam) ?? candidates.find((p) => p.mode === 'PSEUDO') ?? candidates[0] ?? null

  return (
    <div data-mq={slug} className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="flex size-11 items-center justify-center rounded-xl bg-mq-accent text-mq-accent-foreground shadow-sm">
            <Icon className="size-6" />
          </div>
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{meta.name} 实验室</h1>
            <p className="text-sm text-muted-foreground">{meta.tagline}</p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {candidates.length > 1 && (
            <div className="inline-flex rounded-lg border p-0.5" role="radiogroup" aria-label="集群模式">
              {candidates.map((p) => (
                <button
                  key={p.providerId}
                  type="button"
                  role="radio"
                  aria-checked={p.providerId === provider?.providerId}
                  onClick={() => setMode(p.mode.toLowerCase())}
                  className={cn(
                    'rounded-md px-3 py-1 text-sm transition-colors',
                    p.providerId === provider?.providerId
                      ? 'bg-mq-accent text-mq-accent-foreground'
                      : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {p.mode === 'PSEUDO' ? '嵌入式集群' : '真实集群'}
                </button>
              ))}
            </div>
          )}
          <Button variant="outline" size="sm" nativeButton={false} render={<Link href={`/guide/${slug}`} />}>
            <BookOpen className="size-4" /> 学习指南
          </Button>
        </div>
      </div>

      {!loaded ? (
        <DashboardSkeleton />
      ) : backendDown && !provider ? (
        <Card>
          <CardContent className="flex flex-col items-center gap-3 py-12 text-center">
            <ServerCrash className="size-10 text-status-failed" />
            <div className="text-lg font-medium">无法连接后端服务</div>
            <p className="max-w-md text-sm text-muted-foreground">
              请确认 Spring Boot 后端已在 8088 端口启动（java 目录下执行 ./mvnw spring-boot:run），页面会每 10 秒自动重试。
            </p>
            <Button onClick={refresh}>
              <RefreshCw className="size-4" /> 立即重试
            </Button>
          </CardContent>
        </Card>
      ) : !provider ? (
        <Card>
          <CardContent className="py-12 text-center text-sm text-muted-foreground">
            后端未启用 {meta.name}（检查 application.properties 中的 cluster.{slug}.enabled 配置）
          </CardContent>
        </Card>
      ) : (
        <DashboardBody
          key={provider.providerId}
          meta={meta}
          provider={provider}
          tab={tabParam}
          onTabChange={setTab}
          onClusterChanged={refresh}
        />
      )}
    </div>
  )
}

function DashboardBody({
  meta,
  provider,
  tab,
  onTabChange,
  onClusterChanged,
}: {
  meta: MqMeta
  provider: ProviderDescriptor
  tab: string | null
  onTabChange: (tab: DashboardTab) => void
  onClusterChanged: () => void
}) {
  const selection = useMemo(() => selectionOf(provider), [provider])
  const data = useClusterData(selection)
  const { refresh: refreshData } = data
  const refresh = useMemo(
    () => async () => {
      await refreshData()
      onClusterChanged()
    },
    [refreshData, onClusterChanged],
  )
  const ops = useNodeOperations(selection, refresh)
  const topology = overlayPending(data.topology, ops.pending)
  const [confirm, setConfirm] = useState<ConfirmRequest | null>(null)

  const tabs = availableTabs(meta, provider)
  const active = (tabs.find((t) => t.id === tab)?.id ?? 'overview') as DashboardTab
  const nodes = topology?.nodes ?? []
  const running = nodes.filter((n) => n.status === 'RUNNING').length
  const starting = nodes.some((n) => n.status === 'STARTING')
  const canCluster = provider.capabilities.includes('CLUSTER_OPERATIONS')
  const showGuide = data.loaded && !data.error && nodes.length > 0 && running === 0 && !starting && canCluster

  return (
    <>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-muted-foreground">
        <Badge variant="outline" className="font-mono">
          {provider.clusterId}
        </Badge>
        <span>{provider.displayName}</span>
        <span>
          运行中 <span className="font-medium text-foreground tabular-nums">{running}</span>/{nodes.length} 个节点
        </span>
        <span className="inline-flex items-center gap-1">
          <Radio className={cn('size-3.5', data.live ? 'text-status-running' : 'text-muted-foreground')} />
          {data.live ? '实时推送' : '轮询中'}
        </span>
      </div>

      {data.error && (
        <Card>
          <CardContent className="flex flex-wrap items-center justify-between gap-3 py-4">
            <div className="text-sm">
              <span className="font-medium text-status-failed">加载集群数据失败：</span>
              {data.error}
            </div>
            <Button size="sm" variant="outline" onClick={() => void refresh()}>
              <RefreshCw className="size-4" /> 重试
            </Button>
          </CardContent>
        </Card>
      )}

      {showGuide && (
        <Card className="border-mq-accent/40 bg-mq-accent-soft">
          <CardHeader>
            <CardTitle>集群尚未运行</CardTitle>
            <CardDescription>
              所有节点初始均为已停止状态。点击“一键启动集群”
              {Object.keys(meta.roles).length > 1
                ? `会按 ${Object.keys(meta.roles)
                    .map((r) => roleMeta(meta, r).label)
                    .join(' → ')} 的顺序依次拉起`
                : `即可拉起 ${roleMeta(meta, Object.keys(meta.roles)[0]).label}`}
              ，首次启动约需 10–30 秒。
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={() => void ops.operateAll('START')} disabled={ops.clusterBusy}>
              {ops.clusterBusy ? <LoaderCircle className="size-4 animate-spin" /> : <Power className="size-4" />}
              一键启动集群
            </Button>
          </CardContent>
        </Card>
      )}

      {!data.loaded ? (
        <DashboardSkeleton />
      ) : (
        <Tabs value={active} onValueChange={(v) => onTabChange(v as DashboardTab)}>
          <div className="overflow-x-auto overflow-y-hidden pb-1">
            <TabsList variant="line" className="h-10">
              {tabs.map((t) => (
                <TabsTrigger key={t.id} value={t.id} className="px-3 data-active:text-mq-accent after:!bg-mq-accent">
                  {t.label}
                </TabsTrigger>
              ))}
            </TabsList>
          </div>

          <TabsContent value="overview" className="mt-3">
            <div className="grid gap-4 2xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
              <TopologyCard meta={meta} topology={topology} />
              <NodeOperationsCard
                meta={meta}
                provider={provider}
                selection={selection}
                topology={topology}
                ops={ops}
                onConfirm={setConfirm}
                onChanged={refresh}
              />
            </div>
          </TabsContent>
          <TabsContent value="workbench" className="mt-3">
            <MessageWorkbench meta={meta} selection={selection} topology={topology} />
          </TabsContent>
          {meta.slug === 'kafka' && (
            <>
              <TabsContent value="topics" className="mt-3">
                <KafkaTopicsPanel selection={selection} topology={topology} onConfirm={setConfirm} />
              </TabsContent>
              <TabsContent value="groups" className="mt-3">
                <KafkaGroupsPanel selection={selection} topology={topology} />
              </TabsContent>
            </>
          )}
          {meta.slug === 'rabbitmq' && (
            <>
              <TabsContent value="routing" className="mt-3">
                <RabbitRoutingPanel selection={selection} topology={topology} onConfirm={setConfirm} />
              </TabsContent>
              <TabsContent value="ack" className="mt-3">
                <RabbitAckLab selection={selection} topology={topology} />
              </TabsContent>
            </>
          )}
          {tabs.some((t) => t.id === 'stress') && (
            <TabsContent value="stress" className="mt-3">
              <StressPanel meta={meta} selection={selection} />
            </TabsContent>
          )}
          {tabs.some((t) => t.id === 'internals') && (
            <TabsContent value="internals" className="mt-3">
              <InternalsPanel selection={selection} />
            </TabsContent>
          )}
          <TabsContent value="monitoring" className="mt-3">
            <MonitoringPanel meta={meta} selection={selection} topology={topology} metrics={data.metrics} />
          </TabsContent>
          <TabsContent value="logs" className="mt-3">
            <LogsPanel topology={topology} logs={data.logs} live={data.live} onClear={data.clearLogs} />
          </TabsContent>
          {tabs.some((t) => t.id === 'connection') && (
            <TabsContent value="connection" className="mt-3">
              <ConnectionConfigPanel />
            </TabsContent>
          )}
        </Tabs>
      )}

      <ConfirmDialog request={confirm} onClose={() => setConfirm(null)} />
    </>
  )
}

/**
 * 与 URL 查询参数双向同步的本地状态：点击时先更新本地状态（界面即时响应），再用原生
 * history.replaceState 写回 URL（不触发 RSC 请求）；URL 被外部改变（深链接 / 前进后退）时覆盖本地值。
 */
function useUrlParam(key: string, pathname: string, params: Pick<URLSearchParams, 'get'>) {
  const urlValue = params.get(key)
  const [value, setValue] = useState(urlValue)
  const [prevUrlValue, setPrevUrlValue] = useState(urlValue)
  if (urlValue !== prevUrlValue) {
    setPrevUrlValue(urlValue)
    setValue(urlValue)
  }
  function update(next: string) {
    setValue(next)
    const search = new URLSearchParams(window.location.search)
    search.set(key, next)
    window.history.replaceState(null, '', `${pathname}?${search.toString()}`)
  }
  return [value, update] as const
}

function DashboardSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-10 w-full max-w-xl" />
      <div className="grid gap-4 2xl:grid-cols-2">
        <Skeleton className="h-96" />
        <Skeleton className="h-96" />
      </div>
    </div>
  )
}
