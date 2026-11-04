'use client'

/**
 * 全局集群状态 Context——首页卡片与顶栏共用。
 *
 * 每 10 秒轮询一次：
 * - provider 列表（后端是否可达、各 MQ 的 clusterId / capabilities）
 * - 各 PSEUDO 集群拓扑 → 运行中节点数
 * - JVM 堆信息（来自限流接口的 systemProfile），用于资源占用提示
 * 后端不可达时 backendDown=true，页面据此显示重试提示。
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { fetchProviders, fetchRateLimit, fetchTopology } from '@/lib/api'
import { MQ_LIST, slugOf, type MqSlug } from '@/lib/mq-registry'
import type { ClusterSelection, NodeStatus, ProviderDescriptor, SystemProfile } from '@/types/cluster'

export interface MqStatusSummary {
  /** 默认 provider（优先 PSEUDO） */
  provider: ProviderDescriptor | null
  running: number
  starting: number
  total: number
  statuses: NodeStatus[]
}

interface ClusterStatusValue {
  providers: ProviderDescriptor[]
  loaded: boolean
  backendDown: boolean
  summaries: Record<MqSlug, MqStatusSummary>
  systemProfile: SystemProfile | null
  totalRunning: number
  refresh: () => void
}

const EMPTY_SUMMARY: MqStatusSummary = { provider: null, running: 0, starting: 0, total: 0, statuses: [] }

const ClusterStatusContext = createContext<ClusterStatusValue | null>(null)

export function selectionOf(provider: ProviderDescriptor): ClusterSelection {
  return { clusterId: provider.clusterId, mode: provider.mode, middleware: provider.middleware }
}

export function ClusterStatusProvider({ children }: { children: React.ReactNode }) {
  const [providers, setProviders] = useState<ProviderDescriptor[]>([])
  const [loaded, setLoaded] = useState(false)
  const [backendDown, setBackendDown] = useState(false)
  const [summaries, setSummaries] = useState<Record<MqSlug, MqStatusSummary>>({
    rocketmq: EMPTY_SUMMARY,
    kafka: EMPTY_SUMMARY,
    rabbitmq: EMPTY_SUMMARY,
  })
  const [systemProfile, setSystemProfile] = useState<SystemProfile | null>(null)
  const [tick, setTick] = useState(0)

  const refresh = useCallback(() => setTick((t) => t + 1), [])

  useEffect(() => {
    let cancelled = false

    async function load() {
      try {
        const list = await fetchProviders()
        const next: Record<MqSlug, MqStatusSummary> = {
          rocketmq: EMPTY_SUMMARY,
          kafka: EMPTY_SUMMARY,
          rabbitmq: EMPTY_SUMMARY,
        }
        await Promise.all(
          MQ_LIST.map(async (meta) => {
            const candidates = list.filter((p) => p.middleware === meta.middleware)
            const provider = candidates.find((p) => p.mode === 'PSEUDO') ?? candidates[0] ?? null
            if (!provider) return
            if (provider.mode !== 'PSEUDO') {
              next[meta.slug] = { ...EMPTY_SUMMARY, provider }
              return
            }
            try {
              const topo = await fetchTopology(selectionOf(provider))
              const statuses = topo.nodes.map((n) => n.status)
              next[slugOf(provider.middleware)] = {
                provider,
                running: statuses.filter((s) => s === 'RUNNING').length,
                starting: statuses.filter((s) => s === 'STARTING').length,
                total: statuses.length,
                statuses,
              }
            } catch {
              next[meta.slug] = { ...EMPTY_SUMMARY, provider }
            }
          }),
        )
        const rate = await fetchRateLimit().catch(() => null)
        if (cancelled) return
        setProviders(list)
        setSummaries(next)
        setSystemProfile(rate?.systemProfile ?? null)
        setBackendDown(false)
        setLoaded(true)
      } catch {
        if (cancelled) return
        setBackendDown(true)
        setLoaded(true)
      }
    }

    void load()
    const timer = setInterval(() => void load(), 10_000)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [tick])

  const value = useMemo<ClusterStatusValue>(() => {
    const totalRunning = Object.values(summaries).reduce((s, x) => s + x.running, 0)
    return { providers, loaded, backendDown, summaries, systemProfile, totalRunning, refresh }
  }, [providers, loaded, backendDown, summaries, systemProfile, refresh])

  return <ClusterStatusContext.Provider value={value}>{children}</ClusterStatusContext.Provider>
}

export function useClusterStatus(): ClusterStatusValue {
  const ctx = useContext(ClusterStatusContext)
  if (!ctx) throw new Error('useClusterStatus 必须在 ClusterStatusProvider 内使用')
  return ctx
}
