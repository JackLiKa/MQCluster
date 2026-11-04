'use client'

/**
 * 集群数据 hook——三种 MQ 仪表盘共用。
 *
 * 职责：
 * - 首次加载拓扑 / 指标 / 日志（后端未就绪时每 2 秒重试，最多 10 次）
 * - 每 5 秒轮询拓扑和指标
 * - STOMP 实时订阅 /topic/clusters/{clusterId}/metrics 与 /logs
 * - 暴露 refresh()，供节点操作后立即刷新
 *
 * selection 变化时调用方应以 provider 作为 key 重新挂载组件，从而重置状态，
 * 避免在 effect 中同步 setState。
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { Client } from '@stomp/stompjs'
import SockJS from 'sockjs-client'
import { fetchLogs, fetchMetrics, fetchTopology } from '@/lib/api'
import type { ClusterSelection, ClusterTopology, LogEntry, MonitoringSnapshot } from '@/types/cluster'

const MAX_LOGS = 200
const MAX_RETRIES = 10

export interface ClusterData {
  topology: ClusterTopology | null
  metrics: MonitoringSnapshot | null
  logs: LogEntry[]
  /** 首次加载是否完成（成功或放弃重试） */
  loaded: boolean
  /** 首次加载失败的原因 */
  error: string | null
  /** STOMP 是否已连接 */
  live: boolean
  refresh: () => Promise<void>
  clearLogs: () => void
}

const logKey = (l: LogEntry) => `${l.timestamp}|${l.nodeId}|${l.message}`

/** 后端每次推送最近 N 条日志（与已有日志重叠），按 时间+节点+内容 去重后按时间倒序合并 */
function mergeLogs(incoming: LogEntry[], prev: LogEntry[]): LogEntry[] {
  const seen = new Set(prev.map(logKey))
  const fresh = incoming.filter((l) => !seen.has(logKey(l)))
  if (fresh.length === 0) return prev
  return [...fresh, ...prev]
    .sort((a, b) => (a.timestamp < b.timestamp ? 1 : a.timestamp > b.timestamp ? -1 : 0))
    .slice(0, MAX_LOGS)
}

export function useClusterData(selection: ClusterSelection): ClusterData {
  const [topology, setTopology] = useState<ClusterTopology | null>(null)
  const [metrics, setMetrics] = useState<MonitoringSnapshot | null>(null)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [live, setLive] = useState(false)
  const selRef = useRef(selection)

  useEffect(() => {
    selRef.current = selection
  })

  const refresh = useCallback(async () => {
    try {
      const [topo, m] = await Promise.all([fetchTopology(selRef.current), fetchMetrics(selRef.current)])
      setTopology(topo)
      setMetrics(m)
    } catch {
      // 轮询错误忽略，保留上一次数据
    }
  }, [])

  const clearLogs = useCallback(() => setLogs([]), [])

  const { clusterId, mode, middleware } = selection

  // 首次加载（带重试）
  useEffect(() => {
    const sel = { clusterId, mode, middleware }
    let cancelled = false
    let retries = 0
    let timer: ReturnType<typeof setTimeout> | undefined

    async function init() {
      try {
        const [topo, m, l] = await Promise.all([fetchTopology(sel), fetchMetrics(sel), fetchLogs(sel, undefined, 100)])
        if (cancelled) return
        setTopology(topo)
        setMetrics(m)
        setLogs(l)
        setError(null)
        setLoaded(true)
      } catch (err) {
        if (cancelled) return
        retries++
        if (retries < MAX_RETRIES) {
          timer = setTimeout(() => void init(), 2000)
        } else {
          setError(err instanceof Error ? err.message : '加载失败')
          setLoaded(true)
        }
      }
    }

    void init()
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [clusterId, mode, middleware])

  // 定时轮询
  useEffect(() => {
    const timer = setInterval(() => void refresh(), 5000)
    return () => clearInterval(timer)
  }, [refresh, clusterId, mode, middleware])

  // STOMP 实时流
  useEffect(() => {
    const client = new Client({
      // SockJS 工厂——后端 /ws 端点同时支持原生 WebSocket 和 SockJS 回退
      webSocketFactory: () => new SockJS('/ws'),
      heartbeatIncoming: 10000,
      heartbeatOutgoing: 10000,
      reconnectDelay: 5000,
      onConnect: () => {
        setLive(true)
        client.subscribe(`/topic/clusters/${clusterId}/metrics`, (message) => {
          try {
            setMetrics(JSON.parse(message.body) as MonitoringSnapshot)
          } catch {
            // 忽略解析错误
          }
        })
        client.subscribe(`/topic/clusters/${clusterId}/logs`, (message) => {
          try {
            const incoming = JSON.parse(message.body) as LogEntry[]
            if (incoming.length > 0) setLogs((prev) => mergeLogs(incoming, prev))
          } catch {
            // 忽略解析错误
          }
        })
      },
      onWebSocketClose: () => setLive(false),
      // 静默处理错误，避免控制台刷屏
      onStompError: () => {},
      onWebSocketError: () => {},
    })
    client.activate()
    return () => {
      void client.deactivate()
    }
  }, [clusterId])

  return { topology, metrics, logs, loaded, error, live, refresh, clearLogs }
}
