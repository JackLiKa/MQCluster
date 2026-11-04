'use client'

/**
 * 集群拓扑图——按 MQ 注册表中的角色分层布局（每种角色一行）。
 *
 * - 节点形状 / 标签来自 mq-registry 的 roles，颜色来自状态 token
 * - 链路：健康为实线，异常为红色虚线
 * - 主题切换时通过 useChartPalette 重新着色
 */

import { useMemo, useRef } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { EChart } from '@/components/echart'
import { useChartPalette } from '@/hooks/use-chart-palette'
import { roleMeta, STATUS_LABELS, type MqMeta } from '@/lib/mq-registry'
import type { ClusterTopology } from '@/types/cluster'

const LINK_LABELS: Record<string, string> = {
  'tap-overlay': '集群链路',
  'kraft-metadata': 'KRaft 元数据',
  'manual-registration': '手动登记',
}

const SYMBOL_SIZE: Record<string, number | [number, number]> = {
  circle: 46,
  diamond: 54,
  roundRect: [64, 40],
  rect: [60, 40],
  triangle: 48,
}

export function TopologyCard({ meta, topology }: { meta: MqMeta; topology: ClusterTopology | null }) {
  const ref = useRef<HTMLDivElement>(null)
  const palette = useChartPalette(ref)

  const option = useMemo(() => {
    const nodes = topology?.nodes ?? []
    const roleOrder = Object.keys(meta.roles)
    const groups = new Map<string, typeof nodes>()
    for (const node of nodes) {
      const role = node.labels?.role ?? 'unknown'
      groups.set(role, [...(groups.get(role) ?? []), node])
    }
    const orderedRoles = [...groups.keys()].sort((a, b) => {
      const ia = roleOrder.indexOf(a)
      const ib = roleOrder.indexOf(b)
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib)
    })

    const data: Record<string, unknown>[] = orderedRoles.flatMap((role, row) => {
      const members = groups.get(role) ?? []
      const rm = roleMeta(meta, role)
      return members.map((node, col) => {
        const color = palette.status[node.status]
        return {
          id: node.nodeId,
          name: node.displayName || node.nodeId,
          x: (col - (members.length - 1) / 2) * 180,
          y: row * 140,
          symbol: rm.symbol,
          symbolSize: SYMBOL_SIZE[rm.symbol] ?? 46,
          itemStyle: {
            color: node.status === 'RUNNING' ? color : palette.card,
            borderColor: color,
            borderWidth: 2.5,
            shadowBlur: node.status === 'RUNNING' ? 12 : 0,
            shadowColor: color,
          },
          label: {
            show: true,
            position: 'bottom',
            color: palette.foreground,
            fontSize: 12,
            formatter: `{b|${node.displayName || node.nodeId}}\n{r|${rm.label} · ${STATUS_LABELS[node.status]}}`,
            rich: {
              b: { fontWeight: 600, color: palette.foreground, lineHeight: 18 },
              r: { color: palette.muted, fontSize: 11 },
            },
          },
          tooltipHtml: [
            `<b>${node.displayName || node.nodeId}</b>`,
            `${rm.label}：${rm.description}`,
            `状态：${STATUS_LABELS[node.status]}`,
            `主机：${node.hostName}${node.labels?.port && node.labels.port !== '0' ? ':' + node.labels.port : ''}`,
            node.virtualIp ? `虚拟 IP：${node.virtualIp}` : '',
          ]
            .filter(Boolean)
            .join('<br/>'),
        }
      })
    })

    // layout:'none' 会把节点包围盒拉伸铺满画布；每行只有 1 个节点时包围盒宽度为 0 会严重变形，
    // 因此放两个不可见锚点，保证包围盒至少为 3 列宽、上下留白。
    const maxCols = Math.max(3, ...orderedRoles.map((role) => groups.get(role)?.length ?? 0))
    const halfWidth = ((maxCols - 1) / 2) * 180
    const lastRowY = Math.max(1, orderedRoles.length - 1) * 140
    const anchor = (id: string, x: number, y: number) => ({
      id,
      name: '',
      x,
      y,
      symbolSize: 0,
      itemStyle: { opacity: 0 },
      label: { show: false },
      tooltip: { show: false },
      emphasis: { disabled: true },
    })
    if (data.length > 0) {
      data.push(anchor('__anchor-tl', -halfWidth, -30), anchor('__anchor-br', halfWidth, lastRowY + 30))
    }

    const rowOf = new Map<string, number>()
    orderedRoles.forEach((role, row) => groups.get(role)?.forEach((n) => rowOf.set(n.nodeId, row)))
    const links = (topology?.links ?? []).map((link) => ({
      source: link.sourceNodeId,
      target: link.targetNodeId,
      lineStyle: {
        color: link.healthy ? palette.muted : palette.status.FAILED,
        type: link.healthy ? 'solid' : 'dashed',
        width: 1.5,
        opacity: link.healthy ? 0.7 : 0.9,
        // 跨行链路加大弯曲，避免穿过中间行的节点
        curveness:
          Math.abs((rowOf.get(link.sourceNodeId) ?? 0) - (rowOf.get(link.targetNodeId) ?? 0)) > 1 ? 0.35 : 0.08,
      },
      tooltipHtml: `${LINK_LABELS[link.linkType] ?? link.linkType}：${link.healthy ? '正常' : '中断'}${
        link.healthy ? `（${link.latencyMs} ms）` : ''
      }`,
    }))

    return {
      backgroundColor: 'transparent',
      animationDurationUpdate: 400,
      tooltip: {
        backgroundColor: palette.card,
        borderColor: palette.border,
        textStyle: { color: palette.foreground, fontSize: 12 },
        formatter: (p: { data?: { tooltipHtml?: string } }) => p.data?.tooltipHtml ?? '',
      },
      series: [
        {
          type: 'graph',
          layout: 'none',
          preserveAspect: 'contain',
          roam: true,
          data,
          links,
          edgeSymbol: ['none', 'arrow'],
          edgeSymbolSize: 7,
          emphasis: { focus: 'adjacency', lineStyle: { width: 3 } },
          left: 60,
          right: 60,
          top: 40,
          bottom: 60,
        },
      ],
    }
  }, [topology, meta, palette])

  const nodes = topology?.nodes ?? []
  const roleRows = new Set(nodes.map((n) => n.labels?.role ?? 'unknown')).size

  return (
    <Card ref={ref}>
      <CardHeader>
        <CardTitle>集群拓扑</CardTitle>
        <CardDescription>
          {nodes.length} 个节点 · 实心表示运行中，空心表示未运行；拖拽 / 滚轮可平移缩放
        </CardDescription>
      </CardHeader>
      <CardContent>
        {nodes.length === 0 ? (
          <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">暂无节点</div>
        ) : (
          <EChart option={option} className={roleRows >= 3 ? 'h-[28rem]' : 'h-80'} ariaLabel={`${meta.name} 集群拓扑图`} />
        )}
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
          {Object.entries(meta.roles).map(([key, role]) => (
            <span key={key} title={role.description}>
              <span className="font-medium text-foreground">{role.label}</span>：{role.description}
            </span>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
