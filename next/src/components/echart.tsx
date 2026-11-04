'use client'

/**
 * 通用 ECharts 容器——负责 init / setOption / resize / dispose。
 *
 * - option 变化时 setOption（默认 notMerge，避免旧 series 残留）
 * - ResizeObserver 跟随容器尺寸
 * - onEvents 绑定点击等事件（用 ref 保存最新回调，避免重复绑定）
 */

import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'
import { cn } from '@/lib/utils'

type EChartsOption = echarts.EChartsCoreOption
type Handler = (params: unknown) => void

interface EChartProps {
  option: EChartsOption
  className?: string
  notMerge?: boolean
  onEvents?: Record<string, Handler>
  ariaLabel?: string
}

export function EChart({ option, className, notMerge = true, onEvents, ariaLabel }: EChartProps) {
  const elRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<echarts.ECharts | null>(null)
  const eventsRef = useRef(onEvents)

  useEffect(() => {
    eventsRef.current = onEvents
  })

  useEffect(() => {
    const el = elRef.current
    if (!el) return
    const chart = echarts.init(el, undefined, { renderer: 'canvas' })
    chartRef.current = chart
    const names = ['click', 'mouseover', 'mouseout']
    for (const name of names) {
      chart.on(name, (params) => eventsRef.current?.[name]?.(params))
    }
    const observer = new ResizeObserver(() => chart.resize())
    observer.observe(el)
    return () => {
      observer.disconnect()
      chart.dispose()
      chartRef.current = null
    }
  }, [])

  useEffect(() => {
    chartRef.current?.setOption(option, { notMerge })
  }, [option, notMerge])

  return <div ref={elRef} role="img" aria-label={ariaLabel} className={cn('h-80 w-full', className)} />
}
