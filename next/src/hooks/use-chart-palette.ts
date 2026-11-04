'use client'

/**
 * 图表调色板 hook——从 CSS 变量读取颜色，供 ECharts 使用。
 *
 * ECharts（zrender）无法可靠解析 oklch()，因此用 1x1 canvas 把 CSS 颜色转成 rgba 字符串。
 * 依赖 next-themes 的 resolvedTheme：主题切换后重新读取，图表随之重新着色。
 * 传入容器 ref 时从该元素读取（可拿到 [data-mq] 下的强调色），否则读 <html>。
 */

import { useEffect, useState, type RefObject } from 'react'
import { useTheme } from 'next-themes'

export interface ChartPalette {
  accent: string
  accentSoft: string
  foreground: string
  muted: string
  border: string
  card: string
  series: string[]
  status: {
    RUNNING: string
    STOPPED: string
    FAILED: string
    DEGRADED: string
    STARTING: string
  }
}

const FALLBACK: ChartPalette = {
  accent: '#4f46e5',
  accentSoft: '#eef2ff',
  foreground: '#171717',
  muted: '#737373',
  border: '#e5e5e5',
  card: '#ffffff',
  series: ['#3b82f6', '#10b981', '#f59e0b', '#ec4899', '#8b5cf6', '#ef4444'],
  status: { RUNNING: '#16a34a', STOPPED: '#6b7280', FAILED: '#dc2626', DEGRADED: '#d97706', STARTING: '#2563eb' },
}

let ctx: CanvasRenderingContext2D | null = null

/** 把任意 CSS 颜色（含 oklch）转为 rgba() 字符串 */
function toRgb(color: string, fallback: string): string {
  const value = color.trim()
  if (!value) return fallback
  if (!ctx) {
    const canvas = document.createElement('canvas')
    canvas.width = canvas.height = 1
    ctx = canvas.getContext('2d', { willReadFrequently: true })
  }
  if (!ctx) return fallback
  ctx.clearRect(0, 0, 1, 1)
  ctx.fillStyle = '#000'
  ctx.fillStyle = value
  ctx.fillRect(0, 0, 1, 1)
  const [r, g, b, a] = ctx.getImageData(0, 0, 1, 1).data
  return `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(3)})`
}

function readPalette(el: Element): ChartPalette {
  const style = getComputedStyle(el)
  const v = (name: string, fb: string) => toRgb(style.getPropertyValue(name), fb)
  return {
    accent: v('--mq-accent', FALLBACK.accent),
    accentSoft: v('--mq-accent-soft', FALLBACK.accentSoft),
    foreground: v('--foreground', FALLBACK.foreground),
    muted: v('--muted-foreground', FALLBACK.muted),
    border: v('--border', FALLBACK.border),
    card: v('--card', FALLBACK.card),
    series: FALLBACK.series.map((fb, i) => v(`--chart-${i + 1}`, fb)),
    status: {
      RUNNING: v('--status-running', FALLBACK.status.RUNNING),
      STOPPED: v('--status-stopped', FALLBACK.status.STOPPED),
      FAILED: v('--status-failed', FALLBACK.status.FAILED),
      DEGRADED: v('--status-degraded', FALLBACK.status.DEGRADED),
      STARTING: v('--status-starting', FALLBACK.status.STARTING),
    },
  }
}

export function useChartPalette(ref?: RefObject<Element | null>): ChartPalette {
  const { resolvedTheme } = useTheme()
  const [palette, setPalette] = useState<ChartPalette>(FALLBACK)

  useEffect(() => {
    // 等待 next-themes 把 .dark 类写入 DOM 后再读取
    const id = requestAnimationFrame(() => {
      setPalette(readPalette(ref?.current ?? document.documentElement))
    })
    return () => cancelAnimationFrame(id)
  }, [resolvedTheme, ref])

  return palette
}
