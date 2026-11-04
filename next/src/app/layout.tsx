import type { Metadata } from 'next'
import { Geist, Geist_Mono } from 'next/font/google'
import './globals.css'
import { SiteHeader } from '@/components/site-header'
import { ThemeProvider } from '@/components/theme-provider'
import { ClusterStatusProvider } from '@/components/cluster-status-provider'
import { TooltipProvider } from '@/components/ui/tooltip'
import { Toaster } from '@/components/ui/sonner'

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
})

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
})

export const metadata: Metadata = {
  title: {
    default: 'MQCluster — 多 MQ 集群学习平台',
    template: '%s · MQCluster',
  },
  description: '本地优先的消息队列学习平台：在一台机器上运行真实的 RocketMQ、Kafka（KRaft）与 RabbitMQ（AMQP）集群，动手体验路由、副本、消费者组与确认机制。',
}

/**
 * 根布局——所有页面共享的 HTML 外壳。
 *
 * 包含：
 * - 字体加载（Geist Sans + Geist Mono）
 * - 主题（next-themes，html 上切换 .dark）、Tooltip 与 Toast 容器
 * - 全局集群状态（首页卡片与顶栏共用）
 * - 站点顶部导航栏（SiteHeader）
 */
export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html
      lang="zh-CN"
      suppressHydrationWarning
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col">
        <ThemeProvider>
          <TooltipProvider delay={200}>
            <ClusterStatusProvider>
              <SiteHeader />
              <main className="flex-1">{children}</main>
              <Toaster richColors closeButton position="top-right" />
            </ClusterStatusProvider>
          </TooltipProvider>
        </ThemeProvider>
      </body>
    </html>
  )
}
