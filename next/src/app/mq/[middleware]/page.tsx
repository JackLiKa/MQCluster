/**
 * /mq/[middleware]——单个 MQ 的实验仪表盘。
 *
 * 服务端组件：校验路由参数（rocketmq / kafka / rabbitmq），不合法返回 404；
 * 页面主体是客户端组件 MqDashboard（使用 useSearchParams，需包在 Suspense 中）。
 */

import type { Metadata } from 'next'
import { notFound } from 'next/navigation'
import { Suspense } from 'react'
import { MqDashboard } from '@/components/dashboard/mq-dashboard'
import { isMqSlug, MQ_LIST, MQ_REGISTRY } from '@/lib/mq-registry'

export function generateStaticParams() {
  return MQ_LIST.map((m) => ({ middleware: m.slug }))
}

export async function generateMetadata({ params }: { params: Promise<{ middleware: string }> }): Promise<Metadata> {
  const { middleware } = await params
  if (!isMqSlug(middleware)) return { title: '未找到' }
  const meta = MQ_REGISTRY[middleware]
  return { title: `${meta.name} 实验室`, description: meta.tagline }
}

export default async function MqPage({ params }: { params: Promise<{ middleware: string }> }) {
  const { middleware } = await params
  if (!isMqSlug(middleware)) notFound()
  return (
    <div className="mx-auto w-full max-w-[1600px] px-4 py-6 lg:px-6">
      <Suspense>
        <MqDashboard slug={middleware} />
      </Suspense>
    </div>
  )
}
