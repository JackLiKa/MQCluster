'use client'

/**
 * 站点顶部导航栏。
 *
 * - 导航：首页 / RocketMQ / Kafka / RabbitMQ / 对比 / 学习指南，按当前路径高亮
 * - 每个 MQ 入口带运行状态圆点（运行中节点数 / 总节点数）
 * - 右侧：全局运行节点数、后端连接状态、主题切换
 */

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Boxes, GitCompareArrows, BookOpen } from 'lucide-react'
import { ThemeToggle } from '@/components/theme-toggle'
import { StatusDot } from '@/components/status-dot'
import { useClusterStatus } from '@/components/cluster-status-provider'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { MQ_LIST } from '@/lib/mq-registry'
import { cn } from '@/lib/utils'

function NavLink({
  href,
  active,
  mq,
  children,
}: {
  href: string
  active: boolean
  mq?: string
  children: React.ReactNode
}) {
  return (
    <Link
      href={href}
      data-mq={mq}
      aria-current={active ? 'page' : undefined}
      className={cn(
        'inline-flex h-8 items-center gap-1.5 rounded-md px-2.5 text-sm font-medium transition-colors',
        'text-muted-foreground hover:bg-muted hover:text-foreground',
        active && 'bg-muted text-foreground',
        active && mq && 'bg-mq-accent-soft text-mq-accent hover:bg-mq-accent-soft hover:text-mq-accent',
      )}
    >
      {children}
    </Link>
  )
}

export function SiteHeader() {
  const pathname = usePathname()
  const { summaries, totalRunning, backendDown, loaded } = useClusterStatus()

  return (
    <header className="sticky top-0 z-50 border-b bg-background/90 backdrop-blur supports-[backdrop-filter]:bg-background/70">
      <div className="mx-auto flex h-14 max-w-[1600px] items-center gap-4 px-4 lg:px-6">
        <Link href="/" className="flex shrink-0 items-center gap-2 font-semibold">
          <span className="flex size-7 items-center justify-center rounded-md bg-foreground text-background">
            <Boxes className="size-4" />
          </span>
          <span className="text-base">MQCluster</span>
          <span className="hidden text-xs font-normal text-muted-foreground xl:inline">多 MQ 集群学习平台</span>
        </Link>

        <nav aria-label="主导航" className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto">
          <NavLink href="/" active={pathname === '/'}>
            首页
          </NavLink>
          {MQ_LIST.map((meta) => {
            const s = summaries[meta.slug]
            const Icon = meta.icon
            const status = s.starting > 0 ? 'STARTING' : s.running > 0 ? 'RUNNING' : 'STOPPED'
            return (
              <NavLink key={meta.slug} href={`/mq/${meta.slug}`} active={pathname === `/mq/${meta.slug}`} mq={meta.slug}>
                <Icon className="size-4" />
                {meta.name}
                {s.total > 0 && <StatusDot status={status} className="ml-0.5" />}
              </NavLink>
            )
          })}
          <NavLink href="/compare" active={pathname === '/compare'}>
            <GitCompareArrows className="size-4" />
            对比
          </NavLink>
          <NavLink href="/guide" active={pathname.startsWith('/guide')}>
            <BookOpen className="size-4" />
            学习指南
          </NavLink>
        </nav>

        <div className="flex shrink-0 items-center gap-2">
          <Tooltip>
            <TooltipTrigger
              render={
                <span
                  tabIndex={0}
                  className="hidden items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs text-muted-foreground md:inline-flex"
                />
              }
            >
              <StatusDot status={!loaded ? 'STARTING' : backendDown ? 'FAILED' : totalRunning > 0 ? 'RUNNING' : 'STOPPED'} />
              {!loaded ? '连接中…' : backendDown ? '后端未连接' : `${totalRunning} 个节点运行中`}
            </TooltipTrigger>
            <TooltipContent>
              {backendDown
                ? '无法连接后端（8088 端口），请先启动后端服务'
                : MQ_LIST.map((m) => `${m.name} ${summaries[m.slug].running}/${summaries[m.slug].total}`).join(' · ')}
            </TooltipContent>
          </Tooltip>
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
