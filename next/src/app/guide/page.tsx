/**
 * /guide——学习指南索引：三种 MQ 的分步教程入口与推荐学习路径。
 */

import type { Metadata } from 'next'
import Link from 'next/link'
import { ArrowRight } from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { GUIDES } from '@/lib/guides'
import { MQ_LIST } from '@/lib/mq-registry'

export const metadata: Metadata = {
  title: '学习指南',
  description: 'RocketMQ、Kafka、RabbitMQ 的分步教程，每一步都能直接跳到仪表盘动手实验。',
}

export default function GuideIndexPage() {
  return (
    <div className="mx-auto w-full max-w-5xl space-y-8 px-4 py-8 lg:px-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">学习指南</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          每份教程都由「概念 → 动手练习 → 在仪表盘中试试」组成。建议按 RocketMQ → Kafka → RabbitMQ 的顺序学习，最后到
          <Link href="/compare" className="mx-1 underline">
            对比页
          </Link>
          总结三者的差异。
        </p>
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        {MQ_LIST.map((meta, i) => {
          const guide = GUIDES[meta.slug]
          const Icon = meta.icon
          return (
            <Link key={meta.slug} href={`/guide/${meta.slug}`} data-mq={meta.slug} className="group">
              <Card className="h-full transition-shadow group-hover:shadow-md group-hover:ring-mq-accent/50">
                <CardHeader>
                  <div className="flex items-center gap-3">
                    <span className="flex size-10 items-center justify-center rounded-xl bg-mq-accent text-mq-accent-foreground">
                      <Icon className="size-5" />
                    </span>
                    <div>
                      <div className="text-xs text-muted-foreground">第 {i + 1} 站</div>
                      <CardTitle>{guide.title}</CardTitle>
                    </div>
                  </div>
                  <CardDescription className="pt-2">{guide.intro}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <ol className="space-y-1 text-sm text-muted-foreground">
                    {guide.steps.map((s, idx) => (
                      <li key={s.title} className="flex gap-2">
                        <span className="w-4 shrink-0 text-right tabular-nums text-mq-accent">{idx + 1}</span>
                        {s.title}
                      </li>
                    ))}
                  </ol>
                  <span className="inline-flex items-center gap-1 text-sm font-medium text-mq-accent">
                    开始学习 <ArrowRight className="size-4 transition-transform group-hover:translate-x-0.5" />
                  </span>
                </CardContent>
              </Card>
            </Link>
          )
        })}
      </div>
    </div>
  )
}
