/**
 * /guide/[middleware]——单个 MQ 的分步学习教程。
 *
 * 服务端组件：内容来自 lib/guides.ts；每一步的「在仪表盘中试试」深链接到 /mq/{slug}?tab=...。
 */

import type { Metadata } from 'next'
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { ArrowRight, ExternalLink, FlaskConical, Lightbulb } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { buttonVariants } from '@/components/ui/button'
import { GUIDES, type GuideStep } from '@/lib/guides'
import { isMqSlug, MQ_LIST, MQ_REGISTRY, type MqSlug } from '@/lib/mq-registry'
import { cn } from '@/lib/utils'

export function generateStaticParams() {
  return MQ_LIST.map((m) => ({ middleware: m.slug }))
}

export async function generateMetadata({ params }: { params: Promise<{ middleware: string }> }): Promise<Metadata> {
  const { middleware } = await params
  if (!isMqSlug(middleware)) return { title: '未找到' }
  return { title: GUIDES[middleware].title, description: GUIDES[middleware].intro }
}

export default async function GuideDetailPage({ params }: { params: Promise<{ middleware: string }> }) {
  const { middleware } = await params
  if (!isMqSlug(middleware)) notFound()
  const guide = GUIDES[middleware]
  const meta = MQ_REGISTRY[middleware]
  const Icon = meta.icon

  return (
    <div data-mq={middleware} className="mx-auto w-full max-w-5xl space-y-8 px-4 py-8 lg:px-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="flex size-11 items-center justify-center rounded-xl bg-mq-accent text-mq-accent-foreground">
            <Icon className="size-6" />
          </div>
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{guide.title}</h1>
            <p className="max-w-2xl text-sm text-muted-foreground">{guide.intro}</p>
          </div>
        </div>
        <Link href={`/mq/${middleware}`} className={buttonVariants({ variant: 'default' })}>
          进入 {meta.name} 实验室 <ArrowRight className="size-4" />
        </Link>
      </div>

      <nav aria-label="切换指南" className="flex flex-wrap gap-2">
        {MQ_LIST.map((m) => (
          <Link
            key={m.slug}
            href={`/guide/${m.slug}`}
            data-mq={m.slug}
            aria-current={m.slug === middleware ? 'page' : undefined}
            className={cn(
              'rounded-full border px-3 py-1 text-sm transition-colors',
              m.slug === middleware ? 'border-mq-accent bg-mq-accent-soft text-mq-accent' : 'text-muted-foreground hover:bg-muted',
            )}
          >
            {m.name}
          </Link>
        ))}
      </nav>

      <ol className="space-y-4">
        {guide.steps.map((step, i) => (
          <li key={step.title}>
            <StepCard step={step} index={i + 1} slug={middleware} />
          </li>
        ))}
      </ol>

      <section className="space-y-4">
        <div className="border-b pb-2">
          <h2 className="text-xl font-semibold">进阶阅读</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            以下概念超出平台当前的实验范围，但对生产环境使用很重要。建议掌握上面的实验后阅读，并查阅
            <a href={guide.docsUrl} target="_blank" rel="noopener noreferrer" className="mx-1 inline-flex items-center gap-0.5 underline">
              {meta.name} 官方文档 <ExternalLink className="size-3" />
            </a>
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          {guide.advanced.map((step) => (
            <Card key={step.title} size="sm">
              <CardHeader>
                <CardTitle className="text-base">{step.title}</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm leading-relaxed text-muted-foreground">
                {step.body.map((p) => (
                  <p key={p}>{p}</p>
                ))}
                {step.code && <CodeBlock code={step.code} />}
              </CardContent>
            </Card>
          ))}
        </div>
      </section>
    </div>
  )
}

function StepCard({ step, index, slug }: { step: GuideStep; index: number; slug: MqSlug }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-mq-accent text-sm text-mq-accent-foreground">
            {index}
          </span>
          {step.title}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-sm leading-relaxed">
        {step.body.map((p) => (
          <p key={p}>{p}</p>
        ))}
        {step.points && (
          <dl className="grid gap-2 sm:grid-cols-[minmax(0,180px)_1fr]">
            {step.points.map((pt) => (
              <div key={pt.term} className="contents">
                <dt className="font-medium">{pt.term}</dt>
                <dd className="text-muted-foreground">{pt.desc}</dd>
              </div>
            ))}
          </dl>
        )}
        {step.code && <CodeBlock code={step.code} />}
        {step.note && (
          <div className="flex gap-2 rounded-lg bg-muted/60 p-3 text-xs leading-relaxed">
            <Lightbulb className="size-4 shrink-0 text-mq-accent" />
            <span>{step.note}</span>
          </div>
        )}
        {step.tasks && (
          <div className="space-y-1.5">
            <div className="flex items-center gap-1.5 font-medium">
              <FlaskConical className="size-4 text-mq-accent" /> 动手练习
            </div>
            <ol className="list-decimal space-y-1 pl-6 text-muted-foreground">
              {step.tasks.map((t) => (
                <li key={t}>{t}</li>
              ))}
            </ol>
          </div>
        )}
        {step.tryIt && (
          <Link
            href={`/mq/${slug}?tab=${step.tryIt.tab}`}
            className={cn(buttonVariants({ variant: 'outline', size: 'sm' }), 'border-mq-accent/40 text-mq-accent')}
          >
            在仪表盘中试试：{step.tryIt.label} <ArrowRight className="size-3.5" />
          </Link>
        )}
      </CardContent>
    </Card>
  )
}

function CodeBlock({ code }: { code: string }) {
  return (
    <pre className="overflow-x-auto rounded-lg bg-muted p-3 font-mono text-xs leading-relaxed text-foreground">
      <code>{code}</code>
    </pre>
  )
}
