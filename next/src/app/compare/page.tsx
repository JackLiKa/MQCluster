/**
 * /compare——RocketMQ / Kafka / RabbitMQ 静态对比表。
 *
 * 维度：存储模型、路由模型、顺序性、消费模型、高可用、典型场景；
 * 每个单元格附「去实验」链接，跳到对应 MQ 仪表盘的标签页。
 */

import type { Metadata } from 'next'
import Link from 'next/link'
import { ArrowUpRight } from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { MQ_LIST, type DashboardTab, type MqSlug } from '@/lib/mq-registry'

export const metadata: Metadata = {
  title: '三种 MQ 对比',
  description: '从存储、路由、顺序性、消费模型、高可用与典型场景对比 RocketMQ、Kafka 与 RabbitMQ。',
}

interface Cell {
  text: string
  lab?: { tab: DashboardTab; label: string }
}

interface Row {
  dimension: string
  question: string
  cells: Record<MqSlug, Cell>
}

const ROWS: Row[] = [
  {
    dimension: '存储模型',
    question: '消息存在哪里、消费后是否删除？',
    cells: {
      rocketmq: {
        text: '所有 Topic 共用顺序写的 CommitLog，按 ConsumeQueue 索引读取；消费后不删除，按保留时间清理。',
        lab: { tab: 'workbench', label: '发送并消费' },
      },
      kafka: {
        text: '每个分区一份追加日志（按段存储），消费只移动 offset，消息按保留策略删除或压缩，可任意回放。',
        lab: { tab: 'topics', label: '查看分区' },
      },
      rabbitmq: {
        text: '消息存在队列中，被确认（ack）后即从队列删除；默认不支持回放（Streams 例外）。',
        lab: { tab: 'ack', label: '观察 ack 删除' },
      },
    },
  },
  {
    dimension: '路由模型',
    question: '生产者如何决定消息去向？',
    cells: {
      rocketmq: {
        text: '客户端从 NameServer 拉取路由，按 Topic → MessageQueue 选择 Broker；消费端可用 Tag / SQL92 过滤。',
        lab: { tab: 'overview', label: '查看 NameServer 路由' },
      },
      kafka: {
        text: '按 Topic → 分区：指定分区、按 key 哈希，或粘性轮询。没有服务端路由规则。',
        lab: { tab: 'workbench', label: '用 key 选择分区' },
      },
      rabbitmq: {
        text: '发给交换机，由 direct / fanout / topic / headers 交换机按绑定规则路由到零或多个队列，最灵活。',
        lab: { tab: 'routing', label: '路由试算' },
      },
    },
  },
  {
    dimension: '顺序性',
    question: '能保证什么粒度的有序？',
    cells: {
      rocketmq: {
        text: '同一 MessageQueue 内有序；顺序消费需 MessageListenerOrderly 并按业务键选择队列。',
      },
      kafka: {
        text: '分区内严格有序；相同 key 进入同一分区即可保证该 key 有序。',
        lab: { tab: 'workbench', label: '验证同 key 同分区' },
      },
      rabbitmq: {
        text: '单队列单消费者时有序；nack 重新入队、多个消费者或 prefetch > 1 时可能乱序。',
        lab: { tab: 'ack', label: '观察重新入队' },
      },
    },
  },
  {
    dimension: '消费模型',
    question: '消费者如何拿到消息、如何记录进度？',
    cells: {
      rocketmq: {
        text: '消费者组集群消费或广播消费；长轮询 Push，Broker 记录各组消费进度；失败自动延迟重试。',
        lab: { tab: 'workbench', label: '消费者组投递' },
      },
      kafka: {
        text: '消费者组 Pull 消费，每个分区只分给组内一个成员；成员变化触发 rebalance；提交 offset，lag 衡量积压。',
        lab: { tab: 'groups', label: '体验 rebalance' },
      },
      rabbitmq: {
        text: 'Broker 推送（basic.consume），prefetch 限制未确认数；逐条 ack / nack / requeue，失败可进死信。',
        lab: { tab: 'ack', label: 'prefetch 实验' },
      },
    },
  },
  {
    dimension: '高可用',
    question: '节点故障时会发生什么？',
    cells: {
      rocketmq: {
        text: 'Master / Slave 同步或异步复制；5.x 可用 DLedger / Controller 自动选主。',
        lab: { tab: 'overview', label: '停止 Master 观察' },
      },
      kafka: {
        text: '分区多副本 + ISR；leader 故障后 KRaft controller 从 ISR 选新 leader，几秒内恢复读写。',
        lab: { tab: 'topics', label: '观察 leader 切换' },
      },
      rabbitmq: {
        text: '集群 + Quorum Queue（Raft）复制队列；本平台为单节点嵌入，聚焦路由与确认语义。',
        lab: { tab: 'overview', label: '查看 broker' },
      },
    },
  },
  {
    dimension: '典型场景',
    question: '什么时候选它？',
    cells: {
      rocketmq: { text: '电商交易、金融支付：事务消息、延时消息、严格可靠投递与消息轨迹。' },
      kafka: { text: '日志采集、埋点、流处理、事件溯源：超高吞吐、可回放、与流计算生态集成。' },
      rabbitmq: { text: '业务异步解耦、任务队列、复杂路由与 RPC：灵活路由、逐条确认、低延迟。' },
    },
  },
]

export default function ComparePage() {
  return (
    <div className="mx-auto w-full max-w-[1600px] space-y-6 px-4 py-8 lg:px-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">三种 MQ 对比</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          同样是「消息队列」，三者的设计取舍差异很大。点击单元格中的链接，直接到对应实验中验证。
        </p>
      </div>

      <Card>
        <CardContent className="overflow-x-auto p-0">
          <table className="w-full min-w-[960px] border-collapse text-sm">
            <thead>
              <tr className="border-b">
                <th scope="col" className="w-44 p-4 text-left font-medium text-muted-foreground">
                  维度
                </th>
                {MQ_LIST.map((meta) => {
                  const Icon = meta.icon
                  return (
                    <th key={meta.slug} scope="col" data-mq={meta.slug} className="p-4 text-left">
                      <Link href={`/mq/${meta.slug}`} className="inline-flex items-center gap-2 font-semibold hover:underline">
                        <span className="flex size-7 items-center justify-center rounded-lg bg-mq-accent text-mq-accent-foreground">
                          <Icon className="size-4" />
                        </span>
                        {meta.name}
                      </Link>
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody>
              {ROWS.map((row) => (
                <tr key={row.dimension} className="border-b align-top last:border-0">
                  <th scope="row" className="p-4 text-left">
                    <div className="font-medium">{row.dimension}</div>
                    <div className="mt-0.5 text-xs font-normal text-muted-foreground">{row.question}</div>
                  </th>
                  {MQ_LIST.map((meta) => {
                    const cell = row.cells[meta.slug]
                    return (
                      <td key={meta.slug} data-mq={meta.slug} className="p-4 leading-relaxed">
                        <p>{cell.text}</p>
                        {cell.lab && (
                          <Link
                            href={`/mq/${meta.slug}?tab=${cell.lab.tab}`}
                            className="mt-2 inline-flex items-center gap-0.5 text-xs font-medium text-mq-accent hover:underline"
                          >
                            去实验：{cell.lab.label} <ArrowUpRight className="size-3" />
                          </Link>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-3">
        {MQ_LIST.map((meta) => (
          <Card key={meta.slug} data-mq={meta.slug} size="sm">
            <CardHeader>
              <CardTitle className="text-base">一句话记住 {meta.name}</CardTitle>
              <CardDescription>{meta.tagline}</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-1.5">
              {meta.highlights.map((h) => (
                <span key={h} className="rounded-full bg-mq-accent-soft px-2.5 py-0.5 text-xs text-mq-accent">
                  {h}
                </span>
              ))}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
