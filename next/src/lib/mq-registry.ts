/**
 * MQ 元数据注册表——三种中间件在前端的唯一配置来源。
 *
 * 集中定义：名称、图标、强调色 key（对应 globals.css 的 [data-mq]）、
 * 拓扑中各角色的形状 / 标签、可用的标签页（结合后端 capabilities 过滤）、学习要点。
 * 拓扑、监控等通用组件只读这里，不再写死 RocketMQ 的角色。
 */

import type { LucideIcon } from 'lucide-react'
import { Rabbit, Rocket, Workflow } from 'lucide-react'
import type { MiddlewareType, ProviderDescriptor } from '@/types/cluster'

/** URL 中使用的 MQ 标识（/mq/[middleware]） */
export type MqSlug = 'rocketmq' | 'kafka' | 'rabbitmq'

/** 仪表盘标签页 ID（同时用作 ?tab= 参数值） */
export type DashboardTab =
  | 'overview'
  | 'workbench'
  | 'topics'
  | 'groups'
  | 'routing'
  | 'ack'
  | 'stress'
  | 'internals'
  | 'monitoring'
  | 'logs'
  | 'connection'

/** 拓扑角色展示配置 */
export interface RoleMeta {
  /** 中文 / 术语标签 */
  label: string
  /** ECharts 节点形状 */
  symbol: 'circle' | 'roundRect' | 'diamond' | 'rect' | 'triangle'
  /** 一句话说明，用于 tooltip */
  description: string
}

export interface TabMeta {
  id: DashboardTab
  label: string
  /** 需要后端 provider 具备的能力，缺失时不显示该标签页 */
  requires?: string
}

export interface MqMeta {
  slug: MqSlug
  middleware: MiddlewareType
  name: string
  tagline: string
  icon: LucideIcon
  roles: Record<string, RoleMeta>
  tabs: TabMeta[]
  /** 首页卡片上的核心概念 */
  highlights: string[]
  /** 默认 topic / 消费组（消息工作台初值） */
  defaultTopic: string
  defaultGroup: string
}

const COMMON_HEAD: TabMeta[] = [
  { id: 'overview', label: '概览' },
  { id: 'workbench', label: '消息工作台' },
]
/** 实验室标签页——压测制造流量，内部机制看 broker 底层反应（顺序即使用动线） */
const LAB_TABS: TabMeta[] = [
  { id: 'stress', label: '压力实验', requires: 'LAB' },
  { id: 'internals', label: '内部机制', requires: 'LAB' },
]
const COMMON_TAIL: TabMeta[] = [
  { id: 'monitoring', label: '监控' },
  { id: 'logs', label: '日志' },
]

export const MQ_REGISTRY: Record<MqSlug, MqMeta> = {
  rocketmq: {
    slug: 'rocketmq',
    middleware: 'ROCKETMQ',
    name: 'RocketMQ',
    tagline: 'NameServer 路由发现 + Broker 主从复制，金融级可靠投递',
    icon: Rocket,
    roles: {
      nameserver: { label: 'NameServer', symbol: 'circle', description: '轻量路由注册中心，Broker 向其注册，客户端从其拉取路由' },
      'broker-master': { label: 'Broker Master', symbol: 'roundRect', description: '主 Broker，负责消息写入与读取' },
      'broker-slave': { label: 'Broker Slave', symbol: 'diamond', description: '从 Broker，同步主节点数据，提供读高可用' },
    },
    tabs: [...COMMON_HEAD, ...LAB_TABS, ...COMMON_TAIL, { id: 'connection', label: '连接配置', requires: 'CONNECTION_CONFIG' }],
    highlights: ['NameServer 无状态路由', '主从同步 / 异步复制', '消费组集群消费'],
    defaultTopic: 'TestTopic',
    defaultGroup: 'test-consumer-group',
  },
  kafka: {
    slug: 'kafka',
    middleware: 'KAFKA',
    name: 'Kafka',
    tagline: 'KRaft 元数据仲裁 + 分区多副本日志，高吞吐流式平台',
    icon: Workflow,
    roles: {
      'kafka-controller': { label: 'KRaft Controller', symbol: 'diamond', description: '元数据仲裁节点，负责选举分区 leader、维护集群元数据' },
      'kafka-broker': { label: 'Broker', symbol: 'roundRect', description: '存储分区副本，leader 副本负责读写，follower 副本同步' },
    },
    tabs: [
      ...COMMON_HEAD,
      { id: 'topics', label: 'Topic 与分区', requires: 'KAFKA_INSPECTION' },
      { id: 'groups', label: '消费者组', requires: 'KAFKA_INSPECTION' },
      ...LAB_TABS,
      ...COMMON_TAIL,
    ],
    highlights: ['分区 leader / ISR', '消费者组 rebalance', 'offset 与 lag'],
    defaultTopic: 'lab-orders',
    defaultGroup: 'lab-workbench',
  },
  rabbitmq: {
    slug: 'rabbitmq',
    middleware: 'RABBITMQ',
    name: 'RabbitMQ',
    tagline: 'Exchange 路由 + 队列投递 + ack 确认，灵活的消息路由',
    icon: Rabbit,
    roles: {
      'rabbit-broker': { label: 'AMQP Broker', symbol: 'roundRect', description: '嵌入式 Qpid Broker-J，提供 AMQP 0-9-1 协议，RabbitMQ 官方客户端直连' },
    },
    tabs: [
      ...COMMON_HEAD,
      { id: 'routing', label: '交换机与队列', requires: 'RABBIT_INSPECTION' },
      { id: 'ack', label: '确认与死信', requires: 'RABBIT_INSPECTION' },
      ...LAB_TABS,
      ...COMMON_TAIL,
    ],
    highlights: ['四种交换机路由', 'ack / nack / prefetch', '死信队列 DLX'],
    defaultTopic: 'orders.created',
    defaultGroup: 'lab-workbench',
  },
}

export const MQ_LIST: MqMeta[] = [MQ_REGISTRY.rocketmq, MQ_REGISTRY.kafka, MQ_REGISTRY.rabbitmq]

export function isMqSlug(value: string): value is MqSlug {
  return value === 'rocketmq' || value === 'kafka' || value === 'rabbitmq'
}

export function slugOf(middleware: MiddlewareType): MqSlug {
  return middleware.toLowerCase() as MqSlug
}

/** 角色展示信息；未知角色回退为圆形 + 原始标签 */
export function roleMeta(meta: MqMeta, role: string | undefined): RoleMeta {
  if (role && meta.roles[role]) return meta.roles[role]
  return { label: role ?? '未知', symbol: 'circle', description: '' }
}

/** 结合 provider capabilities 过滤出可用标签页 */
export function availableTabs(meta: MqMeta, provider: ProviderDescriptor | null): TabMeta[] {
  const caps = new Set(provider?.capabilities ?? [])
  return meta.tabs.filter((t) => !t.requires || caps.has(t.requires))
}

/** 节点状态中文标签 */
export const STATUS_LABELS = {
  STOPPED: '已停止',
  STARTING: '启动中',
  RUNNING: '运行中',
  DEGRADED: '降级',
  FAILED: '故障',
} as const
