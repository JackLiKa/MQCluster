/**
 * RabbitMQ 检查端口类型——对应后端 core/model/rabbit 下的 record。
 */

export type RabbitExchangeType = 'direct' | 'fanout' | 'topic' | 'headers'

/** 交换机——对应 RabbitExchange */
export interface RabbitExchange {
  name: string
  type: RabbitExchangeType
  durable: boolean
  builtIn: boolean
  arguments: Record<string, string>
}

/** 队列——对应 RabbitQueue */
export interface RabbitQueue {
  name: string
  ready: number
  unacked: number
  consumers: number
  arguments: Record<string, string>
  deadLetterExchange: string | null
  messageTtlMs: number | null
  publishRate: number
  deliverRate: number
  totalEnqueued: number
  totalDequeued: number
}

/** 绑定——对应 RabbitBinding */
export interface RabbitBinding {
  source: string
  destination: string
  routingKey: string
  headers: Record<string, string>
}

/** 常驻消费者——对应 RabbitConsumer */
export interface RabbitConsumer {
  consumerId: string
  queue: string
  prefetch: number
  ackMode: 'auto' | 'manual'
  processingDelayMs: number
  nackRatio: number
  requeue: boolean
  active: boolean
  acked: number
  nacked: number
  inFlight: number
}

/** 完整路由拓扑——对应 RabbitRoutingTopology */
export interface RabbitRoutingTopology {
  capturedAt: string
  exchanges: RabbitExchange[]
  queues: RabbitQueue[]
  bindings: RabbitBinding[]
  consumers: RabbitConsumer[]
}

/** 路由预览结果——对应 RabbitRoutePreview */
export interface RabbitRoutePreview {
  exchange: string
  exchangeType: string
  routingKey: string
  headers: Record<string, string>
  matchedBindings: RabbitBinding[]
  matchedQueues: string[]
  explanation: string
}

export interface RabbitExchangeRequest {
  name: string
  type: RabbitExchangeType
  durable?: boolean
}

export interface RabbitQueueRequest {
  name: string
  deadLetterExchange?: string | null
  messageTtlMs?: number | null
}

export interface RabbitBindingRequest {
  exchange: string
  queue: string
  routingKey?: string
  headers?: Record<string, string>
}

export interface RabbitRoutePreviewRequest {
  exchange: string
  routingKey?: string
  headers?: Record<string, string>
}

export interface RabbitConsumerRequest {
  queue: string
  prefetch?: number
  ackMode?: 'auto' | 'manual'
  processingDelayMs?: number
  nackRatio?: number
  requeue?: boolean
}
