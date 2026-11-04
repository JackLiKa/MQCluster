/**
 * API 客户端与类型化端点函数。
 *
 * 职责：
 * - 创建统一的 axios 实例（baseURL: `/api`），所有 HTTP 请求均通过此实例发出。
 * - 通过响应拦截器归一化后端错误响应，将 ApiExceptionHandler 返回的 ErrorResponse.message
 *   解包为标准 Error 对象抛出，调用方可直接用 error.message 获取可读错误信息。
 * - 提供与后端 ClusterController / KafkaController / RabbitController 一一对应的类型化函数。
 *
 * 前端唯一的数据访问层：组件和页面不直接使用 fetch 或裸 axios，确保错误处理一致和类型安全。
 */

import axios from 'axios'
import type {
  ClusterSelection,
  ClusterTopology,
  LogEntry,
  MessageSimulationRequest,
  MessageSimulationResult,
  MessageTemplate,
  MonitoringSnapshot,
  NodeOperationRequest,
  OperationResult,
  OperationType,
  ProviderDescriptor,
  RateLimitResult,
  RocketMqConnectionConfig,
  ServiceRegistrationRequest,
} from '@/types/cluster'
import type {
  KafkaConsumerGroupDetail,
  KafkaGroupMember,
  KafkaLeaderChange,
  KafkaTopicCreateRequest,
  KafkaTopicDetail,
} from '@/types/kafka'
import type { InternalsSnapshot, StressStartRequest, StressStatus } from '@/types/lab'
import type {
  RabbitBinding,
  RabbitBindingRequest,
  RabbitConsumer,
  RabbitConsumerRequest,
  RabbitExchange,
  RabbitExchangeRequest,
  RabbitQueue,
  RabbitQueueRequest,
  RabbitRoutePreview,
  RabbitRoutePreviewRequest,
  RabbitRoutingTopology,
} from '@/types/rabbit'

/** 统一 axios 实例，baseURL 为 /api，由 Next.js rewrites 代理到后端 http://localhost:8088 */
const api = axios.create({
  baseURL: '/api',
})

/**
 * 响应拦截器：成功时直接透传；失败时解包后端 ErrorResponse.message 为 Error 抛出。
 * 后端 ApiExceptionHandler 统一返回 { code, message, correlationId, timestamp } 结构。
 */
api.interceptors.response.use(
  (response) => response,
  (error) => {
    const backendMessage = error?.response?.data?.message
    if (backendMessage) {
      return Promise.reject(new Error(backendMessage))
    }
    if (error?.response?.status >= 500 || error?.code === 'ERR_NETWORK') {
      return Promise.reject(new Error('无法连接后端服务，请确认后端已在 8088 端口启动'))
    }
    return Promise.reject(error)
  },
)

/**
 * 构造 REST 路径前缀：/clusters/{mode}/{middleware}/{clusterId}（均转小写）。
 */
function pathOf(selection: ClusterSelection): string {
  return `/clusters/${selection.mode.toLowerCase()}/${selection.middleware.toLowerCase()}/${selection.clusterId}`
}

const enc = encodeURIComponent

// ─────────────────────────── 通用集群接口 ───────────────────────────

/** GET /api/clusters/providers——所有可用的集群 Provider 描述 */
export async function fetchProviders(): Promise<ProviderDescriptor[]> {
  const { data } = await api.get<ProviderDescriptor[]>('/clusters/providers')
  return data
}

/** GET /api/clusters/message-templates——预定义消息模板 */
export async function fetchMessageTemplates(): Promise<MessageTemplate[]> {
  const { data } = await api.get<MessageTemplate[]>('/clusters/message-templates')
  return data
}

/** GET /api/clusters/settings/rocketmq——当前 RocketMQ 连接配置 */
export async function fetchRocketMqConfig(): Promise<RocketMqConnectionConfig> {
  const { data } = await api.get<RocketMqConnectionConfig>('/clusters/settings/rocketmq')
  return data
}

/** PUT /api/clusters/settings/rocketmq——更新 RocketMQ 连接配置，立即生效并持久化 */
export async function updateRocketMqConfig(config: RocketMqConnectionConfig): Promise<RocketMqConnectionConfig> {
  const { data } = await api.put<RocketMqConnectionConfig>('/clusters/settings/rocketmq', config)
  return data
}

/** GET .../topology——节点列表 + 链路列表 + 活跃 VIP */
export async function fetchTopology(selection: ClusterSelection): Promise<ClusterTopology> {
  const { data } = await api.get<ClusterTopology>(`${pathOf(selection)}/topology`)
  return data
}

/** GET .../metrics——监控指标快照 */
export async function fetchMetrics(selection: ClusterSelection): Promise<MonitoringSnapshot> {
  const { data } = await api.get<MonitoringSnapshot>(`${pathOf(selection)}/metrics`)
  return data
}

/** GET .../logs——活动日志，可按节点过滤 */
export async function fetchLogs(selection: ClusterSelection, nodeId?: string, limit = 50): Promise<LogEntry[]> {
  const { data } = await api.get<LogEntry[]>(`${pathOf(selection)}/logs`, {
    params: { nodeId: nodeId || undefined, limit },
  })
  return data
}

/** POST .../nodes/{nodeId}/operations——单节点启动 / 停止 / 重启 */
export async function operateNode(
  selection: ClusterSelection,
  nodeId: string,
  operationType: OperationType,
): Promise<OperationResult> {
  const body: NodeOperationRequest = { operationType }
  const { data } = await api.post<OperationResult>(`${pathOf(selection)}/nodes/${enc(nodeId)}/operations`, body)
  return data
}

/** POST .../operations——集群级一键启动 / 停止 / 重启（启动顺序由后端 provider 决定） */
export async function operateCluster(
  selection: ClusterSelection,
  operationType: OperationType,
): Promise<OperationResult[]> {
  const body: NodeOperationRequest = { operationType }
  const { data } = await api.post<OperationResult[]>(`${pathOf(selection)}/operations`, body, {
    timeout: 180_000,
  })
  return data
}

/** POST .../services——登记新服务节点 */
export async function registerService(
  selection: ClusterSelection,
  payload: ServiceRegistrationRequest,
): Promise<OperationResult> {
  const { data } = await api.post<OperationResult>(`${pathOf(selection)}/services`, payload)
  return data
}

/** DELETE .../services/{nodeId}——删除已登记的服务节点 */
export async function deleteService(selection: ClusterSelection, nodeId: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(selection)}/services/${enc(nodeId)}`)
  return data
}

/** POST .../messages/simulate——真实生产 + 消费验证 */
export async function simulateMessages(
  selection: ClusterSelection,
  payload: MessageSimulationRequest,
): Promise<MessageSimulationResult> {
  const { data } = await api.post<MessageSimulationResult>(`${pathOf(selection)}/messages/simulate`, payload)
  return data
}

/** GET /api/clusters/rate-limit——本机动态消息量上限（取决于 CPU / 堆 / 磁盘） */
export async function fetchRateLimit(): Promise<RateLimitResult> {
  const { data } = await api.get<RateLimitResult>('/clusters/rate-limit')
  return data
}

// ─────────────────────────── Kafka 专属接口 ───────────────────────────

export async function fetchKafkaTopics(sel: ClusterSelection, includeInternal = false): Promise<KafkaTopicDetail[]> {
  const { data } = await api.get<KafkaTopicDetail[]>(`${pathOf(sel)}/topics`, { params: { includeInternal } })
  return data
}

export async function createKafkaTopic(sel: ClusterSelection, req: KafkaTopicCreateRequest): Promise<KafkaTopicDetail> {
  const { data } = await api.post<KafkaTopicDetail>(`${pathOf(sel)}/topics`, req)
  return data
}

export async function deleteKafkaTopic(sel: ClusterSelection, topic: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(sel)}/topics/${enc(topic)}`)
  return data
}

export async function fetchKafkaGroups(sel: ClusterSelection): Promise<KafkaConsumerGroupDetail[]> {
  const { data } = await api.get<KafkaConsumerGroupDetail[]>(`${pathOf(sel)}/consumer-groups`)
  return data
}

export async function fetchKafkaGroup(sel: ClusterSelection, group: string): Promise<KafkaConsumerGroupDetail> {
  const { data } = await api.get<KafkaConsumerGroupDetail>(`${pathOf(sel)}/consumer-groups/${enc(group)}`)
  return data
}

export async function addKafkaMember(
  sel: ClusterSelection,
  group: string,
  topic: string,
  ratePerSecond: number,
): Promise<KafkaGroupMember> {
  const { data } = await api.post<KafkaGroupMember>(`${pathOf(sel)}/consumer-groups/${enc(group)}/members`, {
    topic,
    ratePerSecond,
  })
  return data
}

export async function updateKafkaMemberRate(
  sel: ClusterSelection,
  group: string,
  memberId: string,
  ratePerSecond: number,
): Promise<KafkaGroupMember> {
  const { data } = await api.patch<KafkaGroupMember>(
    `${pathOf(sel)}/consumer-groups/${enc(group)}/members/${enc(memberId)}`,
    { ratePerSecond },
  )
  return data
}

export async function removeKafkaMember(sel: ClusterSelection, group: string, memberId: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(
    `${pathOf(sel)}/consumer-groups/${enc(group)}/members/${enc(memberId)}`,
  )
  return data
}

export async function fetchKafkaLeaderChanges(sel: ClusterSelection, limit = 50): Promise<KafkaLeaderChange[]> {
  const { data } = await api.get<KafkaLeaderChange[]>(`${pathOf(sel)}/leader-changes`, { params: { limit } })
  return data
}

// ─────────────────────────── RabbitMQ 专属接口 ───────────────────────────

export async function fetchRabbitRouting(sel: ClusterSelection): Promise<RabbitRoutingTopology> {
  const { data } = await api.get<RabbitRoutingTopology>(`${pathOf(sel)}/routing`)
  return data
}

export async function previewRabbitRoute(sel: ClusterSelection, req: RabbitRoutePreviewRequest): Promise<RabbitRoutePreview> {
  const { data } = await api.post<RabbitRoutePreview>(`${pathOf(sel)}/routing/preview`, req)
  return data
}

export async function declareRabbitExchange(sel: ClusterSelection, req: RabbitExchangeRequest): Promise<RabbitExchange> {
  const { data } = await api.post<RabbitExchange>(`${pathOf(sel)}/exchanges`, req)
  return data
}

export async function deleteRabbitExchange(sel: ClusterSelection, name: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(sel)}/exchanges/${enc(name)}`)
  return data
}

export async function declareRabbitQueue(sel: ClusterSelection, req: RabbitQueueRequest): Promise<RabbitQueue> {
  const { data } = await api.post<RabbitQueue>(`${pathOf(sel)}/queues`, req)
  return data
}

export async function deleteRabbitQueue(sel: ClusterSelection, name: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(sel)}/queues/${enc(name)}`)
  return data
}

export async function purgeRabbitQueue(sel: ClusterSelection, name: string): Promise<OperationResult> {
  const { data } = await api.post<OperationResult>(`${pathOf(sel)}/queues/${enc(name)}/purge`)
  return data
}

export async function bindRabbit(sel: ClusterSelection, req: RabbitBindingRequest): Promise<RabbitBinding> {
  const { data } = await api.post<RabbitBinding>(`${pathOf(sel)}/bindings`, req)
  return data
}

export async function unbindRabbit(sel: ClusterSelection, req: RabbitBindingRequest): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(sel)}/bindings`, { data: req })
  return data
}

export async function addRabbitConsumer(sel: ClusterSelection, req: RabbitConsumerRequest): Promise<RabbitConsumer> {
  const { data } = await api.post<RabbitConsumer>(`${pathOf(sel)}/consumers`, req)
  return data
}

export async function removeRabbitConsumer(sel: ClusterSelection, consumerId: string): Promise<OperationResult> {
  const { data } = await api.delete<OperationResult>(`${pathOf(sel)}/consumers/${enc(consumerId)}`)
  return data
}

// ─────────────────────────── 实验室接口（压测 + 内部机制） ───────────────────────────

/** GET .../lab/internals——broker 此刻的真实内部状态（存储单元 / 流水线 / 计数器） */
export async function fetchInternals(sel: ClusterSelection): Promise<InternalsSnapshot> {
  const { data } = await api.get<InternalsSnapshot>(`${pathOf(sel)}/lab/internals`)
  return data
}

/** POST .../lab/stress——启动后台压测，立即返回初始状态 */
export async function startStress(sel: ClusterSelection, req: StressStartRequest): Promise<StressStatus> {
  const { data } = await api.post<StressStatus>(`${pathOf(sel)}/lab/stress`, req, { timeout: 60_000 })
  return data
}

/** GET .../lab/stress——轮询压测进度与时间序列 */
export async function fetchStressStatus(sel: ClusterSelection): Promise<StressStatus> {
  const { data } = await api.get<StressStatus>(`${pathOf(sel)}/lab/stress`)
  return data
}

/** DELETE .../lab/stress——提前终止压测 */
export async function stopStress(sel: ClusterSelection): Promise<StressStatus> {
  const { data } = await api.delete<StressStatus>(`${pathOf(sel)}/lab/stress`)
  return data
}
