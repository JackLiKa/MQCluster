/**
 * 与后端 DTO 对齐的 TypeScript 类型定义。
 *
 * 所有类型严格映射 Java 后端的 record 字段（含命名差异），
 * 确保前端 API 调用与后端响应结构一致。
 */

/** 集群模式——对应后端 ClusterMode 枚举 */
export type ClusterMode = 'PSEUDO' | 'REAL'

/** 中间件类型——对应后端 MiddlewareType 枚举 */
export type MiddlewareType = 'ROCKETMQ' | 'KAFKA' | 'RABBITMQ'

/** 节点状态——对应后端 NodeStatus 枚举 */
export type NodeStatus = 'STOPPED' | 'STARTING' | 'RUNNING' | 'DEGRADED' | 'FAILED'

/** 操作类型——对应后端 OperationType 枚举 */
export type OperationType = 'START' | 'STOP' | 'RESTART'

/** 集群选择标识——对应后端 ClusterSelection record */
export interface ClusterSelection {
  clusterId: string
  mode: ClusterMode
  middleware: MiddlewareType
}

/** 集群引用——对应后端 ClusterRef record */
export interface ClusterRef {
  clusterId: string
  mode: ClusterMode
  middleware: MiddlewareType
}

/** Provider 能力标识——对应后端 ProviderCapabilities 常量 */
export type ProviderCapability =
  | 'CONNECTION_CONFIG'
  | 'SERVICE_REGISTRATION'
  | 'CLUSTER_OPERATIONS'
  | 'KAFKA_INSPECTION'
  | 'RABBIT_INSPECTION'

/** Provider 描述符——对应后端 ProviderDescriptor record */
export interface ProviderDescriptor {
  providerId: string
  displayName: string
  mode: ClusterMode
  middleware: MiddlewareType
  clusterId: string
  capabilities: string[]
}

/** 集群节点——对应后端 ClusterNode record */
export interface ClusterNode {
  nodeId: string
  displayName: string
  hostName: string
  virtualIp: string | null
  status: NodeStatus
  labels: Record<string, string>
}

/** 网络链路——对应后端 NetworkLink record */
export interface NetworkLink {
  sourceNodeId: string
  targetNodeId: string
  healthy: boolean
  linkType: string
  latencyMs: number
}

/** 集群拓扑——对应后端 ClusterTopology record */
export interface ClusterTopology {
  cluster: ClusterRef
  nodes: ClusterNode[]
  links: NetworkLink[]
  activeVip: string | null
}

/** 节点指标——对应后端 NodeMetrics record */
export interface NodeMetrics {
  nodeId: string
  cpuUsage: number
  memoryUsage: number
  networkInBytesPerSecond: number
  networkOutBytesPerSecond: number
}

/** 监控快照——对应后端 MonitoringSnapshot record */
export interface MonitoringSnapshot {
  capturedAt: string
  nodes: NodeMetrics[]
}

/** 日志条目——对应后端 LogEntry record */
export interface LogEntry {
  timestamp: string
  nodeId: string
  level: string
  message: string
}

/** 操作结果——对应后端 OperationResult record */
export interface OperationResult {
  targetId: string
  operationType: OperationType | null
  success: boolean
  message: string
}

/** 消息投递结果——对应后端 MessageDeliveryResult record */
export interface MessageDeliveryResult {
  messageKey: string
  producerNodeId: string
  consumerNodeId: string
  success: boolean
  detail: string
  /** 各 MQ 特有的结果字段（Kafka: partition/offset；RabbitMQ: queue/outcome 等） */
  attributes?: Record<string, string> | null
}

/** 消息模拟结果——对应后端 MessageSimulationResult record */
export interface MessageSimulationResult {
  executedAt: string
  deliveries: MessageDeliveryResult[]
}

/** 消息模拟请求——对应后端 MessageSimulationRequest DTO */
export interface MessageSimulationRequest {
  topic: string
  consumerGroup: string
  messageCount: number
  payloadTemplate?: string
  producerNodeId: string
  consumerNodeIds?: string[]
  headers?: Record<string, string>
  /** 各 MQ 特有参数（可省略） */
  options?: Record<string, string>
}

/** 消息模板——对应后端 MessageTemplate record */
export interface MessageTemplate {
  id: string
  name: string
  template: string
  description: string
}

/** RocketMQ 连接配置——对应后端 ConfigSnapshot record */
export interface RocketMqConnectionConfig {
  nameServers: string[]
  sendMsgTimeoutMs: number
  consumeTimeoutSeconds: number
  consumerGroupPrefix: string
}

/** 服务登记请求——对应后端 ServiceRegistrationRequest DTO */
export interface ServiceRegistrationRequest {
  nodeId: string
  displayName: string
  role: string
  hostName: string
  address: string
  port: number
  labels?: Record<string, string>
}

/** 节点操作请求——对应后端 NodeOperationRequest DTO */
export interface NodeOperationRequest {
  operationType: OperationType
}

/** 系统配置快照——对应后端 RateLimitService.SystemProfile record */
export interface SystemProfile {
  logicalCores: number
  availableHeapMb: number
  maxHeapMb: number
  totalPhysicalMb: number
  freePhysicalMb: number
  systemCpuLoad: number
  availableDiskGb: number
}

/** 限流计算结果——对应后端 RateLimitService.RateLimitResult record */
export interface RateLimitResult {
  maxMessages: number
  minFloor: number
  baselineCeiling: number
  safetyCoefficient: number
  messagesPerMb: number
  messagesPerCore: number
  messagesPerGb: number
  systemProfile: SystemProfile
}
