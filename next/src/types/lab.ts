/**
 * 实验室功能类型——压力实验与内部机制可视化。
 *
 * 对应后端 LabController 的 4 个端点，三种 MQ 共用同一套结构：
 * - 内部机制：把 broker 此刻的真实存储状态翻译成可直接绘制的阶段 / 存储单元 / 计数器
 * - 压力实验：后台任务，前端按秒轮询 running 状态与时间序列
 */

/** 内部流水线的一个阶段（消息在 broker 内部经过的环节） */
export interface InternalsStage {
  name: string
  /** 该阶段的真实数值，后端已格式化 */
  value: string
  detail: string
}

/** 一个底层存储单元（CommitLog 文件 / Kafka 段文件 / 队列对象） */
export interface InternalsSegment {
  /** 所属节点或分组 */
  group: string
  name: string
  size: number
  /** B 表示字节，条 表示消息条数 */
  unit: string
  detail: string
}

/** 一个内部计数器 */
export interface InternalsMetric {
  label: string
  value: string
  hint: string
}

/** GET .../lab/internals 的响应 */
export interface InternalsSnapshot {
  /** 一句话现状（含真实计数） */
  headline: string
  pipeline: InternalsStage[]
  segments: InternalsSegment[]
  metrics: InternalsMetric[]
  /** 机制说明与注意事项 */
  notes: string[]
}

/** 压测的一个采样点（每秒一个） */
export interface StressSample {
  elapsedMs: number
  produced: number
  failed: number
  produceTps: number
  failTps: number
  p95LatencyMs: number
  p99LatencyMs: number
}

/** 失败原因聚合：真实异常类型 + 首行信息 */
export interface StressError {
  message: string
  count: number
}

export type StressPhase = 'IDLE' | 'RUNNING' | 'DONE'

/** GET .../lab/stress 的响应，也是 POST 的返回 */
export interface StressStatus {
  running: boolean
  phase: StressPhase
  elapsedMs: number
  targetRate: number
  /** 已成功交给客户端的消息数 */
  produced: number
  /** 失败消息数（客户端拒绝 + broker 拒绝） */
  failed: number
  produceTps: number
  failTps: number
  p99LatencyMs: number
  samples: StressSample[]
  errors: StressError[]
  /** 结论：是否被流量冲垮、依据是什么 */
  verdict: string
}

/** POST .../lab/stress 的请求体 */
export interface StressStartRequest {
  topic: string
  targetRate: number
  threads: number
  durationSeconds: number
  payloadBytes: number
  /** 0 / 1 / all */
  acks?: string
  /** 各 MQ 特有参数 */
  options?: Record<string, string>
}
