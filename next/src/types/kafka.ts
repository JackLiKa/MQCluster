/**
 * Kafka 检查端口类型——对应后端 core/model/kafka 下的 record。
 */

/** 分区详情——对应 KafkaPartitionDetail */
export interface KafkaPartitionDetail {
  partition: number
  /** leader broker 的 node.id；-1 表示无 leader（分区离线） */
  leader: number
  replicas: number[]
  isr: number[]
  logEndOffset: number
}

/** Topic 详情——对应 KafkaTopicDetail */
export interface KafkaTopicDetail {
  name: string
  internal: boolean
  partitionCount: number
  replicationFactor: number
  partitions: KafkaPartitionDetail[]
  configs: Record<string, string>
}

/** 消费者组成员——对应 KafkaGroupMember */
export interface KafkaGroupMember {
  memberId: string
  kafkaMemberId: string | null
  clientId: string | null
  host: string | null
  managed: boolean
  ratePerSecond: number
  processed: number
  assignedPartitions: string[]
}

/** 分区 lag——对应 KafkaPartitionLag */
export interface KafkaPartitionLag {
  topic: string
  partition: number
  committedOffset: number
  logEndOffset: number
  lag: number
  ownerMemberId: string | null
}

/** 消费者组详情——对应 KafkaConsumerGroupDetail */
export interface KafkaConsumerGroupDetail {
  groupId: string
  state: string
  topics: string[]
  members: KafkaGroupMember[]
  assignments: Record<string, string[]>
  lagPerPartition: KafkaPartitionLag[]
  totalLag: number
}

/** leader 切换记录——对应 KafkaLeaderChange */
export interface KafkaLeaderChange {
  timestamp: string
  topic: string
  partition: number
  previousLeader: number
  newLeader: number
}

/** 创建 topic 请求——对应 KafkaTopicCreateRequest */
export interface KafkaTopicCreateRequest {
  name: string
  partitions?: number
  replicationFactor?: number
  configs?: Record<string, string>
}
