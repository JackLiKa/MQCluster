package com.example.clustermanager.core.model.kafka;

import java.util.List;

/**
 * Kafka 分区详情，领域核心层的不可变值对象。
 *
 * <p>broker 以 Kafka {@code node.id}（1/2/3）表示，前端通过拓扑节点标签 {@code kafkaNodeId} 映射到节点。
 *
 * @param partition    分区号
 * @param leader       leader broker 的 node.id；无 leader（全部副本离线）时为 {@code -1}
 * @param replicas     副本所在 broker 列表（首个为 preferred leader）
 * @param isr          同步副本集合（In-Sync Replicas）
 * @param logEndOffset 分区日志末端位移（LEO），未知时为 {@code -1}
 */
public record KafkaPartitionDetail(
        int partition,
        int leader,
        List<Integer> replicas,
        List<Integer> isr,
        long logEndOffset
) {
}
