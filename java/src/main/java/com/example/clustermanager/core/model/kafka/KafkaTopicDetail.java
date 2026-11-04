package com.example.clustermanager.core.model.kafka;

import java.util.List;
import java.util.Map;

/**
 * Kafka topic 详情，领域核心层的不可变值对象。
 *
 * @param name              topic 名称
 * @param internal          是否为内部 topic（如 {@code __consumer_offsets}）
 * @param partitionCount    分区数
 * @param replicationFactor 副本因子
 * @param partitions        各分区详情，按分区号升序
 * @param configs           关键 topic 配置（如 {@code min.insync.replicas}）
 */
public record KafkaTopicDetail(
        String name,
        boolean internal,
        int partitionCount,
        int replicationFactor,
        List<KafkaPartitionDetail> partitions,
        Map<String, String> configs
) {
}
