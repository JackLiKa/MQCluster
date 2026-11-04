package com.example.clustermanager.core.model.kafka;

import java.util.List;
import java.util.Map;

/**
 * Kafka 消费者组详情，领域核心层的不可变值对象。
 *
 * @param groupId         消费者组 ID
 * @param state           组状态（Stable / PreparingRebalance / CompletingRebalance / Empty / Dead 等）
 * @param topics          组内订阅/提交过的 topic
 * @param members         组成员
 * @param assignments     成员 ID → 分配到的分区（{@code topic-partition}）
 * @param lagPerPartition 每个分区的积压
 * @param totalLag        总积压
 */
public record KafkaConsumerGroupDetail(
        String groupId,
        String state,
        List<String> topics,
        List<KafkaGroupMember> members,
        Map<String, List<String>> assignments,
        List<KafkaPartitionLag> lagPerPartition,
        long totalLag
) {
}
