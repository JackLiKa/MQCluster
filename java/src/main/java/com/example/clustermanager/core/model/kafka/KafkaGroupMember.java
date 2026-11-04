package com.example.clustermanager.core.model.kafka;

import java.util.List;

/**
 * Kafka 消费者组成员，领域核心层的不可变值对象。
 *
 * @param memberId           平台侧成员 ID（常驻消费者为 {@code member-N}；外部成员为 Kafka member id）
 * @param kafkaMemberId      Kafka 协调者分配的 member id
 * @param clientId           客户端 ID
 * @param host               客户端地址
 * @param managed            是否为平台托管的常驻消费者（可调速/删除）
 * @param ratePerSecond      每秒处理条数；{@code 0} 表示暂停消费；外部成员为 {@code -1}
 * @param processed          已处理消息数；外部成员为 {@code -1}
 * @param assignedPartitions 分配到的分区，格式 {@code topic-partition}
 */
public record KafkaGroupMember(
        String memberId,
        String kafkaMemberId,
        String clientId,
        String host,
        boolean managed,
        int ratePerSecond,
        long processed,
        List<String> assignedPartitions
) {
}
