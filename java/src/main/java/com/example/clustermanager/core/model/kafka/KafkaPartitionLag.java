package com.example.clustermanager.core.model.kafka;

/**
 * 消费者组在单个分区上的消费进度，领域核心层的不可变值对象。
 *
 * @param topic           topic 名称
 * @param partition       分区号
 * @param committedOffset 已提交位移；从未提交时为 {@code -1}
 * @param logEndOffset    分区日志末端位移
 * @param lag             积压量 = LEO - committed（未提交时按 LEO 计）
 * @param ownerMemberId   当前持有该分区的成员 ID；未分配时为 {@code null}
 */
public record KafkaPartitionLag(
        String topic,
        int partition,
        long committedOffset,
        long logEndOffset,
        long lag,
        String ownerMemberId
) {
}
