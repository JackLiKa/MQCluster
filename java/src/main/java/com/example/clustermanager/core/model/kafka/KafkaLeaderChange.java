package com.example.clustermanager.core.model.kafka;

import java.time.Instant;

/**
 * 分区 leader 切换事件，领域核心层的不可变值对象。用于前端时间线展示。
 *
 * @param timestamp      检测到切换的时间
 * @param topic          topic 名称
 * @param partition      分区号
 * @param previousLeader 原 leader 的 node.id（{@code -1} 表示原先无 leader）
 * @param newLeader      新 leader 的 node.id（{@code -1} 表示当前无 leader）
 */
public record KafkaLeaderChange(
        Instant timestamp,
        String topic,
        int partition,
        int previousLeader,
        int newLeader
) {
}
