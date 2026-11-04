package com.example.clustermanager.infrastructure.kafka.metrics;

import com.example.clustermanager.core.model.kafka.KafkaLeaderChange;
import java.time.Instant;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiConsumer;

/**
 * Kafka 吞吐与 leader 变化跟踪器。
 *
 * <p>嵌入式 broker 共享同一 JVM，Kafka 的 Yammer 指标注册表全局唯一，无法按 broker 区分，
 * 因此按以下口径计算：
 * <ul>
 *   <li>broker 入速率 = 该 broker 作为 leader 的分区 LEO 增量 ÷ 采样间隔</li>
 *   <li>broker 出速率 = 各消费者组在该 broker leader 分区上的 committed offset 增量 ÷ 采样间隔</li>
 * </ul>
 */
public final class KafkaThroughputTracker {

    private static final int MAX_LEADER_CHANGES = 100;

    private final Map<String, Long> lastEndOffsets = new HashMap<>();
    private final Map<String, Long> lastCommitted = new HashMap<>();
    private final Map<String, Integer> lastLeaders = new ConcurrentHashMap<>();
    private final Deque<KafkaLeaderChange> leaderChanges = new ConcurrentLinkedDeque<>();
    private long lastSampleNanos;

    /**
     * 一次分区采样。
     *
     * @param topic          topic
     * @param partition      分区
     * @param leader         当前 leader（无则 -1）
     * @param logEndOffset   当前 LEO
     */
    public record PartitionSample(String topic, int partition, int leader, long logEndOffset) {

        String key() {
            return topic + "-" + partition;
        }
    }

    /**
     * 计算一轮采样的每 broker 入/出速率（条/秒）。
     *
     * @param partitions 分区采样
     * @param committed  组 + 分区 → committed offset（键为 {@code group|topic-partition}）
     * @return Kafka node.id → [in, out]
     */
    public synchronized Map<Integer, double[]> sample(List<PartitionSample> partitions, Map<String, Long> committed) {
        long now = System.nanoTime();
        double seconds = lastSampleNanos == 0 ? 0 : (now - lastSampleNanos) / 1_000_000_000.0;
        lastSampleNanos = now;
        Map<Integer, double[]> rates = new HashMap<>();
        Map<String, Integer> leaderByPartition = new HashMap<>();
        for (PartitionSample sample : partitions) {
            leaderByPartition.put(sample.key(), sample.leader());
            Long previous = lastEndOffsets.put(sample.key(), sample.logEndOffset());
            if (seconds > 0 && previous != null && sample.leader() >= 0 && sample.logEndOffset() >= previous) {
                rates.computeIfAbsent(sample.leader(), id -> new double[2])[0] += (sample.logEndOffset() - previous) / seconds;
            }
        }
        committed.forEach((key, offset) -> {
            Long previous = lastCommitted.put(key, offset);
            Integer leader = leaderByPartition.get(key.substring(key.indexOf('|') + 1));
            if (seconds > 0 && previous != null && leader != null && leader >= 0 && offset >= previous) {
                rates.computeIfAbsent(leader, id -> new double[2])[1] += (offset - previous) / seconds;
            }
        });
        return rates;
    }

    /**
     * 对比上次观测的 leader，记录变化。
     *
     * @param partitions 当前分区采样
     * @param onChange   变化回调（用于写审计日志）
     */
    public void observeLeaders(List<PartitionSample> partitions, BiConsumer<String, KafkaLeaderChange> onChange) {
        for (PartitionSample sample : partitions) {
            Integer previous = lastLeaders.put(sample.key(), sample.leader());
            if (previous != null && previous != sample.leader()) {
                KafkaLeaderChange change = new KafkaLeaderChange(Instant.now(), sample.topic(), sample.partition(), previous, sample.leader());
                leaderChanges.addFirst(change);
                while (leaderChanges.size() > MAX_LEADER_CHANGES) {
                    leaderChanges.pollLast();
                }
                onChange.accept(sample.key(), change);
            }
        }
    }

    /** topic 删除后清理其跟踪状态，避免同名重建时误报 leader 切换。 */
    public synchronized void forgetTopic(String topic) {
        String prefix = topic + "-";
        lastLeaders.keySet().removeIf(key -> key.startsWith(prefix));
        lastEndOffsets.keySet().removeIf(key -> key.startsWith(prefix));
        lastCommitted.keySet().removeIf(key -> key.substring(key.indexOf('|') + 1).startsWith(prefix));
    }

    /** 最近的 leader 切换（新到旧）。 */
    public List<KafkaLeaderChange> leaderChanges(int limit) {
        return leaderChanges.stream().limit(Math.max(1, limit)).toList();
    }
}
