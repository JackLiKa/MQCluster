package com.example.clustermanager.infrastructure.kafka.group;

import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.kafka.KafkaGroupMember;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 后端常驻的 Kafka 消费者成员管理器。
 *
 * <p>每个成员一个独立线程运行 {@link KafkaConsumer}（KafkaConsumer 非线程安全）。
 * 通过可调的「每秒处理条数」人为制造 lag；增减成员即可观察 rebalance。
 * 速率为 0 时暂停所有已分配分区但继续 poll，以维持组成员身份。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerGroupManager {

    /** 单次 poll 的最大条数：保证低速率下一批也能在 max.poll.interval 内处理完。 */
    private static final int MAX_POLL_RECORDS = 10;
    private static final int MAX_RATE = 10_000;

    private final EmbeddedKafkaRuntime runtime;
    private final Map<String, ManagedConsumer> members = new ConcurrentHashMap<>();
    private final AtomicInteger sequence = new AtomicInteger();

    public KafkaConsumerGroupManager(EmbeddedKafkaRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * 新增常驻消费者成员。
     *
     * @param groupId       消费者组
     * @param topic         订阅的 topic
     * @param ratePerSecond 每秒处理条数（0 表示暂停）
     * @return 成员快照
     */
    public KafkaGroupMember add(String groupId, String topic, int ratePerSecond) {
        runtime.requireAvailable();
        requireText(groupId, "groupId");
        requireText(topic, "topic");
        String memberId = "m" + sequence.incrementAndGet();
        ManagedConsumer consumer = new ManagedConsumer(memberId, groupId, topic, clampRate(ratePerSecond));
        members.put(memberId, consumer);
        consumer.start();
        runtime.events().append("consumer-group", "INFO",
                "消费者组 %s 新增成员 %s（订阅 %s，%d 条/秒）".formatted(groupId, memberId, topic, consumer.rate));
        return consumer.snapshot();
    }

    /**
     * 调整成员速率。
     *
     * @throws IllegalArgumentException 成员不存在
     */
    public KafkaGroupMember updateRate(String groupId, String memberId, int ratePerSecond) {
        ManagedConsumer consumer = require(groupId, memberId);
        consumer.rate = clampRate(ratePerSecond);
        runtime.events().append("consumer-group", "INFO",
                "消费者组 %s 成员 %s 速率调整为 %d 条/秒".formatted(groupId, memberId, consumer.rate));
        return consumer.snapshot();
    }

    /**
     * 移除成员：成员离组后协调者触发 rebalance。
     *
     * @throws IllegalArgumentException 成员不存在
     */
    public OperationResult remove(String groupId, String memberId) {
        ManagedConsumer consumer = require(groupId, memberId);
        members.remove(memberId);
        consumer.shutdown();
        runtime.events().append("consumer-group", "WARN", "消费者组 %s 移除成员 %s，触发 rebalance".formatted(groupId, memberId));
        return new OperationResult(memberId, OperationType.STOP, true, "成员已离开消费者组 " + groupId);
    }

    /** 指定组的常驻成员快照。 */
    public List<KafkaGroupMember> membersOf(String groupId) {
        return members.values().stream()
                .filter(consumer -> consumer.groupId.equals(groupId))
                .map(ManagedConsumer::snapshot)
                .sorted(java.util.Comparator.comparingInt(member -> Integer.parseInt(member.memberId().substring(1))))
                .toList();
    }

    /** Kafka client.id → 平台成员 ID（用于把 Admin 返回的成员关联回常驻成员）。 */
    public Map<String, KafkaGroupMember> byClientId(String groupId) {
        Map<String, KafkaGroupMember> result = new HashMap<>();
        membersOf(groupId).forEach(member -> result.put(member.clientId(), member));
        return result;
    }

    /** 拥有常驻成员的组。 */
    public java.util.Set<String> managedGroups() {
        return members.values().stream().map(consumer -> consumer.groupId).collect(java.util.stream.Collectors.toSet());
    }

    /** 停止所有常驻成员（一键停止集群前调用）。 */
    @PreDestroy
    public void stopAll() {
        List<ManagedConsumer> all = List.copyOf(members.values());
        members.clear();
        all.forEach(ManagedConsumer::requestStop);
        all.forEach(ManagedConsumer::join);
        if (!all.isEmpty()) {
            runtime.events().append("consumer-group", "WARN", "已停止全部 %d 个常驻消费者".formatted(all.size()));
        }
    }

    private ManagedConsumer require(String groupId, String memberId) {
        ManagedConsumer consumer = members.get(memberId);
        if (consumer == null || !consumer.groupId.equals(groupId)) {
            throw new IllegalArgumentException("消费者成员不存在: " + groupId + "/" + memberId);
        }
        return consumer;
    }

    private static int clampRate(int rate) {
        return Math.max(0, Math.min(MAX_RATE, rate));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
    }

    /** 单个常驻消费者线程。 */
    private final class ManagedConsumer implements Runnable {

        private final String memberId;
        private final String groupId;
        private final String topic;
        private final String clientId;
        private final AtomicLong processed = new AtomicLong();
        private final Thread thread;
        private volatile int rate;
        private volatile boolean active = true;
        private volatile KafkaConsumer<String, String> consumer;
        private volatile List<String> assigned = List.of();
        private volatile String kafkaMemberId = "";

        private ManagedConsumer(String memberId, String groupId, String topic, int rate) {
            this.memberId = memberId;
            this.groupId = groupId;
            this.topic = topic;
            this.rate = rate;
            this.clientId = "lab-" + memberId;
            this.thread = new Thread(this, "kafka-lab-consumer-" + memberId);
            this.thread.setDaemon(true);
        }

        private void start() {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, runtime.bootstrapServers());
            config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
            config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer = new KafkaConsumer<>(config);
            thread.start();
        }

        @Override
        public void run() {
            KafkaConsumer<String, String> client = consumer;
            try {
                client.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        if (!partitions.isEmpty()) {
                            runtime.events().append("consumer-group", "INFO",
                                    "rebalance：%s/%s 释放分区 %s".formatted(groupId, memberId, names(partitions)));
                        }
                        assigned = List.of();
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        assigned = names(partitions);
                        if (rate <= 0) {
                            // 暂停状态下新分配的分区立即 pause，避免本次 poll 返回未处理的消息
                            client.pause(partitions);
                        }
                        runtime.events().append("consumer-group", "INFO",
                                "rebalance：%s/%s 分配到分区 %s".formatted(groupId, memberId, assigned));
                    }
                });
                while (active) {
                    loopOnce(client);
                }
            } catch (WakeupException expected) {
                // 停止信号
            } catch (Exception exception) {
                runtime.events().append("consumer-group", "ERROR", "成员 %s 异常退出: %s".formatted(memberId, exception.getMessage()));
            } finally {
                try {
                    client.close(Duration.ofSeconds(5));
                } catch (Exception ignored) {
                    // 关闭失败不影响其他成员
                }
            }
        }

        private void loopOnce(KafkaConsumer<String, String> client) {
            int currentRate = rate;
            if (currentRate <= 0) {
                client.pause(client.assignment());
                rewind(client, client.poll(Duration.ofMillis(300)));
                refreshMemberId(client);
                return;
            }
            if (!client.paused().isEmpty()) {
                client.resume(client.paused());
            }
            ConsumerRecords<String, String> records = client.poll(Duration.ofMillis(500));
            refreshMemberId(client);
            if (records.isEmpty()) {
                return;
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            long sleepMs = 1000L / Math.max(1, rate);
            for (ConsumerRecord<String, String> record : records) {
                if (!active) {
                    break;
                }
                sleepQuietly(sleepMs);
                processed.incrementAndGet();
                offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }
            if (!offsets.isEmpty()) {
                try {
                    client.commitSync(offsets, Duration.ofSeconds(5));
                } catch (Exception commitFailure) {
                    // rebalance 期间提交可能失败，下一批会重新消费并提交
                }
            }
        }

        /** 未处理就返回的消息回退位点，保证恢复消费后不会跳过。 */
        private void rewind(KafkaConsumer<String, String> client, ConsumerRecords<String, String> records) {
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> batch = records.records(partition);
                if (!batch.isEmpty() && client.assignment().contains(partition)) {
                    client.seek(partition, batch.getFirst().offset());
                }
            }
        }

        private void refreshMemberId(KafkaConsumer<String, String> client) {
            try {
                kafkaMemberId = client.groupMetadata().memberId();
            } catch (Exception ignored) {
                // 尚未加入组
            }
        }

        private void requestStop() {
            active = false;
            KafkaConsumer<String, String> client = consumer;
            if (client != null) {
                client.wakeup();
            }
        }

        private void join() {
            try {
                thread.join(10_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private void shutdown() {
            requestStop();
            join();
        }

        private KafkaGroupMember snapshot() {
            return new KafkaGroupMember(memberId, kafkaMemberId, clientId, "127.0.0.1", true, rate, processed.get(), assigned);
        }

        private static List<String> names(Collection<TopicPartition> partitions) {
            return partitions.stream()
                    .sorted(java.util.Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                    .map(TopicPartition::toString)
                    .toList();
        }

        private void sleepQuietly(long millis) {
            if (millis <= 0) {
                return;
            }
            try {
                Thread.sleep(millis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                active = false;
            }
        }
    }
}
