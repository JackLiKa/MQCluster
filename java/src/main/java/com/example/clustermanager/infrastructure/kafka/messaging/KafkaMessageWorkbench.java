package com.example.clustermanager.infrastructure.kafka.messaging;

import com.example.clustermanager.application.service.MessageTemplateService;
import com.example.clustermanager.core.model.MessageDeliveryResult;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Kafka 消息工作台：用真实 producer 发送、用独立 consumer（assign + seek）回读确认。
 *
 * <p>支持的 options：{@code key}（不填则每条唯一）、{@code partition}、{@code acks}（0/1/all）、
 * {@code compression}（none/gzip/snappy/lz4/zstd）。投递结果带 partition 与 offset。
 * 回读消费者不加入消费者组，不会干扰「消费者组」标签页中的常驻成员与 lag。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaMessageWorkbench {

    static final String TRACE_HEADER = "mq-lab-id";
    private static final Set<String> ACKS = Set.of("0", "1", "all");
    private static final Set<String> COMPRESSION = Set.of("none", "gzip", "snappy", "lz4", "zstd");
    private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_PARTITIONS = 3;

    private final EmbeddedKafkaRuntime runtime;
    private final MessageTemplateService templateService;

    public KafkaMessageWorkbench(EmbeddedKafkaRuntime runtime, MessageTemplateService templateService) {
        this.runtime = runtime;
        this.templateService = templateService;
    }

    /**
     * 执行一次消息模拟。
     *
     * @param scenario 场景
     * @return 每条消息的投递结果
     */
    public List<MessageDeliveryResult> simulate(MessageScenario scenario) {
        runtime.requireAvailable();
        String topic = requireTopic(scenario.topic());
        String acks = scenario.option("acks", "all");
        String compression = scenario.option("compression", "none");
        if (!ACKS.contains(acks)) {
            throw new IllegalArgumentException("acks 仅支持 0 / 1 / all");
        }
        if (!COMPRESSION.contains(compression)) {
            throw new IllegalArgumentException("compression 仅支持 none / gzip / snappy / lz4 / zstd");
        }
        String fixedKey = scenario.option("key", null);
        Integer partition = parsePartition(scenario.option("partition", null));
        int count = Math.max(1, Math.min(scenario.messageCount(), 500));

        Admin admin = runtime.admin();
        TopicDescription description = ensureTopic(admin, topic);
        if (partition != null && partition >= description.partitions().size()) {
            throw new IllegalArgumentException("分区 %d 不存在，topic %s 共 %d 个分区".formatted(partition, topic, description.partitions().size()));
        }
        Map<Integer, Integer> leaders = new HashMap<>();
        for (TopicPartitionInfo info : description.partitions()) {
            leaders.put(info.partition(), info.leader() == null ? -1 : info.leader().id());
        }
        // 发送前记录各分区 LEO，回读从这里开始（acks=0 没有 offset 回执也能回读确认）
        Map<TopicPartition, Long> startOffsets = endOffsets(admin, topic, leaders.keySet());

        String producerNode = scenario.producerNodeId() == null || scenario.producerNodeId().isBlank()
                ? "kafka-producer" : scenario.producerNodeId();
        List<MessageDeliveryResult> results = new ArrayList<>();
        Map<String, Sent> sent = new LinkedHashMap<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(acks, compression))) {
            List<PendingSend> pending = new ArrayList<>();
            for (int index = 1; index <= count; index++) {
                String traceId = UUID.randomUUID().toString();
                String key = fixedKey != null ? fixedKey : "key-" + index + "-" + traceId.substring(0, 8);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key,
                        templateService.render(scenario.payloadTemplate(), topic, index));
                record.headers().add(TRACE_HEADER, traceId.getBytes(StandardCharsets.UTF_8));
                if (scenario.headers() != null) {
                    scenario.headers().forEach((name, value) -> record.headers().add(name, String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
                }
                pending.add(new PendingSend(traceId, key, producer.send(record)));
            }
            producer.flush();
            for (PendingSend send : pending) {
                try {
                    RecordMetadata metadata = send.future().get(15, TimeUnit.SECONDS);
                    sent.put(send.traceId(), new Sent(send.key(), metadata.partition(), metadata.hasOffset() ? metadata.offset() : -1));
                } catch (ExecutionException | TimeoutException exception) {
                    Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
                    results.add(new MessageDeliveryResult(send.key(), producerNode, null, false,
                            "发送失败: " + cause.getMessage(), Map.of("acks", acks, "compression", compression, "key", send.key())));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("发送被中断", interrupted);
                }
            }
        }

        Map<String, Sent> consumed = consumeBack(startOffsets, sent.keySet());
        for (Map.Entry<String, Sent> entry : sent.entrySet()) {
            Sent item = entry.getValue();
            Sent readBack = consumed.get(entry.getKey());
            if (item.offset() < 0 && readBack != null) {
                item = new Sent(item.key(), readBack.partition(), readBack.offset());
            }
            int leader = leaders.getOrDefault(item.partition(), -1);
            String leaderNode = leader < 0 ? null : runtime.nodeIdOf(leader);
            boolean ok = readBack != null;
            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("partition", String.valueOf(item.partition()));
            attributes.put("offset", item.offset() < 0 ? "-" : String.valueOf(item.offset()));
            attributes.put("leader", leaderNode == null ? "-" : leaderNode);
            attributes.put("key", item.key());
            attributes.put("acks", acks);
            attributes.put("compression", compression);
            attributes.put("consumed", String.valueOf(ok));
            String offsetText = item.offset() < 0 ? "acks=0 无 offset 回执" : "offset " + item.offset();
            if ("0".equals(acks) && item.offset() >= 0) {
                offsetText += "（acks=0 不等待回执，offset 由回读得到）";
            }
            String detail = ok
                    ? "已写入分区 %d（leader %s，%s）并被回读确认".formatted(item.partition(), leaderNode, offsetText)
                    : "已写入分区 %d（%s），但 %d 秒内未能回读".formatted(item.partition(), offsetText, CONSUME_TIMEOUT.toSeconds());
            results.add(new MessageDeliveryResult(item.key(), producerNode, leaderNode, ok, detail, attributes));
        }
        runtime.events().append("workbench", "INFO",
                "向 %s 发送 %d 条消息（acks=%s, compression=%s），成功 %d 条".formatted(
                        topic, count, acks, compression, results.stream().filter(MessageDeliveryResult::success).count()));
        return results;
    }

    /**
     * topic 不存在时自动创建：3 分区，副本数 = min(3, 运行中 broker 数)。
     */
    private TopicDescription ensureTopic(Admin admin, String topic) {
        try {
            if (!admin.listTopics().names().get(8, TimeUnit.SECONDS).contains(topic)) {
                short replication = (short) Math.max(1, Math.min(3, runtime.runningBrokerCount()));
                try {
                    admin.createTopics(List.of(new NewTopic(topic, DEFAULT_PARTITIONS, replication))).all().get(10, TimeUnit.SECONDS);
                    runtime.events().append("workbench", "INFO", "自动创建 topic %s（%d 分区 × %d 副本）".formatted(topic, DEFAULT_PARTITIONS, replication));
                } catch (ExecutionException exception) {
                    if (!(exception.getCause() instanceof TopicExistsException)) {
                        throw exception;
                    }
                }
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                TopicDescription description = admin.describeTopics(List.of(topic)).allTopicNames().get(8, TimeUnit.SECONDS).get(topic);
                boolean ready = description.partitions().stream().allMatch(info -> info.leader() != null);
                if (ready || System.nanoTime() > deadline) {
                    return description;
                }
                Thread.sleep(200);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待 topic 就绪被中断", interrupted);
        } catch (ExecutionException | TimeoutException exception) {
            Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
            throw new IllegalStateException("无法准备 topic " + topic + ": " + cause.getMessage(), cause);
        }
    }

    private static Map<TopicPartition, Long> endOffsets(Admin admin, String topic, Set<Integer> partitions) {
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> request = new HashMap<>();
        partitions.forEach(partition -> request.put(new TopicPartition(topic, partition), org.apache.kafka.clients.admin.OffsetSpec.latest()));
        Map<TopicPartition, Long> offsets = new HashMap<>();
        try {
            admin.listOffsets(request).all().get(8, TimeUnit.SECONDS)
                    .forEach((partition, info) -> offsets.put(partition, info.offset()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException exception) {
            // 个别分区无 leader 时退化为从头回读
            request.keySet().forEach(partition -> offsets.putIfAbsent(partition, 0L));
        }
        request.keySet().forEach(partition -> offsets.putIfAbsent(partition, 0L));
        return offsets;
    }

    /** 用不加入消费者组的 consumer 从发送前的 LEO 回读，按追踪 ID 匹配。 */
    private Map<String, Sent> consumeBack(Map<TopicPartition, Long> startOffsets, Set<String> traceIds) {
        Map<String, Sent> consumed = new HashMap<>();
        if (traceIds.isEmpty()) {
            return consumed;
        }
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, runtime.bootstrapServers(),
                ConsumerConfig.CLIENT_ID_CONFIG, "mq-lab-verifier",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(startOffsets.keySet());
            startOffsets.forEach(consumer::seek);
            long deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos();
            while (consumed.size() < traceIds.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    Header header = record.headers().lastHeader(TRACE_HEADER);
                    if (header != null) {
                        String traceId = new String(header.value(), StandardCharsets.UTF_8);
                        if (traceIds.contains(traceId)) {
                            consumed.put(traceId, new Sent(record.key(), record.partition(), record.offset()));
                        }
                    }
                }
            }
        }
        return consumed;
    }

    private Map<String, Object> producerConfig(String acks, String compression) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, runtime.bootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "mq-lab-producer");
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        // 幂等生产要求 acks=all
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(acks));
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return config;
    }

    private static String requireTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (!topic.matches("[a-zA-Z0-9._-]{1,249}")) {
            throw new IllegalArgumentException("topic 名称只能包含字母、数字、'.'、'_'、'-'");
        }
        return topic;
    }

    private static Integer parsePartition(String value) {
        if (value == null) {
            return null;
        }
        try {
            int partition = Integer.parseInt(value.trim());
            if (partition < 0) {
                throw new IllegalArgumentException("partition 不能为负数");
            }
            return partition;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("partition 必须是整数: " + value);
        }
    }

    private record PendingSend(String traceId, String key, Future<RecordMetadata> future) {
    }

    private record Sent(String key, int partition, long offset) {
    }
}
