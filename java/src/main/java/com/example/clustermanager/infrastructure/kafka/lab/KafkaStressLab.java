package com.example.clustermanager.infrastructure.kafka.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import com.example.clustermanager.core.port.IStressLab;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import com.example.clustermanager.infrastructure.lab.StressRunner;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Kafka 压力实验：用真实 KafkaProducer 持续投递，暴露 broker 跟不上时的生产者侧背压。
 *
 * <p>默认参数刻意贴近"会疼"的生产者配置：{@code buffer.memory=16MB}、{@code max.block.ms=2000}、
 * {@code acks=all}。当 broker 消化不了时，{@code send()} 会先阻塞在缓冲区分配上，
 * 随后抛 {@code TimeoutException: Failed to allocate memory within the configured max blocking time}；
 * 已进入缓冲区的批次则在 {@code delivery.timeout.ms} 到期后以 {@code Expiring N records} 失败。
 * 两类异常都会被原样统计，不做任何改写。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaStressLab implements IStressLab {

    private static final Logger log = LoggerFactory.getLogger(KafkaStressLab.class);
    private static final Set<String> ACKS = Set.of("0", "1", "all");
    private static final int DEFAULT_PARTITIONS = 3;

    private final EmbeddedKafkaRuntime runtime;
    private final Object lock = new Object();
    private StressRunner runner;

    public KafkaStressLab(EmbeddedKafkaRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public StressStatus start(ClusterRef clusterRef, StressCommand command) {
        runtime.requireAvailable();
        if (!ACKS.contains(command.resolvedAcks())) {
            throw new IllegalArgumentException("acks 仅支持 0 / 1 / all");
        }
        String topic = command.resolvedTopic();
        ensureTopic(topic);
        synchronized (lock) {
            if (runner != null && runner.snapshot().running()) {
                throw new IllegalStateException("压力实验正在运行中，请先停止");
            }
            KafkaStressSender sender = new KafkaStressSender(runtime, command, topic);
            runner = new StressRunner(command, sender, sender);
            runner.start();
            log.info("Kafka 压测启动: topic={}, rate={}/s, threads={}", topic, command.resolvedTargetRate(), command.resolvedThreads());
            return runner.snapshot();
        }
    }

    @Override
    public StressStatus stop(ClusterRef clusterRef) {
        StressRunner current;
        synchronized (lock) {
            current = runner;
        }
        // 不在锁内 stop：排空在途消息可能耗时数秒，持锁会让 GET /stress 一起卡住
        return current == null ? StressStatus.idle() : current.stop();
    }

    @Override
    public StressStatus status(ClusterRef clusterRef) {
        StressRunner current;
        synchronized (lock) {
            current = runner;
        }
        return current == null ? StressStatus.idle() : current.snapshot();
    }

    /** topic 不存在时按 3 分区、不超过运行中 broker 数创建，避免首条消息触发 auto-create 的默认配置。 */
    private void ensureTopic(String topic) {
        Admin admin = runtime.admin();
        try {
            if (admin.listTopics().names().get(8, TimeUnit.SECONDS).contains(topic)) {
                return;
            }
            short replication = (short) Math.max(1, Math.min(3, runtime.runningBrokerCount()));
            try {
                admin.createTopics(List.of(new NewTopic(topic, DEFAULT_PARTITIONS, replication)))
                        .all().get(10, TimeUnit.SECONDS);
                runtime.events().append("stress", "INFO",
                        "压力实验自动创建 topic %s（%d 分区 × %d 副本）".formatted(topic, DEFAULT_PARTITIONS, replication));
            } catch (ExecutionException exception) {
                if (!(exception.getCause() instanceof TopicExistsException)) {
                    throw exception;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("创建 topic 被中断", interrupted);
        } catch (Exception exception) {
            throw new IllegalStateException("无法准备压测 topic " + topic + ": " + exception.getMessage(), exception);
        }
    }

    /** 真实 producer + 异步回调失败统计。 */
    private static final class KafkaStressSender implements StressRunner.Sender, StressRunner.AsyncSink {

        private final KafkaProducer<String, byte[]> producer;
        private final String topic;
        private final byte[] payload;
        private final AtomicLong failed = new AtomicLong();
        private final Map<String, Long> errors = new LinkedHashMap<>();
        /** 由压测引擎在 start() 时注入，用来记录「入队 → broker 确认」的端到端耗时 */
        private volatile LongConsumer latencySink = value -> { };

        private KafkaStressSender(EmbeddedKafkaRuntime runtime, StressCommand command, String topic) {
            Map<String, Object> config = new LinkedHashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, runtime.bootstrapServers());
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "mq-lab-stress");
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            config.put(ProducerConfig.ACKS_CONFIG, command.resolvedAcks());
            config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(command.resolvedAcks()));
            config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            // 刻意压小生产者缓冲：broker 一慢，send() 就会阻塞在缓冲区分配上
            config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, command.optionInt("bufferMemoryBytes", 16 * 1024 * 1024));
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, command.optionInt("maxBlockMs", 2000));
            config.put(ProducerConfig.LINGER_MS_CONFIG, command.optionInt("lingerMs", 0));
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, command.optionInt("batchSize", 16 * 1024));
            config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, command.optionInt("deliveryTimeoutMs", 15_000));
            config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, command.optionInt("requestTimeoutMs", 5000));
            this.producer = new KafkaProducer<>(config);
            this.topic = topic;
            this.payload = "stress-payload".concat("x".repeat(Math.max(0, command.resolvedPayloadBytes() - 14)))
                    .getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public boolean reportsAsyncLatency() {
            return true;
        }

        @Override
        public void bindLatencySink(LongConsumer sink) {
            this.latencySink = sink;
        }

        @Override
        public void send(long sequence) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, "k-" + (sequence % 64), payload);
            long startNanos = System.nanoTime();
            producer.send(record, (metadata, exception) -> {
                // 缓冲区被塞满时，send() 会先阻塞在缓冲区分配上，这里记的是包含阻塞的端到端耗时
                latencySink.accept(System.nanoTime() - startNanos);
                if (exception != null) {
                    failed.incrementAndGet();
                    recordError(exception);
                }
            });
        }

        private void recordError(Throwable throwable) {
            String message = StressRunner.describe(throwable);
            if (message.contains("closed forcefully")) {
                // 停止实验时 close 超时强制关闭，客户端缓冲区里还有未被 broker 确认的消息。
                // 这不是 broker 拒绝，而是「停止瞬间的积压」，单独命名以免误导。
                message = "停止实验时仍未确认的消息（生产者缓冲区积压）";
            }
            synchronized (errors) {
                if (!errors.containsKey(message) && errors.size() >= 8) {
                    message = "其他错误（超过 8 种）";
                }
                errors.merge(message, 1L, Long::sum);
            }
        }

        @Override
        public long failedCount() {
            return failed.get();
        }

        @Override
        public List<StressStatus.Error> recentErrors() {
            synchronized (errors) {
                return errors.entrySet().stream()
                        .map(entry -> new StressStatus.Error(entry.getKey(), entry.getValue()))
                        .sorted(Comparator.comparingLong(StressStatus.Error::count).reversed())
                        .limit(8)
                        .toList();
            }
        }

        @Override
        public void drain() {
            try {
                producer.flush();
            } catch (RuntimeException ignored) {
                // flush 失败说明 broker 已不可用，失败已由回调统计
            }
        }

        @Override
        public void close() {
            producer.close(java.time.Duration.ofSeconds(3));
        }
    }
}
