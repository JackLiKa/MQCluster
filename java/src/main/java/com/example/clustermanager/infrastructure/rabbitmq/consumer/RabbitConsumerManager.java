package com.example.clustermanager.infrastructure.rabbitmq.consumer;

import com.example.clustermanager.core.model.rabbit.RabbitConsumer;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 常驻 RabbitMQ 消费者管理：每个消费者独占一条连接与 channel，可设置 prefetch、ack 模式、处理延迟与 nack 比例。
 *
 * <p>用于演示 prefetch 限流（unacked 不超过 prefetch）、处理慢导致 ready 堆积、nack + requeue / 死信。
 * 移除消费者时关闭连接，未确认的消息由 broker 重新放回队列。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitConsumerManager {

    public static final Set<String> ACK_MODES = Set.of("auto", "manual");
    static final int MAX_CONSUMERS = 20;
    static final String EVENT_NODE = "consumer";

    private final EmbeddedQpidRuntime runtime;
    private final Map<String, Member> members = new ConcurrentHashMap<>();
    private final AtomicInteger sequence = new AtomicInteger();

    public RabbitConsumerManager(EmbeddedQpidRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * 增加常驻消费者。
     *
     * @throws IllegalArgumentException 参数非法或队列不存在
     * @throws IllegalStateException    broker 未运行
     */
    public synchronized RabbitConsumer add(String queue, int prefetch, String ackMode, long processingDelayMs, double nackRatio, boolean requeue) {
        runtime.requireRunning();
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("队列名不能为空");
        }
        String mode = ackMode == null ? "manual" : ackMode.toLowerCase();
        if (!ACK_MODES.contains(mode)) {
            throw new IllegalArgumentException("ackMode 仅支持 auto / manual");
        }
        if (prefetch < 0 || prefetch > 1000) {
            throw new IllegalArgumentException("prefetch 需在 0~1000 之间（0 表示不限制）");
        }
        if (processingDelayMs < 0 || processingDelayMs > 10_000) {
            throw new IllegalArgumentException("处理延迟需在 0~10000 ms 之间");
        }
        if (nackRatio < 0 || nackRatio > 1) {
            throw new IllegalArgumentException("nackRatio 需在 0~1 之间");
        }
        if (members.values().stream().filter(Member::active).count() >= MAX_CONSUMERS) {
            throw new IllegalArgumentException("常驻消费者最多 " + MAX_CONSUMERS + " 个");
        }
        String consumerId = "c" + sequence.incrementAndGet();
        Member member = new Member(consumerId, queue.trim(), prefetch, mode, processingDelayMs, "auto".equals(mode) ? 0 : nackRatio, requeue);
        try {
            member.open();
        } catch (Exception exception) {
            member.close();
            throw EmbeddedQpidRuntime.translate(exception);
        }
        members.put(consumerId, member);
        runtime.events().append(EVENT_NODE, "INFO", "消费者 %s 订阅 %s（prefetch=%d, ack=%s, 延迟=%dms, nack=%.0f%%, requeue=%s）"
                .formatted(consumerId, member.queue, prefetch, mode, processingDelayMs, member.nackRatio * 100, requeue));
        return member.snapshot();
    }

    /**
     * 移除常驻消费者。
     *
     * @return 是否存在
     */
    public synchronized boolean remove(String consumerId) {
        Member member = members.remove(consumerId);
        if (member == null) {
            return false;
        }
        member.close();
        runtime.events().append(EVENT_NODE, "INFO", "消费者 %s 已取消订阅 %s，未确认的 %d 条消息回到队列"
                .formatted(consumerId, member.queue, member.inFlight.get()));
        return true;
    }

    /** 当前消费者列表（含已失效的，便于 UI 展示原因）。 */
    public List<RabbitConsumer> list() {
        List<RabbitConsumer> result = new ArrayList<>();
        members.values().stream()
                .sorted(Comparator.comparingInt(member -> Integer.parseInt(member.consumerId.substring(1))))
                .forEach(member -> result.add(member.snapshot()));
        return result;
    }

    /** broker 停止前关闭全部消费者。 */
    public synchronized void stopAll() {
        if (members.isEmpty()) {
            return;
        }
        members.values().forEach(Member::close);
        runtime.events().append(EVENT_NODE, "WARN", "已关闭全部 %d 个常驻消费者".formatted(members.size()));
        members.clear();
    }

    @PreDestroy
    public void shutdown() {
        members.values().forEach(Member::close);
        members.clear();
    }

    /** 单个常驻消费者。 */
    private final class Member {
        private final String consumerId;
        private final String queue;
        private final int prefetch;
        private final String ackMode;
        private final long processingDelayMs;
        private final double nackRatio;
        private final boolean requeue;
        private final AtomicLong acked = new AtomicLong();
        private final AtomicLong nacked = new AtomicLong();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicLong received = new AtomicLong();
        private volatile boolean active;
        private volatile boolean closing;
        private Connection connection;

        private Member(String consumerId, String queue, int prefetch, String ackMode, long processingDelayMs, double nackRatio, boolean requeue) {
            this.consumerId = consumerId;
            this.queue = queue;
            this.prefetch = prefetch;
            this.ackMode = ackMode;
            this.processingDelayMs = processingDelayMs;
            this.nackRatio = nackRatio;
            this.requeue = requeue;
        }

        private void open() throws IOException, java.util.concurrent.TimeoutException {
            connection = runtime.newConnection("mq-lab-consumer-" + consumerId);
            Channel channel = connection.createChannel();
            channel.basicQos(prefetch);
            boolean autoAck = "auto".equals(ackMode);
            channel.basicConsume(queue, autoAck, "mq-lab-" + consumerId, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long index = received.incrementAndGet();
                    if (autoAck) {
                        sleep();
                        acked.incrementAndGet();
                        return;
                    }
                    inFlight.incrementAndGet();
                    try {
                        sleep();
                        if (closing) {
                            return;
                        }
                        if (shouldNack(index)) {
                            getChannel().basicNack(envelope.getDeliveryTag(), false, requeue);
                            nacked.incrementAndGet();
                        } else {
                            getChannel().basicAck(envelope.getDeliveryTag(), false);
                            acked.incrementAndGet();
                        }
                    } catch (AlreadyClosedException closed) {
                        // 处理期间消费者被移除或 broker 停止：消息未确认，由 broker 重新入队
                    } finally {
                        inFlight.decrementAndGet();
                    }
                }

                @Override
                public void handleShutdownSignal(String tag, ShutdownSignalException signal) {
                    if (!closing) {
                        runtime.events().append(EVENT_NODE, "WARN", "消费者 %s 连接断开：%s".formatted(consumerId, signal.getMessage()));
                    }
                    active = false;
                }

                @Override
                public void handleCancel(String tag) {
                    runtime.events().append(EVENT_NODE, "WARN", "消费者 %s 被 broker 取消（队列可能已删除）".formatted(consumerId));
                    active = false;
                }
            });
            active = true;
        }

        /** 按序号确定性地分配 nack，使比例稳定可复现。 */
        private boolean shouldNack(long index) {
            if (nackRatio <= 0) {
                return false;
            }
            return Math.floor(index * nackRatio) > Math.floor((index - 1) * nackRatio);
        }

        private void sleep() {
            if (processingDelayMs <= 0) {
                return;
            }
            try {
                Thread.sleep(processingDelayMs);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean active() {
            return active;
        }

        private void close() {
            closing = true;
            active = false;
            if (connection != null) {
                try {
                    connection.abort(2_000);
                } catch (Exception ignored) {
                    // 连接可能已随 broker 停止而断开
                }
            }
        }

        private RabbitConsumer snapshot() {
            return new RabbitConsumer(consumerId, queue, prefetch, ackMode, processingDelayMs, nackRatio, requeue,
                    active, acked.get(), nacked.get(), inFlight.get());
        }
    }
}
