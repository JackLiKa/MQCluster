package com.example.clustermanager.infrastructure.rabbitmq.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import com.example.clustermanager.core.port.IStressLab;
import com.example.clustermanager.infrastructure.lab.StressRunner;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ（Qpid）压力实验：发布 + publisher confirm 全同步，用 confirm 超时暴露 broker 的背压。
 *
 * <p>每个压测线程独占一条 Channel（AMQP Channel 不是线程安全的）。每条消息发布后等待
 * {@code basic.ack}；broker 来不及处理时确认迟迟不来，{@code waitForConfirmsOrDie} 抛
 * {@code TimeoutException}，或者 Qpid 触发流控后 {@code basicPublish} 直接阻塞、
 * 单次发送耗时飙升——两种情况都是真实发生的过载信号。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitStressLab implements IStressLab {

    private static final Logger log = LoggerFactory.getLogger(RabbitStressLab.class);
    private static final String DEFAULT_EXCHANGE = "lab.topic";

    private final EmbeddedQpidRuntime runtime;
    private final RabbitObjectModel model;
    private final Object lock = new Object();
    private StressRunner runner;

    public RabbitStressLab(EmbeddedQpidRuntime runtime, RabbitObjectModel model) {
        this.runtime = runtime;
        this.model = model;
    }

    @Override
    public StressStatus start(ClusterRef clusterRef, StressCommand command) {
        runtime.requireRunning();
        String exchange = command.option("exchange", DEFAULT_EXCHANGE);
        if (!exchange.isEmpty() && model.exchangeType(exchange).isEmpty()) {
            throw new IllegalArgumentException("交换机不存在: " + exchange);
        }
        synchronized (lock) {
            if (runner != null && runner.snapshot().running()) {
                throw new IllegalStateException("压力实验正在运行中，请先停止");
            }
            RabbitStressSender sender = new RabbitStressSender(runtime, command, exchange);
            runner = new StressRunner(command, sender, null);
            runner.start();
            log.info("RabbitMQ 压测启动: exchange={}, routingKey={}, rate={}/s", exchange,
                    command.resolvedTopic(), command.resolvedTargetRate());
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

    /** 一条连接 + 每线程一个 confirm 模式 Channel。 */
    private static final class RabbitStressSender implements StressRunner.Sender {

        private final Connection connection;
        private final String exchange;
        private final String routingKey;
        private final byte[] payload;
        private final long confirmTimeoutMs;
        private final ThreadLocal<Channel> channels;

        private RabbitStressSender(EmbeddedQpidRuntime runtime, StressCommand command, String exchange) {
            try {
                this.connection = runtime.newConnection("mq-lab-stress");
            } catch (Exception exception) {
                throw new IllegalStateException("无法连接 Qpid broker: " + exception.getMessage(), exception);
            }
            this.exchange = exchange;
            this.routingKey = command.resolvedTopic();
            this.payload = "x".repeat(command.resolvedPayloadBytes()).getBytes(StandardCharsets.UTF_8);
            this.confirmTimeoutMs = command.optionLong("confirmTimeoutMs", 3000);
            this.channels = ThreadLocal.withInitial(() -> {
                try {
                    Channel channel = connection.createChannel();
                    channel.confirmSelect();
                    return channel;
                } catch (IOException exception) {
                    throw new IllegalStateException("创建 confirm channel 失败: " + exception.getMessage(), exception);
                }
            });
        }

        @Override
        public void send(long sequence) throws Exception {
            Channel channel = channels.get();
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId("stress-" + sequence)
                    .contentType("text/plain")
                    .deliveryMode(1)
                    .build();
            channel.basicPublish(exchange, routingKey, properties, payload);
            // 同步等待确认：broker 被压垮时这里会超时
            channel.waitForConfirmsOrDie(confirmTimeoutMs);
        }

        @Override
        public void close() {
            try {
                connection.close(3000);
            } catch (Exception ignored) {
                // 关闭失败不影响统计
            }
        }
    }
}
