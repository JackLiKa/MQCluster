package com.example.clustermanager.infrastructure.pseudo.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import com.example.clustermanager.core.port.IStressLab;
import com.example.clustermanager.infrastructure.lab.StressRunner;
import com.example.clustermanager.infrastructure.pseudo.node.ManagedNode;
import com.example.clustermanager.infrastructure.pseudo.node.NodeRegistry;
import com.example.clustermanager.infrastructure.pseudo.runtime.EmbeddedRocketMqRuntime;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * RocketMQ（嵌入式）压力实验：用真实 DefaultMQProducer 异步狂发，逼出 broker 的流控与拒绝。
 *
 * <p>刻意关闭客户端重试（{@code retryTimesWhenSendFailed=0}），否则 RocketMQ 客户端会
 * 悄悄重试，把 broker 的真实拒绝掩盖掉。过载时常见的真实异常：
 * <ul>
 *   <li>{@code RemotingTooMuchRequestException: sendDefaultImpl call timeout}</li>
 *   <li>{@code MQBrokerException: CODE: 2 DESC: [TIMEOUT_CLEAN_QUEUE] broker busy, start flow control for a while}</li>
 *   <li>{@code RemotingConnectException}（broker 被压垮后连接被丢弃）</li>
 * </ul>
 * 这些异常会原样出现在失败原因里。
 */
@Component
public class RocketMqStressLab implements IStressLab {

    private static final Logger log = LoggerFactory.getLogger(RocketMqStressLab.class);
    private static final Set<String> ACKS = Set.of("0", "1", "all");

    private final EmbeddedRocketMqRuntime runtime;
    private final NodeRegistry nodeRegistry;
    private final Object lock = new Object();
    private StressRunner runner;

    public RocketMqStressLab(EmbeddedRocketMqRuntime runtime, NodeRegistry nodeRegistry) {
        this.runtime = runtime;
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public StressStatus start(ClusterRef clusterRef, StressCommand command) {
        requireRunningBroker();
        if (!ACKS.contains(command.resolvedAcks())) {
            throw new IllegalArgumentException("acks 仅支持 0 / 1 / all");
        }
        String topic = command.resolvedTopic();
        synchronized (lock) {
            if (runner != null && runner.snapshot().running()) {
                throw new IllegalStateException("压力实验正在运行中，请先停止");
            }
            RocketMqStressSender sender = new RocketMqStressSender(runtime, nodeRegistry, command, topic);
            try {
                sender.prepareTopic();
            } catch (RuntimeException exception) {
                sender.close();
                throw exception;
            }
            runner = new StressRunner(command, sender, sender);
            runner.start();
            log.info("RocketMQ 压测启动: topic={}, rate={}/s, threads={}", topic,
                    command.resolvedTargetRate(), command.resolvedThreads());
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

    private void requireRunningBroker() {
        boolean running = nodeRegistry.brokers().stream().anyMatch(node -> {
            try {
                runtime.brokerController(node.nodeId());
                return true;
            } catch (IllegalStateException stopped) {
                return false;
            }
        });
        if (!running) {
            throw new IllegalStateException("RocketMQ 集群未运行：请先启动 Broker");
        }
    }

    /** 真实 producer + 异步回调失败统计。 */
    private static final class RocketMqStressSender implements StressRunner.Sender, StressRunner.AsyncSink {

        private final DefaultMQProducer producer;
        private final EmbeddedRocketMqRuntime runtime;
        private final NodeRegistry nodeRegistry;
        private final String topic;
        private final byte[] payload;
        private final AtomicLong failed = new AtomicLong();
        private final Map<String, Long> errors = new LinkedHashMap<>();
        /** 由压测引擎在 start() 时注入，用来记录「发送 → broker 确认」的端到端耗时 */
        private volatile LongConsumer latencySink = value -> { };

        private RocketMqStressSender(EmbeddedRocketMqRuntime runtime, NodeRegistry nodeRegistry,
                                     StressCommand command, String topic) {
            this.runtime = runtime;
            this.nodeRegistry = nodeRegistry;
            this.topic = topic;
            this.producer = new DefaultMQProducer("stress-producer-" + UUID.randomUUID());
            this.producer.setNamesrvAddr(runtime.namesrvAddr());
            this.producer.setSendMsgTimeout(command.optionInt("sendTimeoutMs", 3000));
            // 压测必须关闭重试：否则客户端会把 broker 的拒绝重新排进缓冲区，看不到真实失败
            this.producer.setRetryTimesWhenSendFailed(0);
            this.producer.setRetryTimesWhenSendAsyncFailed(0);
            this.producer.setVipChannelEnabled(false);
            this.payload = "x".repeat(command.resolvedPayloadBytes()).getBytes(StandardCharsets.UTF_8);
            try {
                this.producer.start();
            } catch (Exception exception) {
                throw new IllegalStateException("RocketMQ 生产者启动失败: " + exception.getMessage(), exception);
            }
        }

        /** 确保 topic 已注册到 NameServer，避免首条消息 "No route info of this topic"。 */
        private void prepareTopic() {
            BrokerController controller = null;
            for (ManagedNode node : nodeRegistry.brokers()) {
                try {
                    controller = runtime.brokerController(node.nodeId());
                    break;
                } catch (IllegalStateException ignored) {
                    // 继续寻找运行中的 broker
                }
            }
            if (controller == null) {
                throw new IllegalStateException("RocketMQ 集群未运行：请先启动 Broker");
            }
            try {
                controller.getTopicConfigManager().createTopicInSendMessageMethod(topic, topic, topic, 4, 4);
                controller.registerBrokerAll(true, false, true);
                Thread.sleep(800);
                producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                Thread.sleep(300);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception exception) {
                throw new IllegalStateException("压测 topic 路由刷新失败: " + exception.getMessage(), exception);
            }
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
        public void send(long sequence) throws Exception {
            Message message = new Message(topic, payload);
            message.setKeys("stress-" + sequence);
            long startNanos = System.nanoTime();
            // 异步发送：broker 流控时 send() 会阻塞在 Netty 写队列上，耗时直接体现在 p99
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    // 成功无需额外统计，runner 已在 send() 返回时计入
                    latencySink.accept(System.nanoTime() - startNanos);
                }

                @Override
                public void onException(Throwable throwable) {
                    latencySink.accept(System.nanoTime() - startNanos);
                    failed.incrementAndGet();
                    recordError(throwable);
                }
            });
        }

        private void recordError(Throwable throwable) {
            String message = StressRunner.describe(throwable);
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
        public void close() {
            producer.shutdown();
        }
    }
}
