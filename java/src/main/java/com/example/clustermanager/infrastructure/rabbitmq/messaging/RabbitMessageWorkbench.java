package com.example.clustermanager.infrastructure.rabbitmq.messaging;

import com.example.clustermanager.application.service.MessageTemplateService;
import com.example.clustermanager.core.model.MessageDeliveryResult;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.infrastructure.rabbitmq.consumer.RabbitConsumerManager;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel.QueueStat;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ 消息工作台：用官方客户端真实发布，并用临时消费者按配置 ack / nack，逐条归因结果。
 *
 * <p>支持的 options：
 * <ul>
 *   <li>{@code exchange}（默认 {@code ""} 默认交换机）、{@code routingKey}（默认取 topic 字段）、{@code headers.*}</li>
 *   <li>{@code ackMode}（auto / manual）、{@code nackRatio}（0~1）、{@code requeue}、{@code prefetch}</li>
 *   <li>{@code ttlMs}（消息级 expiration）、{@code confirm}（publisher confirms，默认 true）</li>
 *   <li>{@code consume}（默认 true；false 时消息留在队列中，便于配合常驻消费者观察 prefetch）、
 *       {@code consumeDelayMs}（发布后延迟消费，用于 TTL 过期实验）</li>
 * </ul>
 * 每个（消息, 命中队列）组合返回一行，{@code attributes.outcome} 取值：
 * ACKED / REQUEUED_THEN_ACKED / DEAD_LETTERED / DISCARDED / EXPIRED / UNROUTABLE / BROKER_NACK / QUEUED / PENDING。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitMessageWorkbench {

    static final String TRACE_HEADER = "mq-lab-id";
    private static final Set<String> SUCCESS = Set.of("ACKED", "REQUEUED_THEN_ACKED", "DEAD_LETTERED", "QUEUED");
    private static final long IDLE_TIMEOUT_MS = 2_500;
    private static final long TOTAL_TIMEOUT_MS = 15_000;

    private final EmbeddedQpidRuntime runtime;
    private final RabbitObjectModel model;
    private final MessageTemplateService templateService;

    public RabbitMessageWorkbench(EmbeddedQpidRuntime runtime, RabbitObjectModel model, MessageTemplateService templateService) {
        this.runtime = runtime;
        this.model = model;
        this.templateService = templateService;
    }

    /** 解析后的模拟参数。 */
    record Options(String exchange, String routingKey, Map<String, String> headers, String ackMode, double nackRatio,
                   boolean requeue, int prefetch, Long ttlMs, boolean confirm, boolean consume, long consumeDelayMs) {
    }

    /**
     * 执行一次消息模拟。
     *
     * @throws IllegalArgumentException 参数非法或交换机不存在
     * @throws IllegalStateException    broker 未运行
     */
    public List<MessageDeliveryResult> simulate(MessageScenario scenario) {
        runtime.requireRunning();
        Options options = parse(scenario);
        RabbitRoutePreview route = model.preview(options.exchange(), options.routingKey(), options.headers());
        Map<String, QueueStat> queues = new HashMap<>();
        model.queues().forEach(queue -> queues.put(queue.name(), queue));
        int count = Math.max(1, Math.min(scenario.messageCount(), 500));
        String producer = scenario.producerNodeId() == null || scenario.producerNodeId().isBlank()
                ? "rabbit-producer" : scenario.producerNodeId();

        List<Message> messages = new ArrayList<>();
        try (Connection connection = runtime.newConnection("mq-lab-workbench")) {
            publish(connection, scenario, options, count, messages);
            List<String> targets = route.matchedQueues();
            boolean anyRouted = messages.stream().anyMatch(message -> !message.returned);
            if (options.consume() && anyRouted && !targets.isEmpty()) {
                if (options.consumeDelayMs() > 0) {
                    Thread.sleep(options.consumeDelayMs());
                }
                consume(connection, options, targets, messages, queues);
            }
        } catch (IllegalArgumentException | IllegalStateException exception) {
            throw exception;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("消息模拟被中断", interrupted);
        } catch (Exception exception) {
            throw EmbeddedQpidRuntime.translate(exception);
        }

        List<MessageDeliveryResult> results = toResults(messages, route, options, queues, producer);
        long ok = results.stream().filter(MessageDeliveryResult::success).count();
        runtime.events().append("workbench", "INFO", "向交换机 %s 发布 %d 条消息（routingKey=%s），命中队列 %s，成功 %d/%d 行"
                .formatted(label(options.exchange()), count, options.routingKey(),
                        route.matchedQueues().isEmpty() ? "无" : String.join(",", route.matchedQueues()), ok, results.size()));
        return results;
    }

    private Options parse(MessageScenario scenario) {
        String exchange = scenario.option("exchange", RabbitObjectModel.DEFAULT_EXCHANGE);
        String routingKey = scenario.option("routingKey", scenario.topic() == null ? "" : scenario.topic());
        Map<String, String> headers = new LinkedHashMap<>();
        if (scenario.headers() != null) {
            headers.putAll(scenario.headers());
        }
        scenario.options().forEach((key, value) -> {
            if (key.startsWith("headers.") && key.length() > "headers.".length() && value != null) {
                headers.put(key.substring("headers.".length()), value);
            }
        });
        String ackMode = scenario.option("ackMode", "manual").toLowerCase();
        if (!RabbitConsumerManager.ACK_MODES.contains(ackMode)) {
            throw new IllegalArgumentException("ackMode 仅支持 auto / manual");
        }
        double nackRatio = parseDouble(scenario.option("nackRatio", "0"), "nackRatio");
        if (nackRatio < 0 || nackRatio > 1) {
            throw new IllegalArgumentException("nackRatio 需在 0~1 之间");
        }
        if ("auto".equals(ackMode) && nackRatio > 0) {
            throw new IllegalArgumentException("自动确认（auto）模式下无法 nack，请切换为 manual");
        }
        int prefetch = (int) parseLong(scenario.option("prefetch", "10"), "prefetch");
        if (prefetch < 0 || prefetch > 1000) {
            throw new IllegalArgumentException("prefetch 需在 0~1000 之间（0 表示不限制）");
        }
        String ttlText = scenario.option("ttlMs", null);
        Long ttl = ttlText == null ? null : parseLong(ttlText, "ttlMs");
        if (ttl != null && (ttl < 0 || ttl > 3_600_000)) {
            throw new IllegalArgumentException("ttlMs 需在 0~3600000 之间");
        }
        long delay = parseLong(scenario.option("consumeDelayMs", "0"), "consumeDelayMs");
        if (delay < 0 || delay > 10_000) {
            throw new IllegalArgumentException("consumeDelayMs 需在 0~10000 之间");
        }
        return new Options(exchange, routingKey, headers, ackMode, nackRatio,
                Boolean.parseBoolean(scenario.option("requeue", "false")), prefetch, ttl,
                Boolean.parseBoolean(scenario.option("confirm", "true")),
                Boolean.parseBoolean(scenario.option("consume", "true")), delay);
    }

    private void publish(Connection connection, MessageScenario scenario, Options options, int count, List<Message> messages) throws Exception {
        Channel channel = connection.createChannel();
        Map<String, Message> byTrace = new ConcurrentHashMap<>();
        ConcurrentSkipListMap<Long, Message> outstanding = new ConcurrentSkipListMap<>();
        channel.addReturnListener(returned -> {
            Object trace = returned.getProperties().getHeaders() == null ? null : returned.getProperties().getHeaders().get(TRACE_HEADER);
            Message message = trace == null ? null : byTrace.get(trace.toString());
            if (message != null) {
                message.returned = true;
                message.returnReason = returned.getReplyCode() + " " + returned.getReplyText();
            }
        });
        if (options.confirm()) {
            channel.confirmSelect();
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long seq, boolean multiple) {
                    settle(seq, multiple, Boolean.TRUE);
                }

                @Override
                public void handleNack(long seq, boolean multiple) {
                    settle(seq, multiple, Boolean.FALSE);
                }

                private void settle(long seq, boolean multiple, Boolean value) {
                    Map<Long, Message> range = multiple ? outstanding.headMap(seq, true) : outstanding.subMap(seq, true, seq, true);
                    range.values().forEach(message -> message.confirmed = value);
                    range.clear();
                }
            });
        }
        for (int index = 1; index <= count; index++) {
            String traceId = UUID.randomUUID().toString();
            Message message = new Message(index, traceId, "msg-" + index + "-" + traceId.substring(0, 8));
            Map<String, Object> headers = new LinkedHashMap<>(options.headers());
            headers.put(TRACE_HEADER, traceId);
            AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder()
                    .messageId(message.key)
                    .contentType("text/plain")
                    .deliveryMode(2)
                    .headers(headers);
            if (options.ttlMs() != null) {
                props.expiration(String.valueOf(options.ttlMs()));
            }
            byTrace.put(traceId, message);
            messages.add(message);
            if (options.confirm()) {
                outstanding.put(channel.getNextPublishSeqNo(), message);
            }
            String body = templateService.render(scenario.payloadTemplate(), options.routingKey(), index);
            channel.basicPublish(options.exchange(), options.routingKey(), true, props.build(), body.getBytes(StandardCharsets.UTF_8));
        }
        if (options.confirm()) {
            channel.waitForConfirms(10_000);
        } else {
            // 无 confirm 时用一次同步往返确保 basic.return 已到达
            channel.basicQos(0);
        }
        channel.close();
    }

    private void consume(Connection connection, Options options, List<String> targets, List<Message> messages,
                         Map<String, QueueStat> queues) throws Exception {
        Map<String, Message> byTrace = new HashMap<>();
        int expected = 0;
        for (Message message : messages) {
            if (!message.returned && message.confirmed != Boolean.FALSE) {
                byTrace.put(message.traceId, message);
                expected += targets.size();
            }
        }
        AtomicInteger resolved = new AtomicInteger();
        AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
        boolean autoAck = "auto".equals(options.ackMode());
        List<Channel> channels = new ArrayList<>();
        for (String queue : targets) {
            Channel channel = connection.createChannel();
            channels.add(channel);
            channel.basicQos(options.prefetch());
            channel.basicConsume(queue, autoAck, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    lastActivity.set(System.currentTimeMillis());
                    Object trace = properties.getHeaders() == null ? null : properties.getHeaders().get(TRACE_HEADER);
                    Message message = trace == null ? null : byTrace.get(trace.toString());
                    if (message == null) {
                        // 队列里原有的其他消息：直接确认，计入额外消费数
                        if (!autoAck) {
                            getChannel().basicAck(envelope.getDeliveryTag(), false);
                        }
                        message = messages.getFirst();
                        message.extraConsumed.merge(queue, 1, Integer::sum);
                        return;
                    }
                    Delivery delivery = message.deliveries.computeIfAbsent(queue, ignored -> new Delivery());
                    delivery.redelivered |= envelope.isRedeliver();
                    if (autoAck) {
                        delivery.outcome = "ACKED";
                        resolved.incrementAndGet();
                        return;
                    }
                    if (delivery.nacked) {
                        getChannel().basicAck(envelope.getDeliveryTag(), false);
                        delivery.outcome = "REQUEUED_THEN_ACKED";
                        resolved.incrementAndGet();
                        return;
                    }
                    if (shouldNack(message.index, options.nackRatio())) {
                        delivery.nacked = true;
                        getChannel().basicNack(envelope.getDeliveryTag(), false, options.requeue());
                        if (!options.requeue()) {
                            QueueStat stat = queues.get(queue);
                            delivery.outcome = stat != null && stat.deadLetterExchange() != null ? "DEAD_LETTERED" : "DISCARDED";
                            resolved.incrementAndGet();
                        }
                        return;
                    }
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                    delivery.outcome = "ACKED";
                    resolved.incrementAndGet();
                }
            });
        }
        long started = System.currentTimeMillis();
        while (resolved.get() < expected) {
            long now = System.currentTimeMillis();
            if (now - lastActivity.get() > IDLE_TIMEOUT_MS || now - started > TOTAL_TIMEOUT_MS) {
                break;
            }
            Thread.sleep(50);
        }
        for (Channel channel : channels) {
            try {
                channel.close();
            } catch (Exception ignored) {
                // 关闭临时消费者失败不影响结果
            }
        }
    }

    private List<MessageDeliveryResult> toResults(List<Message> messages, RabbitRoutePreview route, Options options,
                                                  Map<String, QueueStat> queues, String producer) {
        List<MessageDeliveryResult> results = new ArrayList<>();
        Map<String, String> deadLetterTargets = new HashMap<>();
        for (Message message : messages) {
            Map<String, String> base = new LinkedHashMap<>();
            base.put("exchange", label(options.exchange()));
            base.put("exchangeType", route.exchangeType());
            base.put("routingKey", options.routingKey());
            base.put("ackMode", options.ackMode());
            base.put("prefetch", String.valueOf(options.prefetch()));
            if (options.ttlMs() != null) {
                base.put("ttlMs", String.valueOf(options.ttlMs()));
            }
            if (options.confirm()) {
                base.put("confirmed", message.confirmed == null ? "unknown" : String.valueOf(message.confirmed));
            }
            if (message.returned || route.matchedQueues().isEmpty()) {
                Map<String, String> attrs = new LinkedHashMap<>(base);
                attrs.put("queue", "-");
                attrs.put("outcome", "UNROUTABLE");
                results.add(new MessageDeliveryResult(message.key, producer, null, false,
                        "不可路由：没有绑定命中，broker 通过 basic.return 退回" + (message.returnReason == null ? "" : "（" + message.returnReason + "）"), attrs));
                continue;
            }
            if (message.confirmed == Boolean.FALSE) {
                Map<String, String> attrs = new LinkedHashMap<>(base);
                attrs.put("queue", "-");
                attrs.put("outcome", "BROKER_NACK");
                results.add(new MessageDeliveryResult(message.key, producer, null, false, "broker 拒绝确认（publisher confirm nack）", attrs));
                continue;
            }
            for (String queue : route.matchedQueues()) {
                QueueStat stat = queues.get(queue);
                Delivery delivery = message.deliveries.get(queue);
                String outcome;
                if (!options.consume()) {
                    outcome = "QUEUED";
                } else if (delivery == null) {
                    outcome = isExpiring(options, stat) ? "EXPIRED" : "PENDING";
                } else if (delivery.outcome == null) {
                    outcome = "PENDING";
                } else {
                    outcome = delivery.outcome;
                }
                Map<String, String> attrs = new LinkedHashMap<>(base);
                attrs.put("queue", queue);
                attrs.put("outcome", outcome);
                attrs.put("redelivered", String.valueOf(delivery != null && delivery.redelivered));
                if ("DEAD_LETTERED".equals(outcome)) {
                    String dlx = stat.deadLetterExchange();
                    String dlq = deadLetterTargets.computeIfAbsent(dlx + "|" + options.routingKey(),
                            ignored -> String.join(",", model.preview(dlx, options.routingKey(), options.headers()).matchedQueues()));
                    attrs.put("deadLetterExchange", dlx);
                    attrs.put("deadLetterQueue", dlq.isEmpty() ? "-" : dlq);
                }
                Integer extra = message.index == 1 ? message.extraConsumed.get(queue) : null;
                results.add(new MessageDeliveryResult(message.key, producer, queue, SUCCESS.contains(outcome),
                        describe(outcome, queue, stat, attrs, options) + (extra == null ? "" : "；另消费了队列中原有的 " + extra + " 条消息"), attrs));
            }
        }
        return results;
    }

    private static boolean isExpiring(Options options, QueueStat stat) {
        return options.ttlMs() != null || (stat != null && stat.messageTtlMs() != null);
    }

    private static String describe(String outcome, String queue, QueueStat stat, Map<String, String> attrs, Options options) {
        return switch (outcome) {
            case "ACKED" -> "已投递到 " + queue + " 并确认（" + ("auto".equals(options.ackMode()) ? "自动 ack" : "手动 ack") + "）";
            case "REQUEUED_THEN_ACKED" -> "nack 后重新入队（requeue=true），再次投递后确认";
            case "DEAD_LETTERED" -> "nack(requeue=false) → 经死信交换机 " + attrs.get("deadLetterExchange") + " 进入 " + attrs.get("deadLetterQueue");
            case "DISCARDED" -> "nack(requeue=false)，" + queue + " 未配置死信交换机，消息被丢弃";
            case "EXPIRED" -> "TTL 到期未被消费，已过期" + (stat != null && stat.deadLetterExchange() != null
                    ? "（Qpid 不支持过期转死信，真实 RabbitMQ 会进入死信队列）" : "");
            case "QUEUED" -> "已进入 " + queue + "，等待消费者（consume=false）";
            default -> "在超时时间内未收到投递（可能被常驻消费者取走）";
        };
    }

    private static boolean shouldNack(int index, double ratio) {
        if (ratio <= 0) {
            return false;
        }
        return Math.floor(index * ratio) > Math.floor((index - 1) * ratio);
    }

    private static String label(String exchange) {
        return exchange == null || exchange.isEmpty() ? "(默认交换机)" : exchange;
    }

    private static double parseDouble(String value, String name) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " 必须是数字");
        }
    }

    private static long parseLong(String value, String name) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " 必须是整数");
        }
    }

    /** 单条已发布消息。 */
    private static final class Message {
        private final int index;
        private final String traceId;
        private final String key;
        private final Map<String, Delivery> deliveries = new ConcurrentHashMap<>();
        private final Map<String, Integer> extraConsumed = new ConcurrentHashMap<>();
        private volatile boolean returned;
        private volatile String returnReason;
        private volatile Boolean confirmed;

        private Message(int index, String traceId, String key) {
            this.index = index;
            this.traceId = traceId;
            this.key = key;
        }
    }

    /** 某条消息在某个队列上的投递状态。 */
    private static final class Delivery {
        private volatile boolean nacked;
        private volatile boolean redelivered;
        private volatile String outcome;
    }
}
