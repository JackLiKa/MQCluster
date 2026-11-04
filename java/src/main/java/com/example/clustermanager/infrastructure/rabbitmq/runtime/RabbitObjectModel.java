package com.example.clustermanager.infrastructure.rabbitmq.runtime;

import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import com.example.clustermanager.core.model.rabbit.RabbitExchange;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.infrastructure.rabbitmq.routing.RabbitRouteMatcher;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 读取 Qpid 进程内对象模型（Broker → VirtualHost → Exchange / Queue / Binding），并转换为 RabbitMQ 术语。
 *
 * <p>默认交换机 {@code ""} 按 AMQP 0-9-1 语义处理：direct 类型，隐式绑定到每个同名队列。
 * 这些隐式绑定只参与路由计算，不出现在 {@link #bindings()} 中，避免拓扑图过于拥挤。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitObjectModel {

    public static final String DEFAULT_EXCHANGE = "";

    private final EmbeddedQpidRuntime runtime;

    public RabbitObjectModel(EmbeddedQpidRuntime runtime) {
        this.runtime = runtime;
    }

    /** 队列原始统计。 */
    public record QueueStat(String name, long depth, long unacked, int consumers, String deadLetterExchange,
                            Long messageTtlMs, int maxDeliveryAttempts, long totalEnqueued, long totalDequeued) {

        /** 可投递（ready）消息数。 */
        public long ready() {
            return Math.max(0, depth - unacked);
        }

        /** RabbitMQ 风格的队列参数（附 Qpid 实际参数）。 */
        public Map<String, String> arguments() {
            Map<String, String> args = new LinkedHashMap<>();
            if (deadLetterExchange != null) {
                args.put("x-dead-letter-exchange", deadLetterExchange);
            }
            if (messageTtlMs != null) {
                args.put("x-message-ttl", String.valueOf(messageTtlMs));
            }
            if (deadLetterExchange != null) {
                args.put("qpid.alternateBinding", deadLetterExchange);
            }
            if (maxDeliveryAttempts > 0) {
                args.put("qpid.maximumDeliveryAttempts", String.valueOf(maxDeliveryAttempts));
            }
            return args;
        }
    }

    /** 全部交换机（含默认交换机与 amq.* 内置交换机）。 */
    public List<RabbitExchange> exchanges() {
        VirtualHost<?> host = runtime.virtualHost();
        List<RabbitExchange> result = new ArrayList<>();
        boolean hasDefault = false;
        for (Exchange<?> exchange : host.getChildren(Exchange.class)) {
            String name = exchange.getName() == null ? DEFAULT_EXCHANGE : exchange.getName();
            hasDefault |= name.isEmpty();
            Map<String, String> args = new LinkedHashMap<>();
            AlternateBinding alternate = exchange.getAlternateBinding();
            if (alternate != null && alternate.getDestination() != null) {
                args.put("alternate-exchange", alternate.getDestination());
            }
            result.add(new RabbitExchange(name, exchange.getType(), exchange.isDurable(), isBuiltIn(name), args));
        }
        if (!hasDefault) {
            result.add(new RabbitExchange(DEFAULT_EXCHANGE, "direct", true, true, Map.of()));
        }
        result.sort(Comparator.comparing(RabbitExchange::builtIn).thenComparing(RabbitExchange::name));
        return result;
    }

    /** 全部队列统计。 */
    public List<QueueStat> queues() {
        VirtualHost<?> host = runtime.virtualHost();
        List<QueueStat> result = new ArrayList<>();
        for (Queue<?> queue : host.getChildren(Queue.class)) {
            AlternateBinding alternate = queue.getAlternateBinding();
            String dlx = alternate == null ? null : alternate.getDestination();
            long ttl = queue.getMaximumMessageTtl();
            result.add(new QueueStat(queue.getName(), queue.getQueueDepthMessages(), queue.getUnacknowledgedMessages(),
                    queue.getConsumerCount(), dlx, ttl > 0 ? ttl : null, queue.getMaximumDeliveryAttempts(),
                    queue.getTotalEnqueuedMessages(), queue.getTotalDequeuedMessages()));
        }
        result.sort(Comparator.comparing(QueueStat::name));
        return result;
    }

    /** 显式绑定（不含默认交换机的隐式绑定）。 */
    public List<RabbitBinding> bindings() {
        VirtualHost<?> host = runtime.virtualHost();
        List<RabbitBinding> result = new ArrayList<>();
        for (Exchange<?> exchange : host.getChildren(Exchange.class)) {
            String name = exchange.getName() == null ? DEFAULT_EXCHANGE : exchange.getName();
            if (!name.isEmpty()) {
                result.addAll(toBindings(name, exchange.getBindings()));
            }
        }
        result.sort(Comparator.comparing(RabbitBinding::source).thenComparing(RabbitBinding::destination).thenComparing(RabbitBinding::routingKey));
        return result;
    }

    /** 交换机类型；不存在返回空。 */
    public Optional<String> exchangeType(String name) {
        String target = name == null ? DEFAULT_EXCHANGE : name;
        if (target.isEmpty()) {
            return Optional.of("direct");
        }
        return findExchange(target).map(Exchange::getType);
    }

    /** 队列是否存在。 */
    public boolean queueExists(String name) {
        return findQueue(name).isPresent();
    }

    /** 指定队列的统计；不存在返回空。 */
    public Optional<QueueStat> queue(String name) {
        return queues().stream().filter(queue -> queue.name().equals(name)).findFirst();
    }

    /** 指定交换机的绑定（默认交换机返回到每个队列的隐式绑定）。 */
    public List<RabbitBinding> bindingsOf(String exchangeName) {
        String target = exchangeName == null ? DEFAULT_EXCHANGE : exchangeName;
        if (target.isEmpty()) {
            return queues().stream().map(queue -> new RabbitBinding(DEFAULT_EXCHANGE, queue.name(), queue.name(), Map.of())).toList();
        }
        return findExchange(target).map(exchange -> toBindings(target, exchange.getBindings())).orElse(List.of());
    }

    /**
     * 路由预览：纯计算，不发送消息。
     *
     * @throws IllegalArgumentException 交换机不存在
     */
    public RabbitRoutePreview preview(String exchangeName, String routingKey, Map<String, String> headers) {
        String exchange = exchangeName == null ? DEFAULT_EXCHANGE : exchangeName;
        String type = exchangeType(exchange)
                .orElseThrow(() -> new IllegalArgumentException("交换机不存在: " + exchange));
        String key = routingKey == null ? "" : routingKey;
        Map<String, String> messageHeaders = headers == null ? Map.of() : headers;
        List<RabbitBinding> matched = RabbitRouteMatcher.match(type, bindingsOf(exchange), key, messageHeaders);
        List<String> queues = new ArrayList<>(new LinkedHashSet<>(matched.stream().map(RabbitBinding::destination).toList()));
        return new RabbitRoutePreview(exchange, type, key, messageHeaders, matched, queues, explain(exchange, type, key, queues));
    }

    private static String explain(String exchange, String type, String key, List<String> queues) {
        String label = exchange.isEmpty() ? "默认交换机" : exchange;
        String rule = switch (type) {
            case "direct" -> exchange.isEmpty()
                    ? "默认交换机把消息投递到与 routing key 同名的队列"
                    : "direct：binding key 与 routing key「" + key + "」完全相等才命中";
            case "fanout" -> "fanout：忽略 routing key，广播到所有绑定队列";
            case "topic" -> "topic：按 . 分词，* 匹配一个词，# 匹配零个或多个词";
            case "headers" -> "headers：按消息头匹配，x-match=all 需全部相等，any 只需其一";
            default -> type + " 类型";
        };
        String outcome = queues.isEmpty()
                ? "没有队列命中，消息将不可路由（mandatory 时退回生产者，否则丢弃）"
                : "命中 " + queues.size() + " 个队列：" + String.join("、", queues);
        return label + " · " + rule + "。" + outcome;
    }

    private Optional<Exchange<?>> findExchange(String name) {
        for (Exchange<?> exchange : runtime.virtualHost().getChildren(Exchange.class)) {
            if (name.equals(exchange.getName())) {
                return Optional.of(exchange);
            }
        }
        return Optional.empty();
    }

    private Optional<Queue<?>> findQueue(String name) {
        for (Queue<?> queue : runtime.virtualHost().getChildren(Queue.class)) {
            if (queue.getName().equals(name)) {
                return Optional.of(queue);
            }
        }
        return Optional.empty();
    }

    /** 直接调用 Qpid 对象模型清空队列，返回清除条数。 */
    public long clearQueue(String name) {
        return findQueue(name)
                .orElseThrow(() -> new IllegalArgumentException("队列不存在: " + name))
                .clearQueue();
    }

    private static List<RabbitBinding> toBindings(String exchange, java.util.Collection<Binding> bindings) {
        List<RabbitBinding> result = new ArrayList<>();
        for (Binding binding : bindings) {
            Map<String, String> args = new LinkedHashMap<>();
            if (binding.getArguments() != null) {
                binding.getArguments().forEach((name, value) -> args.put(name, String.valueOf(value)));
            }
            result.add(new RabbitBinding(exchange, binding.getDestination(), binding.getBindingKey(), args));
        }
        return result;
    }

    /** 内置交换机：默认交换机与 amq.* 前缀。 */
    public static boolean isBuiltIn(String name) {
        return name == null || name.isEmpty() || name.startsWith("amq.");
    }
}
