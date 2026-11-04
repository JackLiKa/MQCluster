package com.example.clustermanager.infrastructure.rabbitmq.topology;

import java.util.List;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ 拓扑种子器：单 broker 节点 + 一组演示用交换机/队列/绑定。
 *
 * <p>四种交换机各一个，另含死信交换机 {@code lab.dlx} 与带 DLX 的工作队列，broker 每次启动后重新声明
 * （内存虚拟主机，停止即清空）。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitTopologySeeder {

    /** 唯一 broker 节点 ID。 */
    public static final String NODE_ID = "rabbit-node-01";
    /** 拓扑 {@code labels.role}。 */
    public static final String ROLE = "rabbit-broker";
    /** 节点展示名。 */
    public static final String DISPLAY_NAME = "RabbitMQ Broker（Qpid）";

    /** 演示交换机。 */
    public List<ExchangeSeed> exchanges() {
        return List.of(
                new ExchangeSeed("lab.direct", "direct"),
                new ExchangeSeed("lab.fanout", "fanout"),
                new ExchangeSeed("lab.topic", "topic"),
                new ExchangeSeed("lab.headers", "headers"),
                new ExchangeSeed("lab.dlx", "fanout")
        );
    }

    /** 演示队列。 */
    public List<QueueSeed> queues() {
        return List.of(
                new QueueSeed("orders.created", null, null),
                new QueueSeed("orders.paid", null, null),
                new QueueSeed("notify.email", null, null),
                new QueueSeed("notify.sms", null, null),
                new QueueSeed("logs.error", null, null),
                new QueueSeed("logs.all", null, null),
                new QueueSeed("invoice.pdf", null, null),
                new QueueSeed("work.tasks", "lab.dlx", null),
                new QueueSeed("work.delay", "lab.dlx", 5_000L),
                new QueueSeed("dead.letters", null, null)
        );
    }

    /** 演示绑定。 */
    public List<BindingSeed> bindings() {
        return List.of(
                new BindingSeed("lab.direct", "orders.created", "order.created", Map.of()),
                new BindingSeed("lab.direct", "orders.paid", "order.paid", Map.of()),
                new BindingSeed("lab.direct", "work.tasks", "task", Map.of()),
                new BindingSeed("lab.direct", "work.delay", "delay", Map.of()),
                new BindingSeed("lab.fanout", "notify.email", "", Map.of()),
                new BindingSeed("lab.fanout", "notify.sms", "", Map.of()),
                new BindingSeed("lab.topic", "logs.error", "#.error", Map.of()),
                new BindingSeed("lab.topic", "logs.all", "app.#", Map.of()),
                new BindingSeed("lab.headers", "invoice.pdf", "", Map.of("x-match", "all", "type", "invoice", "format", "pdf")),
                new BindingSeed("lab.dlx", "dead.letters", "", Map.of())
        );
    }

    /** 交换机种子。 */
    public record ExchangeSeed(String name, String type) {
    }

    /** 队列种子。 */
    public record QueueSeed(String name, String deadLetterExchange, Long messageTtlMs) {
    }

    /** 绑定种子。 */
    public record BindingSeed(String exchange, String queue, String routingKey, Map<String, String> headers) {
    }
}
