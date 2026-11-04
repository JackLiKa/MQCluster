package com.example.clustermanager.infrastructure.rabbitmq.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;
import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import com.example.clustermanager.core.model.rabbit.RabbitExchange;
import com.example.clustermanager.core.port.IInternalsInspector;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel.QueueStat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ（Qpid）内部机制检查器：直接读 Qpid 进程内的对象模型。
 *
 * <p>与磁盘型 broker 不同，Qpid 用内存存储：这里展示的是真实的
 * Exchange → Binding → Queue → Consumer 对象图与队列深度，
 * 以及每条队列上生效的 DLX / TTL / prefetch 参数。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitInternalsInspector implements IInternalsInspector {

    private final EmbeddedQpidRuntime runtime;
    private final RabbitObjectModel model;

    public RabbitInternalsInspector(EmbeddedQpidRuntime runtime, RabbitObjectModel model) {
        this.runtime = runtime;
        this.model = model;
    }

    @Override
    public InternalsSnapshot inspect(ClusterRef clusterRef) {
        runtime.requireRunning();
        List<RabbitExchange> exchanges = model.exchanges();
        List<QueueStat> queues = model.queues();
        List<RabbitBinding> bindings = model.bindings();
        long ready = queues.stream().mapToLong(QueueStat::ready).sum();
        long unacked = queues.stream().mapToLong(QueueStat::unacked).sum();
        int consumers = queues.stream().mapToInt(QueueStat::consumers).sum();

        String headline = "1 个 broker · %d 个交换机 · %d 个队列 · ready %d · unacked %d".formatted(
                exchanges.size(), queues.size(), ready, unacked);

        List<InternalsSnapshot.Stage> pipeline = List.of(
                new InternalsSnapshot.Stage("发布到 Exchange", exchanges.size() + " 个交换机",
                        "生产者只认交换机名 + routing key，从不直接投递到队列"),
                new InternalsSnapshot.Stage("Binding 路由匹配", bindings.size() + " 条绑定",
                        "按交换机类型匹配：direct 比 key、fanout 全发、topic 通配、headers 比属性"),
                new InternalsSnapshot.Stage("消息入队", ready + " 条 ready",
                        "命中的每个队列各存一份消息副本，队列深度随消费速度变化"),
                new InternalsSnapshot.Stage("投递与确认", unacked + " 条 unacked / " + consumers + " 个消费者",
                        "prefetch 限制未确认数；ack 后消息才真正删除，nack 可按 requeue 重新入队"));

        List<InternalsSnapshot.Segment> segments = new ArrayList<>();
        queues.stream()
                .sorted(Comparator.comparingLong((QueueStat stat) -> stat.depth() + stat.unacked()).reversed())
                .forEach(stat -> segments.add(new InternalsSnapshot.Segment(
                        "queue",
                        stat.name(),
                        stat.depth(),
                        "条",
                        "ready %d · unacked %d · 消费者 %d%s%s".formatted(
                                stat.ready(), stat.unacked(), stat.consumers(),
                                stat.deadLetterExchange() == null ? "" : " · DLX " + stat.deadLetterExchange(),
                                stat.messageTtlMs() == null ? "" : " · TTL " + stat.messageTtlMs() + "ms"))));

        long dlxQueues = queues.stream().filter(queue -> queue.deadLetterExchange() != null).count();
        long ttlQueues = queues.stream().filter(queue -> queue.messageTtlMs() != null).count();
        List<InternalsSnapshot.Metric> metrics = List.of(
                new InternalsSnapshot.Metric("交换机", String.valueOf(exchanges.size()), "含默认交换机（名称为空）"),
                new InternalsSnapshot.Metric("绑定", String.valueOf(bindings.size()), "Exchange → Queue 的路由规则数"),
                new InternalsSnapshot.Metric("队列", String.valueOf(queues.size()), "每条队列是独立的存储单元"),
                new InternalsSnapshot.Metric("待消费消息", String.valueOf(ready), "尚未投递给任何消费者的消息"),
                new InternalsSnapshot.Metric("未确认消息", String.valueOf(unacked), "已投递但消费者还没 ack 的消息"),
                new InternalsSnapshot.Metric("配置死信/TTL", dlxQueues + " / " + ttlQueues, "队列级 x-dead-letter-exchange 与 x-message-ttl"));

        List<String> notes = List.of(
                "RabbitMQ 的「存储」按队列组织：一条消息被路由到 3 个队列，就是 3 份独立副本，各自被消费或过期。",
                "ack 之前消息一直占着队列位置（unacked）；消费者进程崩溃时，未确认消息会按 redelivered 标记重新投递。",
                "prefetch 决定一个消费者最多同时持有多少条未确认消息，它是消费端吞吐与公平性的总开关。",
                "本平台用 Apache Qpid Broker-J 提供 AMQP 0-9-1，队列存储在内存中；重启 broker 队列内容清空，这一点与真实 RabbitMQ 的持久化不同。");

        return new InternalsSnapshot(headline, pipeline, segments, metrics, notes);
    }
}
