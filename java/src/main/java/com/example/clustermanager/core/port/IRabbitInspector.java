package com.example.clustermanager.core.port;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import com.example.clustermanager.core.model.rabbit.RabbitConsumer;
import com.example.clustermanager.core.model.rabbit.RabbitExchange;
import com.example.clustermanager.core.model.rabbit.RabbitQueue;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.core.model.rabbit.RabbitRoutingTopology;
import java.util.Map;

/**
 * RabbitMQ 专属检查端口（六边形架构的出站端口）。
 *
 * <p>由 RabbitMQ provider 实现，提供交换机/队列/绑定的读取与声明、路由预览与常驻消费者管理。
 * 应用层通过 {@code ClusterProviderRegistry#resolveCapability} 获取。
 * broker 未运行时各方法抛 {@link IllegalStateException}；参数非法或对象不存在时抛 {@link IllegalArgumentException}。
 */
public interface IRabbitInspector {

    /** 完整路由拓扑。 */
    RabbitRoutingTopology routing(ClusterRef clusterRef);

    /**
     * 声明交换机。
     *
     * @param type direct / fanout / topic / headers
     */
    RabbitExchange declareExchange(ClusterRef clusterRef, String name, String type, boolean durable);

    /** 删除交换机（内置交换机不可删除）。 */
    OperationResult deleteExchange(ClusterRef clusterRef, String name);

    /**
     * 声明队列。
     *
     * @param deadLetterExchange 死信交换机（可为 {@code null}）
     * @param messageTtlMs       队列级 TTL（可为 {@code null}）
     */
    RabbitQueue declareQueue(ClusterRef clusterRef, String name, String deadLetterExchange, Long messageTtlMs);

    /** 删除队列。 */
    OperationResult deleteQueue(ClusterRef clusterRef, String name);

    /** 清空队列中的 ready 消息。 */
    OperationResult purgeQueue(ClusterRef clusterRef, String name);

    /** 创建绑定。 */
    RabbitBinding bind(ClusterRef clusterRef, String exchange, String queue, String routingKey, Map<String, String> headers);

    /** 删除绑定。 */
    OperationResult unbind(ClusterRef clusterRef, String exchange, String queue, String routingKey);

    /** 计算给定 exchange / routing key / headers 会命中哪些队列。 */
    RabbitRoutePreview previewRoute(ClusterRef clusterRef, String exchange, String routingKey, Map<String, String> headers);

    /**
     * 增加常驻消费者。
     *
     * @param prefetch          预取数（0 表示不限制）
     * @param ackMode           auto / manual
     * @param processingDelayMs 每条消息处理耗时
     * @param nackRatio         nack 比例（0~1，仅 manual 生效）
     * @param requeue           nack 时是否重新入队
     */
    RabbitConsumer addConsumer(ClusterRef clusterRef, String queue, int prefetch, String ackMode,
                               long processingDelayMs, double nackRatio, boolean requeue);

    /** 移除常驻消费者，未确认的消息会回到队列。 */
    OperationResult removeConsumer(ClusterRef clusterRef, String consumerId);
}
