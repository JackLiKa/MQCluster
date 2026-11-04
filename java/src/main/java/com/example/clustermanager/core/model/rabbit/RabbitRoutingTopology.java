package com.example.clustermanager.core.model.rabbit;

import java.time.Instant;
import java.util.List;

/**
 * 完整路由拓扑：Exchange → Binding → Queue → Consumer。
 *
 * @param capturedAt 采集时间
 * @param exchanges  交换机（含内置）
 * @param queues     队列
 * @param bindings   显式绑定（不含默认交换机的隐式绑定）
 * @param consumers  平台托管的常驻消费者
 */
public record RabbitRoutingTopology(
        Instant capturedAt,
        List<RabbitExchange> exchanges,
        List<RabbitQueue> queues,
        List<RabbitBinding> bindings,
        List<RabbitConsumer> consumers
) {
}
