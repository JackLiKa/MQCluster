package com.example.clustermanager.core.model.rabbit;

import java.util.List;
import java.util.Map;

/**
 * 路由预览结果（纯计算，不发送消息）。
 *
 * @param exchange        交换机
 * @param exchangeType    交换机类型
 * @param routingKey      routing key
 * @param headers         消息头
 * @param matchedBindings 命中的绑定
 * @param matchedQueues   命中的队列（去重，按名称排序）
 * @param explanation     中文说明
 */
public record RabbitRoutePreview(
        String exchange,
        String exchangeType,
        String routingKey,
        Map<String, String> headers,
        List<RabbitBinding> matchedBindings,
        List<String> matchedQueues,
        String explanation
) {
}
