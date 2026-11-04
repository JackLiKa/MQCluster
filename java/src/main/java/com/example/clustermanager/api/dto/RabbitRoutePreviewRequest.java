package com.example.clustermanager.api.dto;

import java.util.Map;

/**
 * 路由预览请求：纯计算，不发送消息。
 *
 * @param exchange   交换机（空字符串表示默认交换机）
 * @param routingKey routing key
 * @param headers    消息头
 */
public record RabbitRoutePreviewRequest(
        String exchange,
        String routingKey,
        Map<String, String> headers
) {
}
