package com.example.clustermanager.core.model.rabbit;

import java.util.Map;

/**
 * 交换机到队列的绑定。
 *
 * @param source      源交换机
 * @param destination 目标队列
 * @param routingKey  绑定键（binding key）
 * @param headers     headers 交换机的匹配参数（含 {@code x-match}）
 */
public record RabbitBinding(String source, String destination, String routingKey, Map<String, String> headers) {

    public RabbitBinding {
        routingKey = routingKey == null ? "" : routingKey;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
