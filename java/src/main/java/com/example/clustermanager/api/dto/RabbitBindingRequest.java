package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * 创建 / 删除 RabbitMQ 绑定请求。
 *
 * @param exchange   源交换机
 * @param queue      目标队列
 * @param routingKey binding key（fanout / headers 可为空）
 * @param headers    headers 交换机的匹配参数（含 x-match）
 */
public record RabbitBindingRequest(
        @NotBlank String exchange,
        @NotBlank String queue,
        String routingKey,
        Map<String, String> headers
) {
}
