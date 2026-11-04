package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * 声明 RabbitMQ 交换机请求。
 *
 * @param name    交换机名称
 * @param type    direct / fanout / topic / headers
 * @param durable 是否持久化（默认 true）
 */
public record RabbitExchangeRequest(
        @NotBlank String name,
        @NotBlank @Pattern(regexp = "direct|fanout|topic|headers", message = "交换机类型仅支持 direct / fanout / topic / headers") String type,
        Boolean durable
) {
}
