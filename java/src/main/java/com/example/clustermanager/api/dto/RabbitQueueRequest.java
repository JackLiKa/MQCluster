package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * 声明 RabbitMQ 队列请求。
 *
 * @param name               队列名称
 * @param deadLetterExchange 死信交换机（x-dead-letter-exchange，可为 null）
 * @param messageTtlMs       队列级消息 TTL（x-message-ttl，可为 null）
 */
public record RabbitQueueRequest(
        @NotBlank String name,
        String deadLetterExchange,
        @Min(1) @Max(3_600_000) Long messageTtlMs
) {
}
