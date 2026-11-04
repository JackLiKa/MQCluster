package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * 增加 RabbitMQ 常驻消费者请求。
 *
 * @param queue             订阅的队列
 * @param prefetch          预取数（默认 10，0 表示不限制）
 * @param ackMode           auto / manual（默认 manual）
 * @param processingDelayMs 每条消息处理耗时（默认 200ms）
 * @param nackRatio         nack 比例 0~1（默认 0）
 * @param requeue           nack 时是否重新入队（默认 false）
 */
public record RabbitConsumerRequest(
        @NotBlank String queue,
        @Min(0) @Max(1000) Integer prefetch,
        @Pattern(regexp = "auto|manual", message = "ackMode 仅支持 auto / manual") String ackMode,
        @Min(0) @Max(10_000) Long processingDelayMs,
        @DecimalMin("0.0") @DecimalMax("1.0") Double nackRatio,
        Boolean requeue
) {
}
