package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * 调整常驻消费者速率请求。
 *
 * @param ratePerSecond 每秒处理条数（0 表示暂停）
 */
public record KafkaConsumerRateRequest(
        @NotNull @Min(0) @Max(10000) Integer ratePerSecond
) {
}
