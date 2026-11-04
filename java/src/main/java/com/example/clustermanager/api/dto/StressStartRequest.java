package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * 启动压力实验的请求体。
 *
 * @param topic           目标 topic（RabbitMQ 用作 routingKey）
 * @param targetRate      目标速率（条/秒），1..50000
 * @param threads         并发生产者线程数，1..32
 * @param durationSeconds 持续时长（秒），3..300
 * @param payloadBytes    单条负载字节数，16..65536
 * @param acks            确认级别：0 / 1 / all
 * @param options         各 MQ 特有参数（bufferMemoryBytes、maxBlockMs、exchange、confirmTimeoutMs 等）
 */
public record StressStartRequest(
        @NotBlank String topic,
        @Min(1) @Max(50_000) Integer targetRate,
        @Min(1) @Max(32) Integer threads,
        @Min(3) @Max(300) Integer durationSeconds,
        @Min(16) @Max(65_536) Integer payloadBytes,
        String acks,
        Map<String, String> options
) {
}
