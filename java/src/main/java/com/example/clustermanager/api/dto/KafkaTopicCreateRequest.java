package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * 创建 Kafka topic 请求。
 *
 * @param name              topic 名称
 * @param partitions        分区数（默认 3）
 * @param replicationFactor 副本因子（默认 3）
 * @param configs           topic 级配置（可为 null）
 */
public record KafkaTopicCreateRequest(
        @NotBlank String name,
        @Min(1) @Max(64) Integer partitions,
        @Min(1) @Max(3) Integer replicationFactor,
        Map<String, String> configs
) {
}
