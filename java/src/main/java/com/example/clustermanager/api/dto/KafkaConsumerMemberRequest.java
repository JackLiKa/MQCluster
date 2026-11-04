package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * 新增常驻消费者成员请求。
 *
 * @param topic         订阅的 topic
 * @param ratePerSecond 每秒处理条数（0 表示暂停消费，默认 5）
 */
public record KafkaConsumerMemberRequest(
        @NotBlank String topic,
        @Min(0) @Max(10000) Integer ratePerSecond
) {
}
