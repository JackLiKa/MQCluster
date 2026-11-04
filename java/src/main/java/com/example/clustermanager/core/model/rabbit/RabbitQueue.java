package com.example.clustermanager.core.model.rabbit;

import java.util.Map;

/**
 * RabbitMQ 队列及其实时统计。
 *
 * @param name               队列名称
 * @param ready              待投递消息数（ready）
 * @param unacked            已投递未确认消息数（unacked）
 * @param consumers          消费者数量
 * @param arguments          声明参数（含平台翻译后的 Qpid 参数）
 * @param deadLetterExchange 死信交换机（未配置为 {@code null}）
 * @param messageTtlMs       队列级消息 TTL（毫秒，未配置为 {@code null}）
 * @param publishRate        入队速率（条/秒）
 * @param deliverRate        出队（确认/丢弃）速率（条/秒）
 * @param totalEnqueued      累计入队数
 * @param totalDequeued      累计出队数
 */
public record RabbitQueue(
        String name,
        long ready,
        long unacked,
        int consumers,
        Map<String, String> arguments,
        String deadLetterExchange,
        Long messageTtlMs,
        double publishRate,
        double deliverRate,
        long totalEnqueued,
        long totalDequeued
) {

    public RabbitQueue {
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
