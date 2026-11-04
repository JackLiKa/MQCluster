package com.example.clustermanager.core.model.rabbit;

/**
 * 平台托管的常驻消费者。
 *
 * @param consumerId        平台成员 ID（如 {@code c1}）
 * @param queue             消费的队列
 * @param prefetch          basicQos 预取数（0 表示不限制）
 * @param ackMode           确认模式：auto / manual
 * @param processingDelayMs 每条消息的模拟处理耗时（毫秒）
 * @param nackRatio         手动确认模式下 nack 的比例（0~1）
 * @param requeue           nack 时是否重新入队
 * @param active            是否仍在消费
 * @param acked             已确认条数
 * @param nacked            已拒绝条数
 * @param inFlight          当前已投递未确认条数
 */
public record RabbitConsumer(
        String consumerId,
        String queue,
        int prefetch,
        String ackMode,
        long processingDelayMs,
        double nackRatio,
        boolean requeue,
        boolean active,
        long acked,
        long nacked,
        int inFlight
) {
}
