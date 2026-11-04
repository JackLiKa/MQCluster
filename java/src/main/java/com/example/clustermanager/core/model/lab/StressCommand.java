package com.example.clustermanager.core.model.lab;

import java.util.Map;

/**
 * 压力实验参数。
 *
 * <p>一次实验用 N 个线程按目标速率持续向真实 broker 投递消息，直到时长耗尽或手动停止。
 * 各 MQ 特有的参数通过 {@link #options()} 承载：
 * <ul>
 *   <li>Kafka：{@code bufferMemoryBytes}、{@code maxBlockMs}、{@code lingerMs}、{@code batchSize}</li>
 *   <li>RocketMQ：{@code sendTimeoutMs}</li>
 *   <li>RabbitMQ：{@code exchange}、{@code routingKey}、{@code confirmTimeoutMs}、{@code inflight}</li>
 * </ul>
 *
 * @param topic           目标 topic（RabbitMQ 用作 routingKey）
 * @param targetRate      目标投递速率（条/秒）
 * @param threads         并发生产者线程数
 * @param durationSeconds 持续时长（秒）
 * @param payloadBytes    每条消息负载字节数
 * @param acks            Kafka/RocketMQ 的确认级别：0 / 1 / all
 * @param options         各 MQ 特有参数，可为 null
 */
public record StressCommand(
        String topic,
        Integer targetRate,
        Integer threads,
        Integer durationSeconds,
        Integer payloadBytes,
        String acks,
        Map<String, String> options
) {

    private static final int DEFAULT_RATE = 500;
    private static final int MAX_RATE = 50_000;

    /** 目标速率，缺省 500，范围 1..50000。 */
    public int resolvedTargetRate() {
        int value = targetRate == null ? DEFAULT_RATE : targetRate;
        return Math.max(1, Math.min(value, MAX_RATE));
    }

    /** 并发线程数，缺省 4，范围 1..32。 */
    public int resolvedThreads() {
        int value = threads == null ? 4 : threads;
        return Math.max(1, Math.min(value, 32));
    }

    /** 持续时长（秒），缺省 30，范围 3..300。 */
    public int resolvedDurationSeconds() {
        int value = durationSeconds == null ? 30 : durationSeconds;
        return Math.max(3, Math.min(value, 300));
    }

    /** 单条负载字节数，缺省 256，范围 16..65536。 */
    public int resolvedPayloadBytes() {
        int value = payloadBytes == null ? 256 : payloadBytes;
        return Math.max(16, Math.min(value, 65_536));
    }

    /** 目标 topic，缺省使用 {@code lab-stress}。 */
    public String resolvedTopic() {
        return topic == null || topic.isBlank() ? "lab-stress" : topic.trim();
    }

    /** 确认级别，缺省 {@code all}。 */
    public String resolvedAcks() {
        return acks == null || acks.isBlank() ? "all" : acks.trim().toLowerCase(java.util.Locale.ROOT);
    }

    /** 读取字符串参数，缺失时返回默认值。 */
    public String option(String key, String fallback) {
        if (options == null) {
            return fallback;
        }
        String value = options.get(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /** 读取整数参数，缺失或非法时返回默认值。 */
    public int optionInt(String key, int fallback) {
        String value = option(key, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    /** 读取长整数参数，缺失或非法时返回默认值。 */
    public long optionLong(String key, long fallback) {
        String value = option(key, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    /** 读取布尔参数，缺失时返回默认值。 */
    public boolean optionBoolean(String key, boolean fallback) {
        String value = option(key, null);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }
}
