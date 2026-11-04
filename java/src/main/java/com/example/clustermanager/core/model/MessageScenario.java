package com.example.clustermanager.core.model;

import java.util.List;
import java.util.Map;

/**
 * 消息模拟场景，领域核心层的不可变值对象。
 *
 * <p>描述一次消息模拟的全部输入参数：目标 topic、消费组、消息数量、负载模板、
 * 生产者节点、消费者节点列表、附加消息头及中间件专属选项。由 {@code IMessageWorkbench#simulate}
 * 接收并执行。作为 record 不可变；集合字段应由调用方以不可变集合提供。
 */
public record MessageScenario(
        /** 目标 Topic 名称（RabbitMQ 下为默认队列/routing key 的兜底值）。 */
        String topic,
        /** 消费者组名称，用于标识一组共同消费同一 Topic 的消费者。 */
        String consumerGroup,
        /** 本次模拟要发送的消息总条数，应为正整数。 */
        int messageCount,
        /** 消息负载模板，用于生成消息体；可含占位符由适配器替换。 */
        String payloadTemplate,
        /** 生产者节点标识，对应 {@link ClusterNode#nodeId()}。 */
        String producerNodeId,
        /** 消费者节点标识列表，元素对应 {@link ClusterNode#nodeId()}；不应为 {@code null}。 */
        List<String> consumerNodeIds,
        /** 附加消息头，键值对形式；不应为 {@code null}。 */
        Map<String, String> headers,
        /** 中间件专属选项（Kafka 的 key/partition/acks，RabbitMQ 的 exchange/routingKey/ackMode 等）；不为 {@code null}。 */
        Map<String, String> options
) {

    public MessageScenario {
        options = options == null ? Map.of() : Map.copyOf(options);
    }

    /**
     * 兼容旧调用方的构造器（无 options）。
     */
    public MessageScenario(
            String topic,
            String consumerGroup,
            int messageCount,
            String payloadTemplate,
            String producerNodeId,
            List<String> consumerNodeIds,
            Map<String, String> headers
    ) {
        this(topic, consumerGroup, messageCount, payloadTemplate, producerNodeId, consumerNodeIds, headers, Map.of());
    }

    /**
     * 读取选项值，缺失或空白时返回默认值。
     *
     * @param key          选项名
     * @param defaultValue 默认值
     * @return 选项值
     */
    public String option(String key, String defaultValue) {
        String value = options.get(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
