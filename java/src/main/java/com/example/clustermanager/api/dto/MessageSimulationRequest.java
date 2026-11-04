package com.example.clustermanager.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;

/**
 * 消息模拟请求 DTO，对应 POST /api/clusters/{...}/messages/simulate 端点的请求体。
 *
 * <p>API 层角色：承载前端 MessageWorkbenchCard 提交的消息生产/消费模拟参数，
 * 经 {@link com.example.clustermanager.api.controller.ClusterController} 转换为
 * {@link com.example.clustermanager.application.model.MessageSimulationCommand} 后
 * 委托 {@link com.example.clustermanager.application.service.ClusterFacadeService} 执行。
 *
 * @param topic            目标 topic 名称，不能为空白
 * @param consumerGroup    消费者组名称，不能为空白
 * @param messageCount     模拟消息数量，最小值 1
 * @param payloadTemplate  消息体模板，可为 null（使用默认模板）
 * @param producerNodeId   生产者节点 ID，不能为空白
 * @param consumerNodeIds  消费者节点 ID 列表，可为 null 或空
 * @param headers          消息头键值对，可为 null
 * @param options          中间件专属选项（Kafka：key/partition/acks/compression；RabbitMQ：exchange/routingKey/ackMode 等），可为 null
 */
public record MessageSimulationRequest(
        @NotBlank String topic,
        @NotBlank String consumerGroup,
        @Min(1) int messageCount,
        String payloadTemplate,
        @NotBlank String producerNodeId,
        List<String> consumerNodeIds,
        Map<String, String> headers,
        Map<String, String> options
) {
}
