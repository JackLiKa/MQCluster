package com.example.clustermanager.application.model;

import java.util.List;
import java.util.Map;

/**
 * 消息模擬命令 record —— 應用層用於編排「消息生產/消費模擬」用例的不可變命令對象。
 *
 * <p>由 api 層控制器從請求體構造，傳入
 * {@link com.example.clustermanager.application.service.ClusterFacadeService#simulateMessages(MessageSimulationCommand)}，
 * 門面再將其轉換為 core 層 {@code MessageScenario} 並委託給對應 provider 執行模擬。
 *
 * <p>與各層關係：
 * <ul>
 *   <li>api 層：DTO 校驗後映射為本命令。</li>
 *   <li>application 層：門面組裝 {@code MessageScenario} 並路由 provider。</li>
 *   <li>core 層：{@code MessageScenario} 為領域模型，provider 據此執行模擬。</li>
 *   <li>infrastructure 層：{@code EmbeddedMessageWorkbench}（PSEUDO）或 RocketMQ admin（REAL）執行實際發送/消費。</li>
 * </ul>
 *
 * @param cluster         目標集群選擇，用於解析 provider 適配器
 * @param topic           消息目標 topic 名稱
 * @param consumerGroup   消費者組名稱
 * @param messageCount    模擬發送的消息條數
 * @param payloadTemplate 消息體模板（可含佔位符），用於生成消息 payload
 * @param producerNodeId  生產者節點 id，指向集群中承擔發送角色的節點
 * @param consumerNodeIds 消費者節點 id 列表，指向集群中承擔消費角色的節點集合
 * @param headers         附加消息頭（鍵值對），隨消息一同傳遞
 * @param options         中間件專屬選項（Kafka 的 key/partition/acks，RabbitMQ 的 exchange/routingKey 等），可為 null
 */
public record MessageSimulationCommand(
        ClusterSelection cluster,
        String topic,
        String consumerGroup,
        int messageCount,
        String payloadTemplate,
        String producerNodeId,
        List<String> consumerNodeIds,
        Map<String, String> headers,
        Map<String, String> options
) {

    /**
     * 兼容舊調用方的構造器（無 options）。
     */
    public MessageSimulationCommand(
            ClusterSelection cluster,
            String topic,
            String consumerGroup,
            int messageCount,
            String payloadTemplate,
            String producerNodeId,
            List<String> consumerNodeIds,
            Map<String, String> headers
    ) {
        this(cluster, topic, consumerGroup, messageCount, payloadTemplate, producerNodeId, consumerNodeIds, headers, null);
    }
}
