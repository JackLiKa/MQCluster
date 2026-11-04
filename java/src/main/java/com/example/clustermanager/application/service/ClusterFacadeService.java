package com.example.clustermanager.application.service;

import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.model.MessageSimulationCommand;
import com.example.clustermanager.application.model.NodeOperationCommand;
import com.example.clustermanager.application.model.ServiceRegistrationCommand;
import com.example.clustermanager.core.model.ClusterTopology;
import com.example.clustermanager.core.model.LogEntry;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.core.model.MessageSimulationResult;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.ProviderDescriptor;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 集群門面服務 —— application 層唯一編排點，統一對外暴露所有集群用例。
 *
 * <p>在六邊形架構中，此服務是 api 層控制器與 infrastructure 層 provider 之間的<strong>唯一編排點</strong>：
 * 控制器不應繞過它直接調用 provider，所有用例必須經過此門面，以保證編排邏輯集中、可測試、可擴展。
 *
 * <p>編排流程：接收 {@link ClusterSelection} 或具體命令 → 通過 {@link ClusterProviderRegistry}
 * 按 {@code (mode, middleware)} 解析適配器 → 將 {@code ClusterSelection} 轉換為 core 層
 * {@code ClusterRef} → 委託 {@code IClusterProvider} 執行具體操作並返回 core 層結果對象。
 *
 * <p>與各層關係：
 * <ul>
 *   <li>api 層：控制器依賴此門面，構造命令對象後調用對應方法。</li>
 *   <li>application 層：此服務與 {@link ClusterProviderRegistry} 協作完成編排。</li>
 *   <li>core 層：返回 {@code ClusterTopology}、{@code OperationResult} 等領域模型，不暴露 infrastructure 細節。</li>
 *   <li>infrastructure 層：provider 實現由 registry 注入，門面不直接依賴具體適配器類型。</li>
 * </ul>
 */
@Service
public class ClusterFacadeService {

    /** Provider 註冊表，用於按 {@code (mode, middleware)} 解析適配器。 */
    private final ClusterProviderRegistry providerRegistry;

    /**
     * 構造門面服務，注入 provider 註冊表。
     *
     * @param providerRegistry provider 註冊表（Spring 自動注入所有 {@code IClusterProvider} bean）
     */
    public ClusterFacadeService(ClusterProviderRegistry providerRegistry) {
        this.providerRegistry = providerRegistry;
    }

    /**
     * 列出所有已註冊的 provider 描述符，供前端展示可選集群類型。
     *
     * @return provider 描述符列表，每項包含 mode、middleware、clusterId 等元信息
     */
    public List<ProviderDescriptor> listProviders() {
        return providerRegistry.listProviders();
    }

    /**
     * 加載指定集群的拓撲結構（節點、鏈路、服務登記等）。
     *
     * @param cluster 目標集群選擇
     * @return 集群拓撲領域模型
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public ClusterTopology loadTopology(ClusterSelection cluster) {
        return providerRegistry.resolve(cluster).loadTopology(cluster.toClusterRef());
    }

    /**
     * 加載指定集群的監控指標快照。
     *
     * @param cluster 目標集群選擇
     * @return 監控指標快照（節點狀態、流量、延遲等）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public MonitoringSnapshot loadMetrics(ClusterSelection cluster) {
        return providerRegistry.resolve(cluster).loadMetrics(cluster.toClusterRef());
    }

    /**
     * 加載指定節點的審計日誌。
     *
     * @param cluster 目標集群選擇
     * @param nodeId  節點 id；為 null 時由 provider 決定是否返回全集群日誌
     * @param limit   返回日誌條數上限
     * @return 日誌條目列表（按時間倒序，最多 {@code limit} 條）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public List<LogEntry> loadLogs(ClusterSelection cluster, String nodeId, int limit) {
        return providerRegistry.resolve(cluster).loadLogs(cluster.toClusterRef(), nodeId, limit);
    }

    /**
     * 執行節點生命周期操作（啟停等）。
     *
     * <p>委託流程：解析 provider → 轉換 {@code ClusterRef} → 調用
     * {@code IClusterProvider#operateNode}。PSEUDO provider 調用嵌入式 runtime 啟停節點；
     * REAL provider 記錄審計日誌。
     *
     * @param command 節點操作命令（含集群選擇、節點 id、操作類型）
     * @return 操作結果（成功/失敗 + 描述）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public OperationResult operateNode(NodeOperationCommand command) {
        return providerRegistry.resolve(command.cluster())
                .operateNode(command.cluster().toClusterRef(), command.nodeId(), command.operationType());
    }

    /**
     * 執行集群級一鍵操作（啟動/停止/重啟所有節點），啟停順序由 provider 決定。
     *
     * @param cluster       目標集群選擇
     * @param operationType 操作類型
     * @return 每個節點的操作結果（按執行順序）
     * @throws IllegalArgumentException 若 provider 不存在或不支持集群級操作
     */
    public List<OperationResult> operateCluster(ClusterSelection cluster, OperationType operationType) {
        return providerRegistry.resolve(cluster).operateCluster(cluster.toClusterRef(), operationType);
    }

    /**
     * 解析 provider 的專屬能力端口（如 Kafka/RabbitMQ 檢查器）。
     *
     * @param cluster    目標集群選擇
     * @param capability 能力接口類型
     * @param <T>        能力類型
     * @return provider 實現的能力端口
     * @throws IllegalArgumentException 若 provider 未實現該能力
     */
    public <T> T capability(ClusterSelection cluster, Class<T> capability) {
        return providerRegistry.resolveCapability(cluster, capability);
    }

    /**
     * 登記業務服務到集群節點。
     *
     * <p>委託流程：解析 provider → 轉換 {@code ClusterRef} → 調用
     * {@code IClusterProvider#registerService}。provider 將登記信息合併進 topology 並記錄審計日誌。
     *
     * @param command 服務登記命令（含集群選擇、服務登記信息）
     * @return 操作結果（成功/失敗 + 描述）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public OperationResult registerService(ServiceRegistrationCommand command) {
        return providerRegistry.resolve(command.cluster())
                .registerService(command.cluster().toClusterRef(), command.service());
    }

    /**
     * 刪除指定節點上登記的服務。
     *
     * @param cluster 目標集群選擇
     * @param nodeId  待刪除服務的節點 id
     * @return 操作結果（成功/失敗 + 描述）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public OperationResult deleteService(ClusterSelection cluster, String nodeId) {
        return providerRegistry.resolve(cluster).deleteService(cluster.toClusterRef(), nodeId);
    }

    /**
     * 執行消息模擬（生產 + 消費）。
     *
     * <p>編排流程：從命令組裝 core 層 {@link MessageScenario} → 解析 provider → 轉換
     * {@code ClusterRef} → 調用 {@code IClusterProvider#simulate}。PSEUDO provider 由
     * {@code EmbeddedMessageWorkbench} 統一處理嵌入式或外部 NameServer 消息路徑；
     * REAL provider 通過 RocketMQ admin 執行。
     *
     * @param command 消息模擬命令（含集群選擇、topic、消費組、消息數、模板、節點 id、消息頭等）
     * @return 模擬結果（發送/消費統計、延遲、失敗明細等）
     * @throws IllegalArgumentException 若 {@code (mode, middleware)} 無對應 provider
     */
    public MessageSimulationResult simulateMessages(MessageSimulationCommand command) {
        MessageScenario scenario = new MessageScenario(
                command.topic(),
                command.consumerGroup(),
                command.messageCount(),
                command.payloadTemplate(),
                command.producerNodeId(),
                command.consumerNodeIds(),
                command.headers(),
                command.options()
        );
        return providerRegistry.resolve(command.cluster())
                .simulate(command.cluster().toClusterRef(), scenario);
    }
}
