package com.example.clustermanager.infrastructure.pseudo;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.ClusterTopology;
import com.example.clustermanager.core.model.LogEntry;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.core.model.MessageSimulationResult;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import com.example.clustermanager.core.port.IInternalsInspector;
import com.example.clustermanager.core.port.IStressLab;
import com.example.clustermanager.infrastructure.pseudo.lab.RocketMqInternalsInspector;
import com.example.clustermanager.infrastructure.pseudo.lab.RocketMqStressLab;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.NetworkLink;
import com.example.clustermanager.core.model.NodeMetrics;
import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.ProviderCapabilities;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.model.ServiceRegistration;
import com.example.clustermanager.core.port.IClusterProvider;
import com.example.clustermanager.core.port.IVirtualNetwork;
import com.example.clustermanager.infrastructure.pseudo.messaging.EmbeddedMessageWorkbench;
import com.example.clustermanager.infrastructure.pseudo.node.ManagedNode;
import com.example.clustermanager.infrastructure.pseudo.node.NodeKind;
import com.example.clustermanager.infrastructure.pseudo.node.NodeRegistry;
import com.example.clustermanager.infrastructure.pseudo.runtime.EmbeddedRocketMqRuntime;
import com.example.clustermanager.infrastructure.pseudo.topology.PseudoTopologySeeder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

/**
 * 偽集群 Provider（瘦身編排器）——實現 {@link IClusterProvider}。
 *
 * <p>這是 PSEUDO 模式的核心適配器，在六邊形架構中位於 infrastructure 層。
 * 職責僅限編排，不包含具體的運行時邏輯，所有實際操作委託給子組件：
 * <ul>
 *   <li>種子化拓撲 → 委託 {@link PseudoTopologySeeder}（種子化 3 個默認節點）</li>
 *   <li>節點生命週期（啟動/停止/重啟）→ 委託 {@link EmbeddedRocketMqRuntime}</li>
 *   <li>消息模擬（produce/consume）→ 委託 {@link EmbeddedMessageWorkbench}</li>
 *   <li>節點元數據存儲與狀態流轉 → 委託 {@link NodeRegistry}</li>
 *   <li>審計日誌記錄 → 委託 {@link AuditLog}</li>
 *   <li>虛擬網絡 IP 分配 → 委託 {@link IVirtualNetwork}（TapVirtualNetwork）</li>
 * </ul>
 *
 * <p><b>初始化流程</b>：首次調用任意接口方法時觸發 {@link #initializeIfNecessary()}，
 * 該方法通過 {@code AtomicBoolean.compareAndSet} 保證只執行一次：
 * <ol>
 *   <li>調用 {@link PseudoTopologySeeder#seedIfNeeded()} 種子化默認節點</li>
 *   <li>若 {@code autoStart=true}，按「先 NameServer 後 Broker」順序啟動所有虛擬節點</li>
 * </ol>
 *
 * <p><b>線程安全</b>：組件本身無可變狀態（{@code initialized} 為 AtomicBoolean），
 * 所有狀態管理委託給線程安全的子組件。Spring 單例，構造器注入。
 */
@Component
public class PseudoClusterProvider implements IClusterProvider, IStressLab, IInternalsInspector {

    /** Provider 描述符——標識為 PSEUDO 模式 + ROCKETMQ 中間件，供 {@code ClusterProviderRegistry} 路由；clusterId 來自配置 */
    private final ProviderDescriptor descriptor;

    /** 偽集群配置屬性（cluster-id、CIDR、work-dir、auto-start 等） */
    private final PseudoClusterProperties properties;
    /** 虛擬網絡適配器，負責 TAP 段管理與虛擬 IP 分配 */
    private final IVirtualNetwork virtualNetwork;
    /** 節點註冊表，存儲所有節點的元數據與狀態（線程安全） */
    private final NodeRegistry nodeRegistry;
    /** 嵌入式 RocketMQ 運行時，管理 NameServer/Broker 進程內生命週期 */
    private final EmbeddedRocketMqRuntime runtime;
    /** 消息工作台，統一處理嵌入式與 HOST 路徑的消息模擬 */
    private final EmbeddedMessageWorkbench messageWorkbench;
    /** 拓撲種子化器，負責初始化默認的 3 節點拓撲 */
    private final PseudoTopologySeeder topologySeeder;
    /** 壓力實驗：用真實 DefaultMQProducer 持續投遞，暴露 broker 流控與拒絕 */
    private final RocketMqStressLab stressLab;
    /** 內部機制檢查器：讀 CommitLog 寫指針、ConsumeQueue 構建進度 */
    private final RocketMqInternalsInspector internalsInspector;
    /** 共享審計日誌，記錄節點操作與服務變更 */
    private final AuditLog auditLog;
    /** 初始化標記——AtomicBoolean 保證 {@link #initializeIfNecessary()} 只執行一次，線程安全 */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * 構造器注入所有子組件。Spring 自動裝配，無需 {@code @Autowired}。
     *
     * @param properties      偽集群配置屬性
     * @param virtualNetwork  虛擬網絡適配器
     * @param nodeRegistry    節點註冊表
     * @param runtime         嵌入式 RocketMQ 運行時
     * @param messageWorkbench 消息工作台
     * @param topologySeeder  拓撲種子化器
     * @param auditLog        審計日誌
     */
    public PseudoClusterProvider(
            PseudoClusterProperties properties,
            IVirtualNetwork virtualNetwork,
            NodeRegistry nodeRegistry,
            EmbeddedRocketMqRuntime runtime,
            EmbeddedMessageWorkbench messageWorkbench,
            PseudoTopologySeeder topologySeeder,
            AuditLog auditLog,
            RocketMqStressLab stressLab,
            RocketMqInternalsInspector internalsInspector
    ) {
        this.properties = properties;
        this.virtualNetwork = virtualNetwork;
        this.nodeRegistry = nodeRegistry;
        this.runtime = runtime;
        this.messageWorkbench = messageWorkbench;
        this.topologySeeder = topologySeeder;
        this.auditLog = auditLog;
        this.stressLab = stressLab;
        this.internalsInspector = internalsInspector;
        this.descriptor = new ProviderDescriptor(
                "pseudo-rocketmq",
                "RocketMQ 嵌入式集群",
                com.example.clustermanager.core.model.ClusterMode.PSEUDO,
                MiddlewareType.ROCKETMQ,
                properties.clusterId(),
                Set.of(ProviderCapabilities.CONNECTION_CONFIG,
                        ProviderCapabilities.SERVICE_REGISTRATION,
                        ProviderCapabilities.CLUSTER_OPERATIONS,
                        ProviderCapabilities.LAB)
        );
    }

    /**
     * 返回此 Provider 的描述符，供 {@code ClusterProviderRegistry} 按 (ClusterMode, MiddlewareType) 路由。
     *
     * @return 固定描述符：pseudo-rocketmq / PSEUDO / ROCKETMQ
     */
    @Override
    public ProviderDescriptor descriptor() {
        return descriptor;
    }

    // ---------------------------------------------------------------- 压力实验与内部机制

    @Override
    public StressStatus start(ClusterRef clusterRef, StressCommand command) {
        return stressLab.start(clusterRef, command);
    }

    @Override
    public StressStatus stop(ClusterRef clusterRef) {
        return stressLab.stop(clusterRef);
    }

    @Override
    public StressStatus status(ClusterRef clusterRef) {
        return stressLab.status(clusterRef);
    }

    @Override
    public InternalsSnapshot inspect(ClusterRef clusterRef) {
        return internalsInspector.inspect(clusterRef);
    }

    /**
     * 加載集群拓撲。首次調用觸發延遲初始化，之後每次調用會對帳 auto-start 狀態。
     *
     * @param clusterRef 集群引用（含 clusterId）
     * @return 包含所有節點、網絡鏈路和活躍 VIP 的拓撲快照
     */
    @Override
    public ClusterTopology loadTopology(ClusterRef clusterRef) {
        initializeIfNecessary();
        reconcileAutoStart();
        List<com.example.clustermanager.core.model.ClusterNode> topologyNodes = nodeRegistry.ordered().stream()
                .map(ManagedNode::toClusterNode)
                .toList();
        List<NetworkLink> links = buildLinks(topologyNodes);
        return new ClusterTopology(clusterRef, topologyNodes, links, activeVip());
    }

    /**
     * 對指定節點執行操作（啟動/停止/重啟）。
     *
     * <p>HOST 節點僅做狀態標記（不涉及嵌入式運行時）；VIRTUAL 節點委託 {@link EmbeddedRocketMqRuntime}
     * 執行真實的進程內啟停，操作完成後同步節點狀態到註冊表。
     *
     * @param clusterRef    集群引用
     * @param nodeId        目標節點 ID
     * @param operationType 操作類型（START / STOP / RESTART）
     * @return 操作結果（始終成功，失敗時 runtime 會拋異常由全局異常處理器轉換）
     * @throws IllegalArgumentException 節點不存在時拋出
     * @throws IllegalStateException    嵌入式節點啟停失敗時拋出（如端口衝突、NameServer 未啟動）
     */
    @Override
    public OperationResult operateNode(ClusterRef clusterRef, String nodeId, OperationType operationType) {
        initializeIfNecessary();
        ManagedNode node = nodeRegistry.require(nodeId);
        if (node.hostBound()) {
            // HOST 節點無嵌入式運行時，僅做狀態流轉
            NodeStatus target = switch (operationType) {
                case START, RESTART -> NodeStatus.RUNNING;
                case STOP -> NodeStatus.STOPPED;
            };
            nodeRegistry.transition(nodeId, target);
            auditLog.append(nodeId, "INFO", "Host-bound pseudo node marked as " + target);
            return new OperationResult(nodeId, operationType, true, "Host-bound pseudo node transitioned to " + target);
        }
        // VIRTUAL 節點委託嵌入式運行時執行真實啟停
        switch (operationType) {
            case START -> runtime.start(node);
            case STOP -> runtime.stop(nodeId);
            case RESTART -> runtime.restart(node);
        }
        nodeRegistry.transition(nodeId, runtime.status(nodeId));
        return new OperationResult(nodeId, operationType, true, "Pseudo node transitioned to " + nodeRegistry.require(nodeId).status());
    }

    /**
     * 一鍵啟停整個偽集群。啟動按「先 NameServer 後 Broker」，停止按「先 Broker 後 NameServer」；
     * 已處於目標狀態的節點跳過。RESTART 等價於先整體停止再整體啟動。
     *
     * @param clusterRef    集群引用
     * @param operationType 操作類型
     * @return 每個節點的操作結果（按執行順序）
     */
    @Override
    public List<OperationResult> operateCluster(ClusterRef clusterRef, OperationType operationType) {
        initializeIfNecessary();
        List<OperationResult> results = new ArrayList<>();
        if (operationType == OperationType.STOP || operationType == OperationType.RESTART) {
            List<ManagedNode> stopOrder = new ArrayList<>(orderedForStart());
            java.util.Collections.reverse(stopOrder);
            for (ManagedNode node : stopOrder) {
                if (nodeRegistry.require(node.nodeId()).status() == NodeStatus.STOPPED) {
                    results.add(new OperationResult(node.nodeId(), OperationType.STOP, true, "已停止，跳過"));
                    continue;
                }
                results.add(operateNode(clusterRef, node.nodeId(), OperationType.STOP));
            }
        }
        if (operationType == OperationType.START || operationType == OperationType.RESTART) {
            for (ManagedNode node : orderedForStart()) {
                if (nodeRegistry.require(node.nodeId()).status() == NodeStatus.RUNNING) {
                    results.add(new OperationResult(node.nodeId(), OperationType.START, true, "已運行，跳過"));
                    continue;
                }
                try {
                    results.add(operateNode(clusterRef, node.nodeId(), OperationType.START));
                } catch (RuntimeException exception) {
                    nodeRegistry.transition(node.nodeId(), NodeStatus.FAILED);
                    results.add(new OperationResult(node.nodeId(), OperationType.START, false, exception.getMessage()));
                }
            }
        }
        auditLog.append("cluster", "INFO", "Cluster operation %s finished on %s".formatted(operationType, clusterRef.clusterId()));
        return results;
    }

    /**
     * 啟動順序：NameServer 在前，其餘按註冊表順序。
     *
     * @return 排好序的節點列表
     */
    private List<ManagedNode> orderedForStart() {
        List<ManagedNode> ordered = new ArrayList<>(nodeRegistry.ordered());
        ordered.sort(Comparator.comparingInt((ManagedNode node) -> "nameserver".equals(node.role()) ? 0 : 1));
        return ordered;
    }

    /**
     * 手動註冊一個新的服務節點到偽集群。
     *
     * <p>根據 labels 中的 {@code nodeKind} 區分：
     * <ul>
     *   <li>HOST：綁定真實地址，初始狀態為 RUNNING</li>
     *   <li>VIRTUAL：分配虛擬 IP 並隔離，若 autoStart=true 則立即啟動嵌入式節點</li>
     * </ul>
     *
     * @param clusterRef   集群引用
     * @param registration 服務註冊信息（nodeId、地址、端口、角色、labels）
     * @return 操作結果
     * @throws IllegalArgumentException 節點已存在、地址重複、參數校驗失敗時拋出
     */
    @Override
    public OperationResult registerService(ClusterRef clusterRef, ServiceRegistration registration) {
        initializeIfNecessary();
        validateRegistration(registration);
        if (nodeRegistry.contains(registration.nodeId())) {
            throw new IllegalArgumentException("Node already exists: " + registration.nodeId());
        }
        NodeKind nodeKind = NodeKind.fromLabel(
                registration.labels() != null ? registration.labels().get("nodeKind") : null
        );
        ManagedNode node = new ManagedNode(
                registration.nodeId(),
                registration.displayName(),
                registration.hostName(),
                registration.role(),
                registration.port(),
                true,
                nodeKind,
                NodeStatus.STOPPED,
                null
        );
        if (nodeKind == NodeKind.HOST) {
            // HOST 節點：綁定真實地址，直接標記為 RUNNING
            nodeRegistry.register(node.withAddress(
                    "%s:%d".formatted(registration.address(), registration.port())
            ).withStatus(NodeStatus.RUNNING));
        } else {
            // VIRTUAL 節點：分配虛擬 IP、隔離、可選自動啟動
            String virtualIp = clusterRef != null
                    ? attachVirtualNode(clusterRef.clusterId(), node.nodeId(), registration.address())
                    : attachVirtualNode(properties.clusterId(), node.nodeId(), registration.address());
            nodeRegistry.register(node.withAddress(virtualIp));
            if (properties.resolvedAutoStart()) {
                runtime.start(nodeRegistry.require(node.nodeId()));
                nodeRegistry.transition(node.nodeId(), runtime.status(node.nodeId()));
            }
        }
        auditLog.append(node.nodeId(), "INFO",
                "Manual pseudo service registered at %s:%d".formatted(registration.address(), registration.port()));
        return new OperationResult(node.nodeId(), OperationType.START, true,
                "Pseudo service registered: " + nodeRegistry.require(node.nodeId()).address());
    }

    /**
     * 刪除手動註冊的服務節點。
     *
     * <p>種子化節點（managed=false）不可刪除。VIRTUAL 節點先停止嵌入式運行時再從註冊表移除。
     *
     * @param clusterRef 集群引用
     * @param nodeId     待刪除節點 ID
     * @return 操作結果
     * @throws IllegalArgumentException 節點不存在或為種子化節點時拋出
     */
    @Override
    public OperationResult deleteService(ClusterRef clusterRef, String nodeId) {
        initializeIfNecessary();
        ManagedNode node = nodeRegistry.require(nodeId);
        if (!node.managed()) {
            throw new IllegalArgumentException("Seeded pseudo services cannot be deleted: " + nodeId);
        }
        if (!node.hostBound()) {
            runtime.stop(nodeId);
        }
        nodeRegistry.remove(nodeId);
        auditLog.append(nodeId, "WARN", "Manual pseudo service deleted from cluster " + clusterRef.clusterId());
        return new OperationResult(nodeId, OperationType.STOP, true, "Pseudo service deleted");
    }

    /**
     * 執行消息模擬。委託 {@link EmbeddedMessageWorkbench} 處理，自動選擇嵌入式或 HOST 路徑。
     *
     * @param clusterRef 集群引用
     * @param scenario   消息場景（topic、producer/consumer 節點、消息數量等）
     * @return 模擬結果（含時間戳和每條消息的投遞結果）
     */
    @Override
    public MessageSimulationResult simulate(ClusterRef clusterRef, MessageScenario scenario) {
        initializeIfNecessary();
        reconcileAutoStart();
        return new MessageSimulationResult(Instant.now(), messageWorkbench.simulate(scenario));
    }

    /**
     * 加載監控指標。HOST 節點返回零值指標；VIRTUAL 節點從運行時獲取。
     *
     * @param clusterRef 集群引用
     * @return 監控快照（含時間戳和所有節點的指標）
     */
    @Override
    public MonitoringSnapshot loadMetrics(ClusterRef clusterRef) {
        initializeIfNecessary();
        reconcileAutoStart();
        List<NodeMetrics> metrics = nodeRegistry.ordered().stream()
                .map(node -> {
                    if (node.hostBound()) {
                        return new NodeMetrics(node.nodeId(), 0, 0, 0, 0);
                    }
                    nodeRegistry.transition(node.nodeId(), runtime.status(node.nodeId()));
                    return runtime.metrics(node.nodeId());
                })
                .toList();
        return new MonitoringSnapshot(Instant.now(), metrics);
    }

    /**
     * 加載日誌。合併審計日誌與運行時日誌，按時間倒序排列，截取指定數量。
     *
     * <p>HOST 節點返回一條說明日誌（不暴露嵌入式運行時日誌）。
     *
     * @param clusterRef 集群引用
     * @param nodeId     節點 ID（null 表示所有節點）
     * @param limit      最大返回條數
     * @return 合併後的日誌列表（按時間倒序）
     */
    @Override
    public List<LogEntry> loadLogs(ClusterRef clusterRef, String nodeId, int limit) {
        initializeIfNecessary();
        reconcileAutoStart();
        List<LogEntry> mergedLogs = new ArrayList<>(auditLog.query(nodeId, limit));
        mergedLogs.addAll(nodeRegistry.ordered().stream()
                .filter(node -> nodeId == null || node.nodeId().equals(nodeId))
                .flatMap(node -> {
                    if (node.hostBound()) {
                        return java.util.stream.Stream.of(new LogEntry(
                                Instant.now(),
                                node.nodeId(),
                                "INFO",
                                "Host-bound pseudo node is attached to the topology and does not expose pseudo runtime logs."
                        ));
                    }
                    return runtime.logs(node.nodeId(), limit).stream();
                })
                .toList());
        mergedLogs.sort(Comparator.comparing(LogEntry::timestamp).reversed());
        return mergedLogs.stream()
                .limit(Math.max(1, limit))
                .toList();
    }

    /**
     * 延遲初始化——首次調用時種子化拓撲並按需啟動所有虛擬節點。
     * 通過 {@code AtomicBoolean.compareAndSet} 保證線程安全且只執行一次。
     */
    private void initializeIfNecessary() {
        if (initialized.compareAndSet(false, true)) {
            topologySeeder.seedIfNeeded();
            if (properties.resolvedAutoStart()) {
                startAllVirtualNodes();
            }
        }
    }

    /**
     * 啟動所有虛擬節點。嚴格按「先 NameServer 後 Broker」順序啟動，
     * 因為 Broker 啟動時需要向 NameServer 註冊。
     */
    private void startAllVirtualNodes() {
        // 先啟動 NameServer，再啟動 Broker
        nodeRegistry.ordered().stream()
                .filter(node -> !node.hostBound())
                .filter(node -> "nameserver".equals(node.role()))
                .forEach(node -> {
                    runtime.start(node);
                    nodeRegistry.transition(node.nodeId(), runtime.status(node.nodeId()));
                });
        nodeRegistry.ordered().stream()
                .filter(node -> !node.hostBound())
                .filter(node -> !"nameserver".equals(node.role()))
                .forEach(node -> {
                    runtime.start(node);
                    nodeRegistry.transition(node.nodeId(), runtime.status(node.nodeId()));
                });
    }

    /**
     * 對帳 auto-start：若配置了 auto-start，檢查所有虛擬節點狀態，
     * 將意外停止的節點重新拉起。啟動失敗的節點標記為 FAILED。
     */
    private void reconcileAutoStart() {
        if (!properties.resolvedAutoStart()) {
            return;
        }
        nodeRegistry.ordered().stream()
                .filter(node -> !node.hostBound())
                .forEach(node -> {
                    NodeStatus status = runtime.status(node.nodeId());
                    nodeRegistry.transition(node.nodeId(), status);
                    if (status == NodeStatus.STOPPED) {
                        try {
                            runtime.start(node);
                            nodeRegistry.transition(node.nodeId(), runtime.status(node.nodeId()));
                        } catch (Exception exception) {
                            nodeRegistry.transition(node.nodeId(), NodeStatus.FAILED);
                        }
                    }
                });
    }

    /**
     * 構建節點間的網絡鏈路。按節點 ID 排序後相鄰節點兩兩連接，
     * 鏈路健康度取決於兩端節點是否都處於 RUNNING 狀態。
     *
     * @param topologyNodes 拓撲節點列表（已排序）
     * @return 網絡鏈路列表
     */
    private List<NetworkLink> buildLinks(List<com.example.clustermanager.core.model.ClusterNode> topologyNodes) {
        List<NetworkLink> links = new ArrayList<>();
        for (int index = 0; index < topologyNodes.size() - 1; index++) {
            com.example.clustermanager.core.model.ClusterNode source = topologyNodes.get(index);
            com.example.clustermanager.core.model.ClusterNode target = topologyNodes.get(index + 1);
            boolean healthy = source.status() == NodeStatus.RUNNING && target.status() == NodeStatus.RUNNING;
            links.add(new NetworkLink(source.nodeId(), target.nodeId(), healthy, "tap-overlay", healthy ? 1.2 : 99.0));
        }
        return links;
    }

    /**
     * 獲取活躍 VIP——第一個處於 RUNNING 狀態的 Broker 地址。
     *
     * @return 活躍 Broker 地址，無可用時返回 "unavailable"
     */
    private String activeVip() {
        return nodeRegistry.brokers().stream()
                .filter(node -> node.status() == NodeStatus.RUNNING)
                .map(ManagedNode::address)
                .findFirst()
                .orElse("unavailable");
    }

    /**
     * 校驗服務註冊請求。HOST 節點要求有效端口；VIRTUAL 節點要求合法 IPv4 且不重複。
     *
     * @param registration 服務註冊信息
     * @throws IllegalArgumentException 校驗失敗時拋出
     */
    private void validateRegistration(ServiceRegistration registration) {
        NodeKind nodeKind = NodeKind.fromLabel(
                registration.labels() != null ? registration.labels().get("nodeKind") : null
        );
        if (nodeKind == NodeKind.HOST) {
            if (registration.port() == null || registration.port() <= 0) {
                throw new IllegalArgumentException("Host pseudo node requires a valid port");
            }
            return;
        }
        if (!registration.address().matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
            throw new IllegalArgumentException("Pseudo virtual nodes require a virtual IPv4 address");
        }
        if (nodeRegistry.ordered().stream().anyMatch(node -> registration.address().equals(node.address()))) {
            throw new IllegalArgumentException("Virtual IP already exists: " + registration.address());
        }
        if (registration.port() == null || registration.port() <= 0) {
            throw new IllegalArgumentException("Port must be greater than 0");
        }
    }

    /**
     * 將虛擬節點附加到網絡段並應用隔離規則。
     *
     * @param clusterId   集群 ID（用作網絡段 ID）
     * @param nodeId      節點 ID
     * @param requestedIp 請求的虛擬 IP（可為 null，由地址池自動分配）
     * @return 分配的虛擬 IP 地址
     */
    private String attachVirtualNode(String clusterId, String nodeId, String requestedIp) {
        String virtualIp = virtualNetwork.attachNode(clusterId, nodeId, requestedIp).virtualIp();
        virtualNetwork.isolateNode(clusterId, nodeId);
        return virtualIp;
    }
}
