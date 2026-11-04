package com.example.clustermanager.infrastructure.rocketmq;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.ClusterNode;
import com.example.clustermanager.core.model.ClusterTopology;
import com.example.clustermanager.core.model.LogEntry;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.core.model.MessageSimulationResult;
import com.example.clustermanager.core.model.MessageDeliveryResult;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.NodeMetrics;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.model.ServiceRegistration;
import com.example.clustermanager.core.port.IClusterProvider;
import com.example.clustermanager.infrastructure.pseudo.AuditLog;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * RocketMQ 集群 Provider —— REAL 模式的 {@link IClusterProvider} 适配器实现。
 *
 * <p>本类属于 infrastructure/rocketmq 层，实现核心端口 {@link IClusterProvider}，
 * 对应 {@code (REAL, ROCKETMQ)} 组合。它通过 {@link RocketMqAdminAdapter} 从真实
 * RocketMQ 集群（或 Mock）拉取拓扑、链路、消息流数据，并支持手工登记节点
 * 的增删改操作。手工登记的节点会与 Admin API 拉取的节点合并，统一纳入
 * topology / metrics / logs / simulate 的数据视图。
 *
 * <p><b>共享 AuditLog</b>：本 Provider 复用 {@code infrastructure.pseudo.AuditLog}
 * （伪集群的审计日志组件），将手工节点的登记、删除等操作记录写入共享审计日志，
 * 使 PSEUDO 与 REAL 两种模式的操作日志统一管理。
 *
 * <p><b>手工节点管理</b>：由于真实 RocketMQ 集群的节点由运维侧管理，Admin API
 * 无法直接增删节点。本 Provider 通过 {@link #manualNodes} 维护一份手工登记的节点
 * 列表，支持用户通过 API 登记外部节点并合并到拓扑视图中。
 *
 * <p><b>P1 修复</b>：节点操作（{@link #operateNode}）改用带返回值的
 * {@link RocketMqAdminAdapter#tryInvokeNodeOperation}，失败时如实返回
 * {@code success=false}，避免误导调用方。
 *
 * <p><b>P3 修复</b>：手工登记（{@link #registerService}）补充 port 范围校验
 * （1-65535）和 address 非空校验，与伪集群的 {@code validatePseudoRegistration}
 * 保持一致；同时统一标记手工节点 {@code nodeKind=HOST}，避免前端误显示为 VIRTUAL。
 *
 * <p><b>当前状态</b>：REAL 模式暂时搁置，专注 PSEUDO 模式。底层 Admin 客户端
 * 当前使用 {@link MockRocketMqAdminClient} 返回静态演示数据。
 *
 * @see IClusterProvider
 * @see RocketMqAdminAdapter
 * @see RocketMqClusterProperties
 * @see AuditLog
 */
@Component
public class RocketMqClusterProvider implements IClusterProvider {

    /** Provider 描述符 —— 标识本 Provider 为 REAL 模式的 RocketMQ 适配器；clusterId 来自配置 */
    private final ProviderDescriptor descriptor;

    /** 真实 RocketMQ 集群配置属性（集群 ID、Dashboard 名称、NameServer 地址等） */
    private final RocketMqClusterProperties properties;

    /** Admin API 适配器，负责底层 Admin 客户端调用与领域模型转换 */
    private final RocketMqAdminAdapter adminAdapter;

    /** 共享审计日志（与 PseudoClusterProvider 共用），记录手工节点操作 */
    private final AuditLog auditLog;

    /** Admin 客戶端——用於真實 produce/consume 操作 */
    private final RocketMqAdminClient adminClient;

    /** 運行時連接配置——可變的 NameServer 地址和超時 */
    private final RocketMqConnectionConfig connectionConfig;

    /** 手工登记节点表 —— 用户通过 API 登记的外部节点，与 Admin API 拉取的节点合并展示 */
    private final Map<String, ClusterNode> manualNodes = new ConcurrentHashMap<>();

    /**
     * 构造 Provider，注入配置属性、Admin 适配器、共享审计日志、Admin 客戶端和連接配置。
     *
     * @param properties       真实 RocketMQ 集群配置属性
     * @param adminAdapter     Admin API 适配器
     * @param auditLog         共享审计日志（与伪集群共用）
     * @param adminClient      Admin 客戶端（用於真實 produce/consume）
     * @param connectionConfig 運行時連接配置
     */
    public RocketMqClusterProvider(RocketMqClusterProperties properties, RocketMqAdminAdapter adminAdapter,
                                   AuditLog auditLog, RocketMqAdminClient adminClient,
                                   RocketMqConnectionConfig connectionConfig) {
        this.properties = properties;
        this.adminAdapter = adminAdapter;
        this.auditLog = auditLog;
        this.adminClient = adminClient;
        this.connectionConfig = connectionConfig;
        this.descriptor = new ProviderDescriptor(
                "rocketmq-admin",
                "RocketMQ 真实集群",
                com.example.clustermanager.core.model.ClusterMode.REAL,
                MiddlewareType.ROCKETMQ,
                properties.clusterId(),
                java.util.Set.of(
                        com.example.clustermanager.core.model.ProviderCapabilities.CONNECTION_CONFIG,
                        com.example.clustermanager.core.model.ProviderCapabilities.SERVICE_REGISTRATION)
        );
    }

    /**
     * 返回本 Provider 的描述符，标识为 REAL 模式的 RocketMQ 适配器。
     *
     * @return Provider 描述符（id=rocketmq-admin, mode=REAL, middleware=ROCKETMQ）
     */
    @Override
    public ProviderDescriptor descriptor() {
        return descriptor;
    }

    /**
     * 加载集群拓扑 —— 合并 Admin API 拉取的节点与手工登记节点。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>从 Admin Adapter 加载真实集群节点</li>
     *   <li>合并手工登记节点（{@link #manualNodes}）</li>
     *   <li>基于完整节点列表加载网络链路（含手工节点虚拟链路）</li>
     *   <li>取第一个 NameServer 地址作为拓扑的 NameServer 标识</li>
     * </ol>
     *
     * @param clusterRef 集群引用（包含集群 ID 和模式）
     * @return 集群拓扑（节点列表 + 链路列表 + NameServer 地址）
     */
    @Override
    public ClusterTopology loadTopology(ClusterRef clusterRef) {
        List<ClusterNode> nodes = new ArrayList<>(adminAdapter.loadNodes(properties.resolvedDashboardName()));
        nodes.addAll(manualNodes.values());
        return new ClusterTopology(
                clusterRef,
                nodes,
                adminAdapter.loadLinks(properties.resolvedDashboardName(), nodes),
                properties.resolvedNameServers().isEmpty() ? "unavailable" : properties.resolvedNameServers().get(0)
        );
    }

    /**
     * 对指定节点执行生命周期操作（START / STOP / RESTART）。
     *
     * <p>处理逻辑：
     * <ul>
     *   <li>若节点为手工登记节点，直接更新其状态并返回成功</li>
     *   <li>若节点为 Admin API 管理的节点，委托 Admin Adapter 执行操作</li>
     * </ul>
     *
     * <p>P1 修复：Admin 节点操作改用 {@link RocketMqAdminAdapter#tryInvokeNodeOperation}
     * 带返回值的入口，失败时如实返回 {@code success=false}。
     *
     * @param clusterRef    集群引用
     * @param nodeId        目标节点 ID
     * @param operationType 操作类型（START / STOP / RESTART）
     * @return 操作结果（包含节点 ID、操作类型、是否成功、描述消息）
     */
    @Override
    public OperationResult operateNode(ClusterRef clusterRef, String nodeId, OperationType operationType) {
        ClusterNode manualNode = manualNodes.get(nodeId);
        if (manualNode != null) {
            ClusterNode updated = new ClusterNode(
                    manualNode.nodeId(),
                    manualNode.displayName(),
                    manualNode.hostName(),
                    manualNode.virtualIp(),
                    switch (operationType) {
                        case START, RESTART -> com.example.clustermanager.core.model.NodeStatus.RUNNING;
                        case STOP -> com.example.clustermanager.core.model.NodeStatus.STOPPED;
                    },
                    manualNode.labels()
            );
            manualNodes.put(nodeId, updated);
            return new OperationResult(nodeId, operationType, true, "Manual RocketMQ service updated");
        }
        // P1 修复: admin 节点操作改用带返回值的入口，失败时如实返回 success=false
        boolean delegated = adminAdapter.tryInvokeNodeOperation(nodeId, operationType.name());
        return new OperationResult(
                nodeId,
                operationType,
                delegated,
                delegated ? "Delegated to RocketMQ Admin API" : "RocketMQ Admin API operation failed for " + nodeId
        );
    }

    /**
     * 手工登记服务 —— 将外部 RocketMQ 节点登记到集群拓扑视图中。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>校验节点 ID 不重复</li>
     *   <li>P3 修复：校验 port 范围（1-65535）和 address 非空，与伪集群保持一致</li>
     *   <li>补充标签：role、source=manual、nameserver 地址</li>
     *   <li>P1 修复：统一标记 nodeKind=HOST，避免前端误显示为 VIRTUAL</li>
     *   <li>创建节点并加入手工节点表</li>
     *   <li>写入共享审计日志</li>
     * </ol>
     *
     * @param clusterRef   集群引用
     * @param registration 服务登记信息（节点 ID、显示名、地址、端口、角色、标签）
     * @return 操作结果（成功时包含登记确认消息）
     * @throws IllegalArgumentException 当节点 ID 已存在、port 超范围或 address 为空时抛出
     */
    @Override
    public OperationResult registerService(ClusterRef clusterRef, ServiceRegistration registration) {
        if (manualNodes.containsKey(registration.nodeId())) {
            throw new IllegalArgumentException("Node already exists: " + registration.nodeId());
        }
        // P3 修复: 真实集群手工登记补 port 范围校验，与伪集群 validatePseudoRegistration 保持一致
        if (registration.port() == null || registration.port() <= 0 || registration.port() > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (registration.address() == null || registration.address().isBlank()) {
            throw new IllegalArgumentException("Address is required");
        }
        Map<String, String> labels = new java.util.HashMap<>(registration.labels() == null ? Map.of() : registration.labels());
        labels.putIfAbsent("role", registration.role());
        labels.put("source", "manual");
        labels.put("nameserver", registration.address());
        // P1 修复: 真实集群手工登记的节点都是真实物理节点，统一标记 nodeKind=HOST，避免前端误显示为 VIRTUAL
        labels.putIfAbsent("nodeKind", "HOST");
        manualNodes.put(registration.nodeId(), new ClusterNode(
                registration.nodeId(),
                registration.displayName(),
                registration.hostName(),
                "%s:%d".formatted(registration.address(), registration.port()),
                com.example.clustermanager.core.model.NodeStatus.RUNNING,
                Map.copyOf(labels)
        ));
        auditLog.append(registration.nodeId(), "INFO", "Manual RocketMQ service registered at %s:%d".formatted(
                registration.address(),
                registration.port()
        ));
        return new OperationResult(registration.nodeId(), OperationType.START, true, "RocketMQ manual service registered");
    }

    /**
     * 删除手工登记的服务节点。
     *
     * <p>仅能删除通过 {@link #registerService} 登记的手工节点，
     * Admin API 管理的真实节点无法通过此方法删除。删除时写入共享审计日志。
     *
     * @param clusterRef 集群引用
     * @param nodeId     待删除的手工节点 ID
     * @return 操作结果（成功时包含删除确认消息）
     * @throws IllegalArgumentException 当指定节点不是手工登记节点时抛出
     */
    @Override
    public OperationResult deleteService(ClusterRef clusterRef, String nodeId) {
        ClusterNode removed = manualNodes.remove(nodeId);
        if (removed == null) {
            throw new IllegalArgumentException("Manual service not found: " + nodeId);
        }
        auditLog.append(nodeId, "WARN", "Manual RocketMQ service deleted from cluster " + clusterRef.clusterId());
        return new OperationResult(nodeId, OperationType.STOP, true, "RocketMQ manual service deleted");
    }

    /**
     * 模拟消息流 —— 探测从生产者到消费者的消息投递路径。
     *
     * <p>委托 {@link RocketMqAdminAdapter#probeMessageFlow} 执行探测，
     * 将手工登记节点列表传入以支持消费者回退。
     *
     * @param clusterRef 集群引用
     * @param scenario   消息模拟场景（topic、生产者、消费者列表、消息数量）
     * @return 消息模拟结果（包含时间戳和投递结果列表）
     */
    @Override
    public MessageSimulationResult simulate(ClusterRef clusterRef, MessageScenario scenario) {
        String namesrvAddr = connectionConfig.resolvedNameServerString();
        if (namesrvAddr.isEmpty()) {
            namesrvAddr = String.join(";", properties.resolvedNameServers());
        }
        if (namesrvAddr.isEmpty()) {
            // 無 NameServer 地址時回退到 Admin 探測
            return new MessageSimulationResult(
                    Instant.now(),
                    adminAdapter.probeMessageFlow(
                            scenario.topic(),
                            scenario.producerNodeId(),
                            scenario.consumerNodeIds(),
                            scenario.messageCount(),
                            manualNodes.values().stream().toList()
                    )
            );
        }

        // 真實 produce + consume 流程
        List<MessageDeliveryResult> deliveries = new ArrayList<>();

        // 1. 發送消息
        List<java.util.Map<String, Object>> produceResults = adminClient.produceMessages(
                namesrvAddr, scenario.topic(), scenario.messageCount(), scenario.payloadTemplate());

        // 2. 消費消息
        List<String> consumerNodeIds = scenario.consumerNodeIds();
        if (consumerNodeIds == null || consumerNodeIds.isEmpty()) {
            consumerNodeIds = manualNodes.values().stream()
                    .filter(node -> "broker".equals(node.labels().get("role")))
                    .map(ClusterNode::nodeId)
                    .toList();
            if (consumerNodeIds.isEmpty()) {
                consumerNodeIds = List.of(scenario.producerNodeId());
            }
        }
        List<java.util.Map<String, Object>> consumeResults = adminClient.consumeMessages(
                namesrvAddr, scenario.topic(), scenario.consumerGroup(), scenario.messageCount());

        // 3. 組合投遞結果
        int resultCount = Math.max(produceResults.size(), consumeResults.size());
        for (int i = 0; i < resultCount; i++) {
            java.util.Map<String, Object> prodResult = i < produceResults.size() ? produceResults.get(i) : null;
            java.util.Map<String, Object> consResult = i < consumeResults.size() ? consumeResults.get(i) : null;

            String messageKey = prodResult != null ? String.valueOf(prodResult.getOrDefault("messageKey", "")) : "";
            String consumerNodeId = consumerNodeIds.get(i % consumerNodeIds.size());
            boolean success = (prodResult == null || Boolean.TRUE.equals(prodResult.get("success")))
                    && (consResult == null || Boolean.TRUE.equals(consResult.get("success")));
            StringBuilder detail = new StringBuilder();
            if (prodResult != null) detail.append(prodResult.getOrDefault("detail", ""));
            if (consResult != null) {
                if (detail.length() > 0) detail.append(" | ");
                detail.append(consResult.getOrDefault("detail", ""));
            }

            deliveries.add(new MessageDeliveryResult(
                    messageKey,
                    scenario.producerNodeId(),
                    consumerNodeId,
                    success,
                    detail.toString()
            ));
        }

        return new MessageSimulationResult(Instant.now(), deliveries);
    }

    /**
     * 加载集群监控指标 —— 合并 Admin 节点与手工节点的指标数据。
     *
     * <p><b>注意</b>：当前实现使用 {@link ThreadLocalRandom} 生成随机指标值
     * （CPU 10-75%、内存 20-80%、网络 IO 2048-16384），并非真实监控数据。
     * 待接入真实 Admin API 时应替换为实际采集的指标。
     *
     * @param clusterRef 集群引用
     * @return 监控快照（包含时间戳和各节点的指标列表）
     */
    @Override
    public MonitoringSnapshot loadMetrics(ClusterRef clusterRef) {
        List<ClusterNode> nodes = new ArrayList<>(adminAdapter.loadNodes(properties.resolvedDashboardName()));
        nodes.addAll(manualNodes.values());
        List<NodeMetrics> metrics = nodes.stream()
                .map(node -> new NodeMetrics(
                        node.nodeId(),
                        ThreadLocalRandom.current().nextDouble(10, 75),
                        ThreadLocalRandom.current().nextDouble(20, 80),
                        ThreadLocalRandom.current().nextDouble(2048, 16384),
                        ThreadLocalRandom.current().nextDouble(2048, 16384)
                ))
                .toList();
        return new MonitoringSnapshot(Instant.now(), metrics);
    }

    /**
     * 加载集群日志 —— 合并审计日志与 Admin API 观察到的节点状态日志。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>从共享审计日志查询指定节点的操作记录</li>
     *   <li>为每个匹配节点生成一条 Admin API 观察日志（记录当前状态）</li>
     *   <li>合并两类日志并按时间戳倒序排列</li>
     *   <li>截取指定数量的最新日志</li>
     * </ol>
     *
     * @param clusterRef 集群引用
     * @param nodeId     节点 ID（为 null 时返回所有节点的日志）
     * @param limit      返回日志的最大数量
     * @return 合并后的日志列表（按时间倒序，最多 limit 条）
     */
    @Override
    public List<LogEntry> loadLogs(ClusterRef clusterRef, String nodeId, int limit) {
        List<ClusterNode> nodes = new ArrayList<>(adminAdapter.loadNodes(properties.resolvedDashboardName()));
        nodes.addAll(manualNodes.values());
        List<LogEntry> mergedLogs = new ArrayList<>(auditLog.query(nodeId, limit));
        mergedLogs.addAll(nodes.stream()
                .filter(node -> nodeId == null || node.nodeId().equals(nodeId))
                .map(node -> new LogEntry(
                        Instant.now(),
                        node.nodeId(),
                        "INFO",
                        "Admin API observed %s on %s".formatted(node.status(), node.hostName())
                ))
                .toList());
        mergedLogs.sort(Comparator.comparing(LogEntry::timestamp).reversed());
        return mergedLogs.stream()
                .limit(Math.max(1, limit))
                .toList();
    }

}
