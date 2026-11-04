package com.example.clustermanager.api.controller;

import com.example.clustermanager.api.dto.MessageSimulationRequest;
import com.example.clustermanager.api.dto.NodeOperationRequest;
import com.example.clustermanager.api.dto.ServiceRegistrationRequest;
import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.model.MessageSimulationCommand;
import com.example.clustermanager.application.model.NodeOperationCommand;
import com.example.clustermanager.application.model.ServiceRegistrationCommand;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.LogEntry;
import com.example.clustermanager.core.model.MessageSimulationResult;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.model.ServiceRegistration;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 集群管理主 REST 控制器，承载所有 {@code /api/clusters/*} 端点。
 *
 * <p>API 层角色：作为六边形架构的入站适配器，将 HTTP 请求翻译为应用层命令，
 * 再委托 {@link ClusterFacadeService} 编排。控制器本身不直接访问 infrastructure 层，
 * 所有集群操作（拓扑读取、指标拉取、节点操作、服务登记、消息模拟）均通过 facade 完成，
 * facade 内部按 {@code (ClusterMode, MiddlewareType)} 选择对应 provider 适配器。
 *
 * <p>路径约定：{@code /api/clusters/{mode}/{middleware}/{clusterId}/...}，
 * 其中 mode（PSEUDO/REAL）与 middleware（ROCKETMQ/KAFKA/RABBITMQ）为枚举 path 变量，
 * 通过 {@link #selection} 统一解析为大写枚举值。
 *
 * <p>校验：请求体标注 {@code @Valid} 触发 DTO 字段校验；
 * 类级 {@code @Validated} 用于 path/query 参数校验。
 * 校验失败由 {@link ApiExceptionHandler} 统一处理为 400。
 */
@Validated
@RestController
@RequestMapping("/api/clusters")
public class ClusterController {

    /** 应用层编排服务，所有集群操作的唯一入口。 */
    private final ClusterFacadeService clusterFacadeService;
    /** 消息模板服務——提供預定義模板列表和占位符替換 */
    private final com.example.clustermanager.application.service.MessageTemplateService messageTemplateService;
    /** RocketMQ 連接配置服務——運行時配置管理 */
    private final com.example.clustermanager.application.service.RocketMqConnectionConfigService connectionConfigService;
    /** 動態限流服務——根據本機配置計算最大消息量限制 */
    private final com.example.clustermanager.application.service.RateLimitService rateLimitService;

    /**
     * 构造器注入 facade 服务和模板服务。
     *
     * @param clusterFacadeService 应用层编排服务
     * @param messageTemplateService 消息模板服务
     */
    public ClusterController(ClusterFacadeService clusterFacadeService,
                             com.example.clustermanager.application.service.MessageTemplateService messageTemplateService,
                             com.example.clustermanager.application.service.RocketMqConnectionConfigService connectionConfigService,
                             com.example.clustermanager.application.service.RateLimitService rateLimitService) {
        this.clusterFacadeService = clusterFacadeService;
        this.messageTemplateService = messageTemplateService;
        this.connectionConfigService = connectionConfigService;
        this.rateLimitService = rateLimitService;
    }

    /**
     * GET /api/clusters/providers — 列出所有已注册的集群 provider 描述。
     *
     * @return provider 描述列表（含 mode、middleware、clusterId 等元信息）
     */
    @GetMapping("/providers")
    public List<ProviderDescriptor> listProviders() {
        return clusterFacadeService.listProviders();
    }

    /**
     * GET /api/clusters/message-templates — 列出所有预定义消息模板。
     *
     * @return 消息模板列表（含 id、name、template、description）
     */
    @GetMapping("/message-templates")
    public List<com.example.clustermanager.core.model.MessageTemplate> listMessageTemplates() {
        return messageTemplateService.listTemplates();
    }

    /**
     * GET /api/clusters/settings/rocketmq — 獲取當前 RocketMQ 連接配置。
     *
     * @return 配置快照（NameServer 地址列表、超時、消費者組前綴）
     */
    @GetMapping("/settings/rocketmq")
    public com.example.clustermanager.application.service.RocketMqConnectionConfigService.ConfigSnapshot getRocketMqConfig() {
        return connectionConfigService.getSnapshot();
    }

    /**
     * PUT /api/clusters/settings/rocketmq — 更新 RocketMQ 連接配置，立即生效並持久化。
     *
     * @param snapshot 新配置
     * @return 更新後的配置快照
     */
    @PutMapping("/settings/rocketmq")
    public com.example.clustermanager.application.service.RocketMqConnectionConfigService.ConfigSnapshot updateRocketMqConfig(
            @RequestBody com.example.clustermanager.application.service.RocketMqConnectionConfigService.ConfigSnapshot snapshot) {
        return connectionConfigService.update(snapshot);
    }

    /**
     * GET /api/clusters/rate-limit — 獲取本機動態消息量限制。
     *
     * <p>根據本機 CPU 核數、JVM 堆內存、磁盤空間動態計算最大安全消息量。
     * 每台機器看到的數值不同——取決於實際硬件配置。
     *
     * @return 限流結果（含最大消息量、各因子明細、系統配置快照）
     */
    @GetMapping("/rate-limit")
    public com.example.clustermanager.application.service.RateLimitService.RateLimitResult getRateLimit() {
        return rateLimitService.calculateLimit();
    }

    /**
     * GET /api/clusters/{mode}/{middleware}/{clusterId}/topology — 加载集群拓扑。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @return 拓扑结构（节点列表与链路）
     */
    @GetMapping("/{mode}/{middleware}/{clusterId}/topology")
    public Object topology(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId
    ) {
        return clusterFacadeService.loadTopology(selection(clusterId, mode, middleware));
    }

    /**
     * GET /api/clusters/{mode}/{middleware}/{clusterId}/metrics — 加载集群监控指标快照。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @return 监控指标快照
     */
    @GetMapping("/{mode}/{middleware}/{clusterId}/metrics")
    public MonitoringSnapshot metrics(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId
    ) {
        return clusterFacadeService.loadMetrics(selection(clusterId, mode, middleware));
    }

    /**
     * GET /api/clusters/{mode}/{middleware}/{clusterId}/logs — 加载集群审计日志。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param nodeId     可选，按节点过滤日志
     * @param limit      返回条数上限，默认 20
     * @return 日志条目列表
     */
    @GetMapping("/{mode}/{middleware}/{clusterId}/logs")
    public List<LogEntry> logs(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @RequestParam(required = false) String nodeId,
            @RequestParam(defaultValue = "20") int limit
    ) {
        return clusterFacadeService.loadLogs(selection(clusterId, mode, middleware), nodeId, limit);
    }

    /**
     * POST /api/clusters/{mode}/{middleware}/{clusterId}/nodes/{nodeId}/operations — 对节点执行操作。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param nodeId     目标节点标识
     * @param request    操作请求体，operationType 不能为 null
     * @return 操作结果
     */
    @PostMapping("/{mode}/{middleware}/{clusterId}/nodes/{nodeId}/operations")
    public OperationResult operateNode(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @PathVariable String nodeId,
            @Valid @RequestBody NodeOperationRequest request
    ) {
        return clusterFacadeService.operateNode(new NodeOperationCommand(
                selection(clusterId, mode, middleware),
                nodeId,
                request.operationType()
        ));
    }

    /**
     * POST /api/clusters/{mode}/{middleware}/{clusterId}/operations — 集群级一键启动/停止/重启。
     *
     * <p>启停顺序由 provider 决定（RocketMQ 先 NameServer、Kafka 先 controller，停止时逆序）。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ/KAFKA/RABBITMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param request    操作请求体，operationType 不能为 null
     * @return 每个节点的操作结果（按执行顺序）
     */
    @PostMapping("/{mode}/{middleware}/{clusterId}/operations")
    public List<OperationResult> operateCluster(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @Valid @RequestBody NodeOperationRequest request
    ) {
        return clusterFacadeService.operateCluster(selection(clusterId, mode, middleware), request.operationType());
    }

    /**
     * POST /api/clusters/{mode}/{middleware}/{clusterId}/services — 登记服务节点。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param request    服务登记请求体，各字段校验见 {@link ServiceRegistrationRequest}
     * @return 操作结果
     */
    @PostMapping("/{mode}/{middleware}/{clusterId}/services")
    public OperationResult registerService(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @Valid @RequestBody ServiceRegistrationRequest request
    ) {
        return clusterFacadeService.registerService(new ServiceRegistrationCommand(
                selection(clusterId, mode, middleware),
                new ServiceRegistration(
                        request.nodeId(),
                        request.displayName(),
                        request.role(),
                        request.hostName(),
                        request.address(),
                        request.port(),
                        request.labels() == null ? Map.of() : request.labels()
                )
        ));
    }

    /**
     * DELETE /api/clusters/{mode}/{middleware}/{clusterId}/services/{nodeId} — 删除已登记的服务节点。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param nodeId     待删除的服务节点标识
     * @return 操作结果
     */
    @DeleteMapping("/{mode}/{middleware}/{clusterId}/services/{nodeId}")
    public OperationResult deleteService(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @PathVariable String nodeId
    ) {
        return clusterFacadeService.deleteService(selection(clusterId, mode, middleware), nodeId);
    }

    /**
     * POST /api/clusters/{mode}/{middleware}/{clusterId}/messages/simulate — 模拟消息生产与消费。
     *
     * @param mode       集群模式（PSEUDO/REAL，大小写不敏感）
     * @param middleware 中间件类型（ROCKETMQ，大小写不敏感）
     * @param clusterId  集群标识
     * @param request    消息模拟请求体，字段校验见 {@link MessageSimulationRequest}
     * @return 消息模拟结果
     */
    @PostMapping("/{mode}/{middleware}/{clusterId}/messages/simulate")
    public MessageSimulationResult simulateMessages(
            @PathVariable String mode,
            @PathVariable String middleware,
            @PathVariable String clusterId,
            @Valid @RequestBody MessageSimulationRequest request
    ) {
        // 動態限流校驗——防止消息量超過本機安全上限
        var limit = rateLimitService.calculateLimit();
        if (request.messageCount() > limit.maxMessages()) {
            throw new IllegalArgumentException(
                    "消息數量 " + request.messageCount() + " 超過本機安全上限 " + limit.maxMessages()
                            + "（基於 CPU " + limit.systemProfile().logicalCores() + " 核 / 堆 "
                            + limit.systemProfile().availableHeapMb() + "MB / 磁盤 "
                            + String.format("%.1f", limit.systemProfile().availableDiskGb()) + "GB 計算）");
        }
        return clusterFacadeService.simulateMessages(new MessageSimulationCommand(
                selection(clusterId, mode, middleware),
                request.topic(),
                request.consumerGroup(),
                request.messageCount(),
                request.payloadTemplate(),
                request.producerNodeId(),
                request.consumerNodeIds(),
                request.headers(),
                request.options()
        ));
    }

    /**
     * 将 path 变量组装为应用层 {@link ClusterSelection}。
     * mode 与 middleware 统一转大写后用枚举 valueOf 解析，大小写不敏感。
     *
     * @param clusterId    集群标识
     * @param rawMode      原始 mode 字符串（PSEUDO/REAL）
     * @param rawMiddleware 原始 middleware 字符串（ROCKETMQ）
     * @return 集群选择值对象
     * @throws IllegalArgumentException 当 mode 或 middleware 不是合法枚举值时
     */
    private ClusterSelection selection(String clusterId, String rawMode, String rawMiddleware) {
        return new ClusterSelection(
                clusterId,
                ClusterMode.valueOf(rawMode.toUpperCase(Locale.ROOT)),
                MiddlewareType.valueOf(rawMiddleware.toUpperCase(Locale.ROOT))
        );
    }
}
