package com.example.clustermanager.api.controller;

import com.example.clustermanager.api.config.ClusterStreamProperties;
import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ProviderDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 定时遥测推送服务，通过 WebSocket STOMP 向前端推送集群指标与日志。
 *
 * <p>API 层角色：作为出站适配器，周期性从 {@link ClusterFacadeService} 拉取指标/日志，
 * 经 {@link SimpMessagingTemplate} 推送到 STOMP 主题
 * {@code /topic/clusters/{clusterId}/{metrics|logs}}，前端通过 hooks/use-cluster-data.ts 订阅消费。
 *
 * <p>推送目标：遍历 {@link ClusterFacadeService#listProviders()} 返回的全部 provider
 * （RocketMQ PSEUDO/REAL、Kafka、RabbitMQ 等），按各自 descriptor 中的 clusterId/mode/middleware 推送。
 * 新增中间件无需修改本类。
 *
 * <p>Phase 0-3 优化：无 WebSocket 订阅者时跳过推送（见 {@link #publish}），
 * 避免无意义的 provider 调用——尤其 REAL 模式每次拉指标都会打 RocketMQ Admin API。
 * 订阅者检测通过 {@link SimpUserRegistry} 遍历用户会话的订阅目的地实现。
 */
@Service
public class ClusterTelemetryPushService {

    private static final Logger log = LoggerFactory.getLogger(ClusterTelemetryPushService.class);

    /** STOMP 消息发送模板，用于向主题推送消息。 */
    private final SimpMessagingTemplate messagingTemplate;
    /** STOMP 用户注册表，用于检测某目的地是否有活跃订阅者。 */
    private final SimpUserRegistry userRegistry;
    /** 应用层编排服务，拉取指标与日志的唯一入口。 */
    private final ClusterFacadeService clusterFacadeService;
    /** 遥测推送配置，提供推送周期等参数。 */
    private final ClusterStreamProperties streamProperties;

    /**
     * 构造器注入所有依赖。
     *
     * @param messagingTemplate          STOMP 消息发送模板
     * @param userRegistry               STOMP 用户注册表
     * @param clusterFacadeService       应用层编排服务
     * @param streamProperties           遥测推送配置
     */
    public ClusterTelemetryPushService(
            SimpMessagingTemplate messagingTemplate,
            SimpUserRegistry userRegistry,
            ClusterFacadeService clusterFacadeService,
            ClusterStreamProperties streamProperties
    ) {
        this.messagingTemplate = messagingTemplate;
        this.userRegistry = userRegistry;
        this.clusterFacadeService = clusterFacadeService;
        this.streamProperties = streamProperties;
    }

    /**
     * 定时推送指标快照。周期由 {@code cluster.stream.publish-interval-ms} 控制（默认 5000ms）。
     * 遍历所有 provider 推送 metrics 主题。
     */
    @Scheduled(fixedRateString = "${cluster.stream.publish-interval-ms:5000}")
    public void pushSnapshots() {
        publishAll("metrics");
    }

    /**
     * 定时推送日志。周期同指标推送，但带一个周期的初始延迟，避免与指标推送同时执行。
     * 遍历所有 provider 推送 logs 主题。
     */
    @Scheduled(fixedRateString = "${cluster.stream.publish-interval-ms:5000}", initialDelayString = "${cluster.stream.publish-interval-ms:5000}")
    public void pushLogs() {
        publishAll("logs");
    }

    /**
     * 对所有已注册 provider 推送指定通道。单个集群推送失败不影响其他集群。
     *
     * @param channel 通道名（"metrics" 或 "logs"）
     */
    private void publishAll(String channel) {
        for (ProviderDescriptor descriptor : clusterFacadeService.listProviders()) {
            try {
                publish(selection(descriptor), channel);
            } catch (RuntimeException exception) {
                log.debug("Telemetry push failed for {} / {}: {}", descriptor.clusterId(), channel, exception.getMessage());
            }
        }
    }

    /**
     * 向指定集群通道推送数据。Phase 0-3 优化：若该目的地无 WebSocket 订阅者则直接返回，
     * 跳过无意义的 provider 调用。
     *
     * @param selection 集群选择
     * @param channel   通道名（"metrics" 或 "logs"）
     * @throws IllegalArgumentException 当 channel 不是 metrics 或 logs 时
     */
    private void publish(ClusterSelection selection, String channel) {
        String destination = "/topic/clusters/%s/%s".formatted(selection.clusterId(), channel);
        // Phase 0 优化: 无 WebSocket 订阅者时跳过推送，避免无意义的 provider 调用（尤其 REAL 模式每次都打 Admin API）
        if (!hasSubscribers(destination)) {
            return;
        }
        Object payload = switch (channel) {
            case "metrics" -> clusterFacadeService.loadMetrics(selection);
            case "logs" -> clusterFacadeService.loadLogs(selection, null, 10);
            default -> throw new IllegalArgumentException("Unsupported stream channel: " + channel);
        };
        messagingTemplate.convertAndSend(destination, payload);
    }

    /**
     * 检测指定 STOMP 目的地是否有活跃订阅者。
     * 遍历所有用户的所有会话的所有订阅，匹配目的地字符串。
     *
     * @param destination STOMP 目的地（如 /topic/clusters/local-lab/metrics）
     * @return true 表示至少有一个订阅者
     */
    private boolean hasSubscribers(String destination) {
        return userRegistry.getUsers().stream()
                .anyMatch(user -> user.getSessions().stream()
                        .anyMatch(session -> session.getSubscriptions().stream()
                                .anyMatch(sub -> destination.equals(sub.getDestination()))));
    }

    /**
     * 由 provider 描述符构建集群选择值对象。
     *
     * @param descriptor provider 描述符
     * @return 集群选择值对象
     */
    private ClusterSelection selection(ProviderDescriptor descriptor) {
        return new ClusterSelection(descriptor.clusterId(), descriptor.mode(), descriptor.middleware());
    }
}
