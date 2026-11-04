package com.example.clustermanager.infrastructure.rabbitmq;

import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.ClusterNode;
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
import com.example.clustermanager.infrastructure.rabbitmq.lab.RabbitInternalsInspector;
import com.example.clustermanager.infrastructure.rabbitmq.lab.RabbitStressLab;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.NodeMetrics;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.ProviderCapabilities;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.model.ServiceRegistration;
import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import com.example.clustermanager.core.model.rabbit.RabbitConsumer;
import com.example.clustermanager.core.model.rabbit.RabbitExchange;
import com.example.clustermanager.core.model.rabbit.RabbitQueue;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.core.model.rabbit.RabbitRoutingTopology;
import com.example.clustermanager.core.port.IClusterProvider;
import com.example.clustermanager.core.port.IRabbitInspector;
import com.example.clustermanager.infrastructure.common.JvmProcessMetrics;
import com.example.clustermanager.infrastructure.rabbitmq.consumer.RabbitConsumerManager;
import com.example.clustermanager.infrastructure.rabbitmq.messaging.RabbitMessageWorkbench;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel.QueueStat;
import com.example.clustermanager.infrastructure.rabbitmq.topology.RabbitTopologySeeder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ 嵌入式 Provider：进程内 Qpid Broker-J 单节点，仅 PSEUDO 模式。
 *
 * <p>拓扑重点不在节点，而在 exchange → binding → queue → consumer 的路由关系，由 {@link IRabbitInspector} 提供。
 * 节点初始为 STOPPED，启动后自动声明一组演示交换机与队列（四种交换机类型各一个，外加死信交换机）。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RabbitClusterProvider implements IClusterProvider, IRabbitInspector, IStressLab, IInternalsInspector {

    private static final Set<String> EXCHANGE_TYPES = Set.of("direct", "fanout", "topic", "headers");
    private static final String NAME_PATTERN = "[a-zA-Z0-9._:-]{1,200}";

    private final ProviderDescriptor descriptor;
    private final EmbeddedQpidRuntime runtime;
    private final RabbitObjectModel model;
    private final RabbitMessageWorkbench workbench;
    private final RabbitConsumerManager consumerManager;
    private final RabbitStressLab stressLab;
    private final RabbitInternalsInspector internalsInspector;
    /** 队列速率采样：上一轮 totalEnqueued / totalDequeued 与采样时间。 */
    private final Map<String, long[]> lastQueueSample = new HashMap<>();
    private final Map<String, double[]> queueRates = new HashMap<>();
    private long lastSampleAt;

    public RabbitClusterProvider(
            RabbitClusterProperties properties,
            EmbeddedQpidRuntime runtime,
            RabbitObjectModel model,
            RabbitMessageWorkbench workbench,
            RabbitConsumerManager consumerManager,
            RabbitStressLab stressLab,
            RabbitInternalsInspector internalsInspector
    ) {
        this.runtime = runtime;
        this.model = model;
        this.workbench = workbench;
        this.consumerManager = consumerManager;
        this.stressLab = stressLab;
        this.internalsInspector = internalsInspector;
        this.descriptor = new ProviderDescriptor(
                "rabbitmq-embedded",
                "RabbitMQ 嵌入式 Qpid Broker",
                ClusterMode.PSEUDO,
                MiddlewareType.RABBITMQ,
                properties.resolvedClusterId(),
                Set.of(ProviderCapabilities.CLUSTER_OPERATIONS, ProviderCapabilities.RABBIT_INSPECTION,
                        ProviderCapabilities.LAB)
        );
    }

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

    // ---------------------------------------------------------------- 拓扑与节点

    @Override
    public ClusterTopology loadTopology(ClusterRef clusterRef) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("role", RabbitTopologySeeder.ROLE);
        labels.put("port", String.valueOf(runtime.port()));
        labels.put("managed", "false");
        labels.put("nodeKind", "EMBEDDED");
        labels.put("engine", "Apache Qpid Broker-J");
        Instant startedAt = runtime.startedAt();
        if (startedAt != null) {
            labels.put("startedAt", startedAt.toString());
        }
        ClusterNode node = new ClusterNode(RabbitTopologySeeder.NODE_ID, RabbitTopologySeeder.DISPLAY_NAME,
                "127.0.0.1", runtime.address(), runtime.status(), labels);
        return new ClusterTopology(clusterRef, List.of(node), List.of(), runtime.isRunning() ? runtime.address() : "unavailable");
    }

    @Override
    public OperationResult operateNode(ClusterRef clusterRef, String nodeId, OperationType operationType) {
        if (!RabbitTopologySeeder.NODE_ID.equals(nodeId)) {
            throw new IllegalArgumentException("节点不存在: " + nodeId);
        }
        switch (operationType) {
            case START -> runtime.start();
            case STOP -> {
                consumerManager.stopAll();
                runtime.stop();
            }
            case RESTART -> {
                consumerManager.stopAll();
                runtime.restart();
            }
        }
        resetRates();
        return new OperationResult(nodeId, operationType, true,
                "%s 当前状态：%s".formatted(RabbitTopologySeeder.DISPLAY_NAME, runtime.status()));
    }

    /** 单节点集群：一键启停等价于操作 rabbit-node-01，已处于目标状态时跳过。 */
    @Override
    public List<OperationResult> operateCluster(ClusterRef clusterRef, OperationType operationType) {
        OperationResult result;
        if (operationType == OperationType.START && runtime.isRunning()) {
            result = new OperationResult(RabbitTopologySeeder.NODE_ID, operationType, true, "已运行，跳过");
        } else if (operationType == OperationType.STOP && !runtime.isRunning()) {
            result = new OperationResult(RabbitTopologySeeder.NODE_ID, operationType, true, "已停止，跳过");
        } else {
            try {
                result = operateNode(clusterRef, RabbitTopologySeeder.NODE_ID, operationType);
            } catch (RuntimeException exception) {
                result = new OperationResult(RabbitTopologySeeder.NODE_ID, operationType, false, exception.getMessage());
            }
        }
        return List.of(result);
    }

    @Override
    public OperationResult registerService(ClusterRef clusterRef, ServiceRegistration registration) {
        throw new IllegalArgumentException("RabbitMQ 实验环境为单 broker 节点，不支持手动注册");
    }

    @Override
    public OperationResult deleteService(ClusterRef clusterRef, String nodeId) {
        throw new IllegalArgumentException("RabbitMQ 实验环境为单 broker 节点，不支持删除");
    }

    // ---------------------------------------------------------------- 消息、监控、日志

    @Override
    public MessageSimulationResult simulate(ClusterRef clusterRef, MessageScenario scenario) {
        return new MessageSimulationResult(Instant.now(), workbench.simulate(scenario));
    }

    @Override
    public synchronized MonitoringSnapshot loadMetrics(ClusterRef clusterRef) {
        if (!runtime.isRunning()) {
            return new MonitoringSnapshot(Instant.now(), List.of(new NodeMetrics(RabbitTopologySeeder.NODE_ID, 0, 0, 0, 0)));
        }
        double in = 0;
        double out = 0;
        try {
            for (double[] rate : sampleRates(model.queues()).values()) {
                in += rate[0];
                out += rate[1];
            }
        } catch (RuntimeException exception) {
            // broker 停止过程中读取对象模型可能失败，本轮按 0 处理
        }
        return new MonitoringSnapshot(Instant.now(), List.of(new NodeMetrics(RabbitTopologySeeder.NODE_ID,
                JvmProcessMetrics.processCpuPercent(), JvmProcessMetrics.heapUsagePercent(), round(in), round(out))));
    }

    /** 按 totalEnqueued / totalDequeued 的增量计算每个队列的入队/出队速率（条/秒）。 */
    private synchronized Map<String, double[]> sampleRates(List<QueueStat> queues) {
        long now = System.currentTimeMillis();
        double seconds = lastSampleAt == 0 ? 0 : (now - lastSampleAt) / 1000.0;
        if (lastSampleAt != 0 && seconds < 0.5) {
            return queueRates;
        }
        Map<String, double[]> rates = new HashMap<>();
        for (QueueStat queue : queues) {
            long[] previous = lastQueueSample.get(queue.name());
            double[] rate = new double[2];
            if (previous != null && seconds > 0) {
                rate[0] = Math.max(0, queue.totalEnqueued() - previous[0]) / seconds;
                rate[1] = Math.max(0, queue.totalDequeued() - previous[1]) / seconds;
            }
            rates.put(queue.name(), rate);
            lastQueueSample.put(queue.name(), new long[]{queue.totalEnqueued(), queue.totalDequeued()});
        }
        lastQueueSample.keySet().retainAll(rates.keySet());
        lastSampleAt = now;
        queueRates.clear();
        queueRates.putAll(rates);
        return queueRates;
    }

    private synchronized void resetRates() {
        lastQueueSample.clear();
        queueRates.clear();
        lastSampleAt = 0;
    }

    @Override
    public List<LogEntry> loadLogs(ClusterRef clusterRef, String nodeId, int limit) {
        return runtime.events().query(nodeId, limit);
    }

    // ---------------------------------------------------------------- IRabbitInspector

    @Override
    public RabbitRoutingTopology routing(ClusterRef clusterRef) {
        List<QueueStat> stats = model.queues();
        Map<String, double[]> rates = new HashMap<>(sampleRates(stats));
        List<RabbitQueue> queues = new ArrayList<>();
        for (QueueStat stat : stats) {
            double[] rate = rates.getOrDefault(stat.name(), new double[2]);
            queues.add(new RabbitQueue(stat.name(), stat.ready(), stat.unacked(), stat.consumers(), stat.arguments(),
                    stat.deadLetterExchange(), stat.messageTtlMs(), round(rate[0]), round(rate[1]),
                    stat.totalEnqueued(), stat.totalDequeued()));
        }
        return new RabbitRoutingTopology(Instant.now(), model.exchanges(), queues, model.bindings(), consumerManager.list());
    }

    @Override
    public RabbitExchange declareExchange(ClusterRef clusterRef, String name, String type, boolean durable) {
        String exchange = requireName(name, "交换机");
        if (RabbitObjectModel.isBuiltIn(exchange)) {
            throw new IllegalArgumentException("amq.* 为保留前缀，不能声明");
        }
        String exchangeType = type == null ? "" : type.toLowerCase();
        if (!EXCHANGE_TYPES.contains(exchangeType)) {
            throw new IllegalArgumentException("交换机类型仅支持 direct / fanout / topic / headers");
        }
        runtime.withChannel("declare-exchange", channel -> channel.exchangeDeclare(exchange, exchangeType, durable));
        runtime.events().append("routing", "INFO", "声明交换机 %s（%s）".formatted(exchange, exchangeType));
        return model.exchanges().stream().filter(item -> item.name().equals(exchange)).findFirst()
                .orElse(new RabbitExchange(exchange, exchangeType, durable, false, Map.of()));
    }

    @Override
    public OperationResult deleteExchange(ClusterRef clusterRef, String name) {
        String exchange = requireName(name, "交换机");
        if (RabbitObjectModel.isBuiltIn(exchange)) {
            throw new IllegalArgumentException("内置交换机不能删除: " + exchange);
        }
        if (model.exchangeType(exchange).isEmpty()) {
            throw new IllegalArgumentException("交换机不存在: " + exchange);
        }
        runtime.withChannel("delete-exchange", channel -> channel.exchangeDelete(exchange));
        runtime.events().append("routing", "WARN", "删除交换机 " + exchange);
        return new OperationResult(exchange, null, true, "交换机 " + exchange + " 已删除");
    }

    @Override
    public RabbitQueue declareQueue(ClusterRef clusterRef, String name, String deadLetterExchange, Long messageTtlMs) {
        String queue = requireName(name, "队列");
        String dlx = deadLetterExchange == null || deadLetterExchange.isBlank() ? null : deadLetterExchange.trim();
        if (dlx != null && model.exchangeType(dlx).isEmpty()) {
            throw new IllegalArgumentException("死信交换机不存在: " + dlx);
        }
        if (messageTtlMs != null && (messageTtlMs < 1 || messageTtlMs > 3_600_000)) {
            throw new IllegalArgumentException("队列 TTL 需在 1~3600000 ms 之间");
        }
        runtime.withChannel("declare-queue", channel ->
                channel.queueDeclare(queue, true, false, false, EmbeddedQpidRuntime.queueArguments(dlx, messageTtlMs)));
        runtime.events().append("routing", "INFO", "声明队列 %s%s%s".formatted(queue,
                dlx == null ? "" : "（DLX=" + dlx + "）", messageTtlMs == null ? "" : "（TTL=" + messageTtlMs + "ms）"));
        return routing(clusterRef).queues().stream().filter(item -> item.name().equals(queue)).findFirst()
                .orElseThrow(() -> new IllegalStateException("队列声明后未找到: " + queue));
    }

    @Override
    public OperationResult deleteQueue(ClusterRef clusterRef, String name) {
        String queue = requireName(name, "队列");
        if (!model.queueExists(queue)) {
            throw new IllegalArgumentException("队列不存在: " + queue);
        }
        runtime.withChannel("delete-queue", channel -> channel.queueDelete(queue));
        runtime.events().append("routing", "WARN", "删除队列 " + queue);
        return new OperationResult(queue, null, true, "队列 " + queue + " 已删除");
    }

    @Override
    public OperationResult purgeQueue(ClusterRef clusterRef, String name) {
        String queue = requireName(name, "队列");
        long cleared = model.clearQueue(queue);
        runtime.events().append("routing", "INFO", "清空队列 %s，移除 %d 条消息".formatted(queue, cleared));
        return new OperationResult(queue, null, true, "已清空 " + cleared + " 条 ready 消息");
    }

    @Override
    public RabbitBinding bind(ClusterRef clusterRef, String exchange, String queue, String routingKey, Map<String, String> headers) {
        String source = requireName(exchange, "交换机");
        String destination = requireName(queue, "队列");
        String type = model.exchangeType(source).orElseThrow(() -> new IllegalArgumentException("交换机不存在: " + source));
        if (!model.queueExists(destination)) {
            throw new IllegalArgumentException("队列不存在: " + destination);
        }
        String key = routingKey == null ? "" : routingKey.trim();
        Map<String, String> args = headers == null ? Map.of() : headers;
        if ("headers".equals(type) && args.keySet().stream().allMatch(name -> name.startsWith("x-"))) {
            throw new IllegalArgumentException("headers 交换机的绑定至少需要一个匹配参数（如 type=invoice）");
        }
        if ("headers".equals(type) && args.containsKey("x-match") && !Set.of("all", "any").contains(args.get("x-match"))) {
            throw new IllegalArgumentException("x-match 仅支持 all / any");
        }
        Map<String, Object> bindArgs = args.isEmpty() ? null : new LinkedHashMap<>(args);
        runtime.withChannel("bind", channel -> channel.queueBind(destination, source, key, bindArgs));
        runtime.events().append("routing", "INFO", "绑定 %s → %s（key=%s%s）".formatted(source, destination, key,
                args.isEmpty() ? "" : ", args=" + args));
        return new RabbitBinding(source, destination, key, args);
    }

    @Override
    public OperationResult unbind(ClusterRef clusterRef, String exchange, String queue, String routingKey) {
        String source = requireName(exchange, "交换机");
        String destination = requireName(queue, "队列");
        String key = routingKey == null ? "" : routingKey;
        RabbitBinding binding = model.bindingsOf(source).stream()
                .filter(item -> item.destination().equals(destination) && item.routingKey().equals(key))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("绑定不存在: %s → %s（key=%s）".formatted(source, destination, key)));
        Map<String, Object> args = binding.headers().isEmpty() ? null : new LinkedHashMap<>(binding.headers());
        runtime.withChannel("unbind", channel -> channel.queueUnbind(destination, source, key, args));
        runtime.events().append("routing", "WARN", "解除绑定 %s → %s（key=%s）".formatted(source, destination, key));
        return new OperationResult(source + "→" + destination, null, true, "绑定已删除");
    }

    @Override
    public RabbitRoutePreview previewRoute(ClusterRef clusterRef, String exchange, String routingKey, Map<String, String> headers) {
        runtime.requireRunning();
        return model.preview(exchange, routingKey, headers);
    }

    @Override
    public RabbitConsumer addConsumer(ClusterRef clusterRef, String queue, int prefetch, String ackMode,
                                      long processingDelayMs, double nackRatio, boolean requeue) {
        if (queue != null && !model.queueExists(queue.trim())) {
            throw new IllegalArgumentException("队列不存在: " + queue);
        }
        return consumerManager.add(queue, prefetch, ackMode, processingDelayMs, nackRatio, requeue);
    }

    @Override
    public OperationResult removeConsumer(ClusterRef clusterRef, String consumerId) {
        if (!consumerManager.remove(consumerId)) {
            throw new IllegalArgumentException("消费者不存在: " + consumerId);
        }
        return new OperationResult(consumerId, null, true, "消费者 " + consumerId + " 已移除，未确认的消息已回到队列");
    }

    private static String requireName(String name, String kind) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(kind + "名称不能为空");
        }
        String trimmed = name.trim();
        if (!trimmed.matches(NAME_PATTERN)) {
            throw new IllegalArgumentException(kind + "名称只能包含字母、数字、'.'、'_'、'-'、':'");
        }
        return trimmed;
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }
}

