package com.example.clustermanager.infrastructure.kafka;

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
import com.example.clustermanager.infrastructure.kafka.lab.KafkaInternalsInspector;
import com.example.clustermanager.infrastructure.kafka.lab.KafkaStressLab;
import com.example.clustermanager.core.model.MonitoringSnapshot;
import com.example.clustermanager.core.model.NetworkLink;
import com.example.clustermanager.core.model.NodeMetrics;
import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.ProviderCapabilities;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.model.ServiceRegistration;
import com.example.clustermanager.core.model.kafka.KafkaConsumerGroupDetail;
import com.example.clustermanager.core.model.kafka.KafkaGroupMember;
import com.example.clustermanager.core.model.kafka.KafkaLeaderChange;
import com.example.clustermanager.core.model.kafka.KafkaPartitionDetail;
import com.example.clustermanager.core.model.kafka.KafkaPartitionLag;
import com.example.clustermanager.core.model.kafka.KafkaTopicDetail;
import com.example.clustermanager.core.port.IClusterProvider;
import com.example.clustermanager.core.port.IKafkaInspector;
import com.example.clustermanager.infrastructure.common.JvmProcessMetrics;
import com.example.clustermanager.infrastructure.kafka.group.KafkaConsumerGroupManager;
import com.example.clustermanager.infrastructure.kafka.messaging.KafkaMessageWorkbench;
import com.example.clustermanager.infrastructure.kafka.metrics.KafkaThroughputTracker;
import com.example.clustermanager.infrastructure.kafka.metrics.KafkaThroughputTracker.PartitionSample;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import com.example.clustermanager.infrastructure.kafka.runtime.KafkaNodeSpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Kafka 嵌入式 KRaft 集群 Provider：1 controller + 3 broker，仅 PSEUDO 模式。
 *
 * <p>同时实现 {@link IKafkaInspector}，供 Kafka 专属 API（topic/分区/消费者组/leader 切换）使用。
 * 所有节点初始为 STOPPED，首次访问时才格式化存储，不影响 Spring 上下文启动速度。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaClusterProvider implements IClusterProvider, IKafkaInspector, IStressLab, IInternalsInspector {

    private static final Set<String> HIGHLIGHT_CONFIGS = Set.of("retention.ms", "cleanup.policy", "min.insync.replicas", "segment.bytes");

    private final ProviderDescriptor descriptor;
    private final EmbeddedKafkaRuntime runtime;
    private final KafkaMessageWorkbench workbench;
    private final KafkaConsumerGroupManager groupManager;
    private final KafkaStressLab stressLab;
    private final KafkaInternalsInspector internalsInspector;
    private final KafkaThroughputTracker tracker = new KafkaThroughputTracker();

    public KafkaClusterProvider(
            KafkaClusterProperties properties,
            EmbeddedKafkaRuntime runtime,
            KafkaMessageWorkbench workbench,
            KafkaConsumerGroupManager groupManager,
            KafkaStressLab stressLab,
            KafkaInternalsInspector internalsInspector
    ) {
        this.runtime = runtime;
        this.workbench = workbench;
        this.groupManager = groupManager;
        this.stressLab = stressLab;
        this.internalsInspector = internalsInspector;
        this.descriptor = new ProviderDescriptor(
                "kafka-embedded",
                "Kafka 嵌入式 KRaft 集群",
                ClusterMode.PSEUDO,
                MiddlewareType.KAFKA,
                properties.resolvedClusterId(),
                Set.of(ProviderCapabilities.CLUSTER_OPERATIONS, ProviderCapabilities.KAFKA_INSPECTION,
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
        List<ClusterNode> nodes = runtime.specs().stream().map(this::toClusterNode).toList();
        List<NetworkLink> links = new ArrayList<>();
        ClusterNode controller = nodes.getFirst();
        for (ClusterNode broker : nodes.subList(1, nodes.size())) {
            boolean healthy = controller.status() == NodeStatus.RUNNING && broker.status() == NodeStatus.RUNNING;
            links.add(new NetworkLink(broker.nodeId(), controller.nodeId(), healthy, "kraft-metadata", healthy ? 0.8 : 99.0));
        }
        String activeVip = runtime.specs().stream()
                .filter(spec -> !spec.controller() && runtime.isRunning(spec.nodeId()))
                .map(KafkaNodeSpec::address)
                .findFirst()
                .orElse("unavailable");
        return new ClusterTopology(clusterRef, nodes, links, activeVip);
    }

    private ClusterNode toClusterNode(KafkaNodeSpec spec) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("role", spec.seed().role().label());
        labels.put("port", String.valueOf(spec.port()));
        labels.put("kafkaNodeId", String.valueOf(spec.seed().kafkaNodeId()));
        labels.put("managed", "false");
        labels.put("nodeKind", "EMBEDDED");
        Instant startedAt = runtime.startedAt(spec.nodeId());
        if (startedAt != null) {
            labels.put("startedAt", startedAt.toString());
        }
        return new ClusterNode(spec.nodeId(), spec.seed().displayName(), "127.0.0.1", spec.address(), runtime.status(spec.nodeId()), labels);
    }

    @Override
    public OperationResult operateNode(ClusterRef clusterRef, String nodeId, OperationType operationType) {
        KafkaNodeSpec spec = runtime.spec(nodeId);
        if (operationType != OperationType.START && !spec.controller()) {
            snapshotLeaders();
        }
        switch (operationType) {
            case START -> runtime.start(nodeId);
            case STOP -> runtime.stop(nodeId);
            case RESTART -> runtime.restart(nodeId);
        }
        return new OperationResult(nodeId, operationType, true,
                "%s 当前状态：%s".formatted(spec.seed().displayName(), runtime.status(nodeId)));
    }

    /**
     * 一键启停：启动先 controller 再并行启动 broker；停止先停常驻消费者，再并行停 broker，最后停 controller。
     */
    @Override
    public List<OperationResult> operateCluster(ClusterRef clusterRef, OperationType operationType) {
        List<OperationResult> results = new ArrayList<>();
        List<KafkaNodeSpec> specs = runtime.specs();
        KafkaNodeSpec controller = specs.stream().filter(KafkaNodeSpec::controller).findFirst().orElseThrow();
        List<KafkaNodeSpec> brokers = specs.stream().filter(spec -> !spec.controller()).toList();
        if (operationType == OperationType.STOP || operationType == OperationType.RESTART) {
            groupManager.stopAll();
            results.addAll(parallel(brokers, OperationType.STOP));
            results.add(apply(controller, OperationType.STOP));
        }
        if (operationType == OperationType.START || operationType == OperationType.RESTART) {
            OperationResult controllerResult = apply(controller, OperationType.START);
            results.add(controllerResult);
            if (controllerResult.success()) {
                results.addAll(parallel(brokers, OperationType.START));
            }
        }
        runtime.events().append("cluster", "INFO", "一键%s完成：%d/%d 个节点成功".formatted(
                operationType == OperationType.STOP ? "停止" : operationType == OperationType.START ? "启动" : "重启",
                results.stream().filter(OperationResult::success).count(), results.size()));
        return results;
    }

    private List<OperationResult> parallel(List<KafkaNodeSpec> nodes, OperationType type) {
        List<CompletableFuture<OperationResult>> futures = nodes.stream()
                .map(spec -> CompletableFuture.supplyAsync(() -> apply(spec, type)))
                .toList();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private OperationResult apply(KafkaNodeSpec spec, OperationType type) {
        boolean running = runtime.isRunning(spec.nodeId());
        if (type == OperationType.START && running) {
            return new OperationResult(spec.nodeId(), type, true, "已运行，跳过");
        }
        if (type == OperationType.STOP && !running) {
            return new OperationResult(spec.nodeId(), type, true, "已停止，跳过");
        }
        try {
            if (type == OperationType.START) {
                runtime.start(spec.nodeId());
            } else {
                runtime.stop(spec.nodeId());
            }
            return new OperationResult(spec.nodeId(), type, true, spec.seed().displayName() + (type == OperationType.START ? " 已启动" : " 已停止"));
        } catch (RuntimeException exception) {
            return new OperationResult(spec.nodeId(), type, false, exception.getMessage());
        }
    }

    @Override
    public OperationResult registerService(ClusterRef clusterRef, ServiceRegistration registration) {
        throw new IllegalArgumentException("Kafka 实验集群的节点固定为 1 controller + 3 broker，不支持手动注册");
    }

    @Override
    public OperationResult deleteService(ClusterRef clusterRef, String nodeId) {
        throw new IllegalArgumentException("Kafka 实验集群的节点固定为 1 controller + 3 broker，不支持删除");
    }

    // ---------------------------------------------------------------- 消息、监控、日志

    @Override
    public MessageSimulationResult simulate(ClusterRef clusterRef, MessageScenario scenario) {
        return new MessageSimulationResult(Instant.now(), workbench.simulate(scenario));
    }

    @Override
    public synchronized MonitoringSnapshot loadMetrics(ClusterRef clusterRef) {
        double cpu = JvmProcessMetrics.processCpuPercent();
        double heap = JvmProcessMetrics.heapUsagePercent();
        Map<Integer, double[]> rates = Map.of();
        if (runtime.controllerRunning() && runtime.runningBrokerCount() > 0) {
            try {
                rates = sampleThroughput();
            } catch (RuntimeException exception) {
                // 节点切换期间 Admin 请求可能超时，本轮按 0 处理
            }
        }
        List<NodeMetrics> metrics = new ArrayList<>();
        for (KafkaNodeSpec spec : runtime.specs()) {
            if (!runtime.isRunning(spec.nodeId())) {
                metrics.add(new NodeMetrics(spec.nodeId(), 0, 0, 0, 0));
                continue;
            }
            double[] rate = rates.getOrDefault(spec.seed().kafkaNodeId(), new double[2]);
            metrics.add(new NodeMetrics(spec.nodeId(), cpu, heap, round(rate[0]), round(rate[1])));
        }
        return new MonitoringSnapshot(Instant.now(), metrics);
    }

    private Map<Integer, double[]> sampleThroughput() {
        Admin admin = runtime.admin();
        Map<String, TopicDescription> descriptions = describeAll(admin, false);
        List<PartitionSample> samples = partitionSamples(admin, descriptions.values());
        Map<String, Long> committed = new HashMap<>();
        for (String group : groupIds(admin)) {
            await(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata())
                    .forEach((partition, offset) -> {
                        if (offset != null) {
                            committed.put(group + "|" + partition, offset.offset());
                        }
                    });
        }
        return tracker.sample(samples, committed);
    }

    @Override
    public List<LogEntry> loadLogs(ClusterRef clusterRef, String nodeId, int limit) {
        return runtime.events().query(nodeId, limit);
    }

    // ---------------------------------------------------------------- IKafkaInspector：topic

    @Override
    public List<KafkaTopicDetail> listTopics(ClusterRef clusterRef, boolean includeInternal) {
        Admin admin = runtime.admin();
        Map<String, TopicDescription> descriptions = describeAll(admin, includeInternal);
        Map<String, Long> endOffsets = endOffsets(admin, descriptions.values());
        return descriptions.values().stream()
                .sorted(Comparator.comparing(TopicDescription::isInternal).thenComparing(TopicDescription::name))
                .map(description -> toDetail(description, endOffsets, Map.of()))
                .toList();
    }

    @Override
    public KafkaTopicDetail describeTopic(ClusterRef clusterRef, String topic) {
        Admin admin = runtime.admin();
        TopicDescription description = await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        observeLeaders(List.of(description));
        Map<String, Long> endOffsets = endOffsets(admin, List.of(description));
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        Map<String, String> configs = new TreeMap<>();
        if (config != null) {
            for (ConfigEntry entry : config.entries()) {
                if (HIGHLIGHT_CONFIGS.contains(entry.name()) || entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                    configs.put(entry.name(), entry.value());
                }
            }
        }
        return toDetail(description, endOffsets, configs);
    }

    @Override
    public KafkaTopicDetail createTopic(ClusterRef clusterRef, String topic, int partitions, int replicationFactor, Map<String, String> configs) {
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]{1,249}")) {
            throw new IllegalArgumentException("topic 名称只能包含字母、数字、'.'、'_'、'-'");
        }
        if (partitions < 1 || partitions > 64) {
            throw new IllegalArgumentException("分区数需在 1~64 之间");
        }
        if (replicationFactor < 1 || replicationFactor > 3) {
            throw new IllegalArgumentException("副本因子需在 1~3 之间（集群共 3 个 broker）");
        }
        Admin admin = runtime.admin();
        NewTopic newTopic = new NewTopic(topic, partitions, (short) replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            newTopic.configs(configs);
        }
        await(admin.createTopics(List.of(newTopic)).all());
        runtime.events().append("topic", "INFO", "创建 topic %s（%d 分区 × %d 副本）".formatted(topic, partitions, replicationFactor));
        tracker.forgetTopic(topic);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        KafkaTopicDetail detail = describeTopic(clusterRef, topic);
        while (detail.partitions().stream().anyMatch(partition -> partition.leader() < 0) && System.nanoTime() < deadline) {
            sleep(200);
            detail = describeTopic(clusterRef, topic);
        }
        return detail;
    }

    @Override
    public OperationResult deleteTopic(ClusterRef clusterRef, String topic) {
        await(runtime.admin().deleteTopics(List.of(topic)).all());
        tracker.forgetTopic(topic);
        runtime.events().append("topic", "WARN", "删除 topic " + topic);
        return new OperationResult(topic, OperationType.STOP, true, "已删除 topic " + topic);
    }

    @Override
    public List<KafkaLeaderChange> leaderChanges(ClusterRef clusterRef, int limit) {
        return tracker.leaderChanges(limit);
    }

    // ---------------------------------------------------------------- IKafkaInspector：消费者组

    @Override
    public List<KafkaConsumerGroupDetail> listConsumerGroups(ClusterRef clusterRef) {
        Admin admin = runtime.admin();
        return groupIds(admin).stream().sorted().map(group -> describeGroup(admin, group)).toList();
    }

    @Override
    public KafkaConsumerGroupDetail describeConsumerGroup(ClusterRef clusterRef, String groupId) {
        Admin admin = runtime.admin();
        if (!groupIds(admin).contains(groupId)) {
            throw new IllegalArgumentException("消费者组不存在: " + groupId);
        }
        return describeGroup(admin, groupId);
    }

    @Override
    public KafkaGroupMember addConsumer(ClusterRef clusterRef, String groupId, String topic, int ratePerSecond) {
        Admin admin = runtime.admin();
        if (!await(admin.listTopics().names()).contains(topic)) {
            throw new IllegalArgumentException("topic 不存在: " + topic + "，请先创建 topic 或发送消息");
        }
        return groupManager.add(groupId, topic, ratePerSecond);
    }

    @Override
    public KafkaGroupMember updateConsumerRate(ClusterRef clusterRef, String groupId, String memberId, int ratePerSecond) {
        return groupManager.updateRate(groupId, memberId, ratePerSecond);
    }

    @Override
    public OperationResult removeConsumer(ClusterRef clusterRef, String groupId, String memberId) {
        return groupManager.remove(groupId, memberId);
    }

    private Set<String> groupIds(Admin admin) {
        Set<String> groups = new LinkedHashSet<>();
        for (GroupListing listing : await(admin.listGroups(ListGroupsOptions.forConsumerGroups()).all())) {
            groups.add(listing.groupId());
        }
        groups.addAll(groupManager.managedGroups());
        return groups;
    }

    private KafkaConsumerGroupDetail describeGroup(Admin admin, String groupId) {
        String state = "UNKNOWN";
        List<MemberDescription> described = List.of();
        try {
            ConsumerGroupDescription description = await(admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);
            if (description != null) {
                state = String.valueOf(description.groupState());
                described = List.copyOf(description.members());
            }
        } catch (IllegalArgumentException notFound) {
            state = "DEAD";
        }
        Map<TopicPartition, OffsetAndMetadata> committed = Map.of();
        try {
            committed = await(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
        } catch (IllegalArgumentException notFound) {
            // 组尚未提交过 offset
        }

        Map<String, KafkaGroupMember> managedByClient = groupManager.byClientId(groupId);
        List<KafkaGroupMember> members = new ArrayList<>();
        Map<String, List<String>> assignments = new LinkedHashMap<>();
        Map<TopicPartition, String> owners = new HashMap<>();
        Set<String> topics = new LinkedHashSet<>();
        Set<String> seenManaged = new LinkedHashSet<>();
        for (MemberDescription member : described) {
            List<TopicPartition> partitions = member.assignment().topicPartitions().stream()
                    .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                    .toList();
            List<String> names = partitions.stream().map(TopicPartition::toString).toList();
            KafkaGroupMember managed = managedByClient.get(member.clientId());
            String memberId = managed != null ? managed.memberId() : member.consumerId();
            if (managed != null) {
                seenManaged.add(managed.memberId());
                members.add(new KafkaGroupMember(managed.memberId(), member.consumerId(), member.clientId(), member.host(),
                        true, managed.ratePerSecond(), managed.processed(), names));
            } else {
                members.add(new KafkaGroupMember(member.consumerId(), member.consumerId(), member.clientId(), member.host(),
                        false, 0, 0, names));
            }
            assignments.put(memberId, names);
            partitions.forEach(partition -> {
                owners.put(partition, memberId);
                topics.add(partition.topic());
            });
        }
        // 正在加入组（尚未出现在协调者视图中）的常驻成员
        for (KafkaGroupMember managed : managedByClient.values()) {
            if (!seenManaged.contains(managed.memberId())) {
                members.add(managed);
                assignments.put(managed.memberId(), List.of());
            }
        }
        committed.keySet().forEach(partition -> topics.add(partition.topic()));

        List<KafkaPartitionLag> lags = new ArrayList<>();
        long totalLag = 0;
        if (!topics.isEmpty()) {
            Map<String, TopicDescription> descriptions = describeExisting(admin, topics);
            Map<String, Long> endOffsets = endOffsets(admin, descriptions.values());
            for (TopicDescription description : descriptions.values()) {
                for (TopicPartitionInfo info : description.partitions()) {
                    TopicPartition partition = new TopicPartition(description.name(), info.partition());
                    long end = endOffsets.getOrDefault(partition.toString(), 0L);
                    OffsetAndMetadata offset = committed.get(partition);
                    long committedOffset = offset == null ? -1 : offset.offset();
                    long lag = Math.max(0, end - Math.max(0, committedOffset));
                    totalLag += lag;
                    lags.add(new KafkaPartitionLag(description.name(), info.partition(), committedOffset, end, lag, owners.get(partition)));
                }
            }
        }
        lags.sort(Comparator.comparing(KafkaPartitionLag::topic).thenComparingInt(KafkaPartitionLag::partition));
        return new KafkaConsumerGroupDetail(groupId, state, List.copyOf(topics), members, assignments, lags, totalLag);
    }

    // ---------------------------------------------------------------- Admin 辅助

    private Map<String, TopicDescription> describeAll(Admin admin, boolean includeInternal) {
        Set<String> names = await(admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names());
        if (names.isEmpty()) {
            return Map.of();
        }
        Map<String, TopicDescription> descriptions = await(admin.describeTopics(names).allTopicNames());
        observeLeaders(descriptions.values());
        return descriptions;
    }

    private Map<String, TopicDescription> describeExisting(Admin admin, Set<String> topics) {
        Set<String> existing = new LinkedHashSet<>(topics);
        existing.retainAll(await(admin.listTopics().names()));
        if (existing.isEmpty()) {
            return Map.of();
        }
        return await(admin.describeTopics(existing).allTopicNames());
    }

    /** 停 broker 前记录一次 leader 基线，确保随后的 leader 切换能被时间线捕获。 */
    private void snapshotLeaders() {
        if (!runtime.isAvailable()) {
            return;
        }
        try {
            describeAll(runtime.admin(), false);
        } catch (RuntimeException ignored) {
            // 基线采集失败不影响节点操作
        }
    }

    private void observeLeaders(java.util.Collection<TopicDescription> descriptions) {
        List<PartitionSample> samples = new ArrayList<>();
        for (TopicDescription description : descriptions) {
            for (TopicPartitionInfo info : description.partitions()) {
                samples.add(new PartitionSample(description.name(), info.partition(), leaderId(info), 0));
            }
        }
        tracker.observeLeaders(samples, (key, change) -> runtime.events().append(
                change.newLeader() < 0 ? "topic" : runtime.nodeIdOf(change.newLeader()),
                "WARN",
                "分区 %s leader 切换：%s → %s".formatted(key, leaderName(change.previousLeader()), leaderName(change.newLeader()))));
    }

    private String leaderName(int kafkaNodeId) {
        return kafkaNodeId < 0 ? "无 leader" : "broker " + kafkaNodeId;
    }

    private List<PartitionSample> partitionSamples(Admin admin, java.util.Collection<TopicDescription> descriptions) {
        Map<String, Long> endOffsets = endOffsets(admin, descriptions);
        List<PartitionSample> samples = new ArrayList<>();
        for (TopicDescription description : descriptions) {
            for (TopicPartitionInfo info : description.partitions()) {
                String key = description.name() + "-" + info.partition();
                samples.add(new PartitionSample(description.name(), info.partition(), leaderId(info), endOffsets.getOrDefault(key, 0L)));
            }
        }
        return samples;
    }

    /** 分区 LEO，键为 {@code topic-partition}；无 leader 的分区不查询。 */
    private Map<String, Long> endOffsets(Admin admin, java.util.Collection<TopicDescription> descriptions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicDescription description : descriptions) {
            for (TopicPartitionInfo info : description.partitions()) {
                if (info.leader() != null) {
                    request.put(new TopicPartition(description.name(), info.partition()), OffsetSpec.latest());
                }
            }
        }
        Map<String, Long> result = new HashMap<>();
        if (request.isEmpty()) {
            return result;
        }
        try {
            await(admin.listOffsets(request).all()).forEach((partition, info) -> result.put(partition.toString(), info.offset()));
        } catch (RuntimeException exception) {
            // leader 切换瞬间个别分区查询失败，返回已知部分
        }
        return result;
    }

    private KafkaTopicDetail toDetail(TopicDescription description, Map<String, Long> endOffsets, Map<String, String> configs) {
        List<KafkaPartitionDetail> partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(info -> new KafkaPartitionDetail(
                        info.partition(),
                        leaderId(info),
                        info.replicas().stream().map(Node::id).toList(),
                        info.isr().stream().map(Node::id).toList(),
                        endOffsets.getOrDefault(description.name() + "-" + info.partition(), -1L)))
                .toList();
        int replicationFactor = partitions.isEmpty() ? 0 : partitions.getFirst().replicas().size();
        return new KafkaTopicDetail(description.name(), description.isInternal(), partitions.size(), replicationFactor, partitions, configs);
    }

    private static int leaderId(TopicPartitionInfo info) {
        return info.leader() == null || info.leader().isEmpty() ? -1 : info.leader().id();
    }

    /**
     * 等待 Admin 结果并把 Kafka 异常翻译为平台语义：参数/不存在类 → 400，超时/不可用 → 409。
     */
    private static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get(8, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 请求被中断", interrupted);
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("Kafka 请求超时：集群可能正在切换 leader 或 broker 不可用", timeout);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new IllegalArgumentException("topic 不存在", cause);
            }
            if (cause instanceof TopicExistsException) {
                throw new IllegalArgumentException("topic 已存在", cause);
            }
            if (cause instanceof GroupIdNotFoundException) {
                throw new IllegalArgumentException("消费者组不存在", cause);
            }
            if (cause instanceof InvalidReplicationFactorException || cause instanceof InvalidPartitionsException
                    || cause instanceof InvalidTopicException) {
                throw new IllegalArgumentException(cause.getMessage(), cause);
            }
            throw new IllegalStateException("Kafka 请求失败: " + (cause == null ? exception.getMessage() : cause.getMessage()), cause);
        }
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
