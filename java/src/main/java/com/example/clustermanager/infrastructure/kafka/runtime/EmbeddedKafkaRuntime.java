package com.example.clustermanager.infrastructure.kafka.runtime;

import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.infrastructure.kafka.KafkaClusterProperties;
import com.example.clustermanager.infrastructure.kafka.topology.KafkaTopologySeeder;
import com.example.clustermanager.infrastructure.kafka.topology.KafkaTopologySeeder.KafkaNodeSeed;
import com.example.clustermanager.infrastructure.pseudo.AuditLog;
import com.example.clustermanager.infrastructure.pseudo.runtime.PortPool;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.common.MetadataVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 嵌入式 KRaft 集群运行时：1 个 controller + 3 个 broker，全部运行在当前 JVM 内。
 *
 * <p>生命周期约束（阶段 0 验证所得）：
 * <ul>
 *   <li>首次准备时一次性分配端口、生成集群 UUID 并格式化全部日志目录；端口在进程生命周期内保持不变。</li>
 *   <li>broker 必须在 controller 运行时才能启动（启动会阻塞直到注册完成）。</li>
 *   <li>controller 停止前必须先停所有 broker——否则 broker 的受控关闭会因联系不上 controller 挂起数分钟。</li>
 *   <li>{@code KafkaRaftServer} 不可重复启动，重启 = 新建实例 + 复用同一日志目录。</li>
 * </ul>
 *
 * <p>线程安全：每个节点独立加锁，允许多个 broker 并行启动。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EmbeddedKafkaRuntime {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaRuntime.class);
    private static final Duration PORT_RELEASE_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaClusterProperties properties;
    private final PortPool portPool;
    private final KafkaTopologySeeder seeder;
    /** Kafka 集群内部事件日志（生命周期、rebalance、leader 切换），与 RocketMQ 的共享审计日志隔离。 */
    private final AuditLog events = new AuditLog();

    private final Map<String, KafkaNodeSpec> specs = new LinkedHashMap<>();
    private final Map<String, EmbeddedKafkaNode> running = new ConcurrentHashMap<>();
    private final Set<String> starting = ConcurrentHashMap.newKeySet();
    private final Set<String> failed = ConcurrentHashMap.newKeySet();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    private volatile boolean prepared;
    private String kafkaClusterUuid;
    private String voters;
    private volatile Admin admin;

    public EmbeddedKafkaRuntime(KafkaClusterProperties properties, PortPool portPool, KafkaTopologySeeder seeder) {
        this.properties = properties;
        this.portPool = portPool;
        this.seeder = seeder;
    }

    /**
     * 首次调用时准备集群：清理/创建工作目录、分配端口、格式化 KRaft 存储。
     */
    public synchronized void ensurePrepared() {
        if (prepared) {
            return;
        }
        Path workDir = Path.of(properties.resolvedWorkDir()).toAbsolutePath();
        try {
            if (properties.resolvedCleanStoreOnStart()) {
                deleteRecursively(workDir);
            }
            Files.createDirectories(workDir);
            kafkaClusterUuid = loadOrCreateClusterUuid(workDir);
        } catch (IOException exception) {
            throw new UncheckedIOException("无法准备 Kafka 工作目录: " + workDir, exception);
        }
        for (KafkaNodeSeed seed : seeder.seeds()) {
            specs.put(seed.nodeId(), new KafkaNodeSpec(seed, portPool.allocate(), workDir.resolve(seed.nodeId())));
        }
        voters = specs.values().stream()
                .filter(KafkaNodeSpec::controller)
                .map(spec -> spec.seed().kafkaNodeId() + "@" + spec.address())
                .collect(Collectors.joining(","));
        specs.values().forEach(this::format);
        prepared = true;
        events.append("cluster", "INFO", "KRaft 存储已格式化，集群 UUID=" + kafkaClusterUuid);
    }

    /** 所有节点规格（controller 在前）。 */
    public List<KafkaNodeSpec> specs() {
        ensurePrepared();
        return List.copyOf(specs.values());
    }

    /**
     * 查找节点规格。
     *
     * @throws IllegalArgumentException 节点不存在
     */
    public KafkaNodeSpec spec(String nodeId) {
        ensurePrepared();
        KafkaNodeSpec spec = specs.get(nodeId);
        if (spec == null) {
            throw new IllegalArgumentException("Kafka 节点不存在: " + nodeId);
        }
        return spec;
    }

    /**
     * 启动节点；已运行则直接返回。
     *
     * @throws IllegalStateException broker 启动时 controller 未运行，或启动失败
     */
    public void start(String nodeId) {
        KafkaNodeSpec spec = spec(nodeId);
        synchronized (lock(nodeId)) {
            if (running.containsKey(nodeId)) {
                return;
            }
            if (!spec.controller() && !controllerRunning()) {
                throw new IllegalStateException("请先启动 KRaft Controller，broker 需要向 controller 注册");
            }
            starting.add(nodeId);
            failed.remove(nodeId);
            events.append(nodeId, "INFO", "正在启动 " + spec.seed().displayName() + " @ " + spec.address());
            EmbeddedKafkaNode node = null;
            try {
                node = new EmbeddedKafkaNode(spec, spec.toProperties(voters));
                node.start();
                running.put(nodeId, node);
                events.append(nodeId, "INFO", spec.seed().displayName() + " 已启动");
            } catch (RuntimeException exception) {
                failed.add(nodeId);
                events.append(nodeId, "ERROR", "启动失败: " + exception.getMessage());
                if (node != null) {
                    safeStop(node);
                }
                throw new IllegalStateException("Kafka 节点启动失败: " + nodeId + " - " + exception.getMessage(), exception);
            } finally {
                starting.remove(nodeId);
            }
        }
    }

    /**
     * 停止节点并等待端口释放；未运行则直接返回。
     *
     * @throws IllegalStateException 停止 controller 时仍有 broker 运行
     */
    public void stop(String nodeId) {
        KafkaNodeSpec spec = spec(nodeId);
        synchronized (lock(nodeId)) {
            EmbeddedKafkaNode node = running.get(nodeId);
            failed.remove(nodeId);
            if (node == null) {
                return;
            }
            if (spec.controller() && runningBrokerCount() > 0) {
                throw new IllegalStateException("请先停止所有 broker 再停止 KRaft Controller，否则 broker 无法完成受控关闭");
            }
            events.append(nodeId, "INFO", "正在停止 " + spec.seed().displayName());
            if (!spec.controller() && runningBrokerCount() == 1) {
                closeAdmin();
            }
            node.stop();
            running.remove(nodeId);
            awaitPortReleased(spec.port());
            events.append(nodeId, "WARN", spec.seed().displayName() + " 已停止");
        }
    }

    /** 重启节点：停止旧实例后用同一日志目录新建实例。 */
    public void restart(String nodeId) {
        stop(nodeId);
        start(nodeId);
    }

    /**
     * 节点状态：启动中 → STARTING；broker 运行但 controller 已停 → DEGRADED；启动失败 → FAILED。
     */
    public NodeStatus status(String nodeId) {
        KafkaNodeSpec spec = spec(nodeId);
        if (starting.contains(nodeId)) {
            return NodeStatus.STARTING;
        }
        if (running.containsKey(nodeId)) {
            return !spec.controller() && !controllerRunning() ? NodeStatus.DEGRADED : NodeStatus.RUNNING;
        }
        return failed.contains(nodeId) ? NodeStatus.FAILED : NodeStatus.STOPPED;
    }

    /** 节点启动时间，未运行返回 {@code null}。 */
    public Instant startedAt(String nodeId) {
        EmbeddedKafkaNode node = running.get(nodeId);
        return node == null ? null : node.startedAt();
    }

    public boolean controllerRunning() {
        return specs.values().stream().filter(KafkaNodeSpec::controller).anyMatch(spec -> running.containsKey(spec.nodeId()));
    }

    public int runningBrokerCount() {
        return (int) specs.values().stream().filter(spec -> !spec.controller()).filter(spec -> running.containsKey(spec.nodeId())).count();
    }

    public boolean isRunning(String nodeId) {
        return running.containsKey(nodeId);
    }

    /** 所有 broker 的地址（含已停止的）；客户端会自动跳过不可达节点。 */
    public String bootstrapServers() {
        return specs().stream().filter(spec -> !spec.controller()).map(KafkaNodeSpec::address).collect(Collectors.joining(","));
    }

    /** Kafka node.id → 平台节点 ID。 */
    public String nodeIdOf(int kafkaNodeId) {
        return specs().stream()
                .filter(spec -> spec.seed().kafkaNodeId() == kafkaNodeId)
                .map(KafkaNodeSpec::nodeId)
                .findFirst()
                .orElse("kafka-node-" + kafkaNodeId);
    }

    /**
     * 确认至少一个 broker 在运行。
     *
     * @throws IllegalStateException 集群不可用
     */
    public void requireAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException("Kafka 集群未运行：请先启动 controller 和至少一个 broker");
        }
    }

    /** controller 与至少一个 broker 在运行。 */
    public boolean isAvailable() {
        return controllerRunning() && runningBrokerCount() > 0;
    }

    /** 共享 Admin 客户端（懒创建）。 */
    public Admin admin() {
        requireAvailable();
        Admin current = admin;
        if (current == null) {
            synchronized (this) {
                current = admin;
                if (current == null) {
                    current = Admin.create(Map.of(
                            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                            AdminClientConfig.CLIENT_ID_CONFIG, "mq-lab-admin",
                            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000
                    ));
                    admin = current;
                }
            }
        }
        return current;
    }

    /** Kafka 集群事件日志。 */
    public AuditLog events() {
        return events;
    }

    /** 应用关闭时按 broker → controller 顺序停止，避免 broker 受控关闭挂起。 */
    @PreDestroy
    public void shutdown() {
        closeAdmin();
        specs.values().stream()
                .sorted(Comparator.comparing(KafkaNodeSpec::controller))
                .map(spec -> running.remove(spec.nodeId()))
                .filter(node -> node != null)
                .forEach(this::safeStop);
    }

    private synchronized void closeAdmin() {
        if (admin != null) {
            try {
                admin.close(Duration.ofSeconds(2));
            } catch (Exception exception) {
                log.debug("close kafka admin failed", exception);
            }
            admin = null;
        }
    }

    private void safeStop(EmbeddedKafkaNode node) {
        try {
            node.stop();
        } catch (Exception exception) {
            log.warn("Kafka node {} stop failed: {}", node.spec().nodeId(), exception.getMessage());
        }
    }

    private Object lock(String nodeId) {
        return locks.computeIfAbsent(nodeId, key -> new Object());
    }

    private void format(KafkaNodeSpec spec) {
        Properties props = spec.toProperties(voters);
        try {
            KafkaConfig config = new KafkaConfig(props, false);
            new Formatter()
                    .setPrintStream(new PrintStream(OutputStream.nullOutputStream()))
                    .setClusterId(kafkaClusterUuid)
                    .setNodeId(spec.seed().kafkaNodeId())
                    .setControllerListenerName("CONTROLLER")
                    .setMetadataLogDirectory(config.metadataLogDir())
                    .setDirectories(List.of(spec.logDir().toString()))
                    .setReleaseVersion(MetadataVersion.LATEST_PRODUCTION)
                    .setIgnoreFormatted(true)
                    .run();
        } catch (Exception exception) {
            throw new IllegalStateException("KRaft 存储格式化失败: " + spec.nodeId(), exception);
        }
    }

    /** 不清理存储时复用已有集群 UUID，保证已格式化的日志目录仍可启动。 */
    private static String loadOrCreateClusterUuid(Path workDir) throws IOException {
        Path file = workDir.resolve("cluster.id");
        if (Files.exists(file)) {
            String existing = Files.readString(file, StandardCharsets.UTF_8).trim();
            if (!existing.isEmpty()) {
                return existing;
            }
        }
        String uuid = Uuid.randomUuid().toString();
        Files.writeString(file, uuid, StandardCharsets.UTF_8);
        return uuid;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    /** 等待端口真正释放，避免紧接着的重启撞上 TIME_WAIT/未关闭的 acceptor。 */
    private static void awaitPortReleased(int port) {
        long deadline = System.nanoTime() + PORT_RELEASE_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", port));
                return;
            } catch (IOException busy) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
