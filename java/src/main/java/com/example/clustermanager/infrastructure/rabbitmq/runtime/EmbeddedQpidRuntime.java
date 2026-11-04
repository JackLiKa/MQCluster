package com.example.clustermanager.infrastructure.rabbitmq.runtime;

import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.infrastructure.pseudo.AuditLog;
import com.example.clustermanager.infrastructure.pseudo.runtime.PortPool;
import com.example.clustermanager.infrastructure.rabbitmq.RabbitClusterProperties;
import com.example.clustermanager.infrastructure.rabbitmq.topology.RabbitTopologySeeder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 嵌入式 Qpid Broker-J 运行时：单 broker + 内存虚拟主机，AMQP 0-9-1 端口动态分配。
 *
 * <p>客户端统一使用官方 {@code com.rabbitmq:amqp-client}。Qpid 遇到不认识的声明参数会以 542 关闭整个连接，
 * 因此每个管理操作都使用独立连接（{@link #withChannel}），互不影响。
 *
 * <p>RabbitMQ 专有参数在这里翻译为 Qpid 等价物：
 * <ul>
 *   <li>{@code x-dead-letter-exchange} → {@code alternateBinding} + {@code maximumDeliveryAttempts=1}</li>
 *   <li>{@code x-message-ttl} → {@code maximumMessageTtl}</li>
 * </ul>
 * 所有平台声明的队列都带 {@code maximumDeliveryAttempts=1}，使 reject/nack(requeue=false) 的行为与 RabbitMQ 一致
 * （Qpid 默认会把这类消息重新入队）。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EmbeddedQpidRuntime {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedQpidRuntime.class);
    private static final Duration PORT_RELEASE_TIMEOUT = Duration.ofSeconds(10);

    private final RabbitClusterProperties properties;
    private final PortPool portPool;
    private final RabbitTopologySeeder seeder;
    /** RabbitMQ 实验集群事件日志，与 RocketMQ 的共享审计日志隔离。 */
    private final AuditLog events = new AuditLog();
    private final AtomicReference<Broker<?>> broker = new AtomicReference<>();

    private volatile NodeStatus status = NodeStatus.STOPPED;
    private volatile Instant startedAt;
    private volatile int port;
    private SystemLauncher launcher;

    public EmbeddedQpidRuntime(RabbitClusterProperties properties, PortPool portPool, RabbitTopologySeeder seeder) {
        this.properties = properties;
        this.portPool = portPool;
        this.seeder = seeder;
    }

    /** 启动 broker 并声明演示拓扑；已运行则直接返回。 */
    public synchronized void start() {
        if (status == NodeStatus.RUNNING) {
            return;
        }
        if (port == 0) {
            port = portPool.allocate();
        }
        status = NodeStatus.STARTING;
        events.append(RabbitTopologySeeder.NODE_ID, "INFO", "正在启动 Qpid Broker-J @ " + address());
        Path workDir = Path.of(properties.resolvedWorkDir()).toAbsolutePath();
        try {
            deleteRecursively(workDir);
            Files.createDirectories(workDir);
            SystemLauncher newLauncher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener() {
                @Override
                public void onContainerResolve(SystemConfig<?> systemConfig) {
                    broker.set(systemConfig.getContainer(Broker.class));
                }
            });
            newLauncher.startup(launchAttributes(workDir));
            launcher = newLauncher;
            startedAt = Instant.now();
            status = NodeStatus.RUNNING;
            seedDemoTopology();
            events.append(RabbitTopologySeeder.NODE_ID, "INFO", "Qpid Broker-J 已启动，演示交换机与队列已声明");
        } catch (Exception exception) {
            status = NodeStatus.FAILED;
            events.append(RabbitTopologySeeder.NODE_ID, "ERROR", "启动失败: " + exception.getMessage());
            safeShutdown();
            throw new IllegalStateException("RabbitMQ broker 启动失败: " + exception.getMessage(), exception);
        }
    }

    /** 停止 broker 并等待端口释放；内存虚拟主机中的交换机、队列与消息随之清空。 */
    public synchronized void stop() {
        if (launcher == null) {
            status = NodeStatus.STOPPED;
            return;
        }
        events.append(RabbitTopologySeeder.NODE_ID, "INFO", "正在停止 Qpid Broker-J");
        safeShutdown();
        awaitPortReleased(port);
        status = NodeStatus.STOPPED;
        events.append(RabbitTopologySeeder.NODE_ID, "WARN", "Qpid Broker-J 已停止（内存虚拟主机已清空）");
    }

    /** 重启 broker。 */
    public void restart() {
        stop();
        start();
    }

    public NodeStatus status() {
        return status;
    }

    public boolean isRunning() {
        return status == NodeStatus.RUNNING;
    }

    /** 启动时间，未运行返回 {@code null}。 */
    public Instant startedAt() {
        return isRunning() ? startedAt : null;
    }

    /** AMQP 端口（首次启动前为 0）。 */
    public int port() {
        return port;
    }

    /** AMQP 地址。 */
    public String address() {
        return "127.0.0.1:" + (port == 0 ? "-" : port);
    }

    /**
     * 确认 broker 在运行。
     *
     * @throws IllegalStateException broker 未运行
     */
    public void requireRunning() {
        if (!isRunning()) {
            throw new IllegalStateException("RabbitMQ broker 未运行：请先启动 " + RabbitTopologySeeder.NODE_ID);
        }
    }

    /** 进程内虚拟主机对象模型（Broker → VirtualHostNode → VirtualHost）。 */
    public VirtualHost<?> virtualHost() {
        requireRunning();
        Broker<?> current = broker.get();
        VirtualHostNode<?> node = current.getChildren(VirtualHostNode.class).iterator().next();
        VirtualHost<?> host = node.getVirtualHost();
        if (host == null) {
            throw new IllegalStateException("RabbitMQ 虚拟主机尚未就绪");
        }
        return host;
    }

    /** 新建 AMQP 连接（调用方负责关闭）。 */
    public Connection newConnection(String name) throws IOException, TimeoutException {
        requireRunning();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(port);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setAutomaticRecoveryEnabled(false);
        factory.setConnectionTimeout(5_000);
        return factory.newConnection(name);
    }

    /** 在独立连接的 channel 上执行一次操作。 */
    @FunctionalInterface
    public interface ChannelWork<T> {
        T run(Channel channel) throws Exception;
    }

    /**
     * 用独立连接执行操作，并把 AMQP 错误翻译为平台异常。
     *
     * @throws IllegalArgumentException 404 / 406 / 403 / 542 等客户端可修正的错误
     * @throws IllegalStateException    broker 不可用或其他错误
     */
    public <T> T withChannel(String purpose, ChannelWork<T> work) {
        requireRunning();
        try (Connection connection = newConnection("mq-lab-" + purpose); Channel channel = connection.createChannel()) {
            return work.run(channel);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            throw translate(exception);
        }
    }

    /** 把 AMQP 关闭原因翻译为平台异常。 */
    public static RuntimeException translate(Exception exception) {
        ShutdownSignalException signal = findShutdownSignal(exception);
        if (signal != null && signal.getReason() instanceof AMQP.Channel.Close close) {
            return clientError(close.getReplyCode(), close.getReplyText());
        }
        if (signal != null && signal.getReason() instanceof AMQP.Connection.Close close) {
            return clientError(close.getReplyCode(), close.getReplyText());
        }
        return new IllegalStateException("AMQP 操作失败: " + exception.getMessage(), exception);
    }

    private static RuntimeException clientError(int code, String text) {
        String message = switch (code) {
            case 404 -> "对象不存在（404）：" + text;
            case 406 -> "前置条件不满足（406）：" + text;
            case 403 -> "无权限（403）：" + text;
            case 530, 540, 542 -> "Qpid 不支持该操作或参数（" + code + "）：" + text;
            default -> "AMQP 错误（" + code + "）：" + text;
        };
        return code >= 500 && code != 530 && code != 540 && code != 542
                ? new IllegalStateException(message)
                : new IllegalArgumentException(message);
    }

    private static ShutdownSignalException findShutdownSignal(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ShutdownSignalException signal) {
                return signal;
            }
            current = current.getCause();
        }
        return null;
    }

    /**
     * 构造队列声明参数：RabbitMQ 语义翻译为 Qpid 参数。
     *
     * @param deadLetterExchange 死信交换机（可为 {@code null}）
     * @param messageTtlMs       队列级 TTL（可为 {@code null}）
     */
    public static Map<String, Object> queueArguments(String deadLetterExchange, Long messageTtlMs) {
        Map<String, Object> args = new HashMap<>();
        args.put("maximumDeliveryAttempts", 1);
        if (deadLetterExchange != null && !deadLetterExchange.isBlank()) {
            args.put("alternateBinding", "{\"destination\":\"" + deadLetterExchange + "\"}");
        }
        if (messageTtlMs != null && messageTtlMs > 0) {
            args.put("maximumMessageTtl", messageTtlMs);
        }
        return args;
    }

    /** 事件日志。 */
    public AuditLog events() {
        return events;
    }

    @PreDestroy
    public synchronized void shutdown() {
        safeShutdown();
        status = NodeStatus.STOPPED;
    }

    private void seedDemoTopology() {
        withChannel("seed", channel -> {
            for (RabbitTopologySeeder.ExchangeSeed exchange : seeder.exchanges()) {
                channel.exchangeDeclare(exchange.name(), exchange.type(), true);
            }
            for (RabbitTopologySeeder.QueueSeed queue : seeder.queues()) {
                channel.queueDeclare(queue.name(), true, false, false, queueArguments(queue.deadLetterExchange(), queue.messageTtlMs()));
            }
            for (RabbitTopologySeeder.BindingSeed binding : seeder.bindings()) {
                Map<String, Object> args = binding.headers().isEmpty() ? null : new LinkedHashMap<>(binding.headers());
                channel.queueBind(binding.queue(), binding.exchange(), binding.routingKey(), args);
            }
            return null;
        });
    }

    private Map<String, Object> launchAttributes(Path workDir) {
        URL config = getClass().getClassLoader().getResource("qpid/initial-config.json");
        if (config == null) {
            throw new IllegalStateException("缺少 classpath 资源 qpid/initial-config.json");
        }
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("type", "Memory");
        attrs.put("initialConfigurationLocation", config.toExternalForm());
        attrs.put("startupLoggedToSystemOut", false);
        attrs.put("context", Map.of(
                "qpid.amqp_port", String.valueOf(port),
                "qpid.work_dir", workDir.toString(),
                "qpid.broker.defaultPreferenceStoreAttributes", "{\"type\": \"Noop\"}"));
        return attrs;
    }

    private void safeShutdown() {
        SystemLauncher current = launcher;
        launcher = null;
        broker.set(null);
        startedAt = null;
        if (current != null) {
            try {
                current.shutdown();
            } catch (Exception exception) {
                log.warn("Qpid shutdown failed: {}", exception.getMessage());
            }
        }
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

    private static void awaitPortReleased(int port) {
        if (port == 0) {
            return;
        }
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
