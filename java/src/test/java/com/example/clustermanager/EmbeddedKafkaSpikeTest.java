package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 阶段 0 Spike：验证 Kafka 4.x KRaft 能否在同一 JVM 内以 1 controller + 3 broker 运行，
 * 且与 RocketMQ / Qpid 依赖共存。
 *
 * <p>验证目标：
 * <ul>
 *   <li>代码内用 {@link Formatter} 完成 KRaft 存储格式化（不依赖 kafka-storage.sh）。</li>
 *   <li>AdminClient 创建 3 分区 × 3 副本 topic，并真实 produce。</li>
 *   <li>停掉 partition-0 的 leader broker 后 leader 切换、ISR 收缩。</li>
 *   <li>同 node.id + 同日志目录新建 {@link KafkaRaftServer} 实例重启后重新进入 ISR。</li>
 * </ul>
 *
 * <p>默认禁用（需 {@code -DrunEmbeddedKafkaSpike=true}），避免重型嵌入式 Kafka 纳入常规 CI。
 */
@EnabledIfSystemProperty(named = "runEmbeddedKafkaSpike", matches = "true")
class EmbeddedKafkaSpikeTest {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaSpikeTest.class);
    private static final int CONTROLLER_ID = 3000;

    @Test
    void kraftClusterLeaderFailoverAndRejoin() throws Exception {
        Path baseDir = Files.createTempDirectory("kafka-spike-");
        String clusterId = Uuid.randomUuid().toString();
        int controllerPort = freePort();
        int[] brokerPorts = {freePort(), freePort(), freePort()};
        String voters = CONTROLLER_ID + "@127.0.0.1:" + controllerPort;

        Map<Integer, Properties> configs = new HashMap<>();
        configs.put(CONTROLLER_ID, controllerProps(baseDir, controllerPort, voters));
        for (int i = 0; i < 3; i++) {
            configs.put(i + 1, brokerProps(baseDir, i + 1, brokerPorts[i], voters));
        }
        configs.forEach((id, props) -> format(props, clusterId, id));

        Map<Integer, KafkaRaftServer> servers = new HashMap<>();
        long t0 = System.currentTimeMillis();
        servers.put(CONTROLLER_ID, start(configs.get(CONTROLLER_ID)));
        // broker 启动会阻塞直到向 controller 注册完成，并行启动以缩短耗时
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int id = 1; id <= 3; id++) {
            int nodeId = id;
            futures.add(CompletableFuture.runAsync(() -> {
                KafkaRaftServer server = start(configs.get(nodeId));
                synchronized (servers) {
                    servers.put(nodeId, server);
                }
            }));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);
        log.info("KRaft 1 controller + 3 broker started in {} ms", System.currentTimeMillis() - t0);

        String bootstrap = "127.0.0.1:" + brokerPorts[0] + ",127.0.0.1:" + brokerPorts[1] + ",127.0.0.1:" + brokerPorts[2];
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000))) {
            admin.createTopics(List.of(new NewTopic("spike-topic", 3, (short) 3))).all().get(30, TimeUnit.SECONDS);
            awaitFullIsr(admin, 3);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                for (int i = 0; i < 30; i++) {
                    RecordMetadata md = producer.send(new ProducerRecord<>("spike-topic", "k" + i, "v" + i)).get(10, TimeUnit.SECONDS);
                    assertThat(md.offset()).isGreaterThanOrEqualTo(0);
                }
            }

            TopicPartitionInfo p0 = describe(admin).partitions().get(0);
            int oldLeader = p0.leader().id();
            log.info("partition-0 leader={} isr={}", oldLeader, p0.isr());

            long heapAll = usedHeapMb();
            log.info("Heap used with KRaft 1+3 running: {} MB", heapAll);

            // 停掉 leader 所在 broker，等待 leader 切换
            servers.get(oldLeader).shutdown();
            servers.get(oldLeader).awaitShutdown();
            long deadline = System.currentTimeMillis() + 60_000;
            TopicPartitionInfo after = null;
            while (System.currentTimeMillis() < deadline) {
                after = describe(admin).partitions().get(0);
                if (after.leader() != null && after.leader().id() != oldLeader
                        && after.isr().stream().noneMatch(n -> n.id() == oldLeader)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertThat(after).isNotNull();
            assertThat(after.leader().id()).isNotEqualTo(oldLeader);
            log.info("leader switched {} -> {} isr={}", oldLeader, after.leader().id(), after.isr());

            // 同 node.id、同日志目录新建实例重启，应重新加入 ISR
            servers.put(oldLeader, start(configs.get(oldLeader)));
            deadline = System.currentTimeMillis() + 60_000;
            boolean rejoined = false;
            while (System.currentTimeMillis() < deadline && !rejoined) {
                rejoined = describe(admin).partitions().get(0).isr().stream().anyMatch(n -> n.id() == oldLeader);
                if (!rejoined) {
                    Thread.sleep(500);
                }
            }
            assertThat(rejoined).as("restarted broker rejoins ISR").isTrue();
        } finally {
            for (int id = 1; id <= 3; id++) {
                KafkaRaftServer s = servers.get(id);
                if (s != null) {
                    s.shutdown();
                    s.awaitShutdown();
                }
            }
            servers.get(CONTROLLER_ID).shutdown();
            servers.get(CONTROLLER_ID).awaitShutdown();
        }
    }

    private static TopicDescription describe(Admin admin) throws Exception {
        return admin.describeTopics(List.of("spike-topic")).allTopicNames().get(10, TimeUnit.SECONDS).get("spike-topic");
    }

    private static void awaitFullIsr(Admin admin, int replicas) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (describe(admin).partitions().stream().allMatch(p -> p.isr().size() == replicas)) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("ISR not full");
    }

    private static Properties controllerProps(Path base, int port, String voters) {
        Properties p = new Properties();
        p.put("process.roles", "controller");
        p.put("node.id", String.valueOf(CONTROLLER_ID));
        p.put("controller.quorum.voters", voters);
        p.put("listeners", "CONTROLLER://127.0.0.1:" + port);
        p.put("controller.listener.names", "CONTROLLER");
        p.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        p.put("log.dirs", base.resolve("controller").toString());
        return p;
    }

    private static Properties brokerProps(Path base, int id, int port, String voters) {
        Properties p = new Properties();
        p.put("process.roles", "broker");
        p.put("node.id", String.valueOf(id));
        p.put("controller.quorum.voters", voters);
        p.put("listeners", "PLAINTEXT://127.0.0.1:" + port);
        p.put("advertised.listeners", "PLAINTEXT://127.0.0.1:" + port);
        p.put("controller.listener.names", "CONTROLLER");
        p.put("inter.broker.listener.name", "PLAINTEXT");
        p.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        p.put("log.dirs", base.resolve("broker-" + id).toString());
        p.put("offsets.topic.replication.factor", "3");
        p.put("offsets.topic.num.partitions", "6");
        p.put("transaction.state.log.replication.factor", "3");
        p.put("transaction.state.log.min.isr", "1");
        p.put("share.coordinator.state.topic.replication.factor", "1");
        p.put("share.coordinator.state.topic.min.isr", "1");
        p.put("group.initial.rebalance.delay.ms", "0");
        p.put("replica.lag.time.max.ms", "10000");
        p.put("log.segment.bytes", String.valueOf(16 * 1024 * 1024));
        // 内存调优：log cleaner 默认每个 broker 预分配 128MB 去重缓冲区，3 broker 即 384MB 堆
        p.put("log.cleaner.dedupe.buffer.size", String.valueOf(2 * 1024 * 1024));
        p.put("log.cleaner.threads", "1");
        p.put("num.network.threads", "2");
        p.put("num.io.threads", "2");
        p.put("num.replica.fetchers", "1");
        p.put("background.threads", "2");
        return p;
    }

    private static void format(Properties props, String clusterId, int nodeId) {
        try {
            KafkaConfig config = new KafkaConfig(props, false);
            new Formatter()
                    .setPrintStream(new PrintStream(OutputStream.nullOutputStream()))
                    .setClusterId(clusterId)
                    .setNodeId(nodeId)
                    .setControllerListenerName("CONTROLLER")
                    .setMetadataLogDirectory(config.metadataLogDir())
                    .setDirectories(List.of(props.getProperty("log.dirs")))
                    .setReleaseVersion(MetadataVersion.LATEST_PRODUCTION)
                    .setIgnoreFormatted(true)
                    .run();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static KafkaRaftServer start(Properties props) {
        KafkaRaftServer server = new KafkaRaftServer(new KafkaConfig(props, false), Time.SYSTEM);
        server.startup();
        return server;
    }

    private static long usedHeapMb() {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
