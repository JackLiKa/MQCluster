package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.clustermanager.application.service.MessageTemplateService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.MessageDeliveryResult;
import com.example.clustermanager.core.model.MessageScenario;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import com.example.clustermanager.core.model.kafka.KafkaConsumerGroupDetail;
import com.example.clustermanager.core.model.kafka.KafkaGroupMember;
import com.example.clustermanager.core.model.kafka.KafkaPartitionDetail;
import com.example.clustermanager.core.model.kafka.KafkaTopicDetail;
import com.example.clustermanager.infrastructure.kafka.KafkaClusterProperties;
import com.example.clustermanager.infrastructure.kafka.KafkaClusterProvider;
import com.example.clustermanager.infrastructure.kafka.group.KafkaConsumerGroupManager;
import com.example.clustermanager.infrastructure.kafka.lab.KafkaInternalsInspector;
import com.example.clustermanager.infrastructure.kafka.lab.KafkaStressLab;
import com.example.clustermanager.infrastructure.kafka.messaging.KafkaMessageWorkbench;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import com.example.clustermanager.infrastructure.kafka.topology.KafkaTopologySeeder;
import com.example.clustermanager.infrastructure.pseudo.runtime.PortPool;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Kafka Provider 集成测试：真实启动 1 controller + 3 broker 的嵌入式 KRaft 集群。
 *
 * <p>覆蓋：一键启停与启动顺序约束、消息模拟（partition/offset/acks）、停 leader 后 leader 切换并记录、
 * 重启后重新进入 ISR、常驻消费者制造 lag 与 rebalance 分配。
 *
 * <p>默认禁用（需 {@code -DrunEmbeddedKafka=true}），避免重型嵌入式 Kafka 纳入常规 CI。
 */
@EnabledIfSystemProperty(named = "runEmbeddedKafka", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaClusterProviderTest {

    private static final ClusterRef REF = new ClusterRef("kafka-lab", ClusterMode.PSEUDO, MiddlewareType.KAFKA);
    private static final String TOPIC = "orders";

    private EmbeddedKafkaRuntime runtime;
    private KafkaConsumerGroupManager groupManager;
    private KafkaClusterProvider provider;

    @BeforeAll
    void setUp() throws Exception {
        Path workDir = Files.createTempDirectory("kafka-provider-test-");
        KafkaClusterProperties properties = new KafkaClusterProperties(true, "kafka-lab", workDir.toString(), true);
        runtime = new EmbeddedKafkaRuntime(properties, new PortPool(), new KafkaTopologySeeder());
        groupManager = new KafkaConsumerGroupManager(runtime);
        provider = new KafkaClusterProvider(properties, runtime,
                new KafkaMessageWorkbench(runtime, new MessageTemplateService()), groupManager,
                new KafkaStressLab(runtime), new KafkaInternalsInspector(runtime));
    }

    @AfterAll
    void tearDown() {
        if (groupManager != null) {
            groupManager.stopAll();
        }
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    @Test
    @Order(1)
    void clusterStartsControllerFirstThenBrokers() {
        assertThatThrownBy(() -> provider.operateNode(REF, "kafka-broker-01", OperationType.START))
                .isInstanceOf(IllegalStateException.class);

        List<OperationResult> results = provider.operateCluster(REF, OperationType.START);

        assertThat(results).hasSize(4).allMatch(OperationResult::success);
        assertThat(results.getFirst().targetId()).isEqualTo("kafka-ctrl-01");
        assertThat(provider.loadTopology(REF).nodes()).allMatch(node -> node.status() == NodeStatus.RUNNING);
        assertThat(provider.loadTopology(REF).activeVip()).startsWith("127.0.0.1:");
    }

    @Test
    @Order(2)
    void controllerCannotStopWhileBrokersRun() {
        assertThatThrownBy(() -> provider.operateNode(REF, "kafka-ctrl-01", OperationType.STOP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broker");
    }

    @Test
    @Order(3)
    void createTopicAndSimulateMessagesWithPartitionAndOffset() {
        KafkaTopicDetail topic = provider.createTopic(REF, TOPIC, 3, 3, Map.of());
        assertThat(topic.partitionCount()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo(3);

        MessageScenario scenario = new MessageScenario(TOPIC, "workbench", 6, "order-{index}", "kafka-producer",
                List.of(), Map.of(), Map.of("partition", "1", "acks", "all", "compression", "lz4"));
        List<MessageDeliveryResult> deliveries = provider.simulate(REF, scenario).deliveries();

        assertThat(deliveries).hasSize(6).allMatch(MessageDeliveryResult::success);
        assertThat(deliveries).allMatch(result -> "1".equals(result.attributes().get("partition")));
        assertThat(deliveries).extracting(result -> result.attributes().get("offset"))
                .containsExactly("0", "1", "2", "3", "4", "5");

        // acks=0 也能通过回读拿到 offset
        List<MessageDeliveryResult> fireAndForget = provider.simulate(REF, new MessageScenario(TOPIC, "workbench", 3, "x",
                "kafka-producer", List.of(), Map.of(), Map.of("acks", "0", "key", "same-key"))).deliveries();
        assertThat(fireAndForget).allMatch(MessageDeliveryResult::success);
        assertThat(fireAndForget).extracting(result -> result.attributes().get("partition")).containsOnly(
                fireAndForget.getFirst().attributes().get("partition"));
    }

    @Test
    @Order(4)
    void leaderFailoverIsRecordedAndBrokerRejoinsIsr() throws Exception {
        KafkaPartitionDetail partition = provider.describeTopic(REF, TOPIC).partitions().getFirst();
        int oldLeader = partition.leader();
        String leaderNode = runtime.nodeIdOf(oldLeader);

        provider.operateNode(REF, leaderNode, OperationType.STOP);
        awaitTrue(() -> {
            KafkaPartitionDetail now = provider.describeTopic(REF, TOPIC).partitions().getFirst();
            return now.leader() >= 0 && now.leader() != oldLeader && !now.isr().contains(oldLeader);
        }, 60_000);
        assertThat(provider.leaderChanges(REF, 10))
                .anyMatch(change -> change.topic().equals(TOPIC) && change.partition() == 0 && change.previousLeader() == oldLeader);

        provider.operateNode(REF, leaderNode, OperationType.START);
        awaitTrue(() -> provider.describeTopic(REF, TOPIC).partitions().getFirst().isr().contains(oldLeader), 60_000);
        assertThat(runtime.status(leaderNode)).isEqualTo(NodeStatus.RUNNING);
    }

    @Test
    @Order(5)
    void consumerGroupLagAndRebalance() throws Exception {
        String group = "lag-demo";
        KafkaGroupMember first = provider.addConsumer(REF, group, TOPIC, 0);
        awaitTrue(() -> {
            KafkaConsumerGroupDetail detail = provider.describeConsumerGroup(REF, group);
            return detail.assignments().getOrDefault(first.memberId(), List.of()).size() == 3;
        }, 30_000);
        KafkaConsumerGroupDetail paused = provider.describeConsumerGroup(REF, group);
        assertThat(paused.totalLag()).isEqualTo(9);

        provider.updateConsumerRate(REF, group, first.memberId(), 1000);
        awaitTrue(() -> provider.describeConsumerGroup(REF, group).totalLag() == 0, 30_000);

        KafkaGroupMember second = provider.addConsumer(REF, group, TOPIC, 10);
        awaitTrue(() -> {
            Map<String, List<String>> assignments = provider.describeConsumerGroup(REF, group).assignments();
            int a = assignments.getOrDefault(first.memberId(), List.of()).size();
            int b = assignments.getOrDefault(second.memberId(), List.of()).size();
            return a + b == 3 && a > 0 && b > 0;
        }, 30_000);

        provider.removeConsumer(REF, group, second.memberId());
        awaitTrue(() -> provider.describeConsumerGroup(REF, group).assignments()
                .getOrDefault(first.memberId(), List.of()).size() == 3, 30_000);
        assertThat(provider.listConsumerGroups(REF)).anyMatch(detail -> detail.groupId().equals(group));
        assertThat(provider.loadLogs(REF, "consumer-group", 50)).anyMatch(entry -> entry.message().contains("rebalance"));
    }

    @Test
    @Order(6)
    void metricsReportPerBrokerThroughput() throws Exception {
        provider.loadMetrics(REF);
        provider.simulate(REF, new MessageScenario(TOPIC, "workbench", 30, "m", "kafka-producer", List.of(), Map.of(), Map.of()));
        Thread.sleep(500);
        double totalIn = provider.loadMetrics(REF).nodes().stream().mapToDouble(m -> m.networkInBytesPerSecond()).sum();
        assertThat(totalIn).isPositive();
    }

    @Test
    @Order(7)
    void clusterStopsBrokersBeforeController() {
        List<OperationResult> results = provider.operateCluster(REF, OperationType.STOP);
        assertThat(results).allMatch(OperationResult::success);
        assertThat(results.getLast().targetId()).isEqualTo("kafka-ctrl-01");
        assertThat(provider.loadTopology(REF).nodes()).allMatch(node -> node.status() == NodeStatus.STOPPED);
        assertThat(groupManager.managedGroups()).isEmpty();
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException transientFailure) {
                // leader 切换期间 Admin 请求可能短暂失败
            }
            Thread.sleep(500);
        }
        assertThat(condition.getAsBoolean()).as("condition within %d ms", timeoutMs).isTrue();
    }
}
