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
import com.example.clustermanager.core.model.rabbit.RabbitConsumer;
import com.example.clustermanager.core.model.rabbit.RabbitQueue;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.core.model.rabbit.RabbitRoutingTopology;
import com.example.clustermanager.infrastructure.pseudo.runtime.PortPool;
import com.example.clustermanager.infrastructure.rabbitmq.RabbitClusterProperties;
import com.example.clustermanager.infrastructure.rabbitmq.RabbitClusterProvider;
import com.example.clustermanager.infrastructure.rabbitmq.consumer.RabbitConsumerManager;
import com.example.clustermanager.infrastructure.rabbitmq.lab.RabbitInternalsInspector;
import com.example.clustermanager.infrastructure.rabbitmq.lab.RabbitStressLab;
import com.example.clustermanager.infrastructure.rabbitmq.messaging.RabbitMessageWorkbench;
import com.example.clustermanager.infrastructure.rabbitmq.routing.RabbitRouteMatcher;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.EmbeddedQpidRuntime;
import com.example.clustermanager.infrastructure.rabbitmq.runtime.RabbitObjectModel;
import com.example.clustermanager.infrastructure.rabbitmq.topology.RabbitTopologySeeder;
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

/**
 * RabbitMQ Provider 集成测试：真实启动进程内 Qpid Broker-J（约 0.5s、30MB），用官方客户端收发。
 *
 * <p>覆盖：启停与演示拓扑、四种交换机路由、路由预览、nack → 死信 / 重新入队、TTL 过期、
 * 不可路由退回、prefetch 限制 unacked、常驻消费者、声明/删除对象与错误语义。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RabbitClusterProviderTest {

    private static final ClusterRef REF = new ClusterRef("rabbitmq-lab", ClusterMode.PSEUDO, MiddlewareType.RABBITMQ);

    private EmbeddedQpidRuntime runtime;
    private RabbitConsumerManager consumerManager;
    private RabbitClusterProvider provider;

    @BeforeAll
    void setUp() throws Exception {
        Path workDir = Files.createTempDirectory("rabbit-provider-test-");
        RabbitClusterProperties properties = new RabbitClusterProperties(true, "rabbitmq-lab", workDir.toString());
        runtime = new EmbeddedQpidRuntime(properties, new PortPool(), new RabbitTopologySeeder());
        RabbitObjectModel model = new RabbitObjectModel(runtime);
        consumerManager = new RabbitConsumerManager(runtime);
        provider = new RabbitClusterProvider(properties, runtime, model,
                new RabbitMessageWorkbench(runtime, model, new MessageTemplateService()), consumerManager,
                new RabbitStressLab(runtime, model), new RabbitInternalsInspector(runtime, model));
    }

    @AfterAll
    void tearDown() {
        if (consumerManager != null) {
            consumerManager.shutdown();
        }
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    @Test
    @Order(1)
    void startsStoppedThenSeedsDemoTopology() {
        assertThat(provider.loadTopology(REF).nodes().getFirst().status()).isEqualTo(NodeStatus.STOPPED);
        assertThatThrownBy(() -> provider.routing(REF)).isInstanceOf(IllegalStateException.class);

        List<OperationResult> results = provider.operateCluster(REF, OperationType.START);

        assertThat(results).hasSize(1).allMatch(OperationResult::success);
        assertThat(provider.loadTopology(REF).nodes().getFirst().labels()).containsEntry("role", "rabbit-broker");
        RabbitRoutingTopology routing = provider.routing(REF);
        assertThat(routing.exchanges()).extracting(exchange -> exchange.name())
                .contains("", "lab.direct", "lab.fanout", "lab.topic", "lab.headers", "lab.dlx");
        assertThat(routing.exchanges()).filteredOn(exchange -> exchange.name().equals("lab.headers"))
                .singleElement().satisfies(exchange -> assertThat(exchange.type()).isEqualTo("headers"));
        assertThat(routing.queues()).extracting(RabbitQueue::name).contains("orders.created", "work.tasks", "dead.letters");
        assertThat(routing.queues()).filteredOn(queue -> queue.name().equals("work.delay")).singleElement()
                .satisfies(queue -> {
                    assertThat(queue.deadLetterExchange()).isEqualTo("lab.dlx");
                    assertThat(queue.messageTtlMs()).isEqualTo(5000L);
                    assertThat(queue.arguments()).containsEntry("x-dead-letter-exchange", "lab.dlx");
                });
        assertThat(routing.bindings()).anySatisfy(binding -> {
            assertThat(binding.source()).isEqualTo("lab.headers");
            assertThat(binding.headers()).containsEntry("x-match", "all").containsEntry("type", "invoice");
        });
        assertThat(routing.bindings()).noneMatch(binding -> binding.source().isEmpty());
    }

    @Test
    @Order(2)
    void routePreviewCoversFourExchangeTypes() {
        assertThat(provider.previewRoute(REF, "lab.direct", "order.paid", Map.of()).matchedQueues()).containsExactly("orders.paid");
        assertThat(provider.previewRoute(REF, "lab.fanout", "ignored", Map.of()).matchedQueues())
                .containsExactlyInAnyOrder("notify.email", "notify.sms");
        assertThat(provider.previewRoute(REF, "lab.topic", "app.db.error", Map.of()).matchedQueues())
                .containsExactlyInAnyOrder("logs.error", "logs.all");
        assertThat(provider.previewRoute(REF, "lab.topic", "infra.error", Map.of()).matchedQueues()).containsExactly("logs.error");
        assertThat(provider.previewRoute(REF, "lab.headers", "", Map.of("type", "invoice", "format", "pdf")).matchedQueues())
                .containsExactly("invoice.pdf");
        assertThat(provider.previewRoute(REF, "lab.headers", "", Map.of("type", "invoice")).matchedQueues()).isEmpty();
        assertThat(provider.previewRoute(REF, "", "orders.created", Map.of()).matchedQueues()).containsExactly("orders.created");
        RabbitRoutePreview miss = provider.previewRoute(REF, "lab.direct", "nope", Map.of());
        assertThat(miss.matchedQueues()).isEmpty();
        assertThat(miss.explanation()).contains("不可路由");
        assertThatThrownBy(() -> provider.previewRoute(REF, "missing.exchange", "x", Map.of()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(RabbitRouteMatcher.topicMatches("a.*.c", "a.b.c")).isTrue();
        assertThat(RabbitRouteMatcher.topicMatches("a.*.c", "a.c")).isFalse();
        assertThat(RabbitRouteMatcher.topicMatches("#", "")).isTrue();
        assertThat(RabbitRouteMatcher.headersMatch(Map.of("x-match", "any", "a", "1", "b", "2"), Map.of("b", "2"))).isTrue();
    }

    @Test
    @Order(3)
    void simulateDeliversThroughEachExchangeType() {
        List<MessageDeliveryResult> fanout = simulate(2, Map.of("exchange", "lab.fanout", "routingKey", ""));
        assertThat(fanout).hasSize(4).allMatch(MessageDeliveryResult::success)
                .allSatisfy(result -> assertThat(result.attributes()).containsEntry("outcome", "ACKED").containsEntry("exchangeType", "fanout"));
        assertThat(fanout).extracting(MessageDeliveryResult::consumerNodeId).contains("notify.email", "notify.sms");

        List<MessageDeliveryResult> topic = simulate(1, Map.of("exchange", "lab.topic", "routingKey", "app.api.error"));
        assertThat(topic).extracting(result -> result.attributes().get("queue")).containsExactlyInAnyOrder("logs.error", "logs.all");

        List<MessageDeliveryResult> headers = simulate(1, Map.of("exchange", "lab.headers", "headers.type", "invoice", "headers.format", "pdf"));
        assertThat(headers).singleElement().satisfies(result -> assertThat(result.attributes()).containsEntry("queue", "invoice.pdf"));

        List<MessageDeliveryResult> direct = simulate(3, Map.of("exchange", "lab.direct", "routingKey", "order.created", "confirm", "true"));
        assertThat(direct).hasSize(3).allSatisfy(result -> assertThat(result.attributes())
                .containsEntry("queue", "orders.created").containsEntry("confirmed", "true").containsEntry("outcome", "ACKED"));
    }

    @Test
    @Order(4)
    void unroutableMessagesAreReturned() {
        List<MessageDeliveryResult> results = simulate(2, Map.of("exchange", "lab.direct", "routingKey", "no.such.key"));
        assertThat(results).hasSize(2).noneMatch(MessageDeliveryResult::success)
                .allSatisfy(result -> assertThat(result.attributes()).containsEntry("outcome", "UNROUTABLE"));
    }

    @Test
    @Order(5)
    void nackWithoutRequeueGoesToDeadLetterQueue() {
        provider.purgeQueue(REF, "dead.letters");
        List<MessageDeliveryResult> results = simulate(4, Map.of("exchange", "lab.direct", "routingKey", "task",
                "ackMode", "manual", "nackRatio", "0.5", "requeue", "false"));

        assertThat(results).hasSize(4);
        assertThat(results).filteredOn(result -> "DEAD_LETTERED".equals(result.attributes().get("outcome"))).hasSize(2)
                .allSatisfy(result -> assertThat(result.attributes()).containsEntry("deadLetterQueue", "dead.letters"));
        assertThat(results).filteredOn(result -> "ACKED".equals(result.attributes().get("outcome"))).hasSize(2);
        awaitTrue(() -> queue("dead.letters").ready() == 2, "dead.letters 应收到 2 条死信");
    }

    @Test
    @Order(6)
    void nackWithRequeueIsRedeliveredThenAcked() {
        List<MessageDeliveryResult> results = simulate(2, Map.of("exchange", "lab.direct", "routingKey", "order.paid",
                "ackMode", "manual", "nackRatio", "1", "requeue", "true"));
        assertThat(results).hasSize(2).allSatisfy(result -> assertThat(result.attributes())
                .containsEntry("outcome", "REQUEUED_THEN_ACKED").containsEntry("redelivered", "true"));
        assertThat(queue("orders.paid").ready()).isZero();
    }

    @Test
    @Order(7)
    void messageTtlExpiresBeforeConsumption() {
        List<MessageDeliveryResult> results = simulate(2, Map.of("exchange", "lab.direct", "routingKey", "order.created",
                "ttlMs", "300", "consumeDelayMs", "800"));
        assertThat(results).hasSize(2).allSatisfy(result -> assertThat(result.attributes()).containsEntry("outcome", "EXPIRED"));
    }

    @Test
    @Order(8)
    void prefetchLimitsUnackedForResidentConsumer() {
        provider.purgeQueue(REF, "orders.created");
        List<MessageDeliveryResult> queued = simulate(20, Map.of("exchange", "lab.direct", "routingKey", "order.created", "consume", "false"));
        assertThat(queued).allSatisfy(result -> assertThat(result.attributes()).containsEntry("outcome", "QUEUED"));
        awaitTrue(() -> queue("orders.created").ready() == 20, "20 条消息应处于 ready");

        RabbitConsumer consumer = provider.addConsumer(REF, "orders.created", 3, "manual", 2_000, 0, false);
        awaitTrue(() -> queue("orders.created").unacked() == 3, "unacked 应等于 prefetch=3");
        assertThat(queue("orders.created").ready()).isEqualTo(17);
        assertThat(queue("orders.created").consumers()).isEqualTo(1);
        assertThat(provider.routing(REF).consumers()).singleElement().satisfies(item -> {
            assertThat(item.consumerId()).isEqualTo(consumer.consumerId());
            assertThat(item.active()).isTrue();
        });

        provider.removeConsumer(REF, consumer.consumerId());
        awaitTrue(() -> queue("orders.created").unacked() == 0, "移除后未确认消息应回到队列");
        assertThat(queue("orders.created").ready()).isGreaterThanOrEqualTo(17);
        assertThatThrownBy(() -> provider.removeConsumer(REF, consumer.consumerId())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Order(9)
    void residentConsumerDrainsQueueAndMetricsReportRates() {
        RabbitConsumer fast = provider.addConsumer(REF, "orders.created", 50, "auto", 0, 0, false);
        awaitTrue(() -> queue("orders.created").ready() == 0 && queue("orders.created").unacked() == 0, "auto ack 消费者应清空队列");
        assertThat(provider.loadMetrics(REF).nodes()).singleElement()
                .satisfies(metrics -> assertThat(metrics.nodeId()).isEqualTo("rabbit-node-01"));
        provider.removeConsumer(REF, fast.consumerId());
        assertThatThrownBy(() -> provider.addConsumer(REF, "missing.queue", 1, "manual", 0, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.addConsumer(REF, "orders.created", 1, "bogus", 0, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Order(10)
    void declareBindAndDeleteObjects() {
        provider.declareExchange(REF, "lab.custom", "topic", true);
        RabbitQueue queue = provider.declareQueue(REF, "custom.q", "lab.dlx", 60_000L);
        assertThat(queue.deadLetterExchange()).isEqualTo("lab.dlx");
        provider.bind(REF, "lab.custom", "custom.q", "custom.*", Map.of());
        assertThat(provider.previewRoute(REF, "lab.custom", "custom.x", Map.of()).matchedQueues()).containsExactly("custom.q");

        provider.unbind(REF, "lab.custom", "custom.q", "custom.*");
        assertThat(provider.previewRoute(REF, "lab.custom", "custom.x", Map.of()).matchedQueues()).isEmpty();
        provider.deleteQueue(REF, "custom.q");
        provider.deleteExchange(REF, "lab.custom");
        assertThat(provider.routing(REF).exchanges()).noneMatch(exchange -> exchange.name().equals("lab.custom"));

        assertThatThrownBy(() -> provider.declareExchange(REF, "bad", "x-consistent-hash", true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.deleteExchange(REF, "amq.direct")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.bind(REF, "lab.direct", "missing.q", "k", Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.declareQueue(REF, "q2", "missing.dlx", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.registerService(REF, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Order(11)
    void stopClearsStateAndRestartReseeds() {
        provider.addConsumer(REF, "orders.paid", 1, "manual", 0, 0, false);
        List<OperationResult> stopped = provider.operateCluster(REF, OperationType.STOP);
        assertThat(stopped).singleElement().satisfies(result -> assertThat(result.success()).isTrue());
        assertThat(provider.loadTopology(REF).activeVip()).isEqualTo("unavailable");
        assertThat(provider.loadMetrics(REF).nodes().getFirst().cpuUsage()).isZero();
        assertThatThrownBy(() -> simulate(1, Map.of())).isInstanceOf(IllegalStateException.class);

        provider.operateNode(REF, RabbitTopologySeeder.NODE_ID, OperationType.START);
        RabbitRoutingTopology routing = provider.routing(REF);
        assertThat(routing.consumers()).isEmpty();
        assertThat(queue("dead.letters").ready()).isZero();
        assertThat(provider.loadLogs(REF, null, 100)).isNotEmpty();
    }

    private List<MessageDeliveryResult> simulate(int count, Map<String, String> options) {
        MessageScenario scenario = new MessageScenario("demo", "g", count, "hello-${index}", "rabbit-producer", List.of(), Map.of(), options);
        return provider.simulate(REF, scenario).deliveries();
    }

    private RabbitQueue queue(String name) {
        return provider.routing(REF).queues().stream().filter(queue -> queue.name().equals(name)).findFirst().orElseThrow();
    }

    private static void awaitTrue(BooleanSupplier condition, String message) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError(message);
    }
}
