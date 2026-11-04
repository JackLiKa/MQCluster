package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 阶段 0 Spike：验证 Apache Qpid Broker-J 9.x 进程内嵌入，并用官方 RabbitMQ 客户端
 * （{@code com.rabbitmq:amqp-client}，AMQP 0-9-1）逐项验证特性支持。
 *
 * <p>结论汇总进 {@code docs/MQ_FEATURE_MATRIX.md}。默认禁用（需 {@code -DrunEmbeddedQpidSpike=true}）。
 */
@EnabledIfSystemProperty(named = "runEmbeddedQpidSpike", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddedQpidSpikeTest {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedQpidSpikeTest.class);

    private SystemLauncher launcher;
    private final AtomicReference<Broker<?>> brokerRef = new AtomicReference<>();
    private ConnectionFactory factory;
    private final Map<String, String> matrix = new LinkedHashMap<>();

    @BeforeAll
    void startBroker() throws Exception {
        Path workDir = Files.createTempDirectory("qpid-spike-");
        int port = freePort();
        launcher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener() {
            @Override
            public void onContainerResolve(SystemConfig<?> systemConfig) {
                brokerRef.set(systemConfig.getContainer(Broker.class));
            }
        });
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("type", "Memory");
        attrs.put("initialConfigurationLocation",
                getClass().getClassLoader().getResource("qpid/initial-config.json").toExternalForm());
        attrs.put("startupLoggedToSystemOut", false);
        attrs.put("context", Map.of(
                "qpid.amqp_port", String.valueOf(port),
                "qpid.work_dir", workDir.toString(),
                "qpid.broker.defaultPreferenceStoreAttributes", "{\"type\": \"Noop\"}"));
        long t0 = System.currentTimeMillis();
        launcher.startup(attrs);
        log.info("Qpid started in {} ms on {}", System.currentTimeMillis() - t0, port);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(port);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        this.factory = factory;
    }

    @AfterAll
    void stopBroker() throws Exception {
        log.info("Qpid feature matrix: {}", matrix);
        if (launcher != null) {
            launcher.shutdown();
        }
    }

    @Test
    void appLoggingStillWorksAfterQpidStartup() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger probe = ctx.getLogger("com.example.clustermanager.probe");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        probe.addAppender(appender);
        probe.info("probe after qpid");
        assertThat(appender.list).hasSize(1);
        matrix.put("logback-coexist", "OK");
    }

    @Test
    void directFanoutTopicHeadersRouting() throws Exception {
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            ch.exchangeDeclare("s.direct", "direct");
            ch.exchangeDeclare("s.fanout", "fanout");
            ch.exchangeDeclare("s.topic", "topic");
            ch.exchangeDeclare("s.headers", "headers");
            for (String q : new String[]{"q.d", "q.f1", "q.f2", "q.t1", "q.t2", "q.h"}) {
                ch.queueDeclare(q, false, false, false, null);
            }
            ch.queueBind("q.d", "s.direct", "rk1");
            ch.queueBind("q.f1", "s.fanout", "");
            ch.queueBind("q.f2", "s.fanout", "");
            ch.queueBind("q.t1", "s.topic", "order.*");
            ch.queueBind("q.t2", "s.topic", "#.error");
            ch.queueBind("q.h", "s.headers", "", Map.of("x-match", "all", "type", "invoice", "fmt", "json"));

            ch.basicPublish("s.direct", "rk1", null, bytes("d"));
            ch.basicPublish("s.direct", "nope", null, bytes("lost"));
            ch.basicPublish("s.fanout", "whatever", null, bytes("f"));
            ch.basicPublish("s.topic", "order.created", null, bytes("t1"));
            ch.basicPublish("s.topic", "app.db.error", null, bytes("t2"));
            ch.basicPublish("s.headers", "", new AMQP.BasicProperties.Builder()
                    .headers(Map.of("type", "invoice", "fmt", "json")).build(), bytes("h"));
            ch.basicPublish("s.headers", "", new AMQP.BasicProperties.Builder()
                    .headers(Map.of("type", "invoice")).build(), bytes("h-miss"));
            Thread.sleep(300);

            assertThat(ch.messageCount("q.d")).isEqualTo(1);
            assertThat(ch.messageCount("q.f1")).isEqualTo(1);
            assertThat(ch.messageCount("q.f2")).isEqualTo(1);
            assertThat(ch.messageCount("q.t1")).isEqualTo(1);
            assertThat(ch.messageCount("q.t2")).isEqualTo(1);
            assertThat(ch.messageCount("q.h")).isEqualTo(1);
            matrix.put("exchanges(direct/fanout/topic/headers)", "OK");

            // 进程内对象模型可读：Broker -> VirtualHost -> Exchange/Queue/Binding
            VirtualHost<?> vhost = virtualHost();
            Exchange<?> topic = vhost.getChildByName(Exchange.class, "s.topic");
            assertThat(topic.getBindings()).extracting(b -> b.getBindingKey()).contains("order.*", "#.error");
            Queue<?> qd = vhost.getChildByName(Queue.class, "q.d");
            assertThat(qd.getQueueDepthMessages()).isEqualTo(1);
            matrix.put("object-model-introspection", "OK");
        }
    }

    @Test
    void prefetchAckNackRequeue() throws Exception {
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            ch.queueDeclare("q.qos", false, false, false, null);
            for (int i = 0; i < 5; i++) {
                ch.basicPublish("", "q.qos", null, bytes("m" + i));
            }
            ch.basicQos(2);
            AtomicInteger delivered = new AtomicInteger();
            String tag = ch.basicConsume("q.qos", false, new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    delivered.incrementAndGet();
                }
            });
            Thread.sleep(500);
            assertThat(delivered.get()).isEqualTo(2);
            Queue<?> q = virtualHost().getChildByName(Queue.class, "q.qos");
            assertThat(q.getUnacknowledgedMessages()).isEqualTo(2);
            matrix.put("basicQos(prefetch)", "OK");
            ch.basicCancel(tag);
        }
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            Thread.sleep(200);
            GetResponse r = ch.basicGet("q.qos", false);
            assertThat(r).isNotNull();
            ch.basicNack(r.getEnvelope().getDeliveryTag(), false, true);
            GetResponse again = ch.basicGet("q.qos", false);
            assertThat(again).isNotNull();
            ch.basicAck(again.getEnvelope().getDeliveryTag(), false);
            ch.basicReject(ch.basicGet("q.qos", false).getEnvelope().getDeliveryTag(), false);
            Thread.sleep(200);
            Thread.sleep(300);
            // Qpid 怪癖：未配置 maximumDeliveryAttempts 时 reject(requeue=false) 仍会重新入队（RabbitMQ 会丢弃/死信）
            assertThat(ch.messageCount("q.qos")).isEqualTo(4);
            matrix.put("manual ack/nack/requeue/reject", "OK");
        }
    }

    @Test
    void publisherConfirms() throws Exception {
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            ch.queueDeclare("q.confirm", false, false, false, null);
            ch.confirmSelect();
            ch.basicPublish("", "q.confirm", null, bytes("c"));
            assertThat(ch.waitForConfirms(5000)).isTrue();
            matrix.put("publisher confirms", "OK");
        }
    }

    @Test
    void rabbitOnlyQueueArgumentsAreRejected() {
        // Qpid 对未知 queue.declare 参数默认 FAIL，并以 542 关闭整个连接
        String dlx = probe(ch -> {
            ch.queueDeclare("q.work.dlx", false, false, false, Map.of("x-dead-letter-exchange", "s.dlx"));
            return "OK";
        });
        String ttl = probe(ch -> {
            ch.queueDeclare("q.ttl", false, false, false, Map.of("x-message-ttl", 200));
            return "OK";
        });
        matrix.put("x-dead-letter-exchange", dlx);
        matrix.put("x-message-ttl", ttl);
        assertThat(dlx).startsWith("REJECTED");
        assertThat(ttl).startsWith("REJECTED");
    }

    @Test
    void alternateBindingAsDeadLetter() throws Exception {
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            ch.exchangeDeclare("s.alt", "fanout");
            ch.queueDeclare("q.altdlq", false, false, false, null);
            ch.queueBind("q.altdlq", "s.alt", "");
            // RabbitMQ 的 x-dead-letter-exchange 在 Qpid 中对应 alternateBinding + maximumDeliveryAttempts
            ch.queueDeclare("q.work.alt", false, false, false,
                    Map.of("alternateBinding", "{\"destination\":\"s.alt\"}", "maximumDeliveryAttempts", 1));
            ch.basicPublish("", "q.work.alt", null, bytes("dead-1"));
            ch.basicPublish("", "q.work.alt", null, bytes("dead-2"));
            Thread.sleep(200);
            ch.basicReject(ch.basicGet("q.work.alt", false).getEnvelope().getDeliveryTag(), false);
            ch.basicNack(ch.basicGet("q.work.alt", false).getEnvelope().getDeliveryTag(), false, false);
            Thread.sleep(300);
            assertThat(ch.messageCount("q.altdlq")).isEqualTo(2);
            assertThat(ch.messageCount("q.work.alt")).isZero();
            matrix.put("alternateBinding + maximumDeliveryAttempts (DLX equivalent)", "OK");
        }
    }

    @Test
    void messageTtlVariants() throws Exception {
        matrix.put("qpid maximumMessageTtl", probe(ch -> {
            ch.queueDeclare("q.ttl.qpid", false, false, false, Map.of("maximumMessageTtl", 200));
            ch.basicPublish("", "q.ttl.qpid", null, bytes("ttl"));
            Thread.sleep(800);
            return ch.basicGet("q.ttl.qpid", true) == null ? "OK" : "IGNORED";
        }));
        matrix.put("per-message expiration", probe(ch -> {
            ch.queueDeclare("q.ttl.msg", false, false, false, null);
            ch.basicPublish("", "q.ttl.msg", new AMQP.BasicProperties.Builder().expiration("200").build(), bytes("ttl"));
            Thread.sleep(800);
            return ch.basicGet("q.ttl.msg", true) == null ? "OK" : "IGNORED";
        }));
        matrix.put("expired -> alternate(DLQ)", probe(ch -> {
            ch.exchangeDeclare("s.alt.ttl", "fanout");
            ch.queueDeclare("q.ttl.dlq", false, false, false, null);
            ch.queueBind("q.ttl.dlq", "s.alt.ttl", "");
            ch.queueDeclare("q.ttl.alt", false, false, false, Map.of("maximumMessageTtl", 200, "alternateExchange", "s.alt.ttl"));
            ch.basicPublish("", "q.ttl.alt", null, bytes("ttl"));
            Thread.sleep(800);
            ch.basicGet("q.ttl.alt", true);
            Thread.sleep(200);
            return ch.messageCount("q.ttl.dlq") == 1 ? "OK" : "DISCARDED";
        }));
        matrix.put("maximumDeliveryAttempts -> alternate", probe(ch -> {
            ch.exchangeDeclare("s.alt.mda", "fanout");
            ch.queueDeclare("q.mda.dlq", false, false, false, null);
            ch.queueBind("q.mda.dlq", "s.alt.mda", "");
            ch.queueDeclare("q.mda", false, false, false, Map.of("x-qpid-maximum-delivery-count", 2, "alternateExchange", "s.alt.mda"));
            ch.basicPublish("", "q.mda", null, bytes("poison"));
            Thread.sleep(200);
            for (int i = 0; i < 3; i++) {
                GetResponse r = ch.basicGet("q.mda", false);
                if (r != null) {
                    ch.basicNack(r.getEnvelope().getDeliveryTag(), false, true);
                }
                Thread.sleep(150);
            }
            return ch.messageCount("q.mda.dlq") == 1 ? "OK" : "NOT_ROUTED";
        }));
        matrix.put("mandatory basic.return", probe(ch -> {
            AtomicInteger returned = new AtomicInteger();
            ch.addReturnListener(r -> returned.incrementAndGet());
            ch.exchangeDeclare("s.mand", "direct");
            ch.basicPublish("s.mand", "nobody", true, null, bytes("x"));
            Thread.sleep(400);
            return returned.get() == 1 ? "OK" : "NO_RETURN";
        }));
        assertThat(matrix.get("qpid maximumMessageTtl")).isEqualTo("OK");
        assertThat(matrix.get("per-message expiration")).isEqualTo("OK");
        assertThat(matrix.get("mandatory basic.return")).isEqualTo("OK");
        log.info("TTL & co => {}", matrix);
    }

    @FunctionalInterface
    interface ChannelProbe {
        String run(Channel ch) throws Exception;
    }

    private String probe(ChannelProbe probe) {
        try (Connection c = factory.newConnection(); Channel ch = c.createChannel()) {
            return probe.run(ch);
        } catch (Exception e) {
            String msg = String.valueOf(e.getMessage() != null ? e.getMessage() : e.getCause());
            return "REJECTED: " + msg.substring(0, Math.min(160, msg.length()));
        }
    }

    @Test
    void heapUsage() {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        log.info("Heap used with Qpid running: {} MB", (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024));
    }

    private VirtualHost<?> virtualHost() {
        VirtualHostNode<?> node = brokerRef.get().getChildren(VirtualHostNode.class).iterator().next();
        return node.getVirtualHost();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
