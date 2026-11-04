package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.port.IKafkaInspector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * Kafka API 黑盒测试（不启动 broker）：验证 provider 注册、种子拓撲、集群未运行时的错误语义与参数校验。
 *
 * <p>真实 KRaft 启停 / leader 切换 / lag 见 {@link KafkaClusterProviderTest}（条件启用）。
 */
@SpringBootTest(properties = "cluster.kafka.work-dir=target/kafka-api-test")
class KafkaApiTest {

    private static final String KAFKA_PATH = "/api/clusters/pseudo/kafka/kafka-lab";

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Autowired
    private ClusterFacadeService clusterFacadeService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    @DisplayName("providers 返回 Kafka 描述符：clusterId 与 capabilities")
    void providerDescriptorExposesClusterIdAndCapabilities() throws Exception {
        mockMvc.perform(get("/api/clusters/providers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.providerId=='kafka-embedded')].clusterId").value("kafka-lab"))
                .andExpect(jsonPath("$[?(@.providerId=='kafka-embedded')].middleware").value("KAFKA"))
                .andExpect(jsonPath("$[?(@.providerId=='pseudo-rocketmq')].clusterId").value("local-lab"))
                .andExpect(jsonPath("$[?(@.providerId=='rocketmq-admin')].clusterId").value("rocketmq-demo"));
    }

    @Test
    @DisplayName("种子拓扑：1 controller + 3 broker，初始全部 STOPPED")
    void seededTopologyIsStopped() throws Exception {
        mockMvc.perform(get(KAFKA_PATH + "/topology"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes.length()").value(4))
                .andExpect(jsonPath("$.nodes[0].nodeId").value("kafka-ctrl-01"))
                .andExpect(jsonPath("$.nodes[0].labels.role").value("kafka-controller"))
                .andExpect(jsonPath("$.nodes[1].labels.role").value("kafka-broker"))
                .andExpect(jsonPath("$.nodes[3].nodeId").value("kafka-broker-03"))
                .andExpect(jsonPath("$.nodes[*].status").value(org.hamcrest.Matchers.everyItem(org.hamcrest.Matchers.is("STOPPED"))))
                .andExpect(jsonPath("$.links.length()").value(3))
                .andExpect(jsonPath("$.activeVip").value("unavailable"));
    }

    @Test
    @DisplayName("集群未运行时：topic / 消费者组接口返回 409")
    void inspectionRequiresRunningCluster() throws Exception {
        mockMvc.perform(get(KAFKA_PATH + "/topics")).andExpect(status().isConflict());
        mockMvc.perform(get(KAFKA_PATH + "/consumer-groups")).andExpect(status().isConflict());
        mockMvc.perform(post(KAFKA_PATH + "/messages/simulate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"topic":"demo","consumerGroup":"g","messageCount":1,"payloadTemplate":"x","producerNodeId":"kafka-producer","consumerNodeIds":["c"],"headers":{}}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("controller 未运行时启动 broker → 409")
    void brokerRequiresController() throws Exception {
        mockMvc.perform(post(KAFKA_PATH + "/nodes/kafka-broker-01/operations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"operationType\":\"START\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Controller")));
    }

    @Test
    @DisplayName("未知节点 → 400；手动注册服务 → 400")
    void unsupportedOperations() throws Exception {
        mockMvc.perform(post(KAFKA_PATH + "/nodes/kafka-broker-09/operations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"operationType\":\"START\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_PATH + "/services")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nodeId":"x","displayName":"x","hostName":"x","address":"10.77.0.50","port":9092,"role":"broker","labels":{}}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("创建 topic 参数校验：分区数 / 副本因子越界 → 400")
    void topicCreationValidation() throws Exception {
        mockMvc.perform(post(KAFKA_PATH + "/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"partitions\":0,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_PATH + "/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"partitions\":3,\"replicationFactor\":4}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_PATH + "/consumer-groups/g1/members")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"\",\"ratePerSecond\":5}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("监控：未运行时 4 个节点指标均为 0")
    void metricsAreZeroWhenStopped() throws Exception {
        mockMvc.perform(get(KAFKA_PATH + "/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes.length()").value(4))
                .andExpect(jsonPath("$.nodes[0].cpuUsage").value(0.0));
    }

    @Test
    @DisplayName("一键操作：不支持的 provider（真实 RocketMQ）→ 400")
    void clusterOperationUnsupportedForRealRocketMq() throws Exception {
        mockMvc.perform(post("/api/clusters/real/rocketmq/rocketmq-demo/operations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"operationType\":\"START\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("resolveCapability：RocketMQ provider 不支持 Kafka 检查端口 → IllegalArgumentException")
    void capabilityMismatchIsRejected() {
        assertThatThrownBy(() -> clusterFacadeService.capability(
                new ClusterSelection("local-lab", ClusterMode.PSEUDO, MiddlewareType.ROCKETMQ), IKafkaInspector.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(clusterFacadeService.capability(
                new ClusterSelection("kafka-lab", ClusterMode.PSEUDO, MiddlewareType.KAFKA), IKafkaInspector.class))
                .isNotNull();
    }
}
