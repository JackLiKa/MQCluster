package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.port.IRabbitInspector;
import org.junit.jupiter.api.AfterEach;
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
 * RabbitMQ API 黑盒测试：provider 注册、broker 未运行时的错误语义、参数校验，以及启动后的完整 HTTP 主流程。
 */
@SpringBootTest(properties = "cluster.rabbitmq.work-dir=target/rabbit-api-test")
class RabbitApiTest {

    private static final String RABBIT_PATH = "/api/clusters/pseudo/rabbitmq/rabbitmq-lab";

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Autowired
    private ClusterFacadeService clusterFacadeService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @AfterEach
    void stopBroker() throws Exception {
        mockMvc.perform(post(RABBIT_PATH + "/operations").contentType(MediaType.APPLICATION_JSON).content("{\"operationType\":\"STOP\"}"));
    }

    @Test
    @DisplayName("providers 返回 RabbitMQ 描述符；种子拓扑单节点且初始 STOPPED")
    void descriptorAndSeededTopology() throws Exception {
        mockMvc.perform(get("/api/clusters/providers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.providerId=='rabbitmq-embedded')].clusterId").value("rabbitmq-lab"))
                .andExpect(jsonPath("$[?(@.providerId=='rabbitmq-embedded')].middleware").value("RABBITMQ"));
        mockMvc.perform(get(RABBIT_PATH + "/topology"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes.length()").value(1))
                .andExpect(jsonPath("$.nodes[0].nodeId").value("rabbit-node-01"))
                .andExpect(jsonPath("$.nodes[0].labels.role").value("rabbit-broker"))
                .andExpect(jsonPath("$.nodes[0].status").value("STOPPED"))
                .andExpect(jsonPath("$.activeVip").value("unavailable"));
        assertThat(clusterFacadeService.capability(
                new ClusterSelection("rabbitmq-lab", ClusterMode.PSEUDO, MiddlewareType.RABBITMQ), IRabbitInspector.class)).isNotNull();
        assertThatThrownBy(() -> clusterFacadeService.capability(
                new ClusterSelection("kafka-lab", ClusterMode.PSEUDO, MiddlewareType.KAFKA), IRabbitInspector.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("broker 未运行：路由 / 预览 / 消息模拟 → 409；参数校验 → 400")
    void errorsWhenStoppedAndValidation() throws Exception {
        mockMvc.perform(get(RABBIT_PATH + "/routing")).andExpect(status().isConflict());
        mockMvc.perform(post(RABBIT_PATH + "/routing/preview").contentType(MediaType.APPLICATION_JSON)
                .content("{\"exchange\":\"lab.direct\",\"routingKey\":\"x\"}")).andExpect(status().isConflict());
        mockMvc.perform(post(RABBIT_PATH + "/messages/simulate").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"topic":"demo","consumerGroup":"g","messageCount":1,"payloadTemplate":"x","producerNodeId":"p","consumerNodeIds":["c"],"headers":{}}
                                """))
                .andExpect(status().isConflict());
        mockMvc.perform(post(RABBIT_PATH + "/exchanges").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"x\",\"type\":\"x-delayed\"}")).andExpect(status().isBadRequest());
        mockMvc.perform(post(RABBIT_PATH + "/queues").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"q\",\"messageTtlMs\":0}")).andExpect(status().isBadRequest());
        mockMvc.perform(post(RABBIT_PATH + "/consumers").contentType(MediaType.APPLICATION_JSON)
                .content("{\"queue\":\"q\",\"nackRatio\":1.5}")).andExpect(status().isBadRequest());
        mockMvc.perform(post(RABBIT_PATH + "/nodes/rabbit-node-02/operations").contentType(MediaType.APPLICATION_JSON)
                .content("{\"operationType\":\"START\"}")).andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("启动后 HTTP 主流程：路由拓扑、预览、声明 / 绑定 / 删除、消息模拟、常驻消费者")
    void endToEndFlow() throws Exception {
        mockMvc.perform(post(RABBIT_PATH + "/operations").contentType(MediaType.APPLICATION_JSON).content("{\"operationType\":\"START\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].success").value(true));
        mockMvc.perform(get(RABBIT_PATH + "/routing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.exchanges[?(@.name=='lab.topic')].type").value("topic"))
                .andExpect(jsonPath("$.queues[?(@.name=='work.tasks')].deadLetterExchange").value("lab.dlx"));
        mockMvc.perform(post(RABBIT_PATH + "/routing/preview").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"exchange\":\"lab.topic\",\"routingKey\":\"app.pay.error\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matchedQueues.length()").value(2));

        mockMvc.perform(post(RABBIT_PATH + "/exchanges").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"api.fanout\",\"type\":\"fanout\"}")).andExpect(status().isOk());
        mockMvc.perform(post(RABBIT_PATH + "/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"api.q\",\"deadLetterExchange\":\"lab.dlx\",\"messageTtlMs\":10000}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messageTtlMs").value(10000));
        mockMvc.perform(post(RABBIT_PATH + "/bindings").contentType(MediaType.APPLICATION_JSON)
                .content("{\"exchange\":\"api.fanout\",\"queue\":\"api.q\"}")).andExpect(status().isOk());
        mockMvc.perform(post(RABBIT_PATH + "/messages/simulate").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"topic":"demo","consumerGroup":"g","messageCount":2,"payloadTemplate":"x","producerNodeId":"p","consumerNodeIds":[],"headers":{},
                                 "options":{"exchange":"api.fanout","ackMode":"manual"}}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deliveries.length()").value(2))
                .andExpect(jsonPath("$.deliveries[0].attributes.queue").value("api.q"))
                .andExpect(jsonPath("$.deliveries[0].attributes.outcome").value("ACKED"));
        mockMvc.perform(delete(RABBIT_PATH + "/bindings").contentType(MediaType.APPLICATION_JSON)
                .content("{\"exchange\":\"api.fanout\",\"queue\":\"api.q\"}")).andExpect(status().isOk());
        mockMvc.perform(delete(RABBIT_PATH + "/bindings").contentType(MediaType.APPLICATION_JSON)
                .content("{\"exchange\":\"api.fanout\",\"queue\":\"api.q\"}")).andExpect(status().isBadRequest());

        mockMvc.perform(post(RABBIT_PATH + "/consumers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"api.q\",\"prefetch\":5,\"ackMode\":\"manual\",\"processingDelayMs\":10}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consumerId").value("c1"))
                .andExpect(jsonPath("$.active").value(true));
        mockMvc.perform(delete(RABBIT_PATH + "/consumers/c1")).andExpect(status().isOk());
        mockMvc.perform(delete(RABBIT_PATH + "/consumers/c1")).andExpect(status().isBadRequest());

        mockMvc.perform(delete(RABBIT_PATH + "/queues/api.q")).andExpect(status().isOk());
        mockMvc.perform(delete(RABBIT_PATH + "/exchanges/api.fanout")).andExpect(status().isOk());
        mockMvc.perform(delete(RABBIT_PATH + "/exchanges/amq.direct")).andExpect(status().isBadRequest());
        mockMvc.perform(get(RABBIT_PATH + "/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes[0].nodeId").value("rabbit-node-01"));
        mockMvc.perform(get(RABBIT_PATH + "/logs"))
                .andExpect(status().isOk());
    }
}
