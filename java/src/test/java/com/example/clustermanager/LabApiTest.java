package com.example.clustermanager;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
 * 实验室 API 黑盒测试（不启动 broker）：验证能力声明、集群未运行时的错误语义与参数校验。
 *
 * <p>真实压测（产生拒绝/超时）见 {@code KafkaClusterProviderTest} 等条件启用用例，
 * 避免在常规 verify 中长时间占满 CPU。
 */
@SpringBootTest(properties = {
        "cluster.kafka.work-dir=target/kafka-lab-api-test",
        "cluster.rabbitmq.work-dir=target/rabbit-lab-api-test"
})
class LabApiTest {

    private static final String KAFKA_LAB = "/api/clusters/pseudo/kafka/kafka-lab/lab";
    private static final String RABBIT_LAB = "/api/clusters/pseudo/rabbitmq/rabbitmq-lab/lab";
    private static final String ROCKET_LAB = "/api/clusters/pseudo/rocketmq/local-lab/lab";

    @Autowired
    private WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    @DisplayName("三种 provider 均声明 LAB 能力，前端据此显示压力实验与内部机制标签页")
    void providersDeclareLabCapability() throws Exception {
        mockMvc.perform(get("/api/clusters/providers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.providerId=='kafka-embedded')].capabilities[?(@=='LAB')]").exists())
                .andExpect(jsonPath("$[?(@.providerId=='rabbitmq-embedded')].capabilities[?(@=='LAB')]").exists())
                .andExpect(jsonPath("$[?(@.providerId=='pseudo-rocketmq')].capabilities[?(@=='LAB')]").exists());
    }

    @Test
    @DisplayName("集群未运行时读取内部机制 → 409")
    void internalsRequireRunningCluster() throws Exception {
        mockMvc.perform(get(KAFKA_LAB + "/internals")).andExpect(status().isConflict());
        mockMvc.perform(get(RABBIT_LAB + "/internals")).andExpect(status().isConflict());
        mockMvc.perform(get(ROCKET_LAB + "/internals")).andExpect(status().isConflict());
    }

    @Test
    @DisplayName("未运行过实验时状态为 IDLE，停止操作幂等")
    void statusIsIdleBeforeAnyRun() throws Exception {
        mockMvc.perform(get(KAFKA_LAB + "/stress"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.phase").value("IDLE"))
                .andExpect(jsonPath("$.running").value(false));
        mockMvc.perform(delete(KAFKA_LAB + "/stress"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.phase").value("IDLE"));
    }

    @Test
    @DisplayName("压测参数校验：速率 / 线程数 / 时长越界 → 400")
    void stressValidation() throws Exception {
        mockMvc.perform(post(KAFKA_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"targetRate\":0}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"threads\":99}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"durationSeconds\":1}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post(KAFKA_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"  \"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("集群未运行时启动压测 → 409")
    void stressRequiresRunningCluster() throws Exception {
        mockMvc.perform(post(KAFKA_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"targetRate\":100}"))
                .andExpect(status().isConflict());
        mockMvc.perform(post(RABBIT_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"targetRate\":100}"))
                .andExpect(status().isConflict());
        mockMvc.perform(post(ROCKET_LAB + "/stress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"demo\",\"targetRate\":100}"))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("拒绝不支持的压力实验的 MQ（REAL 模式 RocketMQ）→ 400")
    void realModeDoesNotSupportLab() throws Exception {
        mockMvc.perform(get("/api/clusters/real/rocketmq/rocketmq-demo/lab/internals"))
                .andExpect(status().isBadRequest());
    }
}
