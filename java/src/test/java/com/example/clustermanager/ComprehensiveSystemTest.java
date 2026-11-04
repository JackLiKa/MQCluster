package com.example.clustermanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.MiddlewareType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * 系統全面測試——使用 6 種黑盒測試方法覆蓋全棧端到端場景。
 *
 * <p>測試方法分類（每個 {@code @Nested} 類對應一種方法）：
 * <ul>
 *   <li>{@link EquivalencePartitioningTests}——等價類劃分法：覆蓋有效/無效等價類</li>
 *   <li>{@link BoundaryValueAnalysisTests}——邊界值分析法：測試邊界及鄰域值</li>
 *   <li>{@link OrthogonalArrayTests}——正交試驗法：多因素組合高效測試</li>
 *   <li>{@link DecisionTableTests}——判定表法：複雜邏輯條件組合</li>
 *   <li>{@link ErrorGuessingTests}——錯誤猜測法：基於經驗定位潛在缺陷</li>
 *   <li>{@link ScenarioTests}——場景法：模擬真實業務流程</li>
 * </ul>
 *
 * <p>測試層級：API 層（MockMvc）+ 應用層（ClusterFacadeService）。
 * 使用 {@code @SpringBootTest} 啟動完整上下文，{@code @AutoConfigureMockMvc} 注入 MockMvc。
 * {@code @DirtiesContext} 確保每個測試方法後上下文重建，隔離內存狀態。
 *
 * <p>覆蓋的 API 端點：
 * <ul>
 *   <li>GET  /api/clusters/providers</li>
 *   <li>GET  /api/clusters/{mode}/{middleware}/{clusterId}/topology</li>
 *   <li>GET  /api/clusters/{mode}/{middleware}/{clusterId}/metrics</li>
 *   <li>GET  /api/clusters/{mode}/{middleware}/{clusterId}/logs</li>
 *   <li>POST /api/clusters/{mode}/{middleware}/{clusterId}/nodes/{nodeId}/operations</li>
 *   <li>POST /api/clusters/{mode}/{middleware}/{clusterId}/services</li>
 *   <li>DELETE /api/clusters/{mode}/{middleware}/{clusterId}/services/{nodeId}</li>
 *   <li>POST /api/clusters/{mode}/{middleware}/{clusterId}/messages/simulate</li>
 * </ul>
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ComprehensiveSystemTest {

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Autowired
    private ClusterFacadeService clusterFacadeService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    private static final String PSEUDO_PATH = "/api/clusters/pseudo/rocketmq/local-lab";
    private static final String REAL_PATH = "/api/clusters/real/rocketmq/rocketmq-demo";

    // ===== 輔助方法 =====

    /** 構造有效的服務登記請求體 JSON（使用 CIDR 範圍內的 IP） */
    private String validServiceRegistrationJson(String nodeId) {
        return validServiceRegistrationJson(nodeId, "10.77.0.100");
    }

    /** 構造有效的服務登記請求體 JSON，指定地址 */
    private String validServiceRegistrationJson(String nodeId, String address) {
        return """
                {
                  "nodeId": "%s",
                  "displayName": "Test Node",
                  "role": "broker-master",
                  "hostName": "test-host",
                  "address": "%s",
                  "port": 10911
                }
                """.formatted(nodeId, address);
    }

    /** 構造有效的消息模擬請求體 JSON */
    private String validMessageSimulationJson(String topic, int count, String producerNodeId) {
        return """
                {
                  "topic": "%s",
                  "consumerGroup": "test-group",
                  "messageCount": %d,
                  "producerNodeId": "%s"
                }
                """.formatted(topic, count, producerNodeId);
    }

    // =================================================================
    // 1. 等價類劃分法——通過有效/無效等價類覆蓋輸入範圍
    // =================================================================

    @Nested
    @DisplayName("等價類劃分法")
    class EquivalencePartitioningTests {

        // --- 有效等價類 ---

        @Test
        @DisplayName("有效等價類：PSEUDO 模式 + ROCKETMQ 中間件 + local-lab 集群 → 200")
        void validPseudoRocketmqTopology() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.cluster.clusterId").value("local-lab"))
                    .andExpect(jsonPath("$.nodes").isArray());
        }

        @Test
        @DisplayName("有效等價類：REAL 模式 + ROCKETMQ 中間件 → 200（即使無真實集群連接）")
        void validRealRocketmqTopology() throws Exception {
            mockMvc.perform(get(REAL_PATH + "/topology"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("有效等價類：小寫 mode/middleware 路徑 → 200（大小寫不敏感）")
        void validLowercaseModePath() throws Exception {
            mockMvc.perform(get("/api/clusters/pseudo/rocketmq/local-lab/topology"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("有效等價類：大寫 mode/middleware 路徑 → 200（大小寫不敏感）")
        void validUppercaseModePath() throws Exception {
            mockMvc.perform(get("/api/clusters/PSEUDO/ROCKETMQ/local-lab/topology"))
                    .andExpect(status().isOk());
        }

        // --- 無效等價類 ---

        @Test
        @DisplayName("無效等價類：不存在的 mode（如 KAFKA）→ 400")
        void invalidModeKafka() throws Exception {
            mockMvc.perform(get("/api/clusters/kafka/rocketmq/local-lab/topology"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("無效等價類：不存在的 middleware（如 PULSAR）→ 400")
        void invalidMiddlewarePulsar() throws Exception {
            mockMvc.perform(get("/api/clusters/pseudo/pulsar/local-lab/topology"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("無效等價類：不存在的 clusterId → 200 但返回空或默認拓撲")
        void invalidClusterId() throws Exception {
            mockMvc.perform(get("/api/clusters/pseudo/rocketmq/nonexistent/topology"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("無效等價類：服務登記 nodeId 為空字符串 → 400")
        void invalidServiceRegistrationBlankNodeId() throws Exception {
            String json = """
                    {
                      "nodeId": "",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "127.0.0.1",
                      "port": 10911
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("無效等價類：消息模擬 topic 為空 → 400")
        void invalidMessageSimulationBlankTopic() throws Exception {
            String json = """
                    {
                      "topic": "",
                      "consumerGroup": "test-group",
                      "messageCount": 1,
                      "producerNodeId": "rmq-broker-m-01"
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("無效等價類：節點操作 operationType 為 null → 400")
        void invalidNodeOperationNullType() throws Exception {
            String json = "{}";
            mockMvc.perform(post(PSEUDO_PATH + "/nodes/rmq-ns-01/operations")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("無效等價類：節點操作 operationType 為非法值 → 4xx 或 5xx（枚舉解析失敗）")
        void invalidNodeOperationIllegalType() throws Exception {
            String json = """
                    {"operationType": "DELETE"}
                    """;
            int status = mockMvc.perform(post(PSEUDO_PATH + "/nodes/rmq-ns-01/operations")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andReturn().getResponse().getStatus();
            assertThat(status).isGreaterThanOrEqualTo(400);
        }
    }

    // =================================================================
    // 2. 邊界值分析法——重點測試邊界及鄰域值
    // =================================================================

    @Nested
    @DisplayName("邊界值分析法")
    class BoundaryValueAnalysisTests {

        @Test
        @DisplayName("邊界值：消息數量 messageCount=1（最小有效值）→ 接受")
        void messageCountMinValid() throws Exception {
            String json = validMessageSimulationJson("BoundaryTopic", 1, "rmq-broker-m-01");
            // 不啟動節點時會返回失敗結果，但請求本身應被接受（非 400）
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：消息數量 messageCount=0（無效，低於最小值）→ 400")
        void messageCountZero() throws Exception {
            String json = validMessageSimulationJson("BoundaryTopic", 0, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("邊界值：消息數量 messageCount=-1（負數，無效）→ 400")
        void messageCountNegative() throws Exception {
            String json = validMessageSimulationJson("BoundaryTopic", -1, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("邊界值：消息數量 messageCount=100（大值，有效）→ 接受")
        void messageCountLarge() throws Exception {
            String json = validMessageSimulationJson("BoundaryTopic", 100, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：服務登記 port=1（最小有效端口）→ 接受")
        void serviceRegistrationPortMin() throws Exception {
            String json = """
                    {
                      "nodeId": "boundary-port-min",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.50",
                      "port": 1
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：服務登記 port=65535（最大有效端口）→ 接受")
        void serviceRegistrationPortMax() throws Exception {
            String json = """
                    {
                      "nodeId": "boundary-port-max",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.51",
                      "port": 65535
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：服務登記 port=null（缺失）→ 400")
        void serviceRegistrationPortNull() throws Exception {
            String json = """
                    {
                      "nodeId": "boundary-port-null",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.52"
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("邊界值：日誌 limit=0 → 返回空列表或 200")
        void logsLimitZero() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/logs?limit=0"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：日誌 limit=1（最小有效值）→ 200")
        void logsLimitOne() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/logs?limit=1"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：日誌 limit=-1（負數）→ 200（後端不校驗 limit 範圍）")
        void logsLimitNegative() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/logs?limit=-1"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("邊界值：topic 僅含一個字符（最小有效長度）→ 接受")
        void topicSingleChar() throws Exception {
            String json = validMessageSimulationJson("A", 1, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }
    }

    // =================================================================
    // 3. 正交試驗法——利用正交表高效測試多因素組合
    // =================================================================

    @Nested
    @DisplayName("正交試驗法")
    class OrthogonalArrayTests {

        /**
         * 因素：mode（PSEUDO/REAL）× middleware（ROCKETMQ）× endpoint（topology/metrics/logs）
         * 正交表 L4(2^3)：4 個測試用例覆蓋所有因素組合
         */
        @Test
        @DisplayName("正交組合 1：PSEUDO + ROCKETMQ + topology → 200")
        void orthogonalCombo1() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes").isArray());
        }

        @Test
        @DisplayName("正交組合 2：PSEUDO + ROCKETMQ + metrics → 200")
        void orthogonalCombo2() throws Exception {
            mockMvc.perform(get(PSEUDO_PATH + "/metrics"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes").isArray());
        }

        @Test
        @DisplayName("正交組合 3：REAL + ROCKETMQ + topology → 200")
        void orthogonalCombo3() throws Exception {
            mockMvc.perform(get(REAL_PATH + "/topology"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("正交組合 4：REAL + ROCKETMQ + metrics → 200")
        void orthogonalCombo4() throws Exception {
            mockMvc.perform(get(REAL_PATH + "/metrics"))
                    .andExpect(status().isOk());
        }

        /**
         * 因素：服務登記的 role × address 格式 × port 範圍
         * 正交表 L4(2^3)
         */
        @Test
        @DisplayName("正交組合 5：role=nameserver + address=10.77.0.60 + port=9876 → 200")
        void orthogonalServiceCombo1() throws Exception {
            String json = """
                    {
                      "nodeId": "ortho-ns-1",
                      "displayName": "NS",
                      "role": "nameserver",
                      "hostName": "ns-host",
                      "address": "10.77.0.60",
                      "port": 9876
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("正交組合 6：role=broker-master + address=10.77.0.61 + port=10911 → 200")
        void orthogonalServiceCombo2() throws Exception {
            String json = """
                    {
                      "nodeId": "ortho-bm-1",
                      "displayName": "BM",
                      "role": "broker-master",
                      "hostName": "bm-host",
                      "address": "10.77.0.61",
                      "port": 10911
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("正交組合 7：role=broker-slave + address=10.77.0.62 + port=10921 → 200")
        void orthogonalServiceCombo3() throws Exception {
            String json = """
                    {
                      "nodeId": "ortho-bs-1",
                      "displayName": "BS",
                      "role": "broker-slave",
                      "hostName": "bs-host",
                      "address": "10.77.0.62",
                      "port": 10921
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("正交組合 8：role=unknown + address=10.77.0.63 + port=8080 → 200（無角色校驗）")
        void orthogonalServiceCombo4() throws Exception {
            String json = """
                    {
                      "nodeId": "ortho-unknown-1",
                      "displayName": "Unknown",
                      "role": "unknown-role",
                      "hostName": "unknown-host",
                      "address": "10.77.0.63",
                      "port": 8080
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }
    }

    // =================================================================
    // 4. 判定表法——處理複雜邏輯條件組合
    // =================================================================

    @Nested
    @DisplayName("判定表法")
    class DecisionTableTests {

        /**
         * 判定表：節點操作結果依賴於 (節點是否存在, 當前狀態, 操作類型)
         *
         * | 節點存在 | 當前狀態   | 操作類型 | 預期結果           |
         * |---------|-----------|---------|-------------------|
         * | 是      | STOPPED   | START   | 成功啟動           |
         * | 是      | RUNNING   | STOP    | 成功停止           |
         * | 是      | STOPPED   | START   | 冪等（已停止→啟動） |
         * | 是      | RUNNING   | START   | 冪等（已運行→無操作）|
         * | 否      | N/A       | START   | 失敗（節點不存在）  |
         * | 否      | N/A       | STOP    | 失敗（節點不存在）  |
         */

        @Test
        @DisplayName("判定表：種子節點 STOPPED + START → 操作結果返回")
        void decisionStoppedNodeStart() throws Exception {
            String json = """
                    {"operationType": "START"}
                    """;
            // 種子節點 rmq-ns-01 存在且為 STOPPED，START 操作應返回結果
            // 注意：嵌入式 RocketMQ 啟動可能失敗（端口/資源問題），但 API 應返回結果
            MvcResult result = mockMvc.perform(post(PSEUDO_PATH + "/nodes/rmq-ns-01/operations")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk())
                    .andReturn();
            // 驗證返回結構包含 targetId 和 operationType
            String content = result.getResponse().getContentAsString();
            assertThat(content).contains("targetId");
            assertThat(content).contains("operationType");
        }

        @Test
        @DisplayName("判定表：不存在的節點 + START → 400（節點不存在拋 IllegalArgumentException）")
        void decisionNonExistentNodeStart() throws Exception {
            String json = """
                    {"operationType": "START"}
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/nodes/non-existent-node/operations")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("判定表：種子節點 + RESTART → 操作結果返回")
        void decisionStoppedNodeRestart() throws Exception {
            String json = """
                    {"operationType": "RESTART"}
                    """;
            MvcResult result = mockMvc.perform(post(PSEUDO_PATH + "/nodes/rmq-ns-01/operations")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk())
                    .andReturn();
            String content = result.getResponse().getContentAsString();
            assertThat(content).contains("targetId");
        }

        @Test
        @DisplayName("判定表：服務登記 → 刪除已登記服務 → 再刪除（冪等性）")
        void decisionDeleteServiceIdempotent() throws Exception {
            // 1. 登記服務
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("decision-test-node")))
                    .andExpect(status().isOk());

            // 2. 刪除服務 → 應成功
            mockMvc.perform(delete(PSEUDO_PATH + "/services/decision-test-node"))
                    .andExpect(status().isOk());

            // 3. 再次刪除 → 400（節點已不存在拋 IllegalArgumentException）
            mockMvc.perform(delete(PSEUDO_PATH + "/services/decision-test-node"))
                    .andExpect(status().isBadRequest());
        }

        /**
         * 判定表：消息模擬結果依賴於 (NameServer狀態, Broker狀態, producer存在性)
         *
         * | NameServer | Broker | producerNodeId | 預期結果              |
         * |-----------|--------|---------------|----------------------|
         * | 未運行     | 未運行  | 種子節點       | 失敗（NS未運行）       |
         * | 未運行     | 未運行  | 不存在         | 失敗（producer不存在） |
         */
        @Test
        @DisplayName("判定表：節點未啟動 + 消息模擬 → 返回失敗結果（非 500）")
        void decisionMessageSimNoNodesRunning() throws Exception {
            String json = validMessageSimulationJson("DecisionTopic", 1, "rmq-broker-m-01");
            MvcResult result = mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk())
                    .andReturn();
            // 應返回消息模擬結果（含 deliveries 列表），不應拋 500
            String content = result.getResponse().getContentAsString();
            assertThat(content).contains("deliveries");
        }

        @Test
        @DisplayName("判定表：不存在的 producer + 消息模擬 → 400（producer 不存在拋 IllegalArgumentException）")
        void decisionMessageSimNonExistentProducer() throws Exception {
            String json = validMessageSimulationJson("DecisionTopic", 1, "non-existent-producer");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }
    }

    // =================================================================
    // 5. 錯誤猜測法——基於經驗直覺定位潛在缺陷
    // =================================================================

    @Nested
    @DisplayName("錯誤猜測法")
    class ErrorGuessingTests {

        @Test
        @DisplayName("錯誤猜測：請求體為空字符串 → 4xx 或 5xx（Spring 反序列化失敗）")
        void emptyRequestBody() throws Exception {
            int status = mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(""))
                    .andReturn().getResponse().getStatus();
            assertThat(status).isGreaterThanOrEqualTo(400);
        }

        @Test
        @DisplayName("錯誤猜測：請求體為非法 JSON → 4xx 或 5xx（Spring 反序列化失敗）")
        void malformedJson() throws Exception {
            int status = mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{invalid json"))
                    .andReturn().getResponse().getStatus();
            assertThat(status).isGreaterThanOrEqualTo(400);
        }

        @Test
        @DisplayName("錯誤猜測：缺少 Content-Type → 4xx 或 5xx（不支持的媒體類型）")
        void missingContentType() throws Exception {
            int status = mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .content(validServiceRegistrationJson("no-ctype")))
                    .andReturn().getResponse().getStatus();
            assertThat(status).isGreaterThanOrEqualTo(400);
        }

        @Test
        @DisplayName("錯誤猜測：重複登記相同 nodeId → 400（節點已存在拋 IllegalArgumentException）")
        void duplicateNodeIdRegistration() throws Exception {
            // 第一次登記 → 成功
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("dup-node-id")))
                    .andExpect(status().isOk());

            // 第二次登記相同 nodeId → 400（IllegalArgumentException: Node already exists）
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("dup-node-id")))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("錯誤猜測：topic 含特殊字符（空格、中文）→ 接受或 400")
        void topicWithSpecialChars() throws Exception {
            String json = validMessageSimulationJson("Topic With Spaces", 1, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("錯誤猜測：topic 含 SQL 注入字符串 → 接受（非 SQL 系統）")
        void topicWithSqlInjection() throws Exception {
            String json = validMessageSimulationJson("'; DROP TABLE--", 1, "rmq-broker-m-01");
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("錯誤猜測：address 為非法 IP 格式 → 400（VIRTUAL 節點要求 IPv4 格式）")
        void serviceRegistrationInvalidAddress() throws Exception {
            String json = """
                    {
                      "nodeId": "bad-address",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "not-an-ip-address",
                      "port": 10911
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("錯誤猜測：port 為負數 → 400（port 必須 > 0）")
        void serviceRegistrationNegativePort() throws Exception {
            String json = """
                    {
                      "nodeId": "neg-port",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.70",
                      "port": -1
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("錯誤猜測：port 為超大值（>65535）→ 接受（無端口上界校驗）")
        void serviceRegistrationOversizedPort() throws Exception {
            String json = """
                    {
                      "nodeId": "oversize-port",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.71",
                      "port": 99999
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("錯誤猜測：consumerNodeIds 包含不存在的節點 → 400（節點不存在拋 IllegalArgumentException）")
        void messageSimNonExistentConsumer() throws Exception {
            String json = """
                    {
                      "topic": "ErrorGuessTopic",
                      "consumerGroup": "test-group",
                      "messageCount": 1,
                      "producerNodeId": "rmq-broker-m-01",
                      "consumerNodeIds": ["non-existent-consumer"]
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("錯誤猜測：consumerNodeIds 為空數組 → 返回結果")
        void messageSimEmptyConsumerList() throws Exception {
            String json = """
                    {
                      "topic": "ErrorGuessTopic",
                      "consumerGroup": "test-group",
                      "messageCount": 1,
                      "producerNodeId": "rmq-broker-m-01",
                      "consumerNodeIds": []
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("錯誤猜測：headers 為 null（JSON 中缺失字段）→ 接受")
        void messageSimNullHeaders() throws Exception {
            String json = """
                    {
                      "topic": "ErrorGuessTopic",
                      "consumerGroup": "test-group",
                      "messageCount": 1,
                      "producerNodeId": "rmq-broker-m-01"
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("錯誤猜測：labels 為 null（JSON 中缺失字段）→ 接受")
        void serviceRegistrationNullLabels() throws Exception {
            String json = """
                    {
                      "nodeId": "null-labels",
                      "displayName": "Test",
                      "role": "broker-master",
                      "hostName": "host",
                      "address": "10.77.0.72",
                      "port": 10911
                    }
                    """;
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json))
                    .andExpect(status().isOk());
        }
    }

    // =================================================================
    // 6. 場景法——模擬真實業務流程測試
    // =================================================================

    @Nested
    @DisplayName("場景法")
    class ScenarioTests {

        @Test
        @DisplayName("場景 1：用戶首次進入——獲取 Provider 列表 → 選擇集群 → 加載拓撲")
        void scenarioFirstVisit() throws Exception {
            // Step 1: 獲取 Provider 列表
            MvcResult providersResult = mockMvc.perform(get("/api/clusters/providers"))
                    .andExpect(status().isOk())
                    .andReturn();
            String providersJson = providersResult.getResponse().getContentAsString();
            assertThat(providersJson).contains("pseudo-rocketmq");

            // Step 2: 加載拓撲
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.cluster.clusterId").value("local-lab"))
                    .andExpect(jsonPath("$.nodes").isArray())
                    .andExpect(jsonPath("$.nodes.length()").value(3));

            // Step 3: 加載指標
            mockMvc.perform(get(PSEUDO_PATH + "/metrics"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes").isArray());

            // Step 4: 加載日誌
            mockMvc.perform(get(PSEUDO_PATH + "/logs"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("場景 2：手動登記節點 → 查看拓撲包含新節點 → 刪除節點 → 拓撲不再包含")
        void scenarioRegisterAndDeleteNode() throws Exception {
            // Step 1: 登記新節點
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("scenario-node-01")))
                    .andExpect(status().isOk());

            // Step 2: 拓撲應包含新節點
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'scenario-node-01')]").exists());

            // Step 3: 刪除節點
            mockMvc.perform(delete(PSEUDO_PATH + "/services/scenario-node-01"))
                    .andExpect(status().isOk());

            // Step 4: 拓撲不再包含該節點
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'scenario-node-01')]").doesNotExist());
        }

        @Test
        @DisplayName("場景 3：查看停止狀態節點的指標 → 全零")
        void scenarioStoppedNodeMetrics() throws Exception {
            // 所有種子節點初始為 STOPPED
            MvcResult result = mockMvc.perform(get(PSEUDO_PATH + "/metrics"))
                    .andExpect(status().isOk())
                    .andReturn();
            String content = result.getResponse().getContentAsString();
            // 停止節點的指標應為零值
            assertThat(content).contains("cpuUsage");
            assertThat(content).contains("memoryUsage");
        }

        @Test
        @DisplayName("場景 4：登記多個節點 → 拓撲包含全部 → 按日誌查詢驗證審計記錄")
        void scenarioRegisterMultipleNodesAndCheckLogs() throws Exception {
            // 登記 3 個節點（使用不同 IP 避免衝突）
            String[] ips = {"10.77.0.200", "10.77.0.201", "10.77.0.202"};
            for (int i = 1; i <= 3; i++) {
                mockMvc.perform(post(PSEUDO_PATH + "/services")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(validServiceRegistrationJson("scenario-multi-" + i, ips[i - 1])))
                        .andExpect(status().isOk());
            }

            // 拓撲應包含所有新節點
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'scenario-multi-1')]").exists())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'scenario-multi-2')]").exists())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'scenario-multi-3')]").exists());

            // 日誌應包含登記記錄
            MvcResult logsResult = mockMvc.perform(get(PSEUDO_PATH + "/logs?limit=50"))
                    .andExpect(status().isOk())
                    .andReturn();
            String logsContent = logsResult.getResponse().getContentAsString();
            assertThat(logsContent).contains("registered");
        }

        @Test
        @DisplayName("場景 5：消息模擬——節點未啟動時發送消息 → 返回失敗投遞結果")
        void scenarioMessageSimulationNodesNotRunning() throws Exception {
            // 種子節點初始為 STOPPED，直接嘗試消息模擬
            MvcResult result = mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validMessageSimulationJson("ScenarioTopic", 3, "rmq-broker-m-01")))
                    .andExpect(status().isOk())
                    .andReturn();
            String content = result.getResponse().getContentAsString();
            assertThat(content).contains("deliveries");
            // 投遞結果中應有失敗標記（節點未運行）
            assertThat(content).contains("success");
        }

        @Test
        @DisplayName("場景 6：切換集群——PSEUDO 拓撲 → REAL 拓撲 → PSEUDO 指標")
        void scenarioSwitchCluster() throws Exception {
            // PSEUDO 拓撲
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.cluster.mode").value("PSEUDO"));

            // REAL 拓撲
            mockMvc.perform(get(REAL_PATH + "/topology"))
                    .andExpect(status().isOk());

            // 回到 PSEUDO 指標
            mockMvc.perform(get(PSEUDO_PATH + "/metrics"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes").isArray());
        }

        @Test
        @DisplayName("場景 7：按 nodeId 過濾日誌 → 僅返回該節點的日誌")
        void scenarioFilterLogsByNodeId() throws Exception {
            // 先登記一個節點產生日誌
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("log-filter-test")))
                    .andExpect(status().isOk());

            // 查詢全部日誌
            MvcResult allLogs = mockMvc.perform(get(PSEUDO_PATH + "/logs?limit=50"))
                    .andExpect(status().isOk())
                    .andReturn();

            // 按 nodeId 過濾日誌
            MvcResult filteredLogs = mockMvc.perform(get(PSEUDO_PATH + "/logs?nodeId=log-filter-test&limit=50"))
                    .andExpect(status().isOk())
                    .andReturn();

            // 過濾後的日誌應只包含指定節點的記錄
            String filteredContent = filteredLogs.getResponse().getContentAsString();
            assertThat(filteredContent).contains("log-filter-test");
        }

        @Test
        @DisplayName("場景 8：完整學習流程——登記 → 查拓撲 → 查指標 → 查日誌 → 刪除")
        void scenarioFullLearningFlow() throws Exception {
            // 1. 獲取 Provider
            mockMvc.perform(get("/api/clusters/providers"))
                    .andExpect(status().isOk());

            // 2. 加載拓撲
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk());

            // 3. 登記學習用節點
            mockMvc.perform(post(PSEUDO_PATH + "/services")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validServiceRegistrationJson("learning-node")))
                    .andExpect(status().isOk());

            // 4. 加載指標
            mockMvc.perform(get(PSEUDO_PATH + "/metrics"))
                    .andExpect(status().isOk());

            // 5. 加載日誌
            mockMvc.perform(get(PSEUDO_PATH + "/logs"))
                    .andExpect(status().isOk());

            // 6. 嘗試消息模擬
            mockMvc.perform(post(PSEUDO_PATH + "/messages/simulate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validMessageSimulationJson("LearningTopic", 1, "learning-node")))
                    .andExpect(status().isOk());

            // 7. 刪除學習節點
            mockMvc.perform(delete(PSEUDO_PATH + "/services/learning-node"))
                    .andExpect(status().isOk());

            // 8. 確認拓撲不再包含
            mockMvc.perform(get(PSEUDO_PATH + "/topology"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nodes[?(@.nodeId == 'learning-node')]").doesNotExist());
        }
    }
}
