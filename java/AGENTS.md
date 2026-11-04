# AGENTS.md — MQCluster 後端（Java）

> AI agent 後端導航與規約文件。項目總覽見上層 `../AGENTS.md`，中文文檔見 `../docs/zh/`。

## 1. 項目速覽

| 字段 | 值 |
| --- | --- |
| 名稱 | MQCluster（`io.github.jacklika:mqcluster`） |
| 版本 | `0.1.0-SNAPSHOT` |
| 倉庫 | https://github.com/JackLiKa/MQCluster |
| 許可證 | Apache 2.0 |
| 描述 | Spring Boot 4.1.0 後端，嵌入真實 Apache RocketMQ 5.3.3、Apache Kafka 4.2（KRaft）與 Apache Qpid Broker-J 9.2（AMQP 0-9-1），提供 REST + WebSocket API。 |

### 技術棧

- Java 21、Spring Boot 4.1.0（Web / Validation / WebSocket / Actuator）
- Apache RocketMQ 5.3.3（嵌入式 NameServer + Broker）
- Apache Kafka 4.2（`kafka_2.13` + `kafka-server`，版本取 Spring Boot BOM `kafka.version`；排除 log4j2 實現，統一走 Logback）
- Apache Qpid Broker-J 9.2.1（`qpid-broker-core` / `qpid-broker-plugins-amqp-0-8-protocol` / `qpid-broker-plugins-memory-store`）+ `com.rabbitmq:amqp-client`
- JUnit 5 + AssertJ + Spring Boot Test
- Maven（wrapper `mvnw`/`mvnw.cmd`）

## 2. 架構（六邊形 / 端口與適配器）

```
src/main/java/com/example/clustermanager
├─ api/            → REST 控制器、DTO、WebSocket 配置、異常處理
│  ├─ config/      → CorsConfig、WebSocketBrokerConfig、ClusterStreamProperties
│  ├─ controller/  → ClusterController（通用 REST 端點）、KafkaController、RabbitController、LabController（壓力實驗 + 內部機制）、ClusterTelemetryPushService（遍歷所有 provider 推送 STOMP）
│  └─ dto/         → 請求 DTO（帶校驗）
├─ application/    → 用例編排與 provider 選擇
│  ├─ model/       → 命令 record（ClusterSelection、*Command）
│  └─ service/     → ClusterFacadeService、ClusterProviderRegistry、MessageTemplateService、RocketMqConnectionConfigService
├─ core/           → 領域模型與端口（無外部依賴）
│  ├─ model/       → ClusterMode、ClusterNode、ClusterTopology、NodeStatus、MessageTemplate、ProviderDescriptor（含 clusterId / capabilities）、ProviderCapabilities、kafka/、rabbit/、lab/（StressCommand / StressStatus / InternalsSnapshot）…
│  └─ port/        → IClusterProvider、ITopologyReader、INodeManager（含 operateCluster）、IMonitoringGateway、IMessageWorkbench、IKafkaInspector、IRabbitInspector、IStressLab、IInternalsInspector
└─ infrastructure/ → 適配器實現
   ├─ pseudo/      → PseudoClusterProvider + 子組件
   │  ├─ node/     → ManagedNode、NodeRegistry、NodeRole
   │  ├─ runtime/  → EmbeddedRocketMqRuntime、EmbeddedRocketMqNode、EmbeddedNodeSpec、PortPool
   │  ├─ messaging/→ EmbeddedMessageWorkbench
   │  ├─ topology/ → PseudoTopologySeeder
   │  ├─ lab/       → RocketMqStressLab（RocketMqStressSender）、RocketMqInternalsInspector
   │  └─ 網絡組件  → TapVirtualNetwork、NoOpTapNativeBridge
   ├─ rocketmq/    → RocketMqClusterProvider、RocketMqAdminClient（+ Mock/Real）、RocketMqConnectionConfig
   ├─ kafka/       → KafkaClusterProvider（IClusterProvider + IKafkaInspector + LAB）、KafkaClusterProperties
   │  ├─ lab/      → KafkaStressLab（KafkaStressSender）、KafkaInternalsInspector
   │  ├─ runtime/  → EmbeddedKafkaRuntime、EmbeddedKafkaNode、KafkaNodeSpec
   │  ├─ topology/ → KafkaTopologySeeder（kafka-ctrl-01、kafka-broker-01..03）
   │  ├─ messaging/→ KafkaMessageWorkbench
   │  ├─ group/    → KafkaConsumerGroupManager（常駐消費者線程池）
   │  └─ metrics/  → KafkaThroughputTracker
   ├─ rabbitmq/    → RabbitClusterProvider（IClusterProvider + IRabbitInspector + LAB）、RabbitClusterProperties
   │  ├─ lab/      → RabbitStressLab（RabbitStressSender）、RabbitInternalsInspector
   │  ├─ runtime/  → EmbeddedQpidRuntime、RabbitObjectModel
   │  ├─ topology/ → RabbitTopologySeeder（rabbit-node-01 + 演示交換機 / 隊列）
   │  ├─ messaging/→ RabbitMessageWorkbench
   │  ├─ routing/  → RabbitRouteMatcher（路由預覽，純計算）
   │  └─ consumer/ → RabbitConsumerManager
   ├─ lab/         → StressRunner（共用壓測引擎：限速調度、逐秒採樣、p95/p99、失敗聚合、三檔判定）+ StressSender
   └─ common/      → JvmProcessMetrics 等共享工具
```

**依賴方向**：`api` → `application` → `core` ← `infrastructure`。`core` 零外部依賴。

## 3. 構建、運行、測試

```powershell
.\mvnw.cmd spring-boot:run          # 開發：http://localhost:8088
.\mvnw.cmd clean verify             # 完整構建 + 測試（193 tests, 18 條件跳過）
.\mvnw.cmd test -Dtest=KafkaClusterProviderTest -DrunEmbeddedKafka=true   # 耗時較長的 Kafka 集群測試
.\mvnw.cmd clean package            # fat JAR → target/mqcluster-0.1.0-SNAPSHOT.jar
```

### 啟動前必做步驟（Java 21 + 端口清理 + 殘留清理）

> ⚠️ **Java 版本要求**：後端需要 **Java 21**（class file version 65.0）。
> 若 `JAVA_HOME` 指向 Java 17（version 61.0），`spring-boot:run` 會報
> `UnsupportedClassVersionError: ... has been compiled by a more recent version of the Java Runtime`。
> 必須在啟動前設置 `JAVA_HOME` 為 Java 21。

**Windows (PowerShell):**
```powershell
$env:JAVA_HOME = "C:\Users\<你的用戶名>\.jdks\ms-21.0.9"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
java -version
Get-NetTCPConnection -LocalPort 8088 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
Start-Sleep -Seconds 2
cd A:\project\Cluster\java
if (Test-Path run) { Remove-Item -Recurse -Force run }
.\mvnw.cmd clean
.\mvnw.cmd spring-boot:run
Invoke-WebRequest -Uri "http://localhost:8088/api/clusters/providers" -UseBasicParsing
```

**Linux (Bash):**
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
java -version
lsof -ti:8088 | xargs kill -9 2>/dev/null
sleep 2
cd /path/to/Cluster/java
rm -rf run
./mvnw clean
./mvnw spring-boot:run
curl -s http://localhost:8088/api/clusters/providers
```

**macOS (Zsh):**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH=$JAVA_HOME/bin:$PATH
java -version
lsof -ti:8088 | xargs kill -9 2>/dev/null
sleep 2
cd /path/to/Cluster/java
rm -rf run
./mvnw clean
./mvnw spring-boot:run
curl -s http://localhost:8088/api/clusters/providers
```

### 常見啟動失敗排查

| 症狀 | 原因 | 解決方案 |
| --- | --- | --- |
| `UnsupportedClassVersionError (class file version 65.0)` | JAVA_HOME 指向 Java 17 | 設置 JAVA_HOME 為 Java 21 路徑（見上方命令） |
| `Failed to bind to 0.0.0.0:8088` | 8088 端口被佔用 | Windows: `Get-NetTCPConnection -LocalPort 8088` → `Stop-Process`；Linux/macOS: `lsof -ti:8088 | xargs kill -9` |
| `Address already in use: bind`（Broker 端口） | 上次 Broker 進程未完全退出 | 殺進程 + `rm -rf run`（Linux/macOS）/ `Remove-Item -Recurse -Force run`（Windows）+ 等待 2 秒 |
| `Store lock file occupied` | RocketMQ store 鎖未釋放 | 清理 `run/` 目錄（含 `pseudo-cluster`、`kafka-cluster`、`rabbitmq-cluster`） |
| Kafka / Qpid 在某平台啟動異常 | 嵌入式運行時與平台不兼容 | `cluster.kafka.enabled=false` / `cluster.rabbitmq.enabled=false` 單獨關閉，RocketMQ 不受影響 |
| Linux `./mvnw: Permission denied` | mvnw 無執行權限 | `chmod +x ./mvnw` |

### JVM 參數（重要）

`spring-boot-maven-plugin` 配置了 `--add-opens` JVM 參數：
- `--add-opens java.base/java.nio=ALL-UNNAMED`
- `--add-opens java.base/java.lang.reflect=ALL-UNNAMED`
- `--add-opens java.base/sun.nio.ch=ALL-UNNAMED`
- `--add-opens java.base/java.lang=ALL-UNNAMED`
- `--add-opens java.base/java.util=ALL-UNNAMED`
- `--add-exports java.base/jdk.internal.ref=ALL-UNNAMED`

**這些參數是必需的**——RocketMQ 5.3.3 在 Java 21 上 `Broker.shutdown()` 時需要反射訪問 `DirectByteBuffer.attachment()` 釋放內存映射文件。Java 21 模塊系統更嚴格，需要比 Java 17 多打開 `java.lang` 和 `java.util` 模塊。沒有這些參數，shutdown 拋 `InaccessibleObjectException`，store lock 文件無法刪除，重啟時 Broker 無法啟動。

## 4. 配置

`src/main/resources/application.properties`：

| 屬性 | 值 | 備註 |
| --- | --- | --- |
| `server.port` | `8088` | 後端端口 |
| `cluster.cors.allowed-origins` | `http://localhost:3000` | 前端源 |
| `cluster.pseudo.auto-start` | `false` | 種子節點初始 STOPPED |
| `cluster.pseudo.clean-store-on-start` | `true` | 啟動時清理舊 store 目錄 |
| `cluster.pseudo.work-dir` | `run/pseudo-cluster` | 嵌入式 RocketMQ 存儲 |
| `cluster.pseudo.health-timeout` | `5s` | 健康輪詢超時 |
| `cluster.stream.publish-interval-ms` | `5000` | 遙測推送週期 |
| `cluster.kafka.enabled` / `cluster-id` / `work-dir` / `clean-store-on-start` | `true` / `kafka-lab` / `run/kafka-cluster` / `true` | 嵌入式 Kafka |
| `cluster.rabbitmq.enabled` / `cluster-id` / `work-dir` | `true` / `rabbitmq-lab` / `run/rabbitmq-cluster` | 嵌入式 Qpid |

## 5. 關鍵實現細節

- **Broker 重啟延遲**：`EmbeddedRocketMqRuntime.restart()` 在 stop 後等待 1.5 秒再 start；`EmbeddedRocketMqNode.stop()` 在 shutdown 後等待 1 秒。確保線程池終止和文件句柄釋放。
- **Broker stats key**：RocketMQ `BROKER_PUT_NUMS`/`BROKER_GET_NUMS` 的 stats key 是 `brokerClusterName`（`embedded-cluster`）。
- **消息模板占位符**：`MessageTemplateService.render()` 替換 `{index}`/`{timestamp}`/`{uuid}`/`{random}`/`{topic}`。
- **外部 MQ 通信三層後備**：PSEUDO HOST 路徑按優先級查找 NameServer——登記的 HOST 節點 > 運行時 UI 配置 > 環境變量。
- **運行時配置持久化**：UI 配置保存到 `run/rocketmq-connection.properties`，啟動時自動加載。
- **RealRocketMqAdminClient 條件啟用**：`@ConditionalOnProperty(prefix="cluster.rocketmq", name="name-servers")`。
- **Kafka 生命週期**：先 controller 後 broker；broker 重啟用「新建 `KafkaRaftServer` + 復用同一日誌目錄」；停止後等待資源釋放。端口由 `PortPool` 動態分配。
- **Kafka 吞吐**：Yammer 指標註冊表 JVM 全局共享，按 broker 的速率用「作為 leader 的分區 LEO 增量 ÷ 間隔」，消費速率用 committed offset 增量。
- **Qpid 差異翻譯**：`x-dead-letter-exchange` → `alternateBinding` + `maximumDeliveryAttempts`；`x-message-ttl` → `maximumMessageTtl`；TTL 過期不轉死信（Qpid 不支持，UI 禁用）。詳見 `../docs/MQ_FEATURE_MATRIX.md`。
- **消息 options**：`MessageSimulationRequest.options`（`Map<String,String>`，可為 null）承載 Kafka `key`/`partition`/`acks`/`compression` 與 RabbitMQ `exchange`/`routingKey`/`headers.*`/`ackMode`/`nackRatio`/`requeue`/`prefetch`/`ttlMs`/`confirm`。
- **專屬端口**：`ClusterProviderRegistry.resolveCapability(selection, IKafkaInspector.class)`，provider 不支持時拋 `IllegalArgumentException`（400）。
- **壓測實驗端口**：`IStressLab`（`start/stop/status`）+ `IInternalsInspector`（`inspect`），通過 `IProviderCapability` 暴露能力，路由為 `POST|GET /api/clusters/{mode}/{type}/{cluster}/lab/stress` 與 `GET .../lab/internals`。三個實現分別在 `infrastructure/pseudo/lab`、`infrastructure/kafka/lab`、`infrastructure/rabbitmq/lab`，共用 `infrastructure/lab/StressRunner`（限速調度、逐秒採樣、p95/p99、失敗原因聚合、三檔判定）。
- **壓測發送器**：`StressSender` 由各 MQ 適配器實現（`send` + `drain` + `close` + `reportsAsyncLatency`）。Kafka/RocketMQ 為異步，必須 `reportsAsyncLatency()=true` 並在回調裡 `bindLatencySink` 上報延遲；RabbitMQ 用同步 confirm，返回 false。
- **Broker 端口成對**：`PortPool.allocateBrokerListenPort()` 同時預留 `listenPort` 與 `listenPort - 2`（RocketMQ 隱藏 fast remoting 端口），停止時 `releaseBrokerListenPort()` 成對釋放。
- **`StressRunner` 鎖邊界**：`finish()` 只在鎖內切狀態、定格 `endedAt`，`drain()/close()` 在鎖外執行；各 Lab 的 `status()/stop()` 鎖內取 `runner` 引用後鎖外調用，避免排空期間狀態接口 hang。
- **`reportsAsyncLatency` / `if (running)`**：停止實驗造成的 `InterruptedException` 與 `Producer is closed forcefully` 不算失敗；`describe()` 折疊異常裡的數字避免錯誤桶爆炸。

## 6. 規約

- **DTO/命令/模型用 record**（不可變值對象）
- **構造器注入**貫穿；無字段注入
- **按層分包**（六邊形）：`core` → `application` → `infrastructure` + `api`
- **錯誤處理**：`ApiExceptionHandler` 把 `IllegalArgumentException`→400、`IllegalStateException`→409
- **歷史修復標記**：代碼裡的 `// P0 修復` / `// P1 修復` / `// P3 修復` 註釋不要移除
- **推送前跑** `.\mvnw.cmd clean verify`——CI 跑同一道門
