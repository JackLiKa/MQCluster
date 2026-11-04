# AGENTS.md — MQCluster（多 MQ 學習平台：RocketMQ / Kafka / RabbitMQ）

> AI agent 導航與規約文件。前後端分離架構，Java 後端 + Next.js 前端。

## 1. 項目速覽

| 字段 | 值 |
| --- | --- |
| 名稱 | MQCluster（`io.github.jacklika:mqcluster`） |
| 版本 | `0.1.0-SNAPSHOT` |
| 倉庫 | https://github.com/JackLiKa/MQCluster |
| 許可證 | Apache 2.0 |
| 描述 | 本地優先的多 MQ 學習平台——單機瀏覽器操作嵌入式真實 RocketMQ、Kafka（KRaft）與 AMQP 0-9-1（Qpid Broker-J），學習集群拓撲、路由、消費模型與運維。 |

## 2. 目錄結構

```
MQCluster/
├── .github/       → GitHub Actions CI/CD、Issue 模板
├── docs/          → 架構文檔（ARCHITECTURE.md、zh/ARCHITECTURE.zh.md）、MQ 特性矩陣（MQ_FEATURE_MATRIX.md）
├── java/          → Spring Boot 4.1.0 後端（端口 8088）
│   ├── src/main/java/com/example/clustermanager/
│   │   ├── api/              → REST 控制器、DTO、WebSocket 配置、異常處理
│   │   ├── application/      → 用例編排與 provider 選擇
│   │   ├── core/             → 領域模型與端口（無外部依賴）
│   │   └── infrastructure/   → 適配器實現（pseudo 嵌入式 RocketMQ、rocketmq admin、kafka 嵌入式 KRaft、rabbitmq 嵌入式 Qpid、lab 壓測引擎與各 MQ 實驗室、common 共享工具）
│   ├── src/main/resources/   → application.properties、qpid/（Qpid 初始配置）
│   ├── src/test/             → JUnit 5 + AssertJ 測試
│   ├── pom.xml               → Maven 構建（artifact: mqcluster）
│   └── AGENTS.md             → 後端詳細導航
├── next/          → Next.js 16 前端（端口 3000）
│   ├── src/
│   │   ├── app/              → App Router：page.tsx 首頁、mq/[middleware] 儀表盤、compare/ 對比頁、guide/[middleware] 學習指南
│   │   ├── components/       → dashboard/（通用標籤頁 + MqDashboard）、lab/（壓力實驗台 + 內部機制）、kafka/、rabbit/、ui/（shadcn）、全局狀態與主題
│   │   ├── hooks/            → use-cluster-data（輪詢 + STOMP）、use-node-operations、use-chart-palette
│   │   ├── lib/              → API 客戶端、mq-registry（MQ 元數據）、guides（指南內容）、工具函數
│   │   └── types/            → TypeScript 類型定義
│   ├── next.config.ts        → API 代理配置（/api → localhost:8088）
│   └── .env.local            → 環境變量
├── README.md      → 開源項目主 README
├── CONTRIBUTING.md → 貢獻指南
├── SECURITY.md    → 安全政策
├── LICENSE        → Apache 2.0
└── AGENTS.md      → 本文件
```

## 3. 技術棧

### 後端（java/）
- Java 21、Spring Boot 4.1.0
- Spring Web / Validation / WebSocket / Actuator
- Apache RocketMQ 5.3.3（嵌入式 NameServer + Broker）
- Apache Kafka 4.2（`kafka_2.13` / `kafka-server`，嵌入式 KRaft：1 controller + 3 broker；版本由 Spring Boot BOM `kafka.version` 管理，排除 log4j2 實現走 Logback）
- Apache Qpid Broker-J 9.2（core + amqp-0-8 協議 + memory store）+ 官方 `com.rabbitmq:amqp-client`
- JUnit 5 + AssertJ + Spring Boot Test
- Maven（wrapper `mvnw`/`mvnw.cmd`，artifact: `mqcluster`）

### 前端（next/）
- Next.js 16（App Router）+ React 19
- TypeScript（嚴格模式）
- Tailwind CSS v4 + shadcn/ui
- ECharts 6（拓撲可視化）
- axios（HTTP 客戶端）
- @stomp/stompjs + sockjs-client（WebSocket 實時推送）

## 4. 構建、運行、測試

### 後端
```powershell
cd java
.\mvnw.cmd spring-boot:run          # 開發：http://localhost:8088
.\mvnw.cmd clean verify             # 完整構建 + 測試（193 tests，18 個條件跳過）
.\mvnw.cmd clean package            # fat JAR → target/mqcluster-0.1.0-SNAPSHOT.jar
```

### 前端
```powershell
cd next
npm install
npm run dev                         # 開發：http://localhost:3000
npm run build                       # 生產構建
npm run lint                        # ESLint
npx tsc --noEmit                    # Typecheck
```

### 開發流程（完整啟動步驟）

> ⚠️ **重要**：後端需要 **Java 21**（class file version 65.0）。若 `JAVA_HOME` 指向 Java 17（version 61.0），
> `spring-boot:run` 會報 `UnsupportedClassVersionError`。啟動前必須確認 JDK 版本。
> 若 8088/3000 端口被佔用或上次運行殘留了進程/文件，項目會啟動失敗，需先清理。
>
> 本項目支持 **Windows、Linux、macOS** 三平台。以下按平台分別給出命令。

#### Step 0：確認 Java 21 可用

**Windows (PowerShell):**
```powershell
echo $env:JAVA_HOME
java -version
```

**Linux / macOS (Bash/Zsh):**
```bash
echo $JAVA_HOME
java -version
```

> 若 `java -version` 顯示的版本不是 21.x.x，需先安裝 Java 21 並設置 `JAVA_HOME`（見 Step 3）。

#### Step 1：清理舊進程和端口佔用

**Windows (PowerShell):**
```powershell
Get-NetTCPConnection -LocalPort 8088 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
Start-Sleep -Seconds 2
```

**Linux / macOS (Bash/Zsh):**
```bash
lsof -ti:8088 | xargs kill -9 2>/dev/null
lsof -ti:3000 | xargs kill -9 2>/dev/null
sleep 2
```

#### Step 2：清理運行殘留（可選，遇到啟動失敗時執行）

**Windows (PowerShell):**
```powershell
cd A:\project\Cluster\java
if (Test-Path run) { Remove-Item -Recurse -Force run }
.\mvnw.cmd clean
cd A:\project\Cluster\next
if (Test-Path .next) { Remove-Item -Recurse -Force .next }
```

**Linux / macOS (Bash/Zsh):**
```bash
cd /path/to/Cluster/java
rm -rf run
./mvnw clean
cd /path/to/Cluster/next
rm -rf .next
```

#### Step 3：設置 Java 21 環境變量並啟動後端

**Windows (PowerShell) — 當前 session 臨時生效:**
```powershell
$env:JAVA_HOME = "C:\Users\<你的用戶名>\.jdks\ms-21.0.9"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
java -version
cd A:\project\Cluster\java
.\mvnw.cmd spring-boot:run
```

**Windows — 永久生效（只需執行一次）:**
```powershell
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Users\<你的用戶名>\.jdks\ms-21.0.9", "User")
```

**Linux (Bash) — 當前 session 臨時生效:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
java -version
cd /path/to/Cluster/java
./mvnw spring-boot:run
```

**Linux — 永久生效（只需執行一次）:**
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk' >> ~/.bashrc
source ~/.bashrc
```

**macOS (Zsh) — 當前 session 臨時生效:**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH=$JAVA_HOME/bin:$PATH
java -version
cd /path/to/Cluster/java
./mvnw spring-boot:run
```

**macOS — 永久生效（只需執行一次）:**
```bash
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 21)' >> ~/.zshrc
source ~/.zshrc
```

> **Java 21 安裝方式參考**：
> - Windows: 下載 [Microsoft Build of OpenJDK 21](https://learn.microsoft.com/java/openjdk/) 或 `winget install Microsoft.OpenJDK.21`
> - Linux (Ubuntu/Debian): `sudo apt install openjdk-21-jdk`
> - Linux (Fedora/RHEL): `sudo dnf install java-21-openjdk-devel`
> - macOS (Homebrew): `brew install openjdk@21`
> - macOS (SDKMAN): `sdk install java 21-tem`

#### Step 4：啟動前端

**Windows (PowerShell):**
```powershell
cd A:\project\Cluster\next
npm run dev
```

**Linux / macOS (Bash/Zsh):**
```bash
cd /path/to/Cluster/next
npm run dev
```

> 等待看到 "Ready" 日誌，前端在 http://localhost:3000

#### Step 5：驗證啟動成功

**Windows (PowerShell):**
```powershell
Invoke-WebRequest -Uri "http://localhost:8088/api/clusters/providers" -UseBasicParsing
Invoke-WebRequest -Uri "http://localhost:3000" -UseBasicParsing
```

**Linux / macOS (Bash/Zsh):**
```bash
curl -s http://localhost:8088/api/clusters/providers
curl -s -o /dev/null -w "%{http_code}" http://localhost:3000
```

> 兩個都應返回 200。打開瀏覽器訪問 http://localhost:3000

#### 快速啟動（端口空閒且 Java 21 已配置時）

**Windows (PowerShell):**
```powershell
# 後端（終端 1）
cd A:\project\Cluster\java; .\mvnw.cmd spring-boot:run
# 前端（終端 2）
cd A:\project\Cluster\next; npm run dev
```

**Linux / macOS (Bash/Zsh):**
```bash
# 後端（終端 1）
cd /path/to/Cluster/java && ./mvnw spring-boot:run
# 前端（終端 2）
cd /path/to/Cluster/next && npm run dev
```

### 嵌入式 RocketMQ JVM 參數
RocketMQ 5.3.3 嵌入式運行需要 Java 模块訪問權限（Java 21 需要更多 --add-opens）：
```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
```

> 這些參數已在 `pom.xml` 的 `spring-boot-maven-plugin` 和 `surefire` 中配置，三平台通用，無需手動添加。

### 常見啟動失敗排查

| 症狀 | 原因 | 解決方案 |
| --- | --- | --- |
| `UnsupportedClassVersionError: class file version 65.0` | JAVA_HOME 指向 Java 17 | 設置 JAVA_HOME 為 Java 21 路徑（見 Step 3） |
| `Failed to bind to 0.0.0.0:8088` | 8088 端口被佔用 | 執行 Step 1 殺進程 |
| `Address already in use: bind`（Broker 端口） | 上次 Broker 進程未完全退出 | 殺進程 + 清理 `run/` 目錄 + 等待 2 秒 |
| `Store lock file occupied` | RocketMQ store 鎖未釋放 | 清理 `java/run/` 目錄 |
| 前端頁面空白 / API 404 | 後端未啟動或端口不匹配 | 確認後端 8088 已啟動，檢查 `next/.env.local` 的 `BACKEND_URL` |
| `EADDRINUSE: address already in use 0.0.0.0:3000` | 3000 端口被佔用 | 執行 Step 1 殺進程 |
| macOS `lsof: command not found` | lsof 未安裝 | `brew install lsof` 或用 `kill $(fuser 8088/tcp)` |
| Linux `./mvnw: Permission denied` | mvnw 無執行權限 | `chmod +x ./mvnw` |

## 5. 架構（六邊形 / 端口與適配器）

### 後端
```
api → application → core ← infrastructure
```
- `core`：領域模型與端口（零外部依賴）
- `application`：用例編排，通過 `ClusterProviderRegistry` 按 `(ClusterMode, MiddlewareType)` 選適配器
- `infrastructure/pseudo`：嵌入式 RocketMQ 運行時（進程內 NameServer + Broker）
- `infrastructure/rocketmq`：真實 RocketMQ admin 適配器
- `infrastructure/kafka`：`KafkaClusterProvider`（實現 `IClusterProvider` + `IKafkaInspector`），`runtime/` KRaft 運行時、`group/` 常駐消費者組、`messaging/` 消息工作台、`metrics/` 按 leader 分區 LEO 增量計算吞吐
- `infrastructure/rabbitmq`：`RabbitClusterProvider`（實現 `IClusterProvider` + `IRabbitInspector`），`runtime/` Qpid 運行時與對象模型讀取、`routing/` 路由匹配預覽、`consumer/` 常駐消費者、`messaging/` 消息工作台
- Provider 一覽：

| providerId | mode / middleware | clusterId | 種子節點 |
| --- | --- | --- | --- |
| `pseudo-rocketmq` | PSEUDO / ROCKETMQ | `local-lab` | `rmq-ns-01`、`rmq-broker-m-01`、`rmq-broker-s-01` |
| `rocketmq-admin` | REAL / ROCKETMQ | `rocketmq-demo` | 外部集群 |
| `kafka-embedded` | PSEUDO / KAFKA | `kafka-lab` | `kafka-ctrl-01`、`kafka-broker-01..03` |
| `rabbitmq-embedded` | PSEUDO / RABBITMQ | `rabbitmq-lab` | `rabbit-node-01` |

- `ProviderDescriptor.capabilities`（`CLUSTER_OPERATIONS`、`CONNECTION_CONFIG`、`SERVICE_REGISTRATION`、`KAFKA_INSPECTION`、`RABBIT_INSPECTION`）決定前端顯示哪些標籤頁與按鈕；`ClusterProviderRegistry.resolveCapability()` 取專屬端口，不支持時 400。

### 前端
```
app/（路由） → MqDashboard + 標籤頁組件 → hooks → lib/api.ts → 後端 REST/STOMP
```
- `app/page.tsx`：首頁（三張 MQ 入口卡片 + 學習路徑 + 資源提示）；`app/mq/[middleware]/page.tsx`：服務端組件校驗參數後渲染 `MqDashboard`，當前標籤頁存於 `?tab=`
- `components/dashboard/`：拓撲、節點操作（一鍵啟停、停止前確認）、消息工作台（RocketMQ / Kafka / RabbitMQ 三套表單）、監控、日誌、連接配置
- `components/kafka/`：Topic 與分區（分區矩陣 + leader 切換時間線）、消費者組（分配 + lag + rebalance）
- `components/rabbit/`：交換機與隊列（分層路由圖 + 路由預覽高亮）、確認與死信（預置實驗）
- `hooks/use-cluster-data.ts`：首次加載重試 + 5 秒輪詢 + STOMP 訂閱 `/topic/clusters/{clusterId}/{metrics|logs}`
- `lib/mq-registry.ts`：每種 MQ 的名稱、圖標、強調色、角色形狀 / 標籤、標籤頁（結合 `capabilities`）
- `lib/api.ts`：統一 axios 客戶端 + 類型化端點函數
- 主題：`globals.css` 以 `[data-mq]` 切換 `--mq-accent`（OKLCH），狀態色為 `status-*` token；`next-themes` 深淺切換；UI 文案統一簡體中文

完整架構說明見 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 6. 配置

### 後端（java/src/main/resources/application.properties）
| 屬性 | 值 | 備註 |
| --- | --- | --- |
| `server.port` | `8088` | 後端端口 |
| `cluster.cors.allowed-origins` | `http://localhost:3000` | 前端源 |
| `cluster.pseudo.auto-start` | `false` | 種子節點初始 STOPPED |
| `cluster.pseudo.clean-store-on-start` | `true` | 啟動時清理舊 store（可配置，設 false 保留歷史消息） |
| `cluster.pseudo.work-dir` | `run/pseudo-cluster` | 嵌入式 RocketMQ 存儲 |
| `cluster.pseudo.health-timeout` | `5s` | 健康輪詢超時 |
| `cluster.rocketmq.name-servers` | `${CLUSTER_ROCKETMQ_NAME_SERVERS:}` | 真實集群 NameServer（環境變量外部化） |
| `cluster.stream.publish-interval-ms` | `5000` | 遙測推送週期（遍歷所有 provider） |
| `cluster.kafka.enabled` | `true` | Kafka provider 開關（平台異常時可單獨關閉） |
| `cluster.kafka.cluster-id` | `kafka-lab` | Kafka 集群 ID |
| `cluster.kafka.work-dir` | `run/kafka-cluster` | KRaft 元數據與日誌目錄 |
| `cluster.kafka.clean-store-on-start` | `true` | 首次啟動前清空工作目錄 |
| `cluster.rabbitmq.enabled` | `true` | RabbitMQ（Qpid）provider 開關 |
| `cluster.rabbitmq.cluster-id` | `rabbitmq-lab` | RabbitMQ 集群 ID |
| `cluster.rabbitmq.work-dir` | `run/rabbitmq-cluster` | Qpid 工作目錄（每次啟動前清空） |

端口：RocketMQ NameServer / Master / Slave 固定 `9876` / `10911` / `10921`；Kafka 各節點與 Qpid AMQP 端口由 `PortPool` 動態分配（節點表中可見）。

### 前端（next/.env.local）
| 變量 | 值 | 備註 |
| --- | --- | --- |
| `BACKEND_URL` | `http://localhost:8088` | 後端地址（rewrites 代理目標）|

### 前端（next/next.config.ts）
| 配置 | 值 | 備註 |
| --- | --- | --- |
| `allowedDevOrigins` | `['127.0.0.1']` | 允許的開發預覽源 |
| `experimental.proxyTimeout` | `120_000` | Kafka 一鍵啟動可能超過默認 30 秒 |

### 運行時連接配置（UI 可修改，持久化到 `run/rocketmq-connection.properties`）
| 項目 | 默認值 | 範圍 | 備註 |
| --- | --- | --- | --- |
| `name-servers` | 繼承啟動配置 | 任意地址列表 | NameServer 地址（逗號分隔），空時 PSEUDO 用嵌入式集群 |
| `send-msg-timeout-ms` | `10000` | 1000-600000 | 發送超時（毫秒） |
| `consume-timeout-seconds` | `15` | 1-300 | 消費超時（秒） |
| `consumer-group-prefix` | `mqcluster` | 非空字符串 | 消費者組前綴 |

> UI 配置保存後立即生效，持久化到文件，重啟後保留。API：`GET/PUT /api/clusters/settings/rocketmq`

## 7. 已驗證功能

- ✅ Provider 列表加載
- ✅ 集群拓撲展示（3 節點：狀態着色灰/綠/紅/黃 + 角色定形狀圓/矩形/菱形 + 拖拽 + 圖例）
- ✅ 節點啟動/停止/重啟（嵌入式真實 RocketMQ，含 JVM `--add-opens` 修復 shutdown 異常）
- ✅ 消息 produce/consume 模擬（真實 RocketMQ 投遞，3/3 成功）
- ✅ 消息模板系統（6 預定義模板 + 占位符替換 `{index}`/`{timestamp}`/`{uuid}`/`{random}`/`{topic}`）
- ✅ 外部 MQ 通信（PSEUDO HOST 路徑 + REAL 模式真實 produce/consume）
- ✅ 網頁連接配置面板（NameServer + 超時 + 消費者組前綴，持久化到文件，立即生效）
- ✅ 監控指標（真實 JVM CPU/內存 + RocketMQ broker put/get TPS）
- ✅ 活動日誌（審計日誌 + 實時 STOMP 推送）
- ✅ 主從複製（Master + Slave 同時運行）
- ✅ 後端測試套件（193 tests pass, 18 條件跳過）
- ✅ Kafka：KRaft 1 controller + 3 broker 單節點啟停 / 重啟、leader 切換與 ISR 收縮 / 恢復、topic 創建刪除、常駐消費者 rebalance 與 lag、key / partition / acks / compression 消息選項
- ✅ RabbitMQ（Qpid）：direct / fanout / topic / headers 路由、路由預覽、交換機 / 隊列 / 綁定聲明與刪除、手動 ack / nack / requeue、prefetch、死信、TTL、publisher confirms
- ✅ 多 MQ UI：首頁、`/mq/[middleware]` 標籤頁（`?tab=` 持久化）、對比頁、分 MQ 學習指南、深淺主題與 MQ 強調色
- ✅ 前端 typecheck + ESLint + build 通過
- ✅ 手動節點登記（HOST + VIRTUAL）
- ✅ 前端重試機制（後端未就緒時自動重試 + 錯誤重試按鈕）
- ✅ 動態限流（基於本機 CPU/內存/磁盤多因素綜合公式，每台機器上限不同）
- ✅ 數據面板（ECharts 折線圖時間序列 + 柱狀圖節點對比 + 實時數值卡片）
- ✅ 緊湊投遞結果（統計摘要 + 分頁列表，避免大量消息時頁面過長）
- ✅ 壓力實驗（`?tab=stress`）：三種 MQ 用官方客戶端按目標速率真實壓測（1–50000 條/秒、1–32 線程、16B–64KB 負載），
  逐秒採樣速率/失敗率/p95/p99，失敗原因取自客戶端真實異常；結論分「已被沖垮（失敗率≥1%）/被限流（速率不達標）/未被沖垮」
- ✅ 內部機制可視化（`?tab=internals`）：每 2 秒讀運行時真實對象——RocketMQ `CommitLog` 映射文件寫指針 + `dispatchBehindBytes`、
  Kafka `describeLogDirs` 段文件 + LEO + ISR、Qpid `RabbitObjectModel` 交換機/隊列/綁定 + ready/unacked

## 8. 監控指標語義

`NodeMetrics` 字段映射（教育語義映射，非實際字節吞吐）：

| 字段 | 數據來源 | 說明 |
| --- | --- | --- |
| `cpuUsage` | JVM 進程 CPU | `OperatingSystemMXBean.getProcessCpuLoad()` × 100 |
| `memoryUsage` | JVM 堆內存 | `used / max × 100` |
| `networkInBytesPerSecond` | RocketMQ broker put TPS | 消息投遞速率（msg/s） |
| `networkOutBytesPerSecond` | RocketMQ broker get TPS | 消息消費速率（msg/s） |

- Broker 節點：CPU/內存來自 JVM，網絡值來自 RocketMQ broker stats（key: `brokerClusterName`）
- NameServer 節點：CPU/內存來自 JVM，網絡值為零
- 停止/未知節點：全部為零
- Kafka broker：進速率 = 該 broker 作為 leader 的分區 LEO 增量 ÷ 時間間隔；出速率 = 消費者組 committed offset 增量（Yammer 指標註冊表 JVM 全局共享，無法按 broker 區分）
- RabbitMQ：按隊列統計 ready / unacked 與入隊 / 出隊速率
- Kafka / Qpid 節點 CPU 與內存取進程值（`JvmProcessMetrics`）

## 9. 測試覆蓋

### 測試文件
| 文件 | 測試數 | 方法 |
| --- | --- | --- |
| `ComprehensiveSystemTest` | 57 | 6 種黑盒測試方法（等價類、邊界值、正交試驗、判定表、錯誤猜測、場景法） |
| `MessageTemplateAndConfigTest` | 56 | 6 種黑盒測試方法覆蓋消息模板和連接配置功能 |
| `ClusterServiceRegistrationTest` | 3 | 服務登記集成測試 |
| `ClusterFacadeSmokeTest` | 2 | 門面冒煙測試 |
| `FixesVerificationTest` | 3 | P0/P1/P3 修復回歸 |
| `PseudoClusterRuntimeTest` | 2 | 偽集群拓撲測試 |
| `EmbeddedRocketMqRuntimeMetricsTest` | 3 | 嵌入式指標測試（1 條件啟用） |
| `EmbeddedRocketMqSpikeTest` | 1 | 嵌入式 Spike（條件啟用） |
| `ClusterManagerApplicationTests` | 1 | 應用上下文加載 |
| `RateLimitServiceTest` | 20 | 6 種黑盒測試方法覆蓋動態限流計算 |
| `KafkaApiTest` | 9 | Kafka API 黑盒測試 |
| `KafkaClusterProviderTest` | 7 | 啟動 / 停止 / 重啟、leader 切換、lag（條件啟用 `-DrunEmbeddedKafka=true`） |
| `RabbitApiTest` | 3 | RabbitMQ API 黑盒測試 |
| `RabbitClusterProviderTest` | 11 | 四種路由、死信、TTL、prefetch |
| `EmbeddedKafkaSpikeTest` | 1 | 階段 0 Kafka Spike（條件啟用 `-DrunEmbeddedKafkaSpike=true`） |
| `EmbeddedQpidSpikeTest` | 8 | 階段 0 Qpid Spike（條件啟用 `-DrunEmbeddedQpidSpike=true`） |
| `LabApiTest` | 6 | 壓力實驗啟動 / 狀態 / 停止 + 內部機制快照 API |
| **合計** | **193** | **18 條件跳過** |

### 嵌入式 RocketMQ 測試
需要 JVM 模块參數（已在 `pom.xml` Surefire `argLine` 配置）：
```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
```

## 10. 關鍵設計決策

- **Store 清理可配置**：`cluster.pseudo.clean-store-on-start=true`（默認），學習平台場景下每次啟動為乾淨狀態；設 `false` 保留歷史消息用於調試。
- **Broker stats key**：RocketMQ `BROKER_PUT_NUMS`/`BROKER_GET_NUMS` 的 stats key 是 `brokerClusterName`（`embedded-cluster`），不是 `brokerName`。
- **TPS 滾動窗口**：RocketMQ stats 為 60 秒滾動窗口，消息發送後需在窗口期內查詢才能看到非零值。
- **手動節點登記**：HOST 節點綁定真實地址（localhost/127.0.0.1），VIRTUAL 節點要求 CIDR 範圍內的 IPv4 地址。
- **消息模板占位符**：`MessageTemplateService.render()` 在發送時替換 `{index}`/`{timestamp}`/`{uuid}`/`{random}`/`{topic}`，支持預定義模板和自定義輸入。
- **外部 MQ 通信三層後備**：PSEUDO HOST 路徑按優先級查找 NameServer 地址——登記的 HOST 節點 > 運行時 UI 配置 > 啟動環境變量。
- **運行時配置持久化**：UI 配置保存到 `run/rocketmq-connection.properties`，啟動時自動加載，立即生效無需重啟。
- **RealRocketMqAdminClient 條件啟用**：`@ConditionalOnProperty(prefix="cluster.rocketmq", name="name-servers")`，配置了 NameServer 地址時自動替代 MockRocketMqAdminClient。
- **JVM `--add-opens` 參數**：`spring-boot-maven-plugin` 配置了 `--add-opens java.base/java.nio=ALL-UNNAMED` 等 JVM 參數，確保 RocketMQ 5.3.3 在 Java 21 上 `Broker.shutdown()` 時能反射訪問 `DirectByteBuffer.attachment()` 釋放內存映射文件。Java 21 模塊系統更嚴格，需要比 Java 17 多打開 `java.lang` 和 `java.util` 模塊。
- **Broker 重啟延遲**：`EmbeddedRocketMqRuntime.restart()` 在 stop 後等待 1.5 秒再 start，`EmbeddedRocketMqNode.stop()` 在 shutdown 後等待 1 秒確保線程池終止和文件句柄釋放。不等待會導致端口衝突或 store lock 文件佔用。
- **動態限流公式**：`RateLimitService` 基於多因素綜合公式計算最大消息量：`maxMessages = floor(min(memoryFactor, cpuFactor, diskFactor, ceiling) × 0.7)`。memoryFactor=可用堆MB×50, cpuFactor=邏輯核數×200, diskFactor=可用磁盤GB×100, ceiling=5000。每台機器看到的上限不同。API：`GET /api/clusters/rate-limit`。
- **數據面板**：監控標籤頁（`components/dashboard/monitoring-panel.tsx`）使用 ECharts 展示時間序列折線圖（CPU/內存/吞吐趨勢）+ 節點對比柱狀圖 + 實時數值卡片，顏色從 CSS 變量讀取。
- **緊湊投遞結果**：消息工作台用統計摘要（總數/成功/失敗/成功率）+ 分頁列表（每頁 10 條）替代逐條展示，避免大量消息時頁面過長。
- **Kafka 重啟 = 新實例**：`KafkaRaftServer` 不可復用，broker 重啟時用同一 `node.id` 與日誌目錄新建實例；必須先啟動 controller。`log.cleaner.dedupe.buffer.size` 調小到 2 MB，1+3 集群穩態堆約 68 MB。
- **Qpid 與 RabbitMQ 差異**：`x-dead-letter-exchange` 翻譯為 `alternateBinding` + `maximumDeliveryAttempts`，`x-message-ttl` 翻譯為 `maximumMessageTtl`；TTL 過期轉死信 Qpid 不支持，UI 禁用並標注「Qpid 不支持（真實 RabbitMQ 支持）」，不做假模擬。詳見 `docs/MQ_FEATURE_MATRIX.md`。
- **不強制 `-Xmx`**：三種 MQ 全部運行新增堆約 100 MB，JVM 默認最大堆足夠。
- **Spring 上下文不拉起 broker**：所有 Kafka / Qpid 節點初始 STOPPED，測試啟動速度不受影響。
- **Broker 端口成對預留**：RocketMQ `BrokerController.initializeRemotingServer()` 會額外用 `listenPort - 2` 起一個 fast remoting（VIP）端口，而 `BrokerConfig` 沒有對應字段可配。`PortPool.allocateBrokerListenPort()` 必須把這兩個端口一起預留（並用 `new ServerSocket()` 實際試綁校驗 fast 端口空閒），`releaseBrokerListenPort()` 成對釋放；否則會撞上 NameServer 端口，報 `Failed to bind to 0.0.0.0:<port>`。
- **壓測判定三檔**：`StressRunner.verdict()` 只看客戶端真實結果——失敗率 ≥1% 判「已被沖垮」，失敗率低但實測速率 <85% 目標判「被限流（背壓把流量壓下來）」，速率達標則「未被沖垮」；實驗進行中不下結論。三檔分別對應前端紅 / 琥珀 / 綠三種結論卡。
- **實驗時長用 `endedAt` 定格**：`drain()` 排空在途消息、`close()` 關閉客戶端可能耗時數秒，若用 `now` 算 elapsed，吞吐率會被尾部的排空時間稀釋（RabbitMQ 曾因此顯示 82% 而非 98%）。`finish()` 第一步就把 `endedAt` 定格，之後的統計一律用它。
- **`finish()` 的排空必須在鎖外**：`drain()`/`close()` 可能阻塞數秒。若整段 `finish()` 持鎖，`stop()`→`finish()`、以及三個 Lab 的 `status()` 都會被卡住，表現為壓測結束後 `GET /api/clusters/.../lab/stress` 直接 hang（HTTP 000）。現在的做法是：`finish()` 只在鎖內切換狀態與定格時間，排空與關閉放到鎖外；三個 Lab 的 `status()`/`stop()` 先在 `lock` 內取出 `runner` 引用，再在鎖外調用其方法。
- **中斷不等於失敗**：`shutdownNow()` 會讓阻塞中的 `send()`/`waitForConfirmsOrDie()` 拋 `InterruptedException`，那是停止實驗造成的，不是 MQ 扛不住。`StressRunner.loop()` 的 catch 內用 `if (running)` 判斷，停止後才拋的異常不計入失敗數。
- **異步客戶端的延遲靠回調量**：Kafka / RocketMQ 的 `send()` 只入隊，方法返回時間不代表端到端耗時。兩者的 `StressSender.reportsAsyncLatency()` 返回 `true`，由 `bindLatencySink` 在回調裡上報真實延遲；RabbitMQ 走同步 confirm，在 loop 內直接計時。
- **錯誤聚合要折疊數字**：`StressRunner.describe()` 把異常首行的數字折疊成 `#` 並去掉 `cost=#`。否則 `wait response timeout, cost=3000/3001/3005` 會被拆成幾十個桶，界面上只剩「其他錯誤（超過 8 種）」。
- **RocketMQ 會壓縮消息體**：`compressMsgBodyOverhowmuch=4096`，4 KB 的 `'x'` 負載進 CommitLog 前就被壓到幾十字節，所以壓測後 CommitLog 增量遠小於「條數 × 負載」。內部機制面板的 notes 已寫明，避免誤判成統計 bug。
- **`Producer is closed forcefully` 是停止副作用**：Kafka 停止實驗時強制關閉生產者，緩衝區裡未確認的消息會拋這個異常。`KafkaStressLab.recordError()` 把它歸一為「停止實驗時仍未確認的消息（生產者緩衝區積壓）」，避免被判成「已被沖垮」。
- **「洪峰沖垮」預設要同時收緊客戶端忍耐度**：光靠提高目標速率打不垮嵌入式 MQ，會被背壓吸收成「被限流」。`FLOOD_KNOBS` 在貼近極限的速率之外，還會把客戶端緩衝收緊（Kafka `bufferMemoryMb` 8 / `maxBlockMs` 500、RocketMQ `sendTimeoutMs` 1000、RabbitMQ `confirmTimeoutMs` 300 + 64 KB 負載），讓失敗真的發生在客戶端可見的層面。Qpid 小消息極快，必須用 64 KB 負載才能把吞吐壓下來。
- **內部機制面板直接讀運行時對象**：RocketMQ 讀 `CommitLog` 映射文件寫指針與 `dispatchBehindBytes`，Kafka 走 `AdminClient.describeLogDirs()` 拿段文件大小與 LEO/ISR，Qpid 走 `RabbitObjectModel` 的 exchange/queue/binding + ready/unacked。全部每 2 秒輪詢一次真實對象，不做模擬數據。
