# 架构文档

本文档描述 MQCluster 的架构——一个本地优先的 MQ 学习平台。后端在进程内嵌入真实的 RocketMQ、Kafka（KRaft）与 AMQP 0-9-1（Qpid Broker-J）运行时，通过浏览器仪表盘操作。

---

## 目录

- [总体概览](#总体概览)
- [六边形架构（端口与适配器）](#六边形架构端口与适配器)
- [后端分层](#后端分层)
  - [核心领域层](#核心领域层)
  - [应用层](#应用层)
  - [基础设施层](#基础设施层)
  - [API 层](#api-层)
- [前端架构](#前端架构)
- [嵌入式 RocketMQ](#嵌入式-rocketmq)
- [嵌入式 Kafka](#嵌入式-kafka)
- [嵌入式 RabbitMQ（Qpid Broker-J）](#嵌入式-rabbitmqqpid-broker-j)
- [实验室：压力实验与内部机制](#实验室压力实验与内部机制)
- [实时通信](#实时通信)
- [请求与数据流](#请求与数据流)
- [设计决策](#设计决策)

---

## 总体概览

MQCluster 是一个**前后端分离**的应用：

```
┌─────────────────────────────────────────────────────────┐
│                    浏览器（用户）                         │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │            Next.js 16 前端（端口 3000）            │  │
│  │  React 19 · Tailwind v4 · ECharts 6 · STOMP       │  │
│  └───────────────────┬───────────────────────────────┘  │
│                      │ /api（REST）  /ws（STOMP）        │
│                      │ Next.js rewrites 代理              │
│  ┌───────────────────▼───────────────────────────────┐  │
│  │         Spring Boot 4.1.0 后端（端口 8088）        │  │
│  │  六边形架构 · 嵌入式 RocketMQ 5.3.3                │  │
│  │  嵌入式 Kafka 4.2 KRaft · 嵌入式 Qpid 9.2          │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

- **前端**是 Next.js 16 应用（App Router、React 19、TypeScript 严格模式），包含首页、三种 MQ 的仪表盘、对比页和学习指南。
- **后端**是 Spring Boot 4.1.0 应用，嵌入真实的 Apache RocketMQ 5.3.3、Apache Kafka 4.2（KRaft）与 Apache Qpid Broker-J 9.2 运行时，提供 REST + WebSocket 端点。三种 MQ 可以同时运行，所有节点初始状态都是 `STOPPED`。
- 通过 **REST**（`/api`）发送命令，通过 **STOMP over WebSocket**（`/ws`）接收实时事件流。

---

## 六边形架构（端口与适配器）

后端遵循**六边形架构**（也称端口与适配器）模式。核心思想：业务逻辑位于中心，对外部世界一无所知。所有外部关注点（HTTP、数据库、消息代理）都是插入核心所定义端口的适配器。

```
                         ┌─────────────────────────────┐
   REST / WebSocket  ───▶│         API 层              │
                         │  控制器 · DTO · WebSocket    │
                         └──────────────┬──────────────┘
                                        │ 调用
                         ┌──────────────▼──────────────┐
                         │        应用层               │
                         │  用例 · Provider 选择 · 编排 │
                         └──────────────┬──────────────┘
                                        │ 使用端口
                         ┌──────────────▼──────────────┐
                         │        核心领域             │
                         │  集群 · 节点 · 消息          │
                         │  端口接口（零依赖）          │
                         └──────────────▲──────────────┘
                                        │ 实现端口
                         ┌──────────────┴──────────────┐
                         │       基础设施层            │
                         │  嵌入式 RocketMQ 适配器     │
                         │  RocketMQ admin 适配器      │
                         │  嵌入式 Kafka 适配器        │
                         │  嵌入式 Qpid 适配器         │
                         └─────────────────────────────┘
```

**依赖规则**：依赖始终**向内**。`core` 不依赖 `application`、`infrastructure` 或 `api`；`infrastructure` 依赖 `core`（实现端口），但 `core` 从不依赖 `infrastructure`。

---

## 后端分层

### 核心领域层

**位置**：`java/src/main/java/com/example/clustermanager/core/`

核心包含纯领域模型和端口接口，**零外部依赖**——没有 Spring、没有 MQ 客户端、没有 HTTP 库。

关键概念：

| 概念 | 描述 |
| --- | --- |
| `Cluster` | 聚合根，表示一个含多节点的逻辑 MQ 集群。 |
| `Node` | 单个 broker / nameserver / controller 实例，有生命周期（STOPPED、STARTING、RUNNING、STOPPING）。 |
| `Message` | 生产到 topic / exchange 或从中消费的消息。 |
| 端口接口 | `application` 调用、`infrastructure` 实现的接口（如集群生命周期、消息操作）。 |
| `ProviderDescriptor` | Provider 元数据：`providerId`、`mode`、`middleware`（`ROCKETMQ` / `KAFKA` / `RABBITMQ`）、`clusterId`、`capabilities`（如 `CLUSTER_OPERATIONS`、`CONNECTION_CONFIG`、`SERVICE_REGISTRATION`、`KAFKA_INSPECTION`、`RABBIT_INSPECTION`、`LAB`）。前端据此决定显示哪些标签页和按钮。 |
| `INodeManager.operateCluster` | 集群级一键启动 / 停止 / 重启，启动顺序由各 provider 决定（如先 NameServer、先 Kafka controller）。 |
| `IKafkaInspector` / `IRabbitInspector` | MQ 专属的检查端口（topic / 分区 / 消费者组；交换机 / 队列 / 绑定 / 路由预览）。 |
| `IStressLab` / `IInternalsInspector` | 实验室端口（`LAB` 能力）：对嵌入式 broker 跑真实负载实验并报告吞吐 / 失败 / p95 / p99，以及读取 broker 自身的运行时对象（CommitLog、日志段文件、对象模型）。实现位于 `infrastructure/{pseudo,kafka,rabbitmq}/lab`，共用 `infrastructure/lab/StressRunner`。 |
| `MessageScenario.options` | `Map<String,String>`，承载各 MQ 特有的消息参数（Kafka 的 `key`/`partition`/`acks`/`compression`；RabbitMQ 的 `exchange`/`routingKey`/`headers.*`/`ackMode` 等）。可为 null，旧请求照常兼容。 |

因为核心不依赖框架，可轻松进行单元测试，并可在任何上下文中复用。

### 应用层

**位置**：`java/src/main/java/com/example/clustermanager/application/`

应用层编排用例。它本身不含业务规则——协调领域模型并选择合适的基础设施适配器。

关键组件：**`ClusterProviderRegistry`**——根据 `(ClusterMode, MiddlewareType)` 元组选择适配器。例如 `(PSEUDO, ROCKETMQ)` 选嵌入式 RocketMQ 适配器，`(REAL, ROCKETMQ)` 选外部 admin 适配器，`(PSEUDO, KAFKA)` 选嵌入式 Kafka 适配器。
`resolveCapability(selection, Class<T>)` 用于取 `IKafkaInspector` 等专属端口；所选 provider 不支持时抛 `IllegalArgumentException`，映射为 HTTP 400。

### 基础设施层

**位置**：`java/src/main/java/com/example/clustermanager/infrastructure/`

此层用具体技术实现核心端口。现有四个适配器：

| 适配器 | 包 | 描述 |
| --- | --- | --- |
| **Pseudo（嵌入式 RocketMQ）** | `infrastructure/pseudo` | 进程内运行真实 Apache RocketMQ 5.3.3 NameServer + Broker。 |
| **RocketMQ admin** | `infrastructure/rocketmq` | 通过 RocketMQ admin 客户端连接外部 RocketMQ 集群（REAL 模式）。 |
| **Kafka（嵌入式）** | `infrastructure/kafka` | `KafkaClusterProvider`：KRaft 集群（1 controller + 3 broker）、消息工作台、常驻消费者组、吞吐统计。开关：`cluster.kafka.enabled`。 |
| **RabbitMQ（嵌入式）** | `infrastructure/rabbitmq` | `RabbitClusterProvider`：Qpid Broker-J + 内存虚拟主机，用官方 RabbitMQ 客户端收发；路由检查、常驻消费者。开关：`cluster.rabbitmq.enabled`。 |

`infrastructure/common` 存放共享工具（如 `JvmProcessMetrics`）；空闲端口池 `PortPool` 由所有嵌入式运行时复用。
适配器之间不重复实现压测逻辑：`infrastructure/lab` 里是共用的 `StressRunner`（限速、采样、分位数、结论判定），各 MQ 只需提供自己的 `StressSender`。

### API 层

**位置**：`java/src/main/java/com/example/clustermanager/api/`

API 层是前端入口，包含：

- **REST 控制器**——`ClusterController`（三种 MQ 共用：拓扑、指标、日志、节点与集群操作、消息模拟、RocketMQ 运行时连接配置、动态限流）、`KafkaController`（`/api/clusters/pseudo/kafka/{clusterId}`：topic、消费者组、成员、leader 变更）、`RabbitController`（`/api/clusters/pseudo/rabbitmq/{clusterId}`：路由、路由预览、交换机、队列、绑定、消费者）、`LabController`（`/api/clusters/{mode}/{middleware}/{clusterId}/lab/...`：压力实验与内部机制快照）。
- **DTO**——请求 / 响应 record。
- **WebSocket / STOMP 配置**——设置 `/ws` 端点和实时事件 topic。
- **异常处理**——全局异常处理器：`IllegalArgumentException` → 400，`IllegalStateException` → 409。

---

## 前端架构

**位置**：`next/src/`

前端遵循单向数据流的分层结构：

```
app/（路由）  ──▶  MqDashboard + 标签页组件  ──▶  hooks  ──▶  lib/api.ts  ──▶  后端 REST / STOMP
```

| 层 | 位置 | 职责 |
| --- | --- | --- |
| **页面** | `src/app/` | `page.tsx` 是首页（MQ 入口卡片、学习路径、资源提示）；`mq/[middleware]/page.tsx` 是服务端组件，校验参数（不合法返回 `notFound()`）后渲染客户端 `MqDashboard`；`compare/` 是对比页；`guide/` 与 `guide/[middleware]/` 是分 MQ 的学习指南。 |
| **组件** | `src/components/` | `dashboard/` 是通用标签页（拓扑、节点操作、消息工作台、压力实验、内部机制、监控、日志、连接配置）与 `MqDashboard`（当前标签页存于 `?tab=`）；`lab/` 是压力实验台与内部机制面板；`kafka/`、`rabbit/` 是专属标签页；`cluster-status-provider.tsx` 为顶栏和首页轮询全局运行状态；`ui/` 是 shadcn 组件。 |
| **Hooks** | `src/hooks/` | `use-cluster-data.ts`：首次加载重试、每 5 秒轮询拓扑 / 指标、STOMP 订阅；`use-node-operations.ts`：节点 / 集群操作与 toast 提示；`use-chart-palette.ts`：从 CSS 变量读取 ECharts 颜色，主题切换时重新应用。 |
| **Lib** | `src/lib/` | `api.ts` 是统一 axios 客户端；`mq-registry.ts` 集中定义每种 MQ 的元数据（名称、图标、强调色、角色 → 形状 / 标签、按后端 `capabilities` 过滤的标签页）；`guides.ts` 是指南内容。 |
| **Types** | `src/types/` | 与后端 DTO 对应的 TypeScript 类型（`cluster.ts`、`kafka.ts`、`rabbit.ts`）。 |

### 主题

`globals.css` 按 `[data-mq="rocketmq|kafka|rabbitmq"]` 定义 OKLCH 色值的 `--mq-accent` / `--mq-accent-foreground`（浅色与深色各一套），以及状态色 token（`running`、`stopped`、`failed`、`degraded`、`starting`）。顶栏通过 `next-themes` 切换深浅主题。

### API 代理

`next.config.ts` 配置 rewrites，将 `/api/*` 和 `/ws/*` 代理到 `http://localhost:8088`（后端），避免开发中的 CORS 问题。Kafka 一键启动（controller + 3 broker）可能超过默认的 30 秒，因此 `experimental.proxyTimeout` 放宽到 120 秒。

---

## 嵌入式 RocketMQ

MQCluster 在后端 JVM 内运行**真实**的 Apache RocketMQ 5.3.3——不是 mock 或模拟器。

### 工作原理

1. Pseudo 适配器启动**进程内 NameServer**，负责服务发现。
2. 然后启动一个或多个**进程内 Broker**（Master 和 / 或 Slave），向 NameServer 注册。
3. Broker 使用本地工作目录（默认 `run/pseudo-cluster/`）存储消息（CommitLog、ConsumeQueue 等），与生产 broker 相同，只是规模更小。
4. 消息生产 / 消费走**真实 RocketMQ 客户端 API**，学习者体验真实的 topic 路由、消息存储、消费 offset 与主从复制。

### 为什么嵌入式？

| 方案 | 优点 | 缺点 |
| --- | --- | --- |
| **Mock broker** | 易实现 | 不真实；学习者无法信任其行为 |
| **Docker 集群** | 真实 | 笨重；需要 Docker；不便携 |
| **嵌入式（MQCluster）** ✅ | 真实 broker、零外部依赖、一条命令启动 | 仅限单机规模（学习场景足够） |

### 种子拓扑

| 节点 | 角色 | 端口 | 初始状态 |
| --- | --- | --- | --- |
| `rmq-ns-01` | NameServer | 9876 | STOPPED |
| `rmq-broker-m-01` | Master Broker | 10911 | STOPPED |
| `rmq-broker-s-01` | Slave Broker | 10921 | STOPPED |

节点初始为 STOPPED（`cluster.pseudo.auto-start=false`），学习者可手动启动并观察生命周期变化。

---

## 嵌入式 Kafka

`EmbeddedKafkaRuntime` 在代码中格式化 KRaft 存储（`Formatter`，静态 quorum），并在 JVM 内运行 `KafkaRaftServer`。

| 节点 | 角色（`labels.role`） | 端口 |
| --- | --- | --- |
| `kafka-ctrl-01` | `kafka-controller` | 动态分配（`PortPool`） |
| `kafka-broker-01..03` | `kafka-broker` | 动态分配（`PortPool`） |

- 必须先启动 controller；一键启动按 controller → broker 的顺序执行。
- `KafkaRaftServer` 实例不可复用，broker 重启时**新建实例并复用同一 `node.id` 与日志目录**（`run/kafka-cluster/<nodeId>`），随后重新加入 ISR。
- Kafka 的 Yammer 指标注册表在 JVM 内全局共享，无法按 broker 区分，因此各 broker 的进 / 出速率用"该 broker 作为 leader 的分区 LEO 增量 ÷ 时间间隔"计算，消费速率用消费者组 committed offset 的增量计算；CPU 与内存取进程值。
- `KafkaConsumerGroupManager` 管理后端常驻消费者，可设置每秒处理条数以制造 lag、触发 rebalance。

## 嵌入式 RabbitMQ（Qpid Broker-J）

`EmbeddedQpidRuntime` 通过 `SystemLauncher` 启动 Qpid Broker-J 9.2（内存虚拟主机，AMQP 端口动态分配），客户端使用官方 `com.rabbitmq:amqp-client`（AMQP 0-9-1）。

- 单节点 `rabbit-node-01`（`role=rabbit-broker`）；`RabbitTopologySeeder` 预置四种类型（direct / fanout / topic / headers）各一个演示交换机，以及配套的队列和绑定。
- 交换机、队列、绑定直接从 Qpid 进程内对象模型（Broker → VirtualHost → Exchange / Queue / Binding）读取。
- `x-dead-letter-exchange` 被翻译为 Qpid 的 `alternateBinding`（加 `maximumDeliveryAttempts`），`x-message-ttl` 被翻译为 `maximumMessageTtl`。Qpid 会直接丢弃 TTL 过期的消息而不是转入死信，UI 中该选项禁用并标注"Qpid 不支持（真实 RabbitMQ 支持）"。

完整的特性支持矩阵与内存实测见 [MQ_FEATURE_MATRIX.md](../MQ_FEATURE_MATRIX.md)（三种 MQ 全部运行约新增 100 MB 堆，因此不强制 `-Xmx`）。

---

## 实验室：压力实验与内部机制

`?tab=stress` 与 `?tab=internals` 回答两张图回答不了的问题：**流量超过 broker 能力时它到底怎么表现**，
以及**大量消息流过时它内部在做什么**。两者都由 `LAB` 能力背后的端口支撑，路由统一：

```
POST   /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    启动
GET    /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    读取实时状态（前端轮询）
DELETE /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    提前停止，返回最终结果
GET    /api/clusters/{mode}/{middleware}/{clusterId}/lab/internals 读一次运行时快照
```

`LabController` 通过 `ClusterFacadeService.capability(...)` 取 `IStressLab` / `IInternalsInspector`，
provider 未实现时返回 400。

### 压力实验（`IStressLab`）

引擎只有一份 `infrastructure/lab/StressRunner`，各适配器只提供 `StressSender`
（`send` / `drain` / `close` / `reportsAsyncLatency`）。一次实验：

1. 起 N 个 worker 线程，共享令牌桶按目标速率发送（`1–50000 条/秒`、`1–32` 线程、`16 B – 64 KB` 负载），
   慢 broker 会让整轮一起变慢，而不是某个线程抢跑。
2. 每秒采样一次：实际速率、失败率、p95、p99，最多保留 300 个采样点。
3. 把每个客户端异常归入一个桶（`StressRunner.describe()` 把数字折叠成 `#` 并去掉 `cost=`，
   否则 `wait response timeout, cost=3000/3001/…` 会炸成几十个桶）。
4. 停止时先定格 `endedAt`，再**在锁外**执行 `drain()` / `close()`。排空在途消息可能耗时数秒
   （RabbitMQ 排空 confirm、Kafka 缓冲区落地），持锁排空会让 `GET /stress` 一起挂住。
   因此三个 Lab 的 `status()` / `stop()` 都是锁内取 runner 引用、锁外调用。
5. 只按**客户端视角**给出三档结论：

| 结论 | 条件 | 说明 |
| --- | --- | --- |
| 已被冲垮 | 失败率 ≥ 1% | broker 跟不上，生产端看到超时 / 拒绝 |
| 被限流 | 失败率 < 1%，但实际速率 < 目标 85% | 背压把洪峰吸收了——症状是吞吐，不是错误 |
| 未被冲垮 | 达到目标速率 | 该负载下仍有余量 |

停止动作本身造成的中断（`InterruptedException`、Kafka 的 `Producer is closed forcefully`）不算失败：
`loop()` 只在 `running` 仍为真时计失败。Kafka 与 RocketMQ 是异步发送（`reportsAsyncLatency() == true`），
真实端到端延迟由发送回调上报，而不是 `send()` 的返回时间。

### 内部机制（`IInternalsInspector`）

不做 RPC 探测、不做启发式推测：每个 inspector 直接读嵌入 broker 已经持有的对象。

| MQ | 数据来源 | 呈现方式 |
| --- | --- | --- |
| RocketMQ | `CommitLog` 映射文件（写指针、`dispatchBehindBytes`）、ConsumeQueue 目录、IndexFile | 四阶段写入路径：追加 → 分发 → 建索引 → 消费，附实时字节数 |
| Kafka | `AdminClient.describeLogDirs()` + `EndOffsetSpec` | 按 broker 分组的段文件（`.log` / `.index` / `.timeindex`）、各分区 LEO、ISR |
| RabbitMQ（Qpid） | `RabbitObjectModel`（Broker → VirtualHost → Exchange / Queue / Binding） | 交换机、队列、绑定，以及 ready / unacked 计数 |

两条教学说明直接展示在数据旁边，而不是藏在代码注释里：RocketMQ 对超过 4 KB 的消息体自动压缩
（`compressMsgBodyOverhowmuch`），所以 4 KB 洪峰后 CommitLog 的增量远小于「条数 × 负载」；
Kafka 的段文件是预分配的，磁盘占用包含预留空间，而不只是活着的记录。

---

## 实时通信

MQCluster 使用 **STOMP over WebSocket**（含 SockJS 回退）向前端推送实时事件。`ClusterTelemetryPushService` 每隔 `cluster.stream.publish-interval-ms`（默认 5000 ms）遍历所有已注册的 provider（`ClusterProviderRegistry.listProviders()`）推送。

| 频道 | 方向 | 内容 |
| --- | --- | --- |
| `/topic/clusters/{clusterId}/metrics` | 服务端 → 客户端 | 周期性的各节点 CPU、内存、吞吐 |
| `/topic/clusters/{clusterId}/logs` | 服务端 → 客户端 | 最近的活动日志 |

拓扑不走推送：前端的 `use-cluster-data` hook 每 5 秒轮询拓扑和指标，订阅以上两个 topic，并在每次节点操作后立即刷新。

---

## 请求与数据流

以下是启动节点的典型流程：

```
1. 用户在 UI 点击 rmq-broker-m-01 的「启动」
2. 前端 POST /api/clusters/pseudo/rocketmq/local-lab/nodes/rmq-broker-m-01/operations  {"operationType":"START"}
3. API 控制器 → 应用用例
4. 应用通过 ClusterProviderRegistry 选适配器（PSEUDO, ROCKETMQ）
5. Pseudo 适配器启动嵌入式 broker
6. Broker 向进程内 NameServer 注册
7. 响应返回操作结果；前端弹出 toast 并刷新拓扑
8. 下一次遥测推送把指标和新日志发到 /topic/clusters/local-lab/{metrics|logs}
```

消息生产 / 消费遵循类似路径，适配器内部调用各 MQ 的真实客户端。

---

## 设计决策

### 为什么用六边形架构？

- **可测试性**——核心领域无需 Spring 或 MQ 即可单元测试。
- **可替换性**——`ClusterProviderRegistry` 让我们插入新的中间件类型或部署模式（本次新增 Kafka 与 RabbitMQ 即是如此），无需改动核心或 API。
- **清晰性**——每层单一职责，代码库易于导航和教学（这正是本项目的意义）。

### 为什么用 Next.js（而非 Vue 或纯 React）？

- **App Router** 开箱即用地提供基于文件的路由和服务端组件。
- **API rewrites** 让代理到后端变得简单，没有 CORS 烦恼。
- **React 19** + **TypeScript 严格模式** 提供强类型安全和现代 hooks 体验。
- **Tailwind CSS v4** 保持快速一致的样式。

### 为什么嵌入真实 MQ 而不是 mock？

- 学习者值得与**真实事物**交互。Mock 教的是 mock，不是 MQ。
- 嵌入避免了 Docker 或远程集群的开销，降低入门门槛（一条 Java 命令、一条 npm 命令）。
- 嵌入式 broker 使用与生产相同的存储格式和客户端协议，知识可以直接迁移。
- 当嵌入实现与真实产品有差异（如 Qpid 与 RabbitMQ），UI 明确标注，而不是伪造行为。

---

<div align="center">

**[⬆ 回到顶部](#架构文档)**

</div>
