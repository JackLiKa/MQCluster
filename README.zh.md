<div align="center">

# MQCluster

[English](README.md) · **简体中文**

### 本地优先的 MQ 学习平台 —— 在单台机器的浏览器里，同时运行真实的 RocketMQ、Kafka 与 RabbitMQ（AMQP 0-9-1）集群。

[![CI](https://github.com/JackLiKa/MQCluster/actions/workflows/ci.yml/badge.svg)](https://github.com/JackLiKa/MQCluster/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.0-6DB33F.svg)](https://spring.io/projects/spring-boot)
[![Next.js](https://img.shields.io/badge/Next.js-16-black.svg)](https://nextjs.org/)
[![React](https://img.shields.io/badge/React-19-61DAFB.svg)](https://react.dev/)
[![RocketMQ](https://img.shields.io/badge/RocketMQ-5.3.3-D77916.svg)](https://rocketmq.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-4.2%20KRaft-231F20.svg)](https://kafka.apache.org/)
[![Qpid Broker-J](https://img.shields.io/badge/AMQP%200--9--1-Qpid%20Broker--J%209.2-FF6600.svg)](https://qpid.apache.org/components/broker-j/)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6.svg)](https://www.typescriptlang.org/)

</div>

---

> 在你的笔记本里直接拉起**真实、嵌入式**的消息中间件 —— Apache RocketMQ 集群
> （NameServer + Master + Slave）、Apache Kafka KRaft 集群（1 controller + 3 broker）
> 以及 AMQP 0-9-1 broker（Apache Qpid Broker-J，由官方 RabbitMQ Java 客户端驱动）。
> 看着拓扑活起来、收发真实消息、杀掉 broker 观察故障转移。
> **不需要 Docker，不需要云，也不需要一堆机器。**

MQCluster 是给高校学生和自学者准备的本地优先学习平台：想弄懂不同消息队列如何建模存储、路由、
顺序、消费与高可用，却不愿先搭一整套基础设施。界面里出现的每一个现象都来自真实运行时，
没有一段是脚本动画。

| MQ | 嵌入式运行时 | 集群 ID | 节点 | 模式 |
| --- | --- | --- | --- | --- |
| RocketMQ | Apache RocketMQ 5.3.3（进程内 NameServer + Broker） | `local-lab`（REAL 模式下为 `rocketmq-demo`） | `rmq-ns-01`、`rmq-broker-m-01`、`rmq-broker-s-01` | PSEUDO + REAL |
| Kafka | Apache Kafka 4.2 KRaft（`KafkaRaftServer`） | `kafka-lab` | `kafka-ctrl-01`、`kafka-broker-01..03` | PSEUDO |
| RabbitMQ | Apache Qpid Broker-J 9.2（内存虚拟主机）+ `com.rabbitmq:amqp-client` | `rabbitmq-lab` | `rabbit-node-01` | PSEUDO |

三种 MQ 可以同时运行；所有节点初始都是 `STOPPED`，所以后端启动很快。
凡是 Qpid 与真实 RabbitMQ 行为不同的地方，界面都会明确标注 —— 见
**[docs/MQ_FEATURE_MATRIX.md](docs/MQ_FEATURE_MATRIX.md)**。

---

## 在这里，「学习」到底长什么样

两个标签页把理论变成看得见的东西：

**压力实验（Stress Lab）** —— 用每种 MQ 的官方客户端按目标速率猛打嵌入式 broker，
预设从「稳态」一路到「洪峰冲垮」。以下是在同一台笔记本上跑出来、从界面里抄下来的真实结果：

| MQ | 参数 | 结果 | 判定卡 |
| --- | --- | --- | --- |
| RocketMQ | 50,000 msg/s · 32 线程 · 4 KB · 15 s | 发送 718,912 条，**失败 581,349 条（44.7%）**，p99 2.2 s | 红 —— 已被冲垮 |
| Kafka | 50,000 msg/s · 32 线程 · 4 KB · 8 MB buffer · `max.block.ms` 500 | **18.5 k msg/s（目标的 37%）**，p99 307 ms | 黄 —— 被限流 |
| RabbitMQ | 50,000 msg/s · 32 线程 · 4 KB | 发送 928,421 条，0 失败，**46,414 msg/s**，p99 1.8 ms | 绿 —— 未被冲垮 |
| RabbitMQ | 50,000 msg/s · 32 线程 · **64 KB** | **4,899 msg/s（目标的 10%）** | 黄 —— 被限流 |

失败数从来不是编的：它们就是客户端真实抛出的异常，按原因折叠成桶
（`wait response timeout`、`unknown reason`、`send request failed` ……），并能回看客户端到底看到了什么。
三档判定恰好对应生产环境里能观察到的三种情况 —— 背压吸收了流量、生产者自己的缓冲区顶了回来、
或者 broker 真的被打趴下。数字来自一台 Apple 芯片笔记本的一次运行，你的结果会不一样，而这本身就是一课。

**内部机制（Internals）** —— 后端每 2 秒读取 broker 自己的运行时对象并渲染出来：

| MQ | 你会看到什么 |
| --- | --- |
| RocketMQ | `CommitLog` 映射文件：写指针、`dispatchBehindBytes`，以及它向 ConsumeQueue / IndexFile 的扇出 —— 四段写入路径，带真实字节数 |
| Kafka | `AdminClient.describeLogDirs()`：每个 broker 的段文件（`.log` / `.index` / `.timeindex`）、每个分区的 LEO、ISR 健康度 |
| RabbitMQ（Qpid） | `RabbitObjectModel`：交换机、队列、绑定，以及你在生产消息时不断跳动的 ready / unacked 计数 |

有个细节常让人意外：RocketMQ 对超过 4 KB 的消息体会自动压缩
（`compressMsgBodyOverhowmuch`），所以 4 KB 洪峰过后 CommitLog 的增长远小于「消息数 × 载荷」。
面板会把这句写在旁边，否则学习者只会以为自己看错了。

---

## 可视化仪表盘

界面为简体中文，支持深浅主题，并给每种 MQ 一套专属强调色。

| 路由 | 内容 |
| --- | --- |
| `/` | 首页 —— 三张 MQ 入口卡片（实时状态、一键启动/停止）、学习路径、JVM 堆提示 |
| `/mq/rocketmq` | 标签页：概览 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 · 连接配置 |
| `/mq/kafka` | 标签页：概览 · Topic 与分区 · 消费者组 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/mq/rabbitmq` | 标签页：概览 · 交换机与队列 · 确认与死信 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/compare` | 并排对比（存储、路由、顺序、消费、高可用、典型场景），并链到各自的实验室 |
| `/guide`、`/guide/[mq]` | 分步教程；每一步都有「去仪表盘试试」的深链（`/mq/x?tab=...`） |

当前标签页记在 `?tab=` 查询参数里，刷新不会丢。

- **概览** —— ECharts 拓扑图（形状/标签由 `lib/mq-registry.ts` 按角色决定）、节点表（启动/停止/重启，停止需确认）、一键启停整个集群，以及集群未启动时的引导卡片。
- **消息工作台** —— 真实收发，支持模板与占位符（`{index}`、`{timestamp}`、`{uuid}`、`{random}`、`{topic}`）。Kafka 额外支持 key / 分区 / acks / 压缩，并显示每条消息的分区与 offset；RabbitMQ 额外支持 exchange / routing key / headers / ack 模式 / nack 比例 / requeue / prefetch / TTL / 发布确认，并显示每条消息落到哪个队列、最终结果如何。
- **Kafka · Topic 与分区** —— Topic 列表与创建对话框、分区 × broker 矩阵（leader / follower / ISR），停掉一个 broker 会实时变化，并附 leader 变更时间线。
- **Kafka · 消费者组** —— 成员、按成员着色的分区分配、每个分区的 lag 柱状图；可增删常驻消费者、调节处理速率，观察 rebalance 与 lag。
- **RabbitMQ · 交换机与队列** —— 分层的 Producer → Exchange → Queue → Consumer 图，带绑定键；输入 routing key / headers 可高亮命中的路径（`/routing/preview`）；可声明/删除交换机、队列（DLX / TTL）与绑定。
- **RabbitMQ · 确认与死信** —— 一键实验：手动 ack、nack + requeue、nack → 死信、prefetch 上限、TTL 过期；每个实验都展示 ready → unacked → acked / 死信的流转。Qpid 不支持的特性会被禁用并标注「Qpid 不支持（真实 RabbitMQ 支持）」。
- **监控 / 日志** —— 实时 CPU、内存、吞吐图表与活动日志，通过 STOMP/WebSocket 推送。
- **压力实验** —— 用官方客户端按目标速率（1–50,000 msg/s、1–32 线程、16 B–64 KB 载荷）驱动嵌入式 broker，每秒采样吞吐 / 失败率 / p95 / p99，并把真实客户端异常按原因归并。预设从「稳态」到「洪峰冲垮」；判定为已被冲垮（失败率 ≥1%）/ 被限流（速率不达标）/ 未被冲垮三者之一。
- **内部机制** —— 每 2 秒读取运行中 broker 自己的对象：RocketMQ `CommitLog` 映射文件写指针与 `dispatchBehindBytes` 及 ConsumeQueue / IndexFile 扇出，Kafka `describeLogDirs` 段文件与 LEO / ISR，Qpid `RabbitObjectModel` 的交换机、队列、绑定与 ready / unacked。没有任何模拟数据。
- **连接配置（RocketMQ）** —— 外部 NameServer 地址、超时与消费者组前缀，重启后保留。

---

## 功能特性

- **嵌入式真实 RocketMQ** —— 在进程内运行真正的 Apache RocketMQ 5.3.3 NameServer + Broker。没有假 mock，也不需要 Docker。
- **嵌入式真实 Kafka** —— 进程内的 KRaft 集群（1 controller + 3 broker）；每个节点都能停止与重启（沿用同一个 `node.id` 与日志目录），用来观察 leader 选举与 ISR 收缩/恢复。
- **嵌入式 AMQP 0-9-1 broker** —— Qpid Broker-J，支持 direct / fanout / topic / headers 四类交换机、prefetch、手动 ack / nack / requeue、发布确认、死信与 TTL，由官方 RabbitMQ 客户端驱动。
- **拓扑可视化** —— 每种 MQ 的角色与关系都用可交互的 ECharts 图呈现。
- **消息模拟** —— 通过嵌入式 broker 收发真实消息，实时看到投递成功。
- **消息模板** —— 6 个内置模板（JSON 订单、纯文本、RocketMQ 事件、键值、用户行为）与占位符替换（`{index}`、`{timestamp}`、`{uuid}`、`{random}`、`{topic}`），也支持自定义模板。
- **外部 MQ 通信** —— 通过网页配置面板连接你本地的 RocketMQ 集群。支持 PSEUDO 模式（HOST 节点桥接）与 REAL 模式（真实 admin 客户端收发）。
- **网页连接配置** —— 直接在浏览器里配置 NameServer 地址、超时与消费者组前缀，保存即生效并跨重启保留。
- **监控指标** —— 每个节点的实时 CPU、内存与网络 I/O 图表，通过 STOMP/WebSocket 流式推送。
- **数据面板** —— ECharts 时间序列折线图（CPU/内存/TPS 趋势）、节点对比柱状图，以及带量化坐标轴的实时指标卡。
- **动态限流** —— 依据本机 CPU 核数、可用 JVM 堆与磁盘空间自动算出安全的最大批量。每台机器看到的上限都不一样。
- **紧凑投递结果** —— 统计摘要（总数/成功/失败/成功率）加分页列表，大批量发送时页面不会失控变长。
- **节点生命周期管理** —— 单个节点的启动、停止与重启，观察集群状态迁移。
- **主从复制** —— 同时运行 Master 与 Slave，亲眼看到复制发生。
- **活动日志与审计** —— 每一次操作都被记录，并通过 STOMP 实时推送到界面。
- **学习指南** —— 内置分步引导，边操作边解释集群概念。
- **零外部依赖** —— 后端一条命令，前端一条命令，打开浏览器就行。

---

## 快速开始

> MQCluster 可在 **Windows**、**Linux**、**macOS** 上运行。代码本身完全跨平台，
> 只有终端命令略有差别。

### 环境要求

| 工具 | 版本 | 说明 |
| --- | --- | --- |
| Java (JDK) | 21+ | Spring Boot 后端必需 |
| Maven | 3.9+ | 或用随仓库提供的 `mvnw` wrapper |
| Node.js | 20+ | Next.js 前端必需 |
| npm | 10+ | 随 Node.js 提供 |

<details>
<summary><b>Java 21 安装指引（点击展开）</b></summary>

| 系统 | 命令 |
| --- | --- |
| Windows | `winget install Microsoft.OpenJDK.21`，或从 [Microsoft OpenJDK](https://learn.microsoft.com/java/openjdk/) 下载 |
| Linux (Ubuntu/Debian) | `sudo apt install openjdk-21-jdk` |
| Linux (Fedora/RHEL) | `sudo dnf install java-21-openjdk-devel` |
| macOS (Homebrew) | `brew install openjdk@21` |
| macOS (SDKMAN) | `sdk install java 21-tem` |

验证：`java -version` 应显示 `21.x.x`。
</details>

### 1. 启动后端

**Linux / macOS：**
```bash
cd java
./mvnw spring-boot:run
```

**Windows (PowerShell)：**
```powershell
cd java
.\mvnw.cmd spring-boot:run
```

> Windows 用户也可以在仓库根目录运行 `.\start-backend.ps1`：它会自动找到 Java 21、
> 释放 8088 端口、清理嵌入式存储目录，然后启动后端。

后端运行在 **http://localhost:8088**。

> **排查**：如果看到 `UnsupportedClassVersionError: class file version 65.0`，
> 说明 `JAVA_HOME` 指向了 Java 17，改成 Java 21：
> - **Windows**：`$env:JAVA_HOME = "C:\Path\To\jdk-21"`
> - **Linux**：`export JAVA_HOME=/usr/lib/jvm/java-21-openjdk`
> - **macOS**：`export JAVA_HOME=$(/usr/libexec/java_home -v 21)`

### 2. 启动前端

```bash
cd next
npm install
npm run dev
```

前端运行在 **http://localhost:3000**，并把 `/api` 与 `/ws` 代理到后端。

### 3. 打开浏览器

访问 **http://localhost:3000** —— 首页有三张卡片（RocketMQ、Kafka、RabbitMQ），初始都是停止状态。
点卡片上的**一键启动**（或**进入实验室** → **一键启动集群**）把集群拉起来。
三个集群同时运行大约多占 100 MB 堆，JVM 默认最大堆足够。

<details>
<summary><b>启动遇到问题？（点击展开）</b></summary>

| 现象 | 解决方式 |
| --- | --- |
| 8088 端口被占用 | **Windows**：`Get-NetTCPConnection -LocalPort 8088 \| ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }`<br>**Linux/macOS**：`lsof -ti:8088 \| xargs kill -9` |
| 3000 端口被占用 | **Windows**：`Get-NetTCPConnection -LocalPort 3000 \| ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }`<br>**Linux/macOS**：`lsof -ti:3000 \| xargs kill -9` |
| Broker 起不来（store lock） | 删除 `java/run/` 下的 `pseudo-cluster`、`kafka-cluster`、`rabbitmq-cluster` 后重启 |
| 平台上 Kafka 或 RabbitMQ 运行失败 | 单独关掉它：`cluster.kafka.enabled=false` / `cluster.rabbitmq.enabled=false`，不影响 RocketMQ |
| `./mvnw: Permission denied`（Linux/macOS） | `chmod +x ./mvnw` |
| 前端白屏 | 先确认后端已在 8088 运行 |

完整的启动与清理步骤见 [AGENTS.md](AGENTS.md#4-構建運行測試)。
</details>

---

## 项目结构

```
MQCluster/
├── java/                      # Spring Boot 4.1.0 后端（端口 8088）
│   ├── src/main/java/com/example/clustermanager/
│   │   ├── api/               # REST 控制器、DTO、WebSocket 配置、异常处理
│   │   ├── application/       # 用例编排与 provider 选择
│   │   ├── core/              # 领域模型与端口（零外部依赖）
│   │   └── infrastructure/    # 适配器：pseudo（嵌入式 RocketMQ）、rocketmq（admin）、
│   │                          #         kafka（嵌入式 KRaft）、rabbitmq（嵌入式 Qpid）、
│   │                          #         lab（共享压测引擎 + 每种 MQ 的实验室适配器）
│   ├── src/main/resources/
│   │   └── application.properties
│   ├── src/test/              # JUnit 5 + AssertJ 测试（193 个执行，18 个 opt-in 跳过）
│   └── pom.xml                # Maven 构建
├── next/                      # Next.js 16 前端（端口 3000）
│   ├── src/
│   │   ├── app/               # App Router：首页、mq/[middleware]、compare、guide/[middleware]
│   │   ├── components/        # dashboard/（共享标签页）、kafka/、rabbit/、ui/（shadcn）
│   │   ├── hooks/             # use-cluster-data（轮询 + STOMP）、use-node-operations 等
│   │   ├── lib/               # API 客户端、mq-registry、guides、工具函数
│   │   └── types/             # TypeScript 类型定义
│   ├── next.config.ts         # API 代理配置（/api → localhost:8088）
│   └── .env.local             # 环境变量
├── docs/                      # 文档
│   ├── ARCHITECTURE.md        # 架构详解（英文）
│   ├── zh/                    # 中文版（ARCHITECTURE.zh.md、CONTRIBUTING.zh.md）
│   └── MQ_FEATURE_MATRIX.md   # Kafka / Qpid 特性支持矩阵与内存实测
├── scripts/                   # 维护脚本（doc-lint.py：相对链接检查）
├── LICENSE                    # Apache 2.0
├── CONTRIBUTING.md            # 贡献指南
├── SECURITY.md                # 安全政策
├── README.md                  # 英文 README
└── README.zh.md               # 本文件
```

---

## 配置

### 后端（`java/src/main/resources/application.properties`）

| 属性 | 默认值 | 说明 |
| --- | --- | --- |
| `server.port` | `8088` | 后端 HTTP 端口 |
| `cluster.cors.allowed-origins` | `http://localhost:3000` | 允许的前端源（CORS） |
| `cluster.pseudo.auto-start` | `false` | 种子节点以 STOPPED 状态启动 |
| `cluster.pseudo.work-dir` | `run/pseudo-cluster` | 嵌入式 RocketMQ 存储目录 |
| `cluster.kafka.enabled` | `true` | 启用嵌入式 Kafka provider |
| `cluster.kafka.cluster-id` | `kafka-lab` | Kafka 集群 ID |
| `cluster.kafka.work-dir` | `run/kafka-cluster` | KRaft 元数据与日志目录 |
| `cluster.kafka.clean-store-on-start` | `true` | 首次启动前清空 Kafka 工作目录 |
| `cluster.rabbitmq.enabled` | `true` | 启用嵌入式 RabbitMQ（Qpid）provider |
| `cluster.rabbitmq.cluster-id` | `rabbitmq-lab` | RabbitMQ 集群 ID |
| `cluster.rabbitmq.work-dir` | `run/rabbitmq-cluster` | Qpid 工作目录（每次启动前清空） |

### 端口

| 组件 | 端口 |
| --- | --- |
| 后端 HTTP / STOMP（`/ws`） | `8088` |
| 前端开发服务器 | `3000` |
| RocketMQ NameServer / Master / Slave | `9876` / `10911` / `10921` |
| Kafka controller + broker、Qpid AMQP | 启动时从空闲端口池动态分配（节点表里可见） |

### 集群 API

所有 MQ 共用 `/api/clusters/{mode}/{middleware}/{clusterId}/...`（拓扑、指标、日志、消息、
节点操作、`POST .../operations` 一键启停）。`GET /api/clusters/providers` 返回每个 provider 的
`clusterId` 与 `capabilities`，前端据此决定显示哪些标签页。

| 前缀 | 额外端点 |
| --- | --- |
| `/api/clusters` | `GET /providers`、`GET /message-templates`、`GET/PUT /settings/rocketmq`、`GET /rate-limit`，以及下面共享的 `/{mode}/{middleware}/{clusterId}/...` 端点 |
| `/api/clusters/pseudo/rocketmq/local-lab` | `GET/POST/DELETE /lab/stress`、`GET /lab/internals` |
| `/api/clusters/pseudo/kafka/kafka-lab` | `GET/POST /topics`、`GET/DELETE /topics/{topic}`、`GET /consumer-groups`、`GET /consumer-groups/{group}`、`POST /consumer-groups/{group}/members`、`PATCH/DELETE /consumer-groups/{group}/members/{memberId}`（调速率 / 移除）、`GET /leader-changes`、`GET/POST/DELETE /lab/stress`、`GET /lab/internals` |
| `/api/clusters/pseudo/rabbitmq/rabbitmq-lab` | `GET /routing`、`POST /routing/preview`、`POST /exchanges`、`DELETE /exchanges/{name}`、`POST /queues`、`DELETE /queues/{name}`、`POST /queues/{name}/purge`、`POST/DELETE /bindings`、`POST /consumers`、`DELETE /consumers/{consumerId}`、`GET/POST/DELETE /lab/stress`、`GET /lab/internals` |

### 前端（`next/.env.local`）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BACKEND_URL` | `http://localhost:8088` | 后端地址（rewrites 代理目标） |

---

## 架构

后端采用**六边形架构（端口与适配器）**，前后端彻底分离。

```
                    ┌──────────────────────────────────────────┐
                    │              Frontend (Next.js)           │
                    │   ECharts topology · Message workbench    │
                    │   Metrics · STOMP real-time streams       │
                    └───────────────┬──────────────────────────┘
                          REST /api │ /ws STOMP
                    ┌───────────────▼──────────────────────────┐
                    │               API Layer                   │
                    │   REST controllers · WebSocket · DTOs     │
                    ├──────────────────────────────────────────┤
                    │            Application Layer              │
                    │   Use-case orchestration · Provider       │
                    │   selection by (mode, middleware)         │
                    ├──────────────────────────────────────────┤
                    │              Core Domain                  │
                    │   Cluster · Node · Message models         │
                    │   Port interfaces (zero deps)             │
                    ├──────────────────────────────────────────┤
                    │          Infrastructure Layer             │
                    │   Embedded RocketMQ adapter (pseudo)      │
                    │   RocketMQ admin adapter (real)           │
                    │   Embedded Kafka KRaft adapter            │
                    │   Embedded Qpid (AMQP 0-9-1) adapter      │
                    └──────────────────────────────────────────┘
```

- **Core** —— 纯领域模型与端口接口，零外部依赖。
- **Application** —— 编排用例，通过 `ClusterProviderRegistry` 选择正确的适配器。
- **Infrastructure / pseudo** —— 运行真实的嵌入式 RocketMQ 运行时（进程内 NameServer + Broker）。
- **Infrastructure / rocketmq** —— 通过 admin API 连接外部 RocketMQ 集群。
- **Infrastructure / kafka** —— 嵌入式 KRaft 集群、消费者组管理器与 `IKafkaInspector`（topic、分区、lag）。
- **Infrastructure / rabbitmq** —— 嵌入式 Qpid broker、常驻消费者与 `IRabbitInspector`（交换机、队列、绑定、路由预览）。

完整深入说明见 **[docs/zh/ARCHITECTURE.zh.md](docs/zh/ARCHITECTURE.zh.md)**。

---

## 测试

### 后端

```bash
cd java
./mvnw clean verify            # 完整构建 + 跑全部测试
```

测试套件使用 JUnit 5 + AssertJ + Spring Boot Test，覆盖领域逻辑、应用编排与适配器行为 ——
包括 `KafkaClusterProviderTest`（启停/重启、leader 故障转移、lag）、`RabbitClusterProviderTest`
（四类交换机、死信、TTL、prefetch）、`LabApiTest`（HTTP 上的压力实验启动/状态/停止/内部机制）
以及每种 MQ 的黑盒 API 测试。当前 **193 个测试全部通过**；18 个较重的嵌入式运行时测试为 opt-in
（`-DrunEmbeddedKafka=true`、`-DrunEmbeddedKafkaSpike=true`、`-DrunEmbeddedQpidSpike=true`、
`-DrunEmbeddedRocketMqSpike=true`、`-DrunEmbeddedRocketMqMetrics=true`）。

### 前端

```bash
cd next
npx tsc --noEmit               # 类型检查
npm run lint                   # ESLint
npm run build                  # 生产构建
```

---

## 参与贡献

欢迎贡献！请先读 **[贡献指南](docs/zh/CONTRIBUTING.zh.md)**（[English](CONTRIBUTING.md)）。

如果发现安全问题，请按 **[安全政策](SECURITY.md)** 处理 —— **不要**开公开 issue。

---

## 许可证

本项目基于 **Apache License 2.0** —— 完整文本见 [LICENSE](LICENSE)。

---

## 致谢

- [Apache RocketMQ](https://rocketmq.apache.org/) —— 支撑嵌入式运行时的分布式消息与流平台。
- [Apache Kafka](https://kafka.apache.org/) 与 [Apache Qpid Broker-J](https://qpid.apache.org/components/broker-j/) —— 嵌入式 Kafka 与 AMQP 运行时；[RabbitMQ Java 客户端](https://www.rabbitmq.com/client-libraries/java-api-guide) —— AMQP 0-9-1 客户端。
- [Spring Boot](https://spring.io/projects/spring-boot) —— 后端应用框架。
- [Next.js](https://nextjs.org/) 与 [React](https://react.dev/) —— 前端框架与 UI 库。
- [ECharts](https://echarts.apache.org/) —— 拓扑图与指标图表库。
- 以及所有让分布式系统学习变得触手可及的开源社区成员。

---

## 文档索引

- **[架构详解（英文）](docs/ARCHITECTURE.md)** / **[架构详解（中文）](docs/zh/ARCHITECTURE.zh.md)** —— 六边形架构、前后端分离、三种嵌入式 MQ、压力实验与内部机制
- **[MQ 特性支持矩阵](docs/MQ_FEATURE_MATRIX.md)** —— Kafka / Qpid 实测结论与内存占用
- **[贡献指南](CONTRIBUTING.md)** / **[贡献指南（中文）](docs/zh/CONTRIBUTING.zh.md)** —— 如何参与开发、提交 PR
- **[仓库导航与约定](AGENTS.md)** —— 目录结构、启动排查、配置项、关键设计决策（给人和 AI 看都行）
- **[后端模块导航](java/AGENTS.md)** —— 分层、端口与适配器、测试与 JVM 参数

界面上还有 `/guide`、`/guide/rocketmq`、`/guide/kafka`、`/guide/rabbitmq` 四套分步学习指南，每一步都带「去仪表盘试试」的深链。

---

<div align="center">

**[⬆ 回到顶部](#mqcluster)**

Made with care for learners of distributed message systems.

</div>
