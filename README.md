<div align="center">

# MQCluster

### Local-first MQ learning platform — run real RocketMQ, Kafka and RabbitMQ (AMQP 0-9-1) clusters side by side, in your browser, on a single machine.

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

> Spin up **real, embedded** message brokers right inside your laptop — an Apache RocketMQ cluster
> (NameServer + Master + Slave), an Apache Kafka KRaft cluster (1 controller + 3 brokers) and an
> AMQP 0-9-1 broker (Apache Qpid Broker-J, driven by the official RabbitMQ Java client).
> Watch topologies come alive, produce and consume messages, kill brokers and observe failover.
> **No Docker, no cloud, no cluster of machines required.**

MQCluster is a local-first learning platform designed for university students and self-learners who want to
understand how different message queues model storage, routing, ordering, consumption and high availability —
without provisioning infrastructure. Every phenomenon in the UI comes from a genuine runtime, never from a
scripted animation.

| MQ | Embedded runtime | Cluster ID | Nodes | Modes |
| --- | --- | --- | --- | --- |
| RocketMQ | Apache RocketMQ 5.3.3 (in-process NameServer + Broker) | `local-lab` (`rocketmq-demo` in REAL mode) | `rmq-ns-01`, `rmq-broker-m-01`, `rmq-broker-s-01` | PSEUDO + REAL |
| Kafka | Apache Kafka 4.2 KRaft (`KafkaRaftServer`) | `kafka-lab` | `kafka-ctrl-01`, `kafka-broker-01..03` | PSEUDO |
| RabbitMQ | Apache Qpid Broker-J 9.2 (memory virtual host) + `com.rabbitmq:amqp-client` | `rabbitmq-lab` | `rabbit-node-01` | PSEUDO |

The three MQs can run at the same time; all nodes start in `STOPPED` state, so the backend boots fast.
Where Qpid behaves differently from a real RabbitMQ, the UI says so explicitly — see
**[docs/MQ_FEATURE_MATRIX.md](docs/MQ_FEATURE_MATRIX.md)**.

---

## What "learning" actually looks like here

Two tabs turn theory into something you can watch happen:

**压力实验 (Stress Lab)** — the official client of each MQ hammers the embedded broker at a target rate.
Presets run from 稳态 (steady) to 洪峰冲垮 (flash flood). One run on a single laptop, captured from the UI:

| MQ | Settings | Result | Verdict card |
| --- | --- | --- | --- |
| RocketMQ | 50,000 msg/s · 32 threads · 4 KB · 15 s | 718,912 sent, **581,349 failed (44.7%)**, p99 2.2 s | red — 已被冲垮 |
| Kafka | 50,000 msg/s · 32 threads · 4 KB · 8 MB buffer · `max.block.ms` 500 | **18.5 k msg/s (37% of target)**, p99 307 ms | amber — 被限流 |
| RabbitMQ | 50,000 msg/s · 32 threads · 4 KB | 928,421 sent, 0 failed, **46,414 msg/s**, p99 1.8 ms | green — 未被冲垮 |
| RabbitMQ | 50,000 msg/s · 32 threads · **64 KB** | **4,899 msg/s (10% of target)** | amber — 被限流 |

Failures are never invented: they are the real client exceptions, folded into buckets
(`wait response timeout`, `unknown reason`, `send request failed`, …) with a link back to what the client saw.
The three verdicts are exactly the three things you can observe in production — back-pressure absorbing load,
the producer's own buffers rejecting it, or the broker genuinely falling over. Numbers come from one run on
Apple-silicon laptop; yours will differ, and that is part of the lesson.

**内部机制 (Internals)** — every 2 s the backend reads the broker's own runtime objects and renders them:

| MQ | What you watch |
| --- | --- |
| RocketMQ | `CommitLog` mapped file: write pointer, dispatch-behind bytes, and its fan-out into ConsumeQueue / IndexFile — the four-stage write path, with the real byte counts |
| Kafka | `AdminClient.describeLogDirs()`: per-broker segment files (`.log` / `.index` / `.timeindex`), LEO per partition, ISR health |
| RabbitMQ (Qpid) | `RabbitObjectModel`: exchanges, queues, bindings, and ready / unacked counters moving while you produce |

One detail that usually surprises people: RocketMQ compresses any body above 4 KB
(`compressMsgBodyOverhowmuch`), so after a 4 KB flood the CommitLog grows far less than "messages × payload".
The panel says so, because a learner would otherwise assume the numbers are wrong.

---

## Visual Dashboard

The UI is in Simplified Chinese, supports light/dark themes, and gives each MQ its own accent color.

| Route | Content |
| --- | --- |
| `/` | Home — three MQ entry cards with live status, one-click start/stop, learning path, JVM heap hint |
| `/mq/rocketmq` | Tabs: 概览 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 · 连接配置 |
| `/mq/kafka` | Tabs: 概览 · Topic 与分区 · 消费者组 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/mq/rabbitmq` | Tabs: 概览 · 交换机与队列 · 确认与死信 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/compare` | Side-by-side comparison (storage, routing, ordering, consumption, HA, typical scenarios) with links to labs |
| `/guide`, `/guide/[mq]` | Step-by-step tutorials; every step has a "try it in the dashboard" deep link (`/mq/x?tab=...`) |

The active tab is kept in the `?tab=` query parameter so a refresh does not lose it.

- **Overview** — ECharts topology (shape/label per role from `lib/mq-registry.ts`), node table with start/stop/restart (stop asks for confirmation), one-click cluster start/stop, and an onboarding card while the cluster is down.
- **Message Workbench** — real produce/consume with templates and placeholders (`{index}`, `{timestamp}`, `{uuid}`, `{random}`, `{topic}`). Kafka adds key / partition / acks / compression and shows partition + offset per message; RabbitMQ adds exchange / routing key / headers / ack mode / nack ratio / requeue / prefetch / TTL / publisher confirms and shows the queue and final outcome of each message.
- **Kafka · Topics & Partitions** — topic list and creation dialog, partition × broker matrix (leader / follower / ISR) that changes live when a broker stops, and a timeline of leader changes.
- **Kafka · Consumer Groups** — members, partition assignment colored by member, per-partition lag bars; add/remove resident consumers and tune their processing rate to watch rebalances and lag.
- **RabbitMQ · Exchanges & Queues** — layered Producer → Exchange → Queue → Consumer graph with binding keys; type a routing key / headers to highlight the matched path (`/routing/preview`); declare/delete exchanges, queues (DLX / TTL) and bindings.
- **RabbitMQ · Ack & Dead Letters** — one-click experiments: manual ack, nack + requeue, nack → dead letter, prefetch limit, TTL expiry; each shows the ready → unacked → acked / dead-lettered flow. Unsupported Qpid features are disabled with "Qpid 不支持（真实 RabbitMQ 支持）".
- **Monitoring / Logs** — live CPU, memory, throughput charts and the activity log, pushed over STOMP/WebSocket.
- **Stress Lab** — drive the embedded broker with the official client at a target rate (1–50,000 msg/s, 1–32 threads, 16 B–64 KB payloads), sample throughput / failure rate / p95 / p99 every second, and see the real client-side exceptions grouped by cause. Presets go from 稳态 to 洪峰冲垮; the verdict is one of 已被冲垮（失败率 ≥1%）/ 被限流（速率不达标）/ 未被冲垮.
- **Internals** — every 2 s read the running broker's own objects: RocketMQ `CommitLog` mapped-file write pointer and `dispatchBehindBytes` with the ConsumeQueue / IndexFile fan-out, Kafka `describeLogDirs` segment files with LEO / ISR, Qpid `RabbitObjectModel` exchanges, queues, bindings with ready / unacked. No simulated data.
- **Connection Config (RocketMQ)** — external NameServer addresses, timeouts and consumer group prefix, persisted across restarts.

---

## Features

- **Embedded real RocketMQ** — runs an actual Apache RocketMQ 5.3.3 NameServer + Broker in-process. No fake mocks, no Docker.
- **Embedded real Kafka** — a KRaft cluster (1 controller + 3 brokers) in-process; every node can be stopped and restarted (same `node.id` and log dir) to observe leader election and ISR shrink/expand.
- **Embedded AMQP 0-9-1 broker** — Qpid Broker-J with direct / fanout / topic / headers exchanges, prefetch, manual ack / nack / requeue, publisher confirms, dead-lettering and TTL, driven by the official RabbitMQ client.
- **Topology visualization** — interactive ECharts graphs for each MQ's roles and relationships.
- **Message simulation** — produce and consume real messages through the embedded broker; watch delivery succeed in real time.
- **Message templates** — 6 built-in templates (JSON order, plain text, RocketMQ event, key-value, user behavior) with placeholder substitution (`{index}`, `{timestamp}`, `{uuid}`, `{random}`, `{topic}`). Custom templates supported.
- **External MQ communication** — connect to your local RocketMQ cluster via the web UI configuration panel. Supports PSEUDO mode (HOST node bridge) and REAL mode (real admin client with produce/consume).
- **Web-based connection config** — configure NameServer addresses, timeouts, and consumer group prefix directly in the browser. Changes take effect immediately and persist across restarts.
- **Monitoring metrics** — live CPU, memory, and network I/O charts for each node, streamed over STOMP/WebSocket.
- **Data dashboard** — ECharts time-series line charts (CPU/memory/TPS trends), node comparison bar charts, and real-time metric cards with quantified axes.
- **Dynamic rate limiting** — automatically calculates a safe maximum message batch size based on your machine's CPU cores, available JVM heap, and disk space. Each user sees a different limit tailored to their hardware.
- **Compact delivery results** — statistics summary (total/success/fail/success rate) with paginated list, preventing page overflow when sending large batches.
- **Node lifecycle management** — start, stop, and restart individual nodes; observe cluster state transitions.
- **Master–Slave replication** — run a Master and Slave simultaneously and see replication in action.
- **Activity log & audit trail** — every operation is recorded and pushed to the UI in real time via STOMP.
- **Learning guide** — built-in guided walkthroughs that explain cluster concepts as you interact.
- **Zero external dependencies** — one command for the backend, one for the frontend, open your browser. That's it.

---

## Quick Start

> MQCluster runs on **Windows**, **Linux**, and **macOS**. The codebase is fully cross-platform —
> only the terminal commands differ slightly.

### Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| Java (JDK) | 21+ | Required for the Spring Boot backend |
| Maven | 3.9+ | Or use the bundled `mvnw` wrapper |
| Node.js | 20+ | Required for the Next.js frontend |
| npm | 10+ | Ships with Node.js |

<details>
<summary><b>Java 21 installation guide (click to expand)</b></summary>

| OS | Command |
| --- | --- |
| Windows | `winget install Microsoft.OpenJDK.21` or download from [Microsoft OpenJDK](https://learn.microsoft.com/java/openjdk/) |
| Linux (Ubuntu/Debian) | `sudo apt install openjdk-21-jdk` |
| Linux (Fedora/RHEL) | `sudo dnf install java-21-openjdk-devel` |
| macOS (Homebrew) | `brew install openjdk@21` |
| macOS (SDKMAN) | `sdk install java 21-tem` |

Verify: `java -version` should show `21.x.x`.
</details>

### 1. Start the backend

**Linux / macOS:**
```bash
cd java
./mvnw spring-boot:run
```

**Windows (PowerShell):**
```powershell
cd java
.\mvnw.cmd spring-boot:run
```

> Windows users can also run `.\start-backend.ps1` from the repository root: it locates a Java 21 install,
> frees port 8088, clears the embedded stores and starts the backend.

The backend starts on **http://localhost:8088**.

> **Troubleshooting**: If you see `UnsupportedClassVersionError: class file version 65.0`,
> your `JAVA_HOME` points to Java 17. Set it to Java 21:
> - **Windows**: `$env:JAVA_HOME = "C:\Path\To\jdk-21"`
> - **Linux**: `export JAVA_HOME=/usr/lib/jvm/java-21-openjdk`
> - **macOS**: `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`

### 2. Start the frontend

```bash
cd next
npm install
npm run dev
```

The frontend starts on **http://localhost:3000** and proxies `/api` and `/ws` requests to the backend.

### 3. Open your browser

Navigate to **http://localhost:3000** — the home page shows RocketMQ, Kafka and RabbitMQ cards, all
initially stopped. Click **一键启动** on a card (or **进入实验室** → **一键启动集群**) to bring a cluster to life.
Running all three at once adds roughly 100 MB of heap; the JVM default max heap is enough.

<details>
<summary><b>Having startup issues? (click to expand)</b></summary>

| Symptom | Solution |
| --- | --- |
| Port 8088 already in use | **Windows**: `Get-NetTCPConnection -LocalPort 8088 \| ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }`<br>**Linux/macOS**: `lsof -ti:8088 \| xargs kill -9` |
| Port 3000 already in use | **Windows**: `Get-NetTCPConnection -LocalPort 3000 \| ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }`<br>**Linux/macOS**: `lsof -ti:3000 \| xargs kill -9` |
| Broker won't start (store lock) | Delete `java/run/` directory (`pseudo-cluster`, `kafka-cluster`, `rabbitmq-cluster`) and restart |
| Kafka or RabbitMQ runtime fails on your platform | Disable it alone with `cluster.kafka.enabled=false` / `cluster.rabbitmq.enabled=false`; RocketMQ is unaffected |
| `./mvnw: Permission denied` (Linux/macOS) | `chmod +x ./mvnw` |
| Frontend blank page | Ensure backend is running on 8088 first |

For full startup guide with cleanup steps, see [AGENTS.md](AGENTS.md#4-構建運行測試).
</details>

---

## Project Structure

```
MQCluster/
├── java/                      # Spring Boot 4.1.0 backend (port 8088)
│   ├── src/main/java/com/example/clustermanager/
│   │   ├── api/               # REST controllers, DTOs, WebSocket config, exception handling
│   │   ├── application/       # Use-case orchestration & provider selection
│   │   ├── core/              # Domain model & ports (zero external dependencies)
│   │   └── infrastructure/    # Adapters: pseudo (embedded RocketMQ), rocketmq (admin),
│   │                          #           kafka (embedded KRaft), rabbitmq (embedded Qpid),
│   │                          #           lab (shared stress engine + per-MQ lab adapters)
│   ├── src/main/resources/
│   │   └── application.properties
│   ├── src/test/              # JUnit 5 + AssertJ tests (193 run, 18 opt-in skips)
│   └── pom.xml                # Maven build
├── next/                      # Next.js 16 frontend (port 3000)
│   ├── src/
│   │   ├── app/               # App Router: home, mq/[middleware], compare, guide/[middleware]
│   │   ├── components/        # dashboard/ (shared tabs), kafka/, rabbit/, ui/ (shadcn)
│   │   ├── hooks/             # use-cluster-data (polling + STOMP), use-node-operations, ...
│   │   ├── lib/               # API client, mq-registry, guides, utilities
│   │   └── types/             # TypeScript type definitions
│   ├── next.config.ts         # API proxy config (/api → localhost:8088)
│   └── .env.local             # Environment variables
├── docs/                      # Documentation
│   ├── ARCHITECTURE.md        # Architecture deep-dive (English)
│   ├── zh/                    # Chinese translations (ARCHITECTURE.zh.md, CONTRIBUTING.zh.md)
│   └── MQ_FEATURE_MATRIX.md   # Kafka / Qpid feature support matrix & memory measurements
├── scripts/                   # Maintenance tooling (doc-lint.py: relative link check)
├── LICENSE                    # Apache 2.0
├── CONTRIBUTING.md            # Contribution guidelines
├── SECURITY.md                # Security policy
└── README.md                  # This file
```

---

## Configuration

### Backend (`java/src/main/resources/application.properties`)

| Property | Default | Description |
| --- | --- | --- |
| `server.port` | `8088` | Backend HTTP port |
| `cluster.cors.allowed-origins` | `http://localhost:3000` | Allowed frontend origin (CORS) |
| `cluster.pseudo.auto-start` | `false` | Seed nodes start in STOPPED state |
| `cluster.pseudo.work-dir` | `run/pseudo-cluster` | Embedded RocketMQ storage directory |
| `cluster.kafka.enabled` | `true` | Enable the embedded Kafka provider |
| `cluster.kafka.cluster-id` | `kafka-lab` | Kafka cluster ID |
| `cluster.kafka.work-dir` | `run/kafka-cluster` | KRaft metadata & log directories |
| `cluster.kafka.clean-store-on-start` | `true` | Wipe the Kafka work dir before the first start |
| `cluster.rabbitmq.enabled` | `true` | Enable the embedded RabbitMQ (Qpid) provider |
| `cluster.rabbitmq.cluster-id` | `rabbitmq-lab` | RabbitMQ cluster ID |
| `cluster.rabbitmq.work-dir` | `run/rabbitmq-cluster` | Qpid work directory (wiped before each start) |

### Ports

| Component | Port |
| --- | --- |
| Backend HTTP / STOMP (`/ws`) | `8088` |
| Frontend dev server | `3000` |
| RocketMQ NameServer / Master / Slave | `9876` / `10911` / `10921` |
| Kafka controller + brokers, Qpid AMQP | allocated dynamically from a free-port pool at startup (shown in the node table) |

### Cluster API

All MQs share `/api/clusters/{mode}/{middleware}/{clusterId}/...` (topology, metrics, logs, messages,
node operations, `POST .../operations` for one-click start/stop). `GET /api/clusters/providers` returns each
provider's `clusterId` and `capabilities`, which the frontend uses to decide which tabs to show.

| Prefix | Extra endpoints |
| --- | --- |
| `/api/clusters` | `GET /providers`, `GET /message-templates`, `GET/PUT /settings/rocketmq`, `GET /rate-limit`, plus the shared `/{mode}/{middleware}/{clusterId}/...` endpoints below |
| `/api/clusters/pseudo/rocketmq/local-lab` | `GET/POST/DELETE /lab/stress`, `GET /lab/internals` |
| `/api/clusters/pseudo/kafka/kafka-lab` | `GET/POST /topics`, `GET/DELETE /topics/{topic}`, `GET /consumer-groups`, `GET /consumer-groups/{group}`, `POST /consumer-groups/{group}/members`, `PATCH/DELETE /consumer-groups/{group}/members/{memberId}` (rate / remove), `GET /leader-changes`, `GET/POST/DELETE /lab/stress`, `GET /lab/internals` |
| `/api/clusters/pseudo/rabbitmq/rabbitmq-lab` | `GET /routing`, `POST /routing/preview`, `POST /exchanges`, `DELETE /exchanges/{name}`, `POST /queues`, `DELETE /queues/{name}`, `POST /queues/{name}/purge`, `POST/DELETE /bindings`, `POST /consumers`, `DELETE /consumers/{consumerId}`, `GET/POST/DELETE /lab/stress`, `GET /lab/internals` |

### Frontend (`next/.env.local`)

| Variable | Default | Description |
| --- | --- | --- |
| `BACKEND_URL` | `http://localhost:8088` | Backend address (rewrites proxy target) |

---

## Architecture

MQCluster follows a **Hexagonal Architecture (Ports & Adapters)** on the backend with a clean
front-end / back-end separation.

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

- **Core** — pure domain model and port interfaces with zero external dependencies.
- **Application** — orchestrates use cases and selects the right adapter via `ClusterProviderRegistry`.
- **Infrastructure / pseudo** — runs a real embedded RocketMQ runtime (in-process NameServer + Broker).
- **Infrastructure / rocketmq** — connects to an external RocketMQ cluster via the admin API.
- **Infrastructure / kafka** — embedded KRaft cluster, consumer-group manager and `IKafkaInspector` (topics, partitions, lag).
- **Infrastructure / rabbitmq** — embedded Qpid broker, resident consumers and `IRabbitInspector` (exchanges, queues, bindings, route preview).

See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the full deep-dive.

---

## Testing

### Backend

```bash
cd java
./mvnw clean verify            # Full build + run all tests
```

The test suite uses JUnit 5 + AssertJ + Spring Boot Test and covers domain logic, application
orchestration, and adapter behavior — including `KafkaClusterProviderTest` (start/stop/restart, leader
failover, lag), `RabbitClusterProviderTest` (four exchange types, dead-lettering, TTL, prefetch),
`LabApiTest` (stress start / status / stop / internals over HTTP) and black-box API tests for each MQ.
The suite currently runs **193 tests with 0 failures**; 18 heavier embedded-runtime tests are opt-in
(`-DrunEmbeddedKafka=true`, `-DrunEmbeddedKafkaSpike=true`, `-DrunEmbeddedQpidSpike=true`,
`-DrunEmbeddedRocketMqSpike=true`, `-DrunEmbeddedRocketMqMetrics=true`).

### Frontend

```bash
cd next
npx tsc --noEmit               # Type-check
npm run lint                   # ESLint
npm run build                  # Production build
```

---

## Contributing

Contributions are welcome! Please read our **[Contributing Guidelines](CONTRIBUTING.md)** to get started.

If you find a security issue, please follow the **[Security Policy](SECURITY.md)** — do **not** open a public issue.

---

## License

This project is licensed under the **Apache License 2.0** — see [LICENSE](LICENSE) for the full text.

---

## Acknowledgements

- [Apache RocketMQ](https://rocketmq.apache.org/) — the distributed messaging and streaming platform that powers the embedded runtime.
- [Apache Kafka](https://kafka.apache.org/) and [Apache Qpid Broker-J](https://qpid.apache.org/components/broker-j/) — the embedded Kafka and AMQP runtimes; [RabbitMQ Java client](https://www.rabbitmq.com/client-libraries/java-api-guide) — the AMQP 0-9-1 client.
- [Spring Boot](https://spring.io/projects/spring-boot) — the application framework behind the backend.
- [Next.js](https://nextjs.org/) & [React](https://react.dev/) — the frontend framework and UI library.
- [ECharts](https://echarts.apache.org/) — the charting and visualization library for topology graphs.
- Everyone in the open-source community who makes learning distributed systems accessible.

---

## 中文文档

本项目同时提供中文文档：

- **[架构文档（中文）](docs/zh/ARCHITECTURE.zh.md)** — 六边形架构、前后端分离、三种嵌入式 MQ 详解（含压力实验与内部机制）
- **[MQ 特性支持矩阵](docs/MQ_FEATURE_MATRIX.md)** — Kafka / Qpid 实测结论与内存占用
- **[贡献指南（中文）](docs/zh/CONTRIBUTING.zh.md)** — 如何参与开发、提交 PR
- **[仓库导航与约定](AGENTS.md)** — 目录结构、启动排查、配置项、关键设计决策（给人和 AI 看都行）
- **[后端模块导航](java/AGENTS.md)** — 分层、端口与适配器、测试与 JVM 参数

界面上还有 `/guide`、`/guide/rocketmq`、`/guide/kafka`、`/guide/rabbitmq` 四套分步学习指南，每一步都带「去仪表盘试试」的深链。

---

<div align="center">

**[⬆ Back to top](#mqcluster)**

Made with care for learners of distributed message systems.

</div>
