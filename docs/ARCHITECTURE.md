# Architecture

This document describes the architecture of MQCluster — a local-first MQ learning platform
that embeds real RocketMQ, Kafka (KRaft) and AMQP 0-9-1 (Qpid Broker-J) runtimes and exposes them
through a browser-based dashboard.

---

## Table of Contents

- [High-Level Overview](#high-level-overview)
- [Hexagonal Architecture (Ports & Adapters)](#hexagonal-architecture-ports--adapters)
- [Backend Layers](#backend-layers)
  - [Core Domain](#core-domain)
  - [Application Layer](#application-layer)
  - [Infrastructure Layer](#infrastructure-layer)
  - [API Layer](#api-layer)
- [Frontend Architecture](#frontend-architecture)
- [Embedded RocketMQ](#embedded-rocketmq)
- [Embedded Kafka](#embedded-kafka)
- [Embedded RabbitMQ (Qpid Broker-J)](#embedded-rabbitmq-qpid-broker-j)
- [Lab: Stress Experiments & Broker Internals](#lab-stress-experiments--broker-internals)
- [Real-Time Communication](#real-time-communication)
- [Request & Data Flow](#request--data-flow)
- [Design Decisions](#design-decisions)

---

## High-Level Overview

MQCluster is a **front-end / back-end separated** application:

```
┌─────────────────────────────────────────────────────────┐
│                    Browser (user)                        │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │            Next.js 16 Frontend (port 3000)        │  │
│  │  React 19 · Tailwind v4 · ECharts 6 · STOMP       │  │
│  └───────────────────┬───────────────────────────────┘  │
│                      │ /api (REST)  /ws (STOMP)         │
│                      │ Next.js rewrites proxy            │
│  ┌───────────────────▼───────────────────────────────┐  │
│  │         Spring Boot 4.1.0 Backend (port 8088)     │  │
│  │  Hexagonal architecture · Embedded RocketMQ 5.3.3 │  │
│  │  Embedded Kafka 4.2 KRaft · Embedded Qpid 9.2     │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

- The **frontend** is a Next.js 16 application (App Router, React 19, TypeScript strict mode).
  It renders the dashboard, topology graph, message workbench, and monitoring charts.
- The **backend** is a Spring Boot 4.1.0 application that embeds real Apache RocketMQ 5.3.3,
  Apache Kafka 4.2 (KRaft) and Apache Qpid Broker-J 9.2 runtimes and exposes REST + WebSocket endpoints.
  All three can run at the same time; every node starts in `STOPPED` state.
- Communication happens over **REST** (`/api`) for commands and **STOMP over WebSocket** (`/ws`)
  for real-time event streaming.

---

## Hexagonal Architecture (Ports & Adapters)

The backend follows the **Hexagonal Architecture** (also known as Ports & Adapters) pattern.
The core idea: business logic lives in the center and knows nothing about the outside world.
All external concerns (HTTP, databases, messaging brokers) are adapters that plug into ports
defined by the core.

```
                         ┌─────────────────────────────┐
   REST / WebSocket  ───▶│         API Layer           │
                         │  Controllers · DTOs · WS    │
                         └──────────────┬──────────────┘
                                        │ calls
                         ┌──────────────▼──────────────┐
                         │      Application Layer      │
                         │  Use cases · Provider       │
                         │  selection · Orchestration  │
                         └──────────────┬──────────────┘
                                        │ uses ports
                         ┌──────────────▼──────────────┐
                         │        Core Domain          │
                         │  Cluster · Node · Message   │
                         │  Port interfaces (0 deps)   │
                         └──────────────▲──────────────┘
                                        │ implements ports
                         ┌──────────────┴──────────────┐
                         │    Infrastructure Layer     │
                         │  Embedded RocketMQ adapter  │
                         │  RocketMQ admin adapter     │
                         │  Embedded Kafka adapter     │
                         │  Embedded Qpid adapter      │
                         └─────────────────────────────┘
```

**Dependency rule**: dependencies always point **inward**. `core` has no dependencies on
`application`, `infrastructure`, or `api`. `infrastructure` depends on `core` (to implement ports)
but `core` never depends on `infrastructure`.

---

## Backend Layers

### Core Domain

**Location**: `java/src/main/java/com/example/clustermanager/core/`

The core contains the pure domain model and port interfaces. It has **zero external dependencies**
— no Spring, no RocketMQ, no HTTP libraries.

Key concepts:

| Concept | Description |
| --- | --- |
| `Cluster` | An aggregate root representing a logical MQ cluster with multiple nodes. |
| `Node` | A single broker or nameserver instance with a lifecycle (STOPPED, STARTING, RUNNING, STOPPING). |
| `Message` | A message produced to or consumed from a topic. |
| Port interfaces | Interfaces that `application` calls and `infrastructure` implements (e.g. cluster lifecycle, message operations). |
| `ProviderDescriptor` | Provider metadata: `providerId`, `mode`, `middleware` (`ROCKETMQ` / `KAFKA` / `RABBITMQ`), `clusterId`, and `capabilities` (e.g. `CLUSTER_OPERATIONS`, `CONNECTION_CONFIG`, `SERVICE_REGISTRATION`, `KAFKA_INSPECTION`, `RABBIT_INSPECTION`, `LAB`). The frontend decides which tabs and buttons to show from these. |
| `INodeManager.operateCluster` | Cluster-level start/stop/restart; each provider decides the start order (e.g. NameServer first, Kafka controller first). |
| `IKafkaInspector` / `IRabbitInspector` | Capability ports for MQ-specific inspection (topics/partitions/consumer groups; exchanges/queues/bindings/route preview). |
| `IStressLab` / `IInternalsInspector` | Lab capability ports (gated by `LAB`): run a real load experiment against the embedded broker and report throughput / failures / p95 / p99, and read the broker's own runtime objects (CommitLog, log segments, object model). Implementations live in `infrastructure/{pseudo,kafka,rabbitmq}/lab` and share `infrastructure/lab/StressRunner`. |
| `MessageScenario.options` | `Map<String,String>` carrying MQ-specific message parameters (Kafka `key`/`partition`/`acks`/`compression`; RabbitMQ `exchange`/`routingKey`/`headers.*`/`ackMode`/...). Nullable, so old requests stay compatible. |

Because the core is framework-free, it is trivially unit-testable and can be reused in any context.

### Application Layer

**Location**: `java/src/main/java/com/example/clustermanager/application/`

The application layer orchestrates use cases. It does not contain business rules itself — it
coordinates the domain model and selects the appropriate infrastructure adapter.

Key component: **`ClusterProviderRegistry`** — selects an adapter based on a
`(ClusterMode, MiddlewareType)` tuple. For example, `(PSEUDO, ROCKETMQ)` selects the embedded
RocketMQ adapter, while `(REAL, ROCKETMQ)` selects the external admin adapter.
`resolveCapability(selection, Class<T>)` returns a capability port such as `IKafkaInspector`; if the
selected provider does not implement it, an `IllegalArgumentException` is thrown and mapped to HTTP 400.

### Infrastructure Layer

**Location**: `java/src/main/java/com/example/clustermanager/infrastructure/`

This layer implements the core ports with concrete technology. Four adapters exist today:

| Adapter | Package | Description |
| --- | --- | --- |
| **Pseudo (embedded)** | `infrastructure/pseudo` | Runs a real Apache RocketMQ 5.3.3 NameServer + Broker in-process. This is the default learning mode. |
| **RocketMQ admin** | `infrastructure/rocketmq` | Connects to an external RocketMQ cluster via the RocketMQ admin client. For advanced use. |
| **Kafka (embedded)** | `infrastructure/kafka` | `KafkaClusterProvider`: KRaft cluster (1 controller + 3 brokers), message workbench, resident consumer groups, throughput tracker. Toggle: `cluster.kafka.enabled`. |
| **RabbitMQ (embedded)** | `infrastructure/rabbitmq` | `RabbitClusterProvider`: Qpid Broker-J with a memory virtual host, driven by the official RabbitMQ client; routing inspection, resident consumers. Toggle: `cluster.rabbitmq.enabled`. |

`infrastructure/common` holds shared helpers (e.g. `JvmProcessMetrics`); the free-port pool `PortPool`
is reused by all embedded runtimes. The adapters do not duplicate load-testing logic: `infrastructure/lab`
holds `StressRunner` (pacing, sampling, percentiles, verdicts) and each MQ only supplies a `StressSender`.

The pseudo adapter is the heart of MQCluster — it launches genuine RocketMQ components inside the
JVM, so learners interact with a real broker, not a simulation.

### API Layer

**Location**: `java/src/main/java/com/example/clustermanager/api/`

The API layer is the entry point for the frontend. It contains:

- **REST controllers** — `ClusterController` (shared endpoints for all MQs: topology, metrics, logs,
  node and cluster operations, message simulation, runtime RocketMQ connection config, rate limit),
  `KafkaController` (`/api/clusters/pseudo/kafka/{clusterId}`: topics, consumer groups, members,
  leader changes), `RabbitController` (`/api/clusters/pseudo/rabbitmq/{clusterId}`: routing, route
  preview, exchanges, queues, bindings, consumers) and `LabController`
  (`/api/clusters/{mode}/{middleware}/{clusterId}/lab/...`: stress experiments and internals snapshots).
- **DTOs** — request/response objects that map to/from domain models.
- **WebSocket / STOMP config** — sets up the `/ws` endpoint and topic subscriptions for real-time events.
- **Exception handling** — a global exception handler that translates domain errors into HTTP responses.

---

## Frontend Architecture

**Location**: `next/src/`

The frontend follows a layered structure that flows data in one direction:

```
app/ (routes)  ──▶  MqDashboard + tab components  ──▶  hooks  ──▶  lib/api.ts  ──▶  Backend REST / STOMP
```

| Layer | Location | Responsibility |
| --- | --- | --- |
| **Pages** | `src/app/` | `page.tsx` is the home page (MQ entry cards, learning path, resource hint). `mq/[middleware]/page.tsx` is a server component that validates the param (`notFound()` otherwise) and renders the client `MqDashboard`. `compare/` is the comparison page; `guide/` and `guide/[middleware]/` hold the per-MQ tutorials. |
| **Components** | `src/components/` | `dashboard/` holds the shared tabs (topology, node operations, workbench, stress lab, internals, monitoring, logs, connection config) and `MqDashboard` (tab state in `?tab=`); `lab/` holds the stress console and the internals panel; `kafka/` and `rabbit/` hold MQ-specific tabs. `cluster-status-provider.tsx` polls global run state for the header and home page. `ui/` holds shadcn components. |
| **Hooks** | `src/hooks/` | `use-cluster-data.ts` does initial load with retry, 5 s polling of topology/metrics, and STOMP subscriptions; `use-node-operations.ts` handles node/cluster operations with toasts; `use-chart-palette.ts` reads ECharts colors from CSS variables and re-applies them on theme change. |
| **Lib** | `src/lib/` | `api.ts` is a unified axios client with typed endpoint functions. `mq-registry.ts` centralizes per-MQ metadata (name, icon, accent, role → shape/label, tabs gated by backend `capabilities`). `guides.ts` holds tutorial content. |
| **Types** | `src/types/` | Shared TypeScript type definitions mirroring backend DTOs (`cluster.ts`, `kafka.ts`, `rabbit.ts`). |

### Theming

`globals.css` defines `--mq-accent` / `--mq-accent-foreground` per `[data-mq="rocketmq|kafka|rabbitmq"]`
in OKLCH (light and dark), plus status tokens (`running`, `stopped`, `failed`, `degraded`, `starting`).
`next-themes` toggles light/dark from the header.

### API proxying

`next.config.ts` configures rewrites so that `/api/*` and `/ws/*` are proxied to
`http://localhost:8088` (the backend). This avoids CORS issues in development and keeps the
frontend code environment-agnostic. `experimental.proxyTimeout` is raised to 120 s because a
one-click Kafka start (controller + 3 brokers) can exceed the default 30 s.

---

## Embedded RocketMQ

The defining feature of MQCluster is that it runs a **real** Apache RocketMQ 5.3.3 runtime
inside the backend JVM — not a mock or a simulator.

### How it works

1. The pseudo adapter starts an **in-process NameServer** that handles service discovery.
2. It then starts one or more **in-process Brokers** (Master and/or Slave) that register with
   the NameServer.
3. The broker uses a local working directory (`run/pseudo-cluster/` by default) for message storage
   (CommitLog, ConsumeQueue, etc.) — exactly like a production broker, just on a smaller scale.
4. Message produce/consume operations go through the **real RocketMQ client API**, so learners
   experience authentic broker behavior: topic routing, message storage, consumer offsets,
   and master–slave replication.

### Why embedded?

| Approach | Pros | Cons |
| --- | --- | --- |
| **Mock broker** | Easy to build | Not realistic; learners can't trust behavior |
| **Docker cluster** | Realistic | Heavy; requires Docker; not portable |
| **Embedded (MQCluster)** ✅ | Real broker, zero external deps, single command | Limited to single-machine scale (which is fine for learning) |

### Seed topology

By default, MQCluster seeds a 3-node topology:

| Node | Role | Port | Initial state |
| --- | --- | --- | --- |
| `rmq-ns-01` | NameServer | 9876 | STOPPED |
| `rmq-broker-m-01` | Master Broker | 10911 | STOPPED |
| `rmq-broker-s-01` | Slave Broker | 10921 | STOPPED |

Nodes start in the STOPPED state (`cluster.pseudo.auto-start=false`) so learners can start them
manually and observe the lifecycle transitions.

---

## Embedded Kafka

`EmbeddedKafkaRuntime` formats KRaft storage in code (`Formatter`, static quorum) and runs
`KafkaRaftServer` instances inside the JVM.

| Node | Role (`labels.role`) | Port |
| --- | --- | --- |
| `kafka-ctrl-01` | `kafka-controller` | dynamic (`PortPool`) |
| `kafka-broker-01..03` | `kafka-broker` | dynamic (`PortPool`) |

- The controller must start first; one-click start does controller → brokers.
- A `KafkaRaftServer` cannot be restarted, so a broker restart creates a **new instance with the same
  `node.id` and log directory** (`run/kafka-cluster/<nodeId>`), which then rejoins the ISR.
- Per-broker in/out rates are computed from the LEO delta of partitions the broker leads, and
  consume rates from committed-offset deltas, because Kafka's Yammer metrics registry is JVM-global.
  CPU and memory are process-wide values.
- `KafkaConsumerGroupManager` runs resident consumers with a configurable records-per-second rate to create lag
  and trigger rebalances.

## Embedded RabbitMQ (Qpid Broker-J)

`EmbeddedQpidRuntime` starts Qpid Broker-J 9.2 via `SystemLauncher` with a memory virtual host on a
dynamic AMQP port. Clients use the official `com.rabbitmq:amqp-client` (AMQP 0-9-1).

- Single node `rabbit-node-01` (`role=rabbit-broker`); `RabbitTopologySeeder` pre-declares one demo
  exchange of each type (direct / fanout / topic / headers), queues and bindings.
- Exchanges, queues and bindings are read directly from Qpid's in-process object model
  (Broker → VirtualHost → Exchange / Queue / Binding).
- `x-dead-letter-exchange` is translated to Qpid's `alternateBinding` (+ `maximumDeliveryAttempts`), and
  `x-message-ttl` to `maximumMessageTtl`. TTL-expired messages are discarded by Qpid instead of being
  dead-lettered; the UI disables that option with "Qpid 不支持（真实 RabbitMQ 支持）".

See [MQ_FEATURE_MATRIX.md](MQ_FEATURE_MATRIX.md) for the full support matrix and memory measurements
(all three MQs running add about 100 MB of heap, so no `-Xmx` is forced).

---

## Lab: Stress Experiments & Broker Internals

The `?tab=stress` and `?tab=internals` tabs answer two questions that diagrams cannot:
*"what does this broker do when traffic outruns it?"* and *"what is it actually doing underneath while
messages stream in?"*. Both are backed by ports behind the `LAB` capability and share the same routes:

```
POST   /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    → start
GET    /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    → live status (polled)
DELETE /api/clusters/{mode}/{middleware}/{clusterId}/lab/stress    → stop, returns the final result
GET    /api/clusters/{mode}/{middleware}/{clusterId}/lab/internals → one runtime snapshot
```

`LabController` resolves `IStressLab` / `IInternalsInspector` through
`ClusterFacadeService.capability(...)`, so a provider that does not implement them yields HTTP 400.

### Stress experiments (`IStressLab`)

`infrastructure/lab/StressRunner` is the only engine; each adapter only supplies a `StressSender`
(`send` / `drain` / `close` / `reportsAsyncLatency`). A run:

1. Spawns N worker threads, each pacing sends to the target rate (`1–50,000 msg/s`, `1–32` threads,
   `16 B – 64 KB` payloads) with a shared token budget, so a slow broker slows the whole run instead of
   letting one thread sprint.
2. Samples once per second: achieved rate, failure rate, p95, p99 — keeping up to 300 samples.
3. Classifies every client exception into a bucket (`StressRunner.describe()` folds digits to `#` and
   drops `cost=`, otherwise `wait response timeout, cost=3000/3001/…` explodes into dozens of buckets).
4. On stop: freezes `endedAt`, then drains and closes **outside the lock**. Draining in-flight messages
   can take seconds (RabbitMQ confirm drain, Kafka buffer flush); holding the lock there would block
   `GET /stress` and hang the UI. `status()`/`stop()` in all three labs therefore take the runner
   reference under the lock and call it outside.
5. Produces one of three verdicts from the **client's own view**:

| Verdict | Condition | What it teaches |
| --- | --- | --- |
| 已被冲垮 (collapsed) | failure ratio ≥ 1% | the broker could not keep up; the producer saw timeouts / rejections |
| 被限流 (throttled) | failure ratio < 1% but achieved rate < 85% of target | back-pressure absorbed the flood — throughput, not errors, is the symptom |
| 未被冲垮 (held) | rate reached the target | headroom remains for this workload |

Interruptions caused by the stop itself (`InterruptedException`, Kafka's
`Producer is closed forcefully`) are not failures — `loop()` only counts an exception while `running`
is still true. Kafka and RocketMQ are asynchronous (`reportsAsyncLatency() == true`), so real end-to-end
latency is reported from the send callback, not from the `send()` return.

### Internals (`IInternalsInspector`)

No RPC fan-out, no heuristics: each inspector reads the objects the embedded broker already has.

| MQ | Source | Rendered as |
| --- | --- | --- |
| RocketMQ | `CommitLog` mapped file (write position, `dispatchBehindBytes`), ConsumeQueue dirs, IndexFile | four-stage write path: append → dispatch → index → consume, with live byte counts |
| Kafka | `AdminClient.describeLogDirs()` + `EndOffsetSpec` | per-broker segment files (`.log` / `.index` / `.timeindex`), LEO per partition, ISR |
| RabbitMQ (Qpid) | `RabbitObjectModel` (Broker → VirtualHost → Exchange / Queue / Binding) | exchanges, queues, bindings, ready / unacked counters |

Two teaching notes are rendered with the data rather than hidden in code comments:
RocketMQ compresses bodies over 4 KB (`compressMsgBodyOverhowmuch`), so a 4 KB flood grows the CommitLog
much less than "messages × payload"; and Kafka pre-allocates segment files, so on-disk usage counts
reserved space, not only live records.

---

## Real-Time Communication

MQCluster pushes live events to the frontend using **STOMP over WebSocket** (with SockJS fallback).
`ClusterTelemetryPushService` iterates over every registered provider (`ClusterProviderRegistry.listProviders()`)
every `cluster.stream.publish-interval-ms` (default 5000 ms).

| Channel | Direction | Content |
| --- | --- | --- |
| `/topic/clusters/{clusterId}/metrics` | Server → Client | Periodic CPU, memory, throughput per node |
| `/topic/clusters/{clusterId}/logs` | Server → Client | Recent activity log entries |

Topology is not pushed; the frontend's `use-cluster-data` hook polls topology and metrics every 5 s,
subscribes to the two topics above, and refreshes immediately after each node operation.

---

## Request & Data Flow

Here is a typical flow for starting a node:

```
1. User clicks "启动" on rmq-broker-m-01 in the UI
2. Frontend POST /api/clusters/pseudo/rocketmq/local-lab/nodes/rmq-broker-m-01/operations  {"operationType":"START"}
3. API controller → Application use case
4. Application selects adapter via ClusterProviderRegistry (PSEUDO, ROCKETMQ)
5. Pseudo adapter starts the embedded broker process
6. Broker registers with the in-process NameServer
7. The response returns the operation result; the frontend shows a toast and refreshes topology
8. The next telemetry push sends metrics and the new log entry to /topic/clusters/local-lab/{metrics|logs}
```

Message produce/consume follows a similar path, with the addition of real RocketMQ client calls
inside the adapter.

---

## Design Decisions

### Why hexagonal architecture?

- **Testability** — the core domain can be unit-tested without Spring or RocketMQ.
- **Swappability** — the `ClusterProviderRegistry` lets us plug in new middleware types or
  deployment modes without touching the core or API.
- **Clarity** — each layer has a single responsibility, making the codebase easier to navigate
  and teach (which is the whole point of this project).

### Why Next.js (not Vue or plain React)?

- **App Router** gives us file-based routing and server components out of the box.
- **API rewrites** make proxying to the backend trivial with zero CORS headaches.
- **React 19** + **TypeScript strict** provide strong type safety and modern hooks ergonomics.
- **Tailwind CSS v4** keeps styling fast and consistent without context switching.

### Why embed RocketMQ instead of mocking?

- Learners deserve to interact with the **real thing**. A mock teaches the mock, not RocketMQ.
- Embedding avoids the overhead of Docker or remote clusters, keeping the barrier to entry low
  (one Java command, one npm command).
- The embedded broker uses the same storage format and client protocol as production, so
  knowledge transfers directly.

---

<div align="center">

**[⬆ Back to top](#architecture)**

</div>
