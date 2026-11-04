# MQ 特性支持矩阵（阶段 0 Spike 结论）

本文件记录 Kafka / RabbitMQ(Qpid Broker-J) 嵌入式运行时的实测结论，来源于：

- `java/src/test/java/com/example/clustermanager/EmbeddedKafkaSpikeTest.java`（`-DrunEmbeddedKafkaSpike=true`）
- `java/src/test/java/com/example/clustermanager/EmbeddedQpidSpikeTest.java`（`-DrunEmbeddedQpidSpike=true`）

## 依赖共存

| 组件 | 版本 | 说明 |
|---|---|---|
| RocketMQ | 5.3.3 | 原有，不变 |
| Kafka（`kafka_2.13` / `kafka-server` / `kafka-clients`） | 4.2.1（Spring Boot 4.1.0 BOM `kafka.version`） | 排除 `log4j-core` 等 log4j2 实现，`log4j-api` 经 `log4j-to-slf4j` 走 Logback |
| Qpid Broker-J（core / amqp-0-8 / memory-store） | 9.2.1（9.x 最新） | 引入 Guava 33.4.0-jre、BouncyCastle 1.80 |
| RabbitMQ Java Client `amqp-client` | 5.30.0（BOM 管理） | 使用 Netty 4.2.15（与 RocketMQ 同版本，BOM 统一） |
| Scala | 2.13.17 | 仅 Kafka 使用，无冲突 |
| Jackson | 2.21.4（`com.fasterxml`）+ 3.1.4（`tools.jackson`，Spring Boot 4） | 两套包名不同，可共存 |

`mvn dependency:tree` 核对无重复版本冲突；原有测试在新 classpath 下保持通过（本轮全量 `clean verify`：193 个测试，0 失败，18 个条件跳过）。
Qpid 启动后应用自身 Logback 日志仍正常输出（`appLoggingStillWorksAfterQpidStartup`）。

## Kafka（KRaft，1 controller + 3 broker）

| 特性 | 结论 | 备注 |
|---|---|---|
| 代码内格式化 KRaft 存储（`Formatter`） | ✅ | 静态 quorum：`controller.quorum.voters` |
| 1 controller + 3 broker 同 JVM 启动 | ✅ | 实测约 0.8 s |
| AdminClient 创建 3 分区 × 3 副本 topic | ✅ | |
| 停止 leader broker → leader 切换 / ISR 收缩 | ✅ | 实测约 2 s 完成切换 |
| 同 node.id + 同日志目录新建实例重启 → 重新进入 ISR | ✅ | `KafkaRaftServer` 不可复用，必须新建实例 |
| 多 broker 共享 Yammer 指标注册表 | ⚠️ | JMX 出现 `InstanceAlreadyExistsException` 警告（无害）；按 broker 的速率改用分区 LEO 增量计算 |

内存调优：`log.cleaner.dedupe.buffer.size` 默认每个 broker 预分配 128 MB，3 个 broker 即 384 MB 堆。
调成 2 MB 并降低线程数后，1+3 集群稳态堆占用约 **68 MB**（调优前 452 MB）。

## RabbitMQ（Qpid Broker-J 9.2.1 + 官方 RabbitMQ 客户端，AMQP 0-9-1）

| 特性 | Qpid 表现 | 平台处理方式 |
|---|---|---|
| direct / fanout / topic / headers 交换机 | ✅ | 原样使用 |
| `basicQos`（prefetch） | ✅ | 原样使用 |
| 手动 ack / nack / requeue | ✅ | 原样使用 |
| publisher confirms | ✅ | 原样使用 |
| mandatory + basic.return | ✅ | 原样使用 |
| 进程内对象模型（Broker → VirtualHost → Exchange/Queue/Binding） | ✅ | 用于路由拓扑与队列监控 |
| 队列参数 `x-dead-letter-exchange` | ❌ 声明即失败（542，关闭连接） | 平台把它翻译为 Qpid 的 `alternateBinding` + `maximumDeliveryAttempts=1`，UI 标注"Qpid 以 alternate binding 实现" |
| `alternateBinding` + `maximumDeliveryAttempts`（死信等价物） | ✅ reject/nack(requeue=false) 进入死信队列 | 死信实验基于此实现 |
| `reject(requeue=false)`（未配置最大投递次数） | ⚠️ Qpid 会重新入队 | 平台创建的队列默认带最大投递次数，行为与 RabbitMQ 一致 |
| 队列参数 `x-message-ttl` | ❌ 声明即失败 | 平台翻译为 Qpid 的 `maximumMessageTtl` |
| `maximumMessageTtl`（队列级 TTL） | ✅ 过期后被丢弃 | |
| 消息级 `expiration` | ✅ | 原样使用 |
| TTL 过期后进入死信队列 | ❌ Qpid 直接丢弃过期消息 | UI 禁用并标注"Qpid 不支持（真实 RabbitMQ 支持）" |
| 集群 / 镜像队列 / Quorum Queue | ❌ | 单节点嵌入，不做模拟 |

Qpid 运行时堆占用约 **27 MB**，启动约 0.4 s。

## 内存结论

三种 MQ 全部运行时（RocketMQ 3 节点 + Kafka 1+3 + Qpid 1）新增堆约 100 MB，JVM 默认最大堆
（物理内存的 1/4）足够，因此 **不在 `pom.xml` 的 `jvmArguments` / surefire `argLine` 中强制 `-Xmx`**。
RocketMQ commitlog 使用 mmap（堆外），不计入堆。

## 实验室能力（压力实验 / 内部机制）

三种 MQ 共用一套端口与路由（`IStressLab` / `IInternalsInspector`，能力名 `LAB`），
引擎只有一份 `StressRunner`，各适配器只提供 `StressSender` 与内部对象读取。

| MQ | 压力实验实现 | 内部机制数据源 | 实测时容易误判的地方 |
|---|---|---|---|
| RocketMQ | `RocketMqStressSender`（异步发送，回调上报延迟） | `CommitLog` 映射文件写指针 + `dispatchBehindBytes`、ConsumeQueue、IndexFile | 超过 4 KB 的消息体会被 `compressMsgBodyOverhowmuch` 压缩，CommitLog 增量远小于「条数 × 负载」 |
| Kafka | `KafkaStressSender`（异步 + 缓冲区，`max.block.ms` 决定阻塞行为） | `AdminClient.describeLogDirs()` 段文件 + LEO + ISR | 段文件预分配，磁盘占用含预留空间；停止实验时缓冲区的未确认消息会抛 `Producer is closed forcefully`，平台归一为「未确认」而非失败 |
| RabbitMQ（Qpid） | `RabbitStressSender`（同步 publisher confirm） | `RabbitObjectModel` 交换机 / 队列 / 绑定 + ready / unacked | 小消息极快（4 KB 下 0 失败、p99 约 1.8 ms），必须用 64 KB 负载才能把吞吐压到目标的 10% 左右 |

判定只看客户端真实结果：失败率 ≥ 1% → 已被冲垮；否则实际速率 < 目标 85% → 被限流；达标 → 未被冲垮。
