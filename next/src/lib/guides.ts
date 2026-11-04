/**
 * 学习指南内容——三种 MQ 各自的分步教程（静态数据，由 /guide/[middleware] 渲染）。
 *
 * 每一步可带「在仪表盘中试试」深链接，跳转到 /mq/{slug}?tab=...。
 * RocketMQ 内容迁移自原 /guide 页面并统一为简体中文。
 */

import type { DashboardTab, MqSlug } from '@/lib/mq-registry'

export interface GuidePoint {
  term: string
  desc: string
}

export interface GuideStep {
  title: string
  /** 正文段落 */
  body: string[]
  /** 概念列表 */
  points?: GuidePoint[]
  /** 动手步骤 */
  tasks?: string[]
  /** 代码片段 */
  code?: string
  /** 补充提示（平台现状、注意事项） */
  note?: string
  /** 「在仪表盘中试试」 */
  tryIt?: { tab: DashboardTab; label: string }
}

export interface Guide {
  slug: MqSlug
  title: string
  intro: string
  steps: GuideStep[]
  /** 进阶阅读：超出平台实验范围但对生产使用重要 */
  advanced: GuideStep[]
  docsUrl: string
}

const ROCKETMQ: Guide = {
  slug: 'rocketmq',
  title: 'RocketMQ 学习指南',
  intro: '理解 NameServer + Broker 主从架构，并在嵌入式 RocketMQ 5.3 集群上完成启停、收发与监控实验。',
  docsUrl: 'https://rocketmq.apache.org/docs/',
  steps: [
    {
      title: '架构基础：NameServer 与 Broker',
      body: ['RocketMQ 集群由三类核心组件组成：'],
      points: [
        {
          term: 'NameServer',
          desc: '轻量级路由注册中心。Broker 启动时向其注册，Producer / Consumer 从其拉取路由。多个 NameServer 之间互不通信，各自独立维护路由表。',
        },
        { term: 'Broker Master', desc: '消息存储与转发的核心节点，接收消息并持久化到 commitlog，同时为消费者提供读取服务。' },
        { term: 'Broker Slave', desc: '从 Master 同步数据的副本节点，提供高可用；Master 故障时消费者可切换到 Slave 继续消费。' },
      ],
      note: '拓扑图中圆形为 NameServer（rmq-ns-01），圆角方块为 Broker Master（rmq-broker-m-01），菱形为 Broker Slave（rmq-broker-s-01）。',
      tryIt: { tab: 'overview', label: '查看集群拓扑' },
    },
    {
      title: '节点生命周期',
      body: ['在「概览」中可以对每个节点执行启动、停止、重启，也可以一键启动整个集群（平台会自动按依赖顺序启动）。'],
      note: '启动顺序：必须先启动 NameServer，再启动 Broker——Broker 启动时要向 NameServer 注册，NameServer 未运行则注册失败。',
      tasks: [
        '启动 rmq-ns-01，等待状态变为「运行中」',
        '启动 rmq-broker-m-01，观察拓扑中 Broker → NameServer 的注册链路由灰色虚线变为实线',
        '停止 Broker，观察拓扑变化；再重启，观察恢复过程',
      ],
      tryIt: { tab: 'overview', label: '去启动节点' },
    },
    {
      title: '消息模型：Topic、Producer、Consumer',
      body: ['「消息工作台」通过嵌入式集群真实生产并消费消息，逐条返回投递结果。'],
      points: [
        { term: 'Topic', desc: '消息的主题分类。Producer 向 Topic 发送，Consumer 订阅 Topic 接收。' },
        { term: 'Producer', desc: '消息生产者，负责创建消息并发送到 Broker。' },
        { term: 'Consumer Group', desc: '消费者组，同组消费者共享消费进度（集群消费）；广播模式下每个消费者都收到全量消息。' },
        { term: 'Message Key', desc: '业务键，用于唯一标识和追踪消息。' },
      ],
      tasks: [
        '确保 NameServer 和至少一个 Broker 已启动',
        '在消息工作台输入 Topic（如 TestTopic），条数设为 3，点击发送',
        '查看投递结果——成功表示消息已真实经过嵌入式 RocketMQ 生产并被消费',
      ],
      tryIt: { tab: 'workbench', label: '发送消息' },
    },
    {
      title: '主从复制与高可用',
      body: ['平台默认种子化 1 个 Master 和 1 个 Slave，模拟真实主从复制架构。'],
      points: [
        { term: '同步复制', desc: 'Master 写入后等待 Slave 同步完成才返回 ACK，数据安全但延迟略高。' },
        { term: '异步复制', desc: 'Master 写入后立即返回 ACK，Slave 异步追赶，性能高但故障时可能丢少量数据。' },
      ],
      tasks: ['启动 Master 与 Slave，在拓扑中观察 Master → Slave 的复制链路', '停止 Master，观察 Slave 仍保持运行，可继续提供读取'],
      tryIt: { tab: 'overview', label: '观察主从链路' },
    },
    {
      title: '监控与可观测性',
      body: ['平台通过 WebSocket（STOMP）实时推送监控与日志，无需手动刷新。'],
      points: [
        { term: '监控', desc: 'CPU、内存、投递 / 消费 TPS 的趋势折线图（30 个采样点）与各节点对比柱状图；左轴为百分比，右轴为 TPS。' },
        { term: '日志', desc: '节点操作、服务变更与消息模拟的审计日志，可按节点、级别筛选。' },
      ],
      tryIt: { tab: 'monitoring', label: '打开监控' },
    },
    {
      title: '动态限流与资源安全',
      body: ['平台根据本机硬件动态计算单批次最大消息量，防止本地系统卡死。'],
      points: [
        { term: '综合公式', desc: '上限 = min(内存因子, CPU 因子, 磁盘因子, 绝对上限) × 安全系数 0.7' },
        { term: '内存因子', desc: '可用 JVM 堆 MB × 50 条/MB' },
        { term: 'CPU 因子', desc: '逻辑核心数 × 200 条/核' },
        { term: '磁盘因子', desc: '可用磁盘 GB × 100 条/GB' },
      ],
      note: '示例：16 核、3883 MB 可用堆、238 GB 磁盘 → 上限约 2240 条/批次。超过上限的请求会被后端拒绝（HTTP 400）。',
      tryIt: { tab: 'connection', label: '查看本机上限' },
    },
  ],
  advanced: [
    {
      title: '消息发送模式：同步、异步、单向',
      body: ['同步发送阻塞等待 Broker ACK，可靠性最高（消息工作台使用此模式）；异步发送通过回调接收结果；单向发送不等待 ACK，延迟最低，适合日志收集等容忍丢失的场景。'],
      code: 'SendResult result = producer.send(msg);          // 同步\nproducer.send(msg, callback);                      // 异步\nproducer.sendOneway(msg);                          // 单向',
    },
    {
      title: '消费模式：Push / Pull 与顺序消费',
      body: [
        'Push 消费者由 Broker「推送」（实现上是长轮询），适合大多数业务；Pull 消费者主动拉取，可精确控制速率与 offset。',
        '并发消费多线程处理、无顺序保证；顺序消费保证同一 MessageQueue 内按发送顺序处理，需配置 MessageListenerOrderly。',
      ],
    },
    {
      title: '事务消息：最终一致性',
      body: [
        '发送对消费者不可见的「半消息」→ 执行本地事务 → 成功则 COMMIT（消息可见），失败则 ROLLBACK（删除）。',
        '若 Broker 迟迟未收到二次确认，会回查 Producer 的本地事务状态。',
      ],
      code: 'TransactionMQProducer producer = new TransactionMQProducer("group");\nproducer.setTransactionListener(new TransactionListener() {\n  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {\n    return LocalTransactionState.COMMIT_MESSAGE; // 执行本地事务\n  }\n  public LocalTransactionState checkLocalTransaction(MessageExt msg) {\n    return LocalTransactionState.COMMIT_MESSAGE; // 回查\n  }\n});',
    },
    {
      title: '死信队列与重试',
      body: [
        '消费失败的消息会延迟重试，间隔递增（10s、30s、1m … 2h），默认最多 16 次；仍失败则进入死信 Topic %DLQ%{消费者组}。',
        '可用 mqadmin queryDLQMsg -g 消费者组 -t Topic 查询死信消息。对比 RabbitMQ：后者的死信由队列参数 x-dead-letter-exchange 路由。',
      ],
    },
    {
      title: '消息过滤：Tag 与 SQL92',
      body: ['Tag 过滤简单高效：consumer.subscribe("Topic", "TagA || TagB")；SQL92 可按消息属性写表达式，如 MessageSelector.bySql("age > 18")。'],
    },
    {
      title: '生产运维要点',
      body: [
        '关注消息堆积（消费跟不上生产时扩容消费者）、开启消息轨迹排查问题、新增 Broker 时配置相同 NameServer 地址。',
        'RocketMQ 5.x 推荐 DLedger / Controller 模式，基于 Raft 实现自动选主与故障转移。',
      ],
    },
  ],
}

const KAFKA: Guide = {
  slug: 'kafka',
  title: 'Kafka 学习指南',
  intro: '在嵌入式 KRaft 集群（1 个 controller + 3 个 broker）上观察分区副本、leader 切换、消费者组 rebalance 与 lag。',
  docsUrl: 'https://kafka.apache.org/documentation/',
  steps: [
    {
      title: 'KRaft 架构：controller 与 broker',
      body: ['Kafka 4.x 已移除 ZooKeeper，元数据由 KRaft controller 仲裁维护。'],
      points: [
        { term: 'Controller（kafka-ctrl-01）', desc: '保存集群元数据（topic、分区、副本分布），负责选举分区 leader。' },
        { term: 'Broker（kafka-broker-01..03）', desc: '存储分区副本。每个分区有一个 leader 副本负责读写，其余 follower 副本从 leader 拉取同步。' },
      ],
      note: '必须先启动 controller，broker 才能注册——「一键启动集群」会自动按此顺序执行。',
      tasks: ['点击「一键启动集群」', '在拓扑中观察 3 个 broker 都连接到 controller'],
      tryIt: { tab: 'overview', label: '启动 Kafka 集群' },
    },
    {
      title: 'Topic、分区与副本',
      body: ['Topic 被切分为多个分区以实现并行；每个分区按副本因子复制到多个 broker。'],
      points: [
        { term: '分区（Partition）', desc: '有序、不可变的追加日志，是并行与顺序性的基本单位——只保证分区内有序。' },
        { term: '副本因子（RF）', desc: '每个分区的副本数，不能超过运行中的 broker 数。' },
        { term: 'ISR', desc: 'In-Sync Replicas：与 leader 保持同步的副本集合。acks=all 时需要 ISR 中全部副本确认。' },
      ],
      tasks: ['在「Topic 与分区」中创建一个 6 分区、RF=3 的 topic', '在分区矩阵中查看每个分区的 leader（皇冠）与 ISR 分布'],
      tryIt: { tab: 'topics', label: '创建 topic' },
    },
    {
      title: 'leader 切换与故障恢复',
      body: ['当 leader 所在 broker 宕机，controller 会从 ISR 中选出新的 leader，客户端自动重新路由。'],
      tasks: [
        '在「概览」中停止 kafka-broker-01',
        '回到「Topic 与分区」：原本由它担任 leader 的分区已切换，ISR 收缩，时间线记录了每次切换',
        '重新启动 kafka-broker-01，几秒后它重新进入 ISR（但不会自动抢回 leader）',
      ],
      tryIt: { tab: 'topics', label: '观察分区矩阵' },
    },
    {
      title: '生产者：key、分区与 acks',
      body: ['消息工作台的 Kafka 表单支持 key、指定分区、acks 与压缩方式，结果表展示每条消息落到的 partition 与 offset。'],
      points: [
        { term: 'key', desc: '相同 key 的消息经哈希总是写入同一分区，从而保证同一业务实体的顺序。' },
        { term: 'acks', desc: '0：不等确认；1：leader 写入即确认；all：ISR 全部写入才确认，最可靠。' },
        { term: '压缩', desc: 'gzip / snappy / lz4 / zstd，按批压缩以换取吞吐。' },
      ],
      tasks: ['用固定 key 连续发送 10 条，确认它们的 partition 相同、offset 递增', '去掉 key 再发送，观察消息分布到多个分区'],
      tryIt: { tab: 'workbench', label: '发送消息' },
    },
    {
      title: '消费者组与 rebalance',
      body: ['同一消费者组内，每个分区同一时刻只分配给一个成员；成员增减会触发 rebalance 重新分配分区。'],
      tasks: [
        '在「消费者组」中为 lab-consumers 添加 1 个成员，观察它独占全部分区',
        '再添加 2 个成员，rebalance 记录展示分配前后的对比',
        '继续添加成员直到超过分区数——多出的成员会空闲',
      ],
      tryIt: { tab: 'groups', label: '管理消费者组' },
    },
    {
      title: 'offset 与 lag',
      body: ['lag = 分区末端 offset（LEO）− 消费者组已提交 offset，是衡量消费是否跟得上的核心指标。'],
      tasks: ['把成员消费速率调到 1 条/秒，然后写入 300 条消息', '观察 lag 柱状图持续升高；调高速率后 lag 回落'],
      tryIt: { tab: 'groups', label: '制造 lag' },
    },
    {
      title: '监控',
      body: ['各 broker 的写入 / 读取速率按「该 broker 作为 leader 的分区 LEO 增量」计算，消费速率按消费者组 committed offset 的增量计算。'],
      note: '所有节点运行在同一个 JVM 中，CPU 与内存只能取进程级数值。',
      tryIt: { tab: 'monitoring', label: '打开监控' },
    },
  ],
  advanced: [
    {
      title: '幂等与事务生产者',
      body: ['enable.idempotence=true 通过 producer id + 序列号去重，避免重试导致重复写入；事务生产者可跨分区原子写入，配合 read_committed 实现 exactly-once。'],
    },
    {
      title: '日志保留与压缩',
      body: ['retention.ms / retention.bytes 按时间或大小删除旧段；cleanup.policy=compact 只保留每个 key 的最新值，常用于状态表与变更日志。'],
    },
    {
      title: 'min.insync.replicas',
      body: ['与 acks=all 配合：ISR 少于该值时写入失败（NotEnoughReplicas），以可用性换取不丢数据。常见配置 RF=3、min.insync.replicas=2。'],
    },
  ],
}

const RABBITMQ: Guide = {
  slug: 'rabbitmq',
  title: 'RabbitMQ 学习指南',
  intro: '用 RabbitMQ 官方客户端连接嵌入式 AMQP 0-9-1 broker（Qpid Broker-J），掌握交换机路由、确认机制、prefetch 与死信。',
  docsUrl: 'https://www.rabbitmq.com/docs',
  steps: [
    {
      title: 'AMQP 模型：Exchange → Binding → Queue',
      body: ['生产者从不直接写队列，而是把消息发给交换机；交换机根据绑定（binding）规则把消息路由到零个或多个队列，消费者从队列取消息。'],
      points: [
        { term: 'Exchange', desc: '路由器，按类型决定匹配规则。' },
        { term: 'Binding', desc: '交换机与队列之间的规则，带 binding key 或 headers 条件。' },
        { term: 'Queue', desc: '存放消息的缓冲区，消息在被确认前一直保留。' },
      ],
      note: '平台以单个嵌入式 broker（rabbit-node-01）运行，启动后自动声明 lab.direct / lab.fanout / lab.topic / lab.headers / lab.dlx 等演示交换机与队列。',
      tasks: ['启动 rabbit-node-01', '打开「交换机与队列」，查看 Producer → Exchange → Queue → Consumer 分层图'],
      tryIt: { tab: 'overview', label: '启动 broker' },
    },
    {
      title: '四种交换机',
      body: ['在「路由试算」中输入 routing key 或 headers，图中会实时高亮命中的路径（只计算不发送）。'],
      points: [
        { term: 'direct', desc: 'routing key 与 binding key 完全相等才命中。试试 lab.direct + order.created。' },
        { term: 'fanout', desc: '忽略 routing key，广播到所有绑定队列。试试 lab.fanout。' },
        { term: 'topic', desc: '按 . 分词通配：* 匹配一个词，# 匹配零或多个词。试试 lab.topic + app.order.error（同时命中 #.error 与 app.#）。' },
        { term: 'headers', desc: '按消息头匹配，x-match=all 要求全部相等，any 满足其一即可。试试 lab.headers + type=invoice, format=pdf。' },
      ],
      tryIt: { tab: 'routing', label: '路由试算' },
    },
    {
      title: '声明自己的拓扑',
      body: ['在「交换机与队列」的管理区可以声明交换机、队列（可设死信交换机与 TTL）与绑定，也可以清空或删除。'],
      tasks: ['声明一个 topic 交换机 my.events 与队列 my.audit', '用 binding key audit.# 绑定，再用路由试算验证 audit.user.login 能命中'],
      tryIt: { tab: 'routing', label: '管理拓扑' },
    },
    {
      title: '消息确认：ack / nack / requeue',
      body: ['手动确认模式下，消费者处理完才发送 basic.ack，broker 收到后才删除消息；处理失败可 basic.nack，选择 requeue=true 退回队列或 requeue=false 丢弃 / 进入死信。'],
      tasks: ['在「确认与死信」运行「手动 ack」实验，观察 ready → unacked → acked', '运行「nack 后重新入队」，结果中 redelivered=是 的消息即被重投过'],
      tryIt: { tab: 'ack', label: '运行确认实验' },
    },
    {
      title: 'prefetch（basic.qos）',
      body: ['prefetch 限制一个消费者同时持有的未确认消息数。超出部分留在队列 ready 中，直到有消息被 ack。合理的 prefetch 可以在吞吐与公平分发之间取得平衡。'],
      tasks: ['运行「超出 prefetch」实验：常驻消费者 prefetch=2、每条处理 1 秒', '观察 unacked 始终不超过 2，ready 逐步减少'],
      tryIt: { tab: 'ack', label: '运行 prefetch 实验' },
    },
    {
      title: '死信与 TTL',
      body: [
        '队列可配置死信交换机（DLX）：被 nack(requeue=false) 的消息会转发到 DLX，再路由到死信队列，便于排查与补偿。',
        'TTL 可设置在消息（expiration）或队列上，到期未被消费的消息会过期。',
      ],
      note: '嵌入式 Qpid 以 alternate binding 实现死信；真实 RabbitMQ 中过期消息也会进入死信队列，而 Qpid 会直接丢弃——平台对此在界面上禁用并注明「Qpid 不支持（真实 RabbitMQ 支持）」。',
      tasks: ['运行「nack 进入死信」，观察 dead.letters 队列的 ready 增加', '运行「TTL 过期」，结果显示为「已过期」'],
      tryIt: { tab: 'ack', label: '运行死信实验' },
    },
    {
      title: 'publisher confirms 与不可路由消息',
      body: ['开启 confirm 后 broker 对每条消息回 ack / nack，生产者据此知道消息是否被可靠接收；mandatory 消息若没有任何队列命中，会通过 basic.return 退回。'],
      tasks: ['在消息工作台向 lab.direct 发送 routing key 为 nothing 的消息，结果显示「不可路由」'],
      tryIt: { tab: 'workbench', label: '发送消息' },
    },
  ],
  advanced: [
    {
      title: '集群、镜像队列与 Quorum Queue',
      body: ['生产环境的 RabbitMQ 通常以 3 节点集群部署，使用基于 Raft 的 Quorum Queue 实现队列高可用（经典镜像队列已废弃）。平台为单节点嵌入，不模拟集群。'],
    },
    {
      title: 'Streams',
      body: ['RabbitMQ 3.9+ 提供 Stream：可重复读取的追加日志，按 offset 消费，语义接近 Kafka 分区，适合回放与大扇出场景。'],
    },
    {
      title: '延迟消息',
      body: ['RabbitMQ 原生没有延迟队列，常见做法是「TTL + 死信」或使用 rabbitmq_delayed_message_exchange 插件。'],
    },
  ],
}

export const GUIDES: Record<MqSlug, Guide> = { rocketmq: ROCKETMQ, kafka: KAFKA, rabbitmq: RABBITMQ }
