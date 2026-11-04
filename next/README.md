# MQCluster · Web 前端

MQCluster 的浏览器端：一个 Next.js 16（App Router）+ React 19 + TypeScript 严格模式的控制台，
把后端三种嵌入式的真实 MQ 集群（RocketMQ / Kafka / RabbitMQ-兼容的 Qpid）变成可点的拓扑、
可读的指标、可压垮的流量。

> 完整说明、启动步骤与架构深挖在仓库根目录：
> **[README.md](../README.md)** · **[README.zh.md](../README.zh.md)** · **[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)**

## 快速开始

```bash
npm install
npm run dev          # http://localhost:3000，需要后端已在 8088 运行
```

`/api/*` 与 `/ws/*` 由 `next.config.ts` 的 rewrites 代理到后端，默认 `http://localhost:8088`，
可用 `next/.env.local` 的 `BACKEND_URL` 覆盖。

## 页面

| 路由 | 内容 |
| --- | --- |
| `/` | 首页：三个 MQ 入口卡片、实时运行状态、一键启停、学习路径 |
| `/mq/rocketmq` | 概览 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 · 连接配置 |
| `/mq/kafka` | 概览 · Topic 与分区 · 消费者组 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/mq/rabbitmq` | 概览 · 交换机与队列 · 确认与死信 · 消息工作台 · 压力实验 · 内部机制 · 监控 · 日志 |
| `/compare` | 三种 MQ 的存储、路由、顺序性、消费模型、高可用对比 |
| `/guide`, `/guide/rocketmq` 等 | 分步学习指南，每步带「去仪表盘试试」的深链 |

标签页状态保存在 `?tab=`，刷新不丢。`压力实验` / `内部机制` 两个标签页由后端 `LAB` 能力控制，
只有实现了对应端口的 provider 才会出现。

## 目录

```
src/
├── app/            App Router：/, mq/[middleware], compare, guide/[middleware]
├── components/     dashboard/（通用标签页 + MqDashboard）、lab/、kafka/、rabbit/、ui/（shadcn）
├── hooks/          use-cluster-data（轮询 + STOMP）、use-node-operations、use-chart-palette
├── lib/            api.ts（类型化 axios 客户端）、mq-registry.ts（MQ 元数据与标签页）、guides.ts
└── types/          cluster.ts、kafka.ts、rabbit.ts、lab.ts（与后端 DTO 对应）
```

## 校验

```bash
npx tsc --noEmit     # 类型检查
npm run lint         # ESLint
npm run build        # 生产构建（12 个静态页面）
```
