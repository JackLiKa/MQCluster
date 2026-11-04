package com.example.clustermanager.infrastructure.kafka.runtime;

import com.example.clustermanager.infrastructure.kafka.topology.KafkaTopologySeeder.KafkaNodeSeed;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 嵌入式 Kafka 节点规格：种子定义 + 固定端口 + 日志目录。
 *
 * <p>端口在集群首次准备时分配并在进程生命周期内保持不变——controller 端口写死在
 * {@code controller.quorum.voters} 中，broker 重启复用同端口与同日志目录才能以原身份重新加入 ISR。
 *
 * @param seed   种子节点定义
 * @param port   监听端口（controller 为 CONTROLLER listener，broker 为 PLAINTEXT listener）
 * @param logDir 日志目录（KRaft 元数据日志也存放于此）
 */
public record KafkaNodeSpec(KafkaNodeSeed seed, int port, Path logDir) {

    public String nodeId() {
        return seed.nodeId();
    }

    public boolean controller() {
        return seed.role() == KafkaNodeSeed.Role.CONTROLLER;
    }

    public String address() {
        return "127.0.0.1:" + port;
    }

    /**
     * 生成 KafkaConfig 所需的属性。
     *
     * @param voters {@code controller.quorum.voters} 取值
     * @return Kafka 节点属性
     */
    public Properties toProperties(String voters) {
        Properties p = new Properties();
        p.put("node.id", String.valueOf(seed.kafkaNodeId()));
        p.put("controller.quorum.voters", voters);
        p.put("controller.listener.names", "CONTROLLER");
        p.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        p.put("log.dirs", logDir.toString());
        if (controller()) {
            p.put("process.roles", "controller");
            p.put("listeners", "CONTROLLER://127.0.0.1:" + port);
            return p;
        }
        p.put("process.roles", "broker");
        p.put("listeners", "PLAINTEXT://127.0.0.1:" + port);
        p.put("advertised.listeners", "PLAINTEXT://127.0.0.1:" + port);
        p.put("inter.broker.listener.name", "PLAINTEXT");
        p.put("auto.create.topics.enable", "false");
        p.put("num.partitions", "3");
        p.put("offsets.topic.replication.factor", "3");
        p.put("offsets.topic.num.partitions", "6");
        p.put("transaction.state.log.replication.factor", "3");
        p.put("transaction.state.log.min.isr", "1");
        p.put("share.coordinator.state.topic.replication.factor", "1");
        p.put("share.coordinator.state.topic.min.isr", "1");
        p.put("group.initial.rebalance.delay.ms", "0");
        p.put("replica.lag.time.max.ms", "10000");
        p.put("log.segment.bytes", String.valueOf(16 * 1024 * 1024));
        // 阶段 0 实测：log cleaner 默认每个 broker 预分配 128MB 去重缓冲区，3 broker 即 384MB 堆，调小到 2MB
        p.put("log.cleaner.dedupe.buffer.size", String.valueOf(2 * 1024 * 1024));
        p.put("log.cleaner.threads", "1");
        p.put("num.network.threads", "2");
        p.put("num.io.threads", "2");
        p.put("num.replica.fetchers", "1");
        p.put("background.threads", "2");
        return p;
    }
}
