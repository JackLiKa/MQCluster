package com.example.clustermanager.infrastructure.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 嵌入式 Kafka 集群配置属性，绑定 {@code cluster.kafka.*}。
 *
 * @param enabled           是否启用 Kafka provider（默认 true；某平台嵌入式运行异常时可单独关闭）
 * @param clusterId         平台侧集群 ID（默认 {@code kafka-lab}）
 * @param workDir           KRaft 日志与元数据目录（默认 {@code run/kafka-cluster}）
 * @param cleanStoreOnStart 首次启动前是否清空工作目录（默认 true，保证每次进程启动都是干净集群）
 */
@ConfigurationProperties(prefix = "cluster.kafka")
public record KafkaClusterProperties(
        Boolean enabled,
        String clusterId,
        String workDir,
        Boolean cleanStoreOnStart
) {

    public String resolvedClusterId() {
        return clusterId == null || clusterId.isBlank() ? "kafka-lab" : clusterId;
    }

    public String resolvedWorkDir() {
        return workDir == null || workDir.isBlank() ? "run/kafka-cluster" : workDir;
    }

    public boolean resolvedCleanStoreOnStart() {
        return cleanStoreOnStart == null || cleanStoreOnStart;
    }
}
