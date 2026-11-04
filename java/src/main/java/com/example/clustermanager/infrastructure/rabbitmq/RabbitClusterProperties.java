package com.example.clustermanager.infrastructure.rabbitmq;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 嵌入式 RabbitMQ（Qpid Broker-J）配置属性，绑定 {@code cluster.rabbitmq.*}。
 *
 * @param enabled   是否启用 RabbitMQ provider（默认 true；某平台嵌入式运行异常时可单独关闭）
 * @param clusterId 平台侧集群 ID（默认 {@code rabbitmq-lab}）
 * @param workDir   Qpid 工作目录（默认 {@code run/rabbitmq-cluster}，每次启动前清空）
 */
@ConfigurationProperties(prefix = "cluster.rabbitmq")
public record RabbitClusterProperties(
        Boolean enabled,
        String clusterId,
        String workDir
) {

    public String resolvedClusterId() {
        return clusterId == null || clusterId.isBlank() ? "rabbitmq-lab" : clusterId;
    }

    public String resolvedWorkDir() {
        return workDir == null || workDir.isBlank() ? "run/rabbitmq-cluster" : workDir;
    }
}
