package com.example.clustermanager.core.model;

/**
 * Provider 能力常量，领域核心层的字符串常量集合。
 *
 * <p>{@link ProviderDescriptor#capabilities()} 的取值来源。前端据此决定展示哪些标签页与操作按钮，
 * 后端 {@code ClusterProviderRegistry#resolveCapability} 据此校验 provider 是否实现了专属检查端口。
 */
public final class ProviderCapabilities {

    /** 支持运行时连接配置（RocketMQ NameServer 地址等）。 */
    public static final String CONNECTION_CONFIG = "CONNECTION_CONFIG";
    /** 支持手动登记/删除服务节点。 */
    public static final String SERVICE_REGISTRATION = "SERVICE_REGISTRATION";
    /** 支持集群级一键启停。 */
    public static final String CLUSTER_OPERATIONS = "CLUSTER_OPERATIONS";
    /** 支持 Kafka topic/分区/消费者组检查（实现 {@code IKafkaInspector}）。 */
    public static final String KAFKA_INSPECTION = "KAFKA_INSPECTION";
    /** 支持 RabbitMQ 交换机/队列/绑定检查（实现 {@code IRabbitInspector}）。 */
    public static final String RABBIT_INSPECTION = "RABBIT_INSPECTION";
    /** 支持压力实验与内部机制可视化（实现 {@code IStressLab} 与 {@code IInternalsInspector}）。 */
    public static final String LAB = "LAB";

    private ProviderCapabilities() {
    }
}
