package com.example.clustermanager.core.model;

/**
 * 中间件类型枚举。
 *
 * <p>位于六边形架构的领域核心层，用于标识集群所承载的中间件品类。
 * 该枚举是 {@code ClusterProviderRegistry} 选择适配器的关键维度之一
 * （另一个维度为 {@link ClusterMode}）。新增中间件支持时，
 * 只需新增枚举值并在 infrastructure 层实现对应适配器，core 层无需改动。
 * 作为 Java 枚举，其实例天然不可变且线程安全。
 */
public enum MiddlewareType {
    /** Apache RocketMQ 消息中间件。 */
    ROCKETMQ,
    /** Apache Kafka 消息中间件（嵌入式 KRaft 集群）。 */
    KAFKA,
    /** RabbitMQ / AMQP 0-9-1 消息中间件（嵌入式 Qpid Broker-J）。 */
    RABBITMQ
}
