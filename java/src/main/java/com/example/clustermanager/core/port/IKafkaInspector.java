package com.example.clustermanager.core.port;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.kafka.KafkaConsumerGroupDetail;
import com.example.clustermanager.core.model.kafka.KafkaGroupMember;
import com.example.clustermanager.core.model.kafka.KafkaLeaderChange;
import com.example.clustermanager.core.model.kafka.KafkaTopicDetail;
import java.util.List;
import java.util.Map;

/**
 * Kafka 专属检查端口（六边形架构的出站端口）。
 *
 * <p>由 Kafka provider 实现，提供 topic/分区/副本/ISR、消费者组/rebalance/lag 的读取与管理能力。
 * 应用层通过 {@code ClusterProviderRegistry#resolveCapability} 获取。
 */
public interface IKafkaInspector {

    /**
     * 列出 topic（含分区 leader/副本/ISR/LEO）。
     *
     * @param clusterRef      集群引用
     * @param includeInternal 是否包含内部 topic
     * @return topic 列表，按名称排序
     * @throws IllegalStateException 集群没有可用 broker 时
     */
    List<KafkaTopicDetail> listTopics(ClusterRef clusterRef, boolean includeInternal);

    /**
     * 查询单个 topic。
     *
     * @throws IllegalArgumentException topic 不存在时
     */
    KafkaTopicDetail describeTopic(ClusterRef clusterRef, String topic);

    /**
     * 创建 topic。
     *
     * @param partitions        分区数（≥1）
     * @param replicationFactor 副本因子（1..运行中 broker 数）
     * @param configs           topic 级配置，如 {@code min.insync.replicas}；可为空
     * @throws IllegalArgumentException 参数非法或 topic 已存在时
     */
    KafkaTopicDetail createTopic(ClusterRef clusterRef, String topic, int partitions, int replicationFactor, Map<String, String> configs);

    /** 删除 topic。 */
    OperationResult deleteTopic(ClusterRef clusterRef, String topic);

    /** 列出消费者组（含成员、分配与 lag）。 */
    List<KafkaConsumerGroupDetail> listConsumerGroups(ClusterRef clusterRef);

    /**
     * 查询单个消费者组。
     *
     * @throws IllegalArgumentException 组不存在时
     */
    KafkaConsumerGroupDetail describeConsumerGroup(ClusterRef clusterRef, String groupId);

    /**
     * 增加一个常驻消费者成员，加入后触发 rebalance。
     *
     * @param ratePerSecond 每秒处理条数，{@code 0} 表示暂停（用于人为制造 lag）
     */
    KafkaGroupMember addConsumer(ClusterRef clusterRef, String groupId, String topic, int ratePerSecond);

    /** 调整常驻消费者的处理速率。 */
    KafkaGroupMember updateConsumerRate(ClusterRef clusterRef, String groupId, String memberId, int ratePerSecond);

    /** 移除常驻消费者成员，离开后触发 rebalance。 */
    OperationResult removeConsumer(ClusterRef clusterRef, String groupId, String memberId);

    /** 最近的分区 leader 切换事件（新到旧）。 */
    List<KafkaLeaderChange> leaderChanges(ClusterRef clusterRef, int limit);
}
