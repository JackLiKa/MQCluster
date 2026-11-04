package com.example.clustermanager.infrastructure.kafka.topology;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Kafka 拓扑种子器：定义 1 个 KRaft controller + 3 个 broker 的固定节点集合。
 *
 * <p>节点 ID 与 Kafka {@code node.id} 的对应关系固定：controller 为 3000，broker 为 1/2/3。
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopologySeeder {

    /** controller 的 Kafka node.id。 */
    public static final int CONTROLLER_NODE_ID = 3000;

    /**
     * 返回种子节点（controller 在前）。
     *
     * @return 种子节点列表
     */
    public List<KafkaNodeSeed> seeds() {
        return List.of(
                new KafkaNodeSeed("kafka-ctrl-01", "KRaft Controller", KafkaNodeSeed.Role.CONTROLLER, CONTROLLER_NODE_ID),
                new KafkaNodeSeed("kafka-broker-01", "Broker 1", KafkaNodeSeed.Role.BROKER, 1),
                new KafkaNodeSeed("kafka-broker-02", "Broker 2", KafkaNodeSeed.Role.BROKER, 2),
                new KafkaNodeSeed("kafka-broker-03", "Broker 3", KafkaNodeSeed.Role.BROKER, 3)
        );
    }

    /**
     * 种子节点定义。
     *
     * @param nodeId      平台节点 ID
     * @param displayName 展示名
     * @param role        角色
     * @param kafkaNodeId Kafka node.id
     */
    public record KafkaNodeSeed(String nodeId, String displayName, Role role, int kafkaNodeId) {

        /** 节点角色。 */
        public enum Role {
            CONTROLLER("kafka-controller"),
            BROKER("kafka-broker");

            private final String label;

            Role(String label) {
                this.label = label;
            }

            /** 拓扑 {@code labels.role} 的取值。 */
            public String label() {
                return label;
            }
        }
    }
}
