package com.example.clustermanager.core.port;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.OperationType;
import java.util.List;

/**
 * 节点管理端口（六边形架构的出站端口）。
 *
 * <p>定义领域核心层对节点生命周期操作（启动/停止/重启）能力的抽象。
 * infrastructure 层的适配器实现该端口，执行真实的节点启停逻辑。
 * 该端口不引入任何外部依赖。
 */
public interface INodeManager {

    /**
     * 对指定集群中的目标节点执行生命周期操作。
     *
     * @param clusterRef    目标集群引用
     * @param nodeId        目标节点标识
     * @param operationType 操作类型（START / STOP / RESTART）
     * @return 操作结果，包含目标标识、操作类型、成败与提示信息
     * @throws IllegalArgumentException 若节点不存在或操作类型为 {@code null}
     * @throws IllegalStateException    若节点当前状态不允许该操作（如对已停止节点执行 STOP）
     */
    OperationResult operateNode(ClusterRef clusterRef, String nodeId, OperationType operationType);

    /**
     * 对整个集群执行一键生命周期操作。
     *
     * <p>启动/停止顺序由各 provider 决定（如 RocketMQ 先 NameServer 后 Broker、Kafka 先 controller 后 broker；
     * 停止时逆序）。已处于目标状态的节点会被跳过，并在结果中注明。
     *
     * @param clusterRef    目标集群引用
     * @param operationType 操作类型（START / STOP / RESTART）
     * @return 每个节点的操作结果，按执行顺序排列
     * @throws IllegalArgumentException 若 provider 不支持集群级操作
     */
    default List<OperationResult> operateCluster(ClusterRef clusterRef, OperationType operationType) {
        throw new IllegalArgumentException("该集群不支持一键操作: " + clusterRef.clusterId());
    }
}
