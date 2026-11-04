package com.example.clustermanager.core.port;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;

/**
 * 压力实验端口（六边形架构的出站端口）。
 *
 * <p>用真实客户端按目标速率向 broker 持续投递消息，暴露 broker 在过载时的真实反应：
 * Kafka 生产者缓冲区耗尽、RocketMQ 流控拒绝、RabbitMQ publisher confirm 超时。
 *
 * <p>实现必须保证：同一集群同一时刻只有一个实验；实验结束后客户端资源全部释放。
 */
public interface IStressLab {

    /**
     * 启动一次实验（立即返回，后台线程持续投递）。
     *
     * @param clusterRef 集群引用
     * @param command    实验参数
     * @return 启动瞬间的状态（phase=RUNNING）
     * @throws IllegalArgumentException 集群未运行或目标 topic/交换机不存在
     * @throws IllegalStateException    已有实验在运行
     */
    StressStatus start(ClusterRef clusterRef, StressCommand command);

    /** 停止当前实验；没有实验时直接返回最近一次结果。 */
    StressStatus stop(ClusterRef clusterRef);

    /** 读取当前/最近一次实验的状态。 */
    StressStatus status(ClusterRef clusterRef);
}
