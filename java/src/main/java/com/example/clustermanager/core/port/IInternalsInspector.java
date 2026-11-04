package com.example.clustermanager.core.port;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;

/**
 * 内部机制检查端口（六边形架构的出站端口）。
 *
 * <p>由各 MQ provider 实现，读取 broker 此刻的真实内部状态：CommitLog 写指针、
 * ConsumeQueue 构建进度、Kafka 段文件大小、Qpid 对象模型与队列深度。
 *
 * <p>应用层通过 {@code ClusterProviderRegistry#resolveCapability} 获取，不做任何模拟。
 */
public interface IInternalsInspector {

    /**
     * 采集一次内部机制快照。
     *
     * @param clusterRef 集群引用
     * @return 真实内部状态；集群未运行时抛 {@link IllegalArgumentException}
     * @throws IllegalStateException broker 已停止或内部对象不可用时
     */
    InternalsSnapshot inspect(ClusterRef clusterRef);
}
