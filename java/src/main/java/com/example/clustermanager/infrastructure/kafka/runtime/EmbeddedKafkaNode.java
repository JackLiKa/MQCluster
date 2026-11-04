package com.example.clustermanager.infrastructure.kafka.runtime;

import java.time.Instant;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import org.apache.kafka.common.utils.Time;

/**
 * 单个嵌入式 KRaft 节点（controller 或 broker）的进程内封装。
 *
 * <p>{@link KafkaRaftServer} 停止后不能再次启动，重启必须用同一份配置新建实例，
 * 因此本类一次性使用：{@link #start()} → {@link #stop()} 后即丢弃。
 */
public final class EmbeddedKafkaNode {

    private final KafkaNodeSpec spec;
    private final KafkaRaftServer server;
    private volatile Instant startedAt;

    /**
     * @param spec   节点规格
     * @param config Kafka 节点属性（由 {@link KafkaNodeSpec#toProperties(String)} 生成）
     */
    public EmbeddedKafkaNode(KafkaNodeSpec spec, Properties config) {
        this.spec = spec;
        this.server = new KafkaRaftServer(new KafkaConfig(config, false), Time.SYSTEM);
    }

    /** 启动节点。broker 会阻塞直到向 controller 注册完成。 */
    public void start() {
        server.startup();
        startedAt = Instant.now();
    }

    /** 停止节点并等待资源释放（线程、端口、日志文件句柄）。 */
    public void stop() {
        server.shutdown();
        server.awaitShutdown();
    }

    public KafkaNodeSpec spec() {
        return spec;
    }

    public Instant startedAt() {
        return startedAt;
    }
}
