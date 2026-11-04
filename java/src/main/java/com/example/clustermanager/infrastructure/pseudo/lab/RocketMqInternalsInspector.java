package com.example.clustermanager.infrastructure.pseudo.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;
import com.example.clustermanager.core.port.IInternalsInspector;
import com.example.clustermanager.infrastructure.pseudo.node.ManagedNode;
import com.example.clustermanager.infrastructure.pseudo.node.NodeRegistry;
import com.example.clustermanager.infrastructure.pseudo.runtime.EmbeddedRocketMqRuntime;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * RocketMQ 内部机制检查器：直接读取运行中 Broker 的存储引擎对象。
 *
 * <p>四项真实数据：
 * <ul>
 *   <li>{@code CommitLog#getMappedFileQueue()} —— 每个 1 GB 映射文件实际的写指针；</li>
 *   <li>{@code MessageStore#dispatchBehindBytes()} —— 写入速度超过 ConsumeQueue 构建速度时积压的字节，过载最直观的信号；</li>
 *   <li>{@code StoreStatsService} —— 累计写入次数、写入字节、失败次数；</li>
 *   <li>磁盘上的 {@code consumequeue/} 与 {@code index/} 文件数量。</li>
 * </ul>
 */
@Component
public class RocketMqInternalsInspector implements IInternalsInspector {

    private static final Logger log = LoggerFactory.getLogger(RocketMqInternalsInspector.class);

    private final EmbeddedRocketMqRuntime runtime;
    private final NodeRegistry nodeRegistry;

    public RocketMqInternalsInspector(EmbeddedRocketMqRuntime runtime, NodeRegistry nodeRegistry) {
        this.runtime = runtime;
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public InternalsSnapshot inspect(ClusterRef clusterRef) {
        List<BrokerView> brokers = runningBrokers();
        if (brokers.isEmpty()) {
            throw new IllegalStateException("RocketMQ 集群未运行：请先启动 Broker");
        }

        long writtenBytes = 0;
        long putTimes = 0;
        long putFailed = 0;
        long behindBytes = 0;
        long consumeQueueFiles = 0;
        long indexFiles = 0;
        long diskFree = 0;
        List<InternalsSnapshot.Segment> segments = new ArrayList<>();

        for (BrokerView view : brokers) {
            MessageStore store = view.controller().getMessageStore();
            CommitLog commitLog = store.getCommitLog();
            writtenBytes += Math.max(0, commitLog.getMaxOffset() - commitLog.getMinOffset());
            behindBytes += Math.max(0, store.dispatchBehindBytes());
            if (store instanceof DefaultMessageStore defaultStore) {
                putTimes += defaultStore.getStoreStatsService().getPutMessageTimesTotal();
                putFailed += defaultStore.getStoreStatsService().getPutMessageFailedTimes().sum();
            }
            for (MappedFile file : commitLog.getMappedFileQueue().getMappedFiles()) {
                segments.add(new InternalsSnapshot.Segment(
                        view.node().nodeId(),
                        file.getFileName(),
                        file.getWrotePosition(),
                        "B",
                        "offset %d 起 · 文件上限 %s".formatted(file.getFileFromOffset(), humanBytes(file.getFileSize()))));
            }
            Path root = Path.of(view.controller().getMessageStoreConfig().getStorePathRootDir());
            consumeQueueFiles += countFiles(root.resolve("consumequeue"));
            indexFiles += countFiles(root.resolve("index"));
            diskFree = Math.max(diskFree, usableSpace(root));
        }

        String headline = "%d 个 Broker · CommitLog 已写 %s · 累计 %s 条 · 分发落后 %s".formatted(
                brokers.size(), humanBytes(writtenBytes), humanCount(putTimes), humanBytes(behindBytes));

        List<InternalsSnapshot.Stage> pipeline = List.of(
                new InternalsSnapshot.Stage("写入 CommitLog", humanBytes(writtenBytes),
                        "所有 topic 共用一条顺序追加日志；消息体 + 属性 + 索引全部写在这一个文件里"),
                new InternalsSnapshot.Stage("构建 ConsumeQueue", humanBytes(behindBytes) + " 待分发",
                        "按 topic/队列拆出 20 字节定长索引（offset+size+tag hash），落后越多说明写入越猛"),
                new InternalsSnapshot.Stage("构建 IndexFile", indexFiles + " 个索引文件",
                        "按消息 Key 建哈希索引，用于按 key 查消息；不影响主流程，但同样消耗 IO"),
                new InternalsSnapshot.Stage("消费拉取", consumeQueueFiles + " 个 ConsumeQueue 文件",
                        "消费者按队列 offset 顺序拉取，拉取位置完全由 ConsumerQueue 的定长索引驱动"));

        List<InternalsSnapshot.Metric> metrics = List.of(
                new InternalsSnapshot.Metric("累计写入消息", humanCount(putTimes) + " 条", "StoreStatsService 的真实计数"),
                new InternalsSnapshot.Metric("写入失败", String.valueOf(putFailed), "存储层拒绝的写入次数"),
                new InternalsSnapshot.Metric("分发落后", humanBytes(behindBytes), ">0 表示写入速度超过 ConsumeQueue 构建速度"),
                new InternalsSnapshot.Metric("ConsumeQueue 文件", String.valueOf(consumeQueueFiles), "每个 topic/队列一个目录，每个文件存 30 万条索引"),
                new InternalsSnapshot.Metric("IndexFile 数量", String.valueOf(indexFiles), "每个文件约 2000 万条 key 索引槽位"),
                new InternalsSnapshot.Metric("磁盘剩余", humanBytes(diskFree), "CommitLog 所在文件系统的可用空间"));

        List<String> notes = List.of(
                "RocketMQ 的顺序写来自：所有 topic 只追加同一个 CommitLog，写指针到了就换下一个 1 GB 映射文件。",
                "只有 CommitLog 是真正的消息存储；ConsumeQueue 是定长 20 字节的「目录」，消息被消费后 CommitLog 里的数据不会立刻删除。",
                "压测时可以盯着「分发落后」：它持续增长，说明 broker 的写线程已经把分发线程甩在后面了。",
                "消息体超过 4 KB 时 broker 会自动压缩（compressMsgBodyOverhowmuch），所以压测后 CommitLog 的增量往往远小于「条数 × 负载大小」。",
                "本页数据直接来自进程内的 BrokerController 与磁盘文件，不经过任何统计估算。");

        return new InternalsSnapshot(headline, pipeline, segments, metrics, notes);
    }

    private List<BrokerView> runningBrokers() {
        List<BrokerView> views = new ArrayList<>();
        for (ManagedNode node : nodeRegistry.brokers()) {
            try {
                views.add(new BrokerView(node, runtime.brokerController(node.nodeId())));
            } catch (IllegalStateException stopped) {
                // 节点未运行：跳过
            }
        }
        return views;
    }

    private static long countFiles(Path dir) {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (Stream<Path> stream = Files.walk(dir, 4)) {
            return stream.filter(Files::isRegularFile).count();
        } catch (IOException exception) {
            log.debug("count files in {} failed: {}", dir, exception.getMessage());
            return 0;
        }
    }

    private static long usableSpace(Path path) {
        try {
            return Files.getFileStore(path).getUsableSpace();
        } catch (IOException exception) {
            return 0;
        }
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return "%.1f KB".formatted(bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return "%.1f MB".formatted(bytes / 1024.0 / 1024);
        }
        return "%.2f GB".formatted(bytes / 1024.0 / 1024 / 1024);
    }

    private static String humanCount(long value) {
        if (value < 10_000) {
            return String.valueOf(value);
        }
        if (value < 1_000_000) {
            return "%.1f 万".formatted(value / 10_000.0);
        }
        return "%.2f 亿".formatted(value / 100_000_000.0);
    }

    /** 一个运行中的 broker 与它的控制器。 */
    private record BrokerView(ManagedNode node, BrokerController controller) {
    }
}
