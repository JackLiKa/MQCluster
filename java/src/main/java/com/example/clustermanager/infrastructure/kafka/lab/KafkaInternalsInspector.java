package com.example.clustermanager.infrastructure.kafka.lab;

import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;
import com.example.clustermanager.core.port.IInternalsInspector;
import com.example.clustermanager.infrastructure.kafka.runtime.EmbeddedKafkaRuntime;
import com.example.clustermanager.infrastructure.kafka.runtime.KafkaNodeSpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Kafka 内部机制检查器：读取各 broker 的日志目录、段文件与分区末端 offset。
 *
 * <p>数据来源全部是真实文件与 Admin API：
 * <ul>
 *   <li>{@code describeLogDirs} —— 每个副本的磁盘占用与 offsetLag；</li>
 *   <li>{@code listOffsets(latest)} —— 每个分区的 LEO（末端 offset）；</li>
 *   <li>{@code Files.walk} —— 磁盘上真实存在的 {@code .log / .index / .timeindex} 段文件。</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(prefix = "cluster.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaInternalsInspector implements IInternalsInspector {

    private static final Logger log = LoggerFactory.getLogger(KafkaInternalsInspector.class);
    private static final int MAX_SEGMENTS_PER_BROKER = 8;

    private final EmbeddedKafkaRuntime runtime;

    public KafkaInternalsInspector(EmbeddedKafkaRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public InternalsSnapshot inspect(ClusterRef clusterRef) {
        runtime.requireAvailable();
        List<KafkaNodeSpec> brokers = runtime.specs().stream()
                .filter(spec -> !spec.controller() && runtime.isRunning(spec.nodeId()))
                .toList();
        Map<Integer, List<LogDirDescription>> logDirs = describeLogDirs(brokers);
        Map<TopicPartition, Long> logEndOffsets = logEndOffsets(logDirs);

        long totalBytes = 0;
        long totalReplicas = 0;
        long offsetLagSum = 0;
        for (List<LogDirDescription> dirs : logDirs.values()) {
            for (LogDirDescription dir : dirs) {
                for (ReplicaInfo info : dir.replicaInfos().values()) {
                    totalReplicas++;
                    totalBytes += info.size();
                    offsetLagSum += Math.max(0, info.offsetLag());
                }
            }
        }
        long underReplicated = countUnderReplicated();

        long totalRecords = logEndOffsets.values().stream().mapToLong(Long::longValue).sum();
        List<InternalsSnapshot.Segment> segments = segmentFiles(brokers);
        long indexFiles = countIndexFiles(brokers);

        String headline = "%d 个 broker · %d 个分区副本 · 磁盘占用 %s · 共 %s 条记录（LEO 合计）".formatted(
                brokers.size(), totalReplicas, humanBytes(totalBytes), humanCount(totalRecords));

        List<InternalsSnapshot.Stage> pipeline = List.of(
                new InternalsSnapshot.Stage("Producer 追加", humanCount(totalRecords) + " 条",
                        "每条记录先进入客户端缓冲区，按 batch 打包后一次性发送；offset 在此刻分配"),
                new InternalsSnapshot.Stage("顺序写段文件", humanBytes(totalBytes),
                        "多条记录合成 RecordBatch 追加到 .log 段末尾；段文件按 segment.bytes 预分配"),
                new InternalsSnapshot.Stage("稀疏索引", indexFiles + " 个索引文件",
                        ".index 每写入 4 KB 记录一个 offset→物理位置，查找时先二分索引再顺序扫段"),
                new InternalsSnapshot.Stage("副本同步（ISR）", underReplicated + " 个分区 ISR 不足",
                        "follower 主动向 leader 拉取；ISR 不足时 acks=all 的写入会等待或失败"));

        List<InternalsSnapshot.Metric> metrics = List.of(
                new InternalsSnapshot.Metric("运行中 broker", String.valueOf(brokers.size()), "只有 broker 持有日志副本，controller 不存数据"),
                new InternalsSnapshot.Metric("分区副本总数", String.valueOf(totalReplicas), "describeLogDirs 返回的副本目录数量"),
                new InternalsSnapshot.Metric("副本磁盘占用", humanBytes(totalBytes), "含预分配：Kafka 按 segment.bytes 预留段文件空间"),
                new InternalsSnapshot.Metric("记录总数", humanCount(totalRecords), "所有分区 LEO（末端 offset）之和"),
                new InternalsSnapshot.Metric("副本落后合计", humanBytes(offsetLagSum), "follower 相对 leader 尚未追上的字节数"),
                new InternalsSnapshot.Metric("ISR 不足分区", String.valueOf(underReplicated), "副本数少于配置值的分区，容灾能力下降"));

        List<String> notes = List.of(
                "Kafka 不为每条消息单独建文件：一个分区一个目录，目录内是多组 .log + .index + .timeindex 段。",
                "offset 是分区内的逻辑位置，只增不减；删除消息靠保留策略把整个旧段文件删掉，而不是逐条删除。",
                "读取顺序：先看 .index 找到最近的 offset 锚点，再从 .log 顺序扫到目标位置，所以顺序读极快、随机读代价高。",
                "段文件按 segment.bytes 预分配（本平台 16 MB），所以磁盘占用会整段跳增，而不是随每条消息线性增长；"
                        + "下面列出的是 .log 数据段，索引文件单独计数。");

        return new InternalsSnapshot(headline, pipeline, segments, metrics, notes);
    }

    private Map<Integer, List<LogDirDescription>> describeLogDirs(List<KafkaNodeSpec> brokers) {
        Map<Integer, List<LogDirDescription>> result = new HashMap<>();
        if (brokers.isEmpty()) {
            return result;
        }
        List<Integer> ids = brokers.stream().map(spec -> spec.seed().kafkaNodeId()).toList();
        try {
            var byBroker = runtime.admin().describeLogDirs(ids).allDescriptions().get(10, TimeUnit.SECONDS);
            byBroker.forEach((brokerId, dirs) -> result.put(brokerId, new ArrayList<>(dirs.values())));
        } catch (Exception exception) {
            log.debug("describeLogDirs failed: {}", exception.getMessage());
        }
        return result;
    }

    private Map<TopicPartition, Long> logEndOffsets(Map<Integer, List<LogDirDescription>> logDirs) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (List<LogDirDescription> dirs : logDirs.values()) {
            for (LogDirDescription dir : dirs) {
                dir.replicaInfos().keySet().forEach(partition -> request.put(partition, OffsetSpec.latest()));
            }
        }
        Map<TopicPartition, Long> result = new HashMap<>();
        if (request.isEmpty()) {
            return result;
        }
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                    runtime.admin().listOffsets(request).all().get(10, TimeUnit.SECONDS);
            offsets.forEach((partition, info) -> result.put(partition, info.offset()));
        } catch (Exception exception) {
            log.debug("listOffsets failed: {}", exception.getMessage());
        }
        return result;
    }

    private long countUnderReplicated() {
        long underReplicated = 0;
        try {
            var names = runtime.admin().listTopics().names().get(8, TimeUnit.SECONDS);
            if (names.isEmpty()) {
                return 0;
            }
            var descriptions = runtime.admin().describeTopics(names).allTopicNames().get(10, TimeUnit.SECONDS);
            for (TopicDescription description : descriptions.values()) {
                for (TopicPartitionInfo info : description.partitions()) {
                    if (info.isr().size() < info.replicas().size()) {
                        underReplicated++;
                    }
                }
            }
        } catch (Exception exception) {
            log.debug("describeTopics failed: {}", exception.getMessage());
        }
        return underReplicated;
    }

    /** 遍历运行中 broker 的日志目录，取出真实的数据段文件（.log），按最后写入时间取最新的若干个。 */
    private List<InternalsSnapshot.Segment> segmentFiles(List<KafkaNodeSpec> brokers) {
        List<InternalsSnapshot.Segment> segments = new ArrayList<>();
        for (KafkaNodeSpec spec : brokers) {
            Path dir = spec.logDir();
            if (!Files.isDirectory(dir)) {
                continue;
            }
            List<InternalsSnapshot.Segment> perBroker = new ArrayList<>();
            try (Stream<Path> stream = Files.walk(dir, 4)) {
                stream.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".log"))
                        .forEach(path -> {
                            try {
                                perBroker.add(new InternalsSnapshot.Segment(
                                        spec.nodeId(),
                                        dir.relativize(path).toString(),
                                        Files.size(path),
                                        "B",
                                        "最后写入 " + Instant.ofEpochMilli(Files.getLastModifiedTime(path).toMillis())
                                                .toString().substring(11, 19)));
                            } catch (IOException ignored) {
                                // 文件在读取过程中被滚动删除，跳过即可
                            }
                        });
            } catch (IOException exception) {
                log.debug("walk {} failed: {}", dir, exception.getMessage());
            }
            // 同一条分区目录里最新的段就是正在写入的段，优先展示
            perBroker.sort(Comparator.comparing(InternalsSnapshot.Segment::detail).reversed());
            segments.addAll(perBroker.stream().limit(MAX_SEGMENTS_PER_BROKER).toList());
        }
        return segments;
    }

    /** 索引文件数量：.index 与 .timeindex 都是预分配大文件，只统计个数，不参与容量比较。 */
    private long countIndexFiles(List<KafkaNodeSpec> brokers) {
        long count = 0;
        for (KafkaNodeSpec spec : brokers) {
            Path dir = spec.logDir();
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (Stream<Path> stream = Files.walk(dir, 4)) {
                count += stream.filter(Files::isRegularFile)
                        .filter(path -> {
                            String name = path.getFileName().toString();
                            return name.endsWith(".index") || name.endsWith(".timeindex");
                        })
                        .count();
            } catch (IOException exception) {
                log.debug("count index files in {} failed: {}", dir, exception.getMessage());
            }
        }
        return count;
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
}
