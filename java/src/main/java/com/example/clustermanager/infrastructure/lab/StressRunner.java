package com.example.clustermanager.infrastructure.lab;

import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;

/**
 * 压测引擎——三种 MQ 共用的"按目标速率持续投递 + 每秒采样"实现。
 *
 * <p>引擎只负责节奏、统计与生命周期；真正的发送动作由各 MQ 通过 {@link Sender} 提供，
 * 因此 Kafka 的缓冲区阻塞、RocketMQ 的流控异常、RabbitMQ 的 confirm 超时都能被原样
 * 计入失败原因，不做任何包装或模拟。
 *
 * <p>关键设计：
 * <ul>
 *   <li>按线程分摊目标速率，落后超过 4 个间隔时重置节拍，避免"补发雪崩"掩盖真实过载；</li>
 *   <li>延迟只统计最近 {@value #LATENCY_WINDOW} 次发送的窗口，用于 p95/p99，不做全量累积；</li>
 *   <li>异步客户端（Kafka/RocketMQ）的失败通过 {@link AsyncSink} 汇入同一份统计。</li>
 * </ul>
 */
public final class StressRunner {

    private static final int LATENCY_WINDOW = 4096;
    private static final int MAX_SAMPLES = 300;
    private static final int MAX_ERRORS = 8;

    /** 单条消息的发送动作，由各 MQ 用真实客户端实现。 */
    public interface Sender extends AutoCloseable {

        /**
         * 发送一条消息。
         *
         * @param sequence 全局递增序号，可用于生成消息 key
         * @throws Exception 客户端拒绝、超时或 broker 返回错误时抛出（原样计入失败原因）
         */
        void send(long sequence) throws Exception;

        /**
         * 是否由客户端在异步回调里上报耗时。
         *
         * <p>异步客户端（Kafka / RocketMQ）的 {@code send()} 只把消息放进本地缓冲就返回，
         * 排队耗时接近 0，真正有意义的是「入队 → broker 确认」的端到端耗时；返回 {@code true}
         * 时引擎改用回调上报的值计算 p95/p99。
         */
        default boolean reportsAsyncLatency() {
            return false;
        }

        /** 引擎通过它把异步回调里的端到端耗时写回延迟窗口。 */
        default void bindLatencySink(LongConsumer sink) {
        }

        /** 等待在途消息结束（异步客户端用），默认无需等待。 */
        default void drain() throws Exception {
        }

        @Override
        default void close() throws Exception {
        }
    }

    /** 异步客户端的失败反馈通道。 */
    public interface AsyncSink {

        /** 累计异步失败数。 */
        long failedCount();

        /** 最近的异步失败原因（可为空列表）。 */
        List<StressStatus.Error> recentErrors();

        /** 空实现，供同步客户端使用。 */
        AsyncSink EMPTY = new AsyncSink() {
            @Override
            public long failedCount() {
                return 0;
            }

            @Override
            public List<StressStatus.Error> recentErrors() {
                return List.of();
            }
        };
    }

    private final StressCommand command;
    private final Sender sender;
    private final AsyncSink asyncSink;

    private final AtomicLong produced = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final long[] latencies = new long[LATENCY_WINDOW];
    private final Map<String, Long> errorCounts = new LinkedHashMap<>();
    private final List<StressStatus.Sample> samples = new ArrayList<>();

    private int latencyCursor;
    private volatile boolean running;
    private volatile String phase = "IDLE";
    private long startedAt;
    /** 实验结束时刻；结束后 elapsed 不再增长，速率也按「发送持续时长」而不是「读取时刻」计算 */
    private long endedAt;
    private long lastSampleAt;
    private long lastProduced;
    private long lastFailed;
    private double lastTps;
    private double lastFailTps;
    private double lastP99;
    private ExecutorService workers;
    private ScheduledExecutorService ticker;

    public StressRunner(StressCommand command, Sender sender, AsyncSink asyncSink) {
        this.command = command;
        this.sender = sender;
        this.asyncSink = asyncSink == null ? AsyncSink.EMPTY : asyncSink;
    }

    /**
     * 启动压测线程与采样线程，立即返回。
     *
     * @throws IllegalStateException 已经有实验在运行
     */
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("压力实验正在运行中，请先停止");
        }
        running = true;
        phase = "RUNNING";
        startedAt = System.currentTimeMillis();
        endedAt = 0;
        lastSampleAt = startedAt;
        sender.bindLatencySink(this::recordLatency);
        int threadCount = command.resolvedThreads();
        workers = Executors.newFixedThreadPool(threadCount, daemonFactory("mq-stress-worker"));
        for (int i = 0; i < threadCount; i++) {
            workers.submit(this::loop);
        }
        ticker = Executors.newSingleThreadScheduledExecutor(daemonFactory("mq-stress-sampler"));
        ticker.scheduleAtFixedRate(this::sampleSafely, 1000, 1000, TimeUnit.MILLISECONDS);
        ticker.schedule(this::finish, command.resolvedDurationSeconds(), TimeUnit.SECONDS);
    }

    /**
     * 停止实验并返回最终状态。
     *
     * <p>不持有实例锁调用 {@link #finish()}：排空在途消息时仍要允许读取快照。
     */
    public StressStatus stop() {
        if (running) {
            finish();
        }
        return snapshot();
    }

    /** 读取当前状态快照。 */
    public synchronized StressStatus snapshot() {
        long totalFailed = failed.get() + asyncSink.failedCount();
        long end = endedAt > 0 ? endedAt : System.currentTimeMillis();
        long elapsed = startedAt == 0 ? 0 : end - startedAt;
        return new StressStatus(
                running,
                phase,
                elapsed,
                command.resolvedTargetRate(),
                produced.get(),
                totalFailed,
                round(lastTps),
                round(lastFailTps),
                round(lastP99),
                List.copyOf(samples),
                mergedErrors(),
                verdict(totalFailed)
        );
    }

    // ------------------------------------------------------------------ 内部实现

    private void loop() {
        int threadCount = command.resolvedThreads();
        long targetRate = command.resolvedTargetRate();
        long perThread = Math.max(1, targetRate / threadCount);
        long intervalNanos = 1_000_000_000L / perThread;
        long next = System.nanoTime();
        long sequence = 0;
        while (running) {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
                continue;
            }
            long before = System.nanoTime();
            try {
                sender.send(sequence++);
                produced.incrementAndGet();
            } catch (Throwable throwable) {
                // 停止实验时 shutdownNow 会打断阻塞中的发送（RabbitMQ 的 waitForConfirmsOrDie 尤其明显），
                // 这类中断不是 broker 的拒绝，不能计进失败率
                if (running) {
                    failed.incrementAndGet();
                    recordError(throwable);
                }
            } finally {
                // 异步客户端在回调里上报端到端耗时，这里统计的只是入队耗时，会掩盖真实过载
                if (!sender.reportsAsyncLatency()) {
                    recordLatency(System.nanoTime() - before);
                }
            }
            next += intervalNanos;
            if (System.nanoTime() - next > intervalNanos * 4) {
                next = System.nanoTime();
            }
        }
    }

    private void recordLatency(long nanos) {
        synchronized (latencies) {
            latencies[latencyCursor] = nanos;
            latencyCursor = (latencyCursor + 1) % LATENCY_WINDOW;
        }
    }

    private void recordError(Throwable throwable) {
        String key = describe(throwable);
        synchronized (errorCounts) {
            if (!errorCounts.containsKey(key) && errorCounts.size() >= MAX_ERRORS) {
                key = "其他错误（超过 " + MAX_ERRORS + " 种）";
            }
            errorCounts.merge(key, 1L, Long::sum);
        }
    }

    /**
     * 把异常归一化成一条可聚合的失败原因。
     *
     * <p>只取首行、截断超长文本，并把变动的数字折叠成 {@code #}——否则
     * 「wait response timeout, cost=3000 / 3001 / 3005」会被拆成几十个桶，
     * 最终只剩一个看不出原因的「其他错误」。
     *
     * @param throwable 客户端抛出的原始异常
     * @return 形如 {@code MQClientException: wait response timeout, cost=#} 的描述
     */
    public static String describe(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root && root.getMessage() == null) {
            root = root.getCause();
        }
        String message = root.getMessage() == null ? "" : root.getMessage().split("\n")[0].trim();
        // 数字折叠成 #，并去掉 RocketMQ 附带的具体耗时（cost=3000 与 cost=3001 是同一类故障）
        message = message.replaceAll("\\d+", "#").replaceAll(",?\\s*cost=#", "");
        if (message.length() > 220) {
            message = message.substring(0, 220) + "…";
        }
        String type = root.getClass().getSimpleName();
        return message.isEmpty() ? type : type + ": " + message;
    }

    private void sampleSafely() {
        try {
            sample();
        } catch (RuntimeException ignored) {
            // 采样失败不影响压测线程
        }
    }

    private synchronized void sample() {
        if (!running) {
            return;
        }
        long now = System.currentTimeMillis();
        long totalProduced = produced.get();
        long totalFailed = failed.get() + asyncSink.failedCount();
        double seconds = Math.max(0.001, (now - lastSampleAt) / 1000.0);
        lastTps = (totalProduced - lastProduced) / seconds;
        lastFailTps = (totalFailed - lastFailed) / seconds;
        lastP99 = percentile(99);
        double p95 = percentile(95);
        samples.add(new StressStatus.Sample(now - startedAt, totalProduced, totalFailed,
                round(lastTps), round(lastFailTps), round(p95), round(lastP99)));
        if (samples.size() > MAX_SAMPLES) {
            samples.removeFirst();
        }
        lastProduced = totalProduced;
        lastFailed = totalFailed;
        lastSampleAt = now;
    }

    /**
     * 结束实验。
     *
     * <p>只有状态切换在锁内完成：{@code drain()} / {@code close()} 可能阻塞数秒
     * （RabbitMQ 在排空 publisher confirm、Kafka 在等缓冲区落地），
     * 若在锁内执行，前端每次轮询 {@code GET /stress} 都会被卡住。
     */
    private void finish() {
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
            phase = "DONE";
            // 结束时刻在 drain/close 之前定格：排空在途消息的耗时不属实验时长
            endedAt = System.currentTimeMillis();
        }
        if (ticker != null) {
            ticker.shutdownNow();
        }
        if (workers != null) {
            workers.shutdownNow();
        }
        try {
            sender.drain();
        } catch (Exception ignored) {
            // 排空失败不影响结果统计
        }
        try {
            sender.close();
        } catch (Exception ignored) {
            // 关闭失败不影响结果统计
        }
        synchronized (this) {
            long totalProduced = produced.get();
            long totalFailed = failed.get() + asyncSink.failedCount();
            samples.add(new StressStatus.Sample(endedAt - startedAt, totalProduced, totalFailed,
                    round(lastTps), round(lastFailTps), round(percentile(95)), round(lastP99)));
            lastProduced = totalProduced;
            lastFailed = totalFailed;
        }
    }

    private double percentile(int percentile) {
        long[] snapshot;
        synchronized (latencies) {
            snapshot = new long[LATENCY_WINDOW];
            int count = 0;
            for (long value : latencies) {
                if (value > 0) {
                    snapshot[count++] = value;
                }
            }
            snapshot = java.util.Arrays.copyOf(snapshot, count);
        }
        if (snapshot.length == 0) {
            return 0;
        }
        java.util.Arrays.sort(snapshot);
        int index = (int) Math.ceil(percentile / 100.0 * snapshot.length) - 1;
        return snapshot[Math.max(0, Math.min(index, snapshot.length - 1))] / 1_000_000.0;
    }

    private List<StressStatus.Error> mergedErrors() {
        Map<String, Long> merged = new LinkedHashMap<>();
        synchronized (errorCounts) {
            merged.putAll(errorCounts);
        }
        for (StressStatus.Error error : asyncSink.recentErrors()) {
            merged.merge(error.message(), error.count(), Long::sum);
        }
        return merged.entrySet().stream()
                .map(entry -> new StressStatus.Error(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingLong(StressStatus.Error::count).reversed())
                .limit(MAX_ERRORS)
                .toList();
    }

    /**
     * 给出实验结论。
     *
     * <p>三种结果都是真实信号，区别在于生产端「怎么输的」：
     * <ul>
     *   <li><b>已被冲垮</b>——失败率 ≥ 1%：客户端被明确拒绝/超时（Kafka 缓冲区分配超时、RocketMQ 流控、RabbitMQ confirm 超时）；</li>
     *   <li><b>被限流</b>——失败率很低但整轮平均速率明显低于目标：发送线程被阻塞在客户端缓冲区里，
     *       broker 用背压把流量压了下来，这同样是被压垮的表现；</li>
     *   <li><b>勉强扛住 / 未被冲垮</b>——速率达标，只有零星失败或完全没有失败。</li>
     * </ul>
     */
    private String verdict(long totalFailed) {
        if (phase.equals("IDLE")) {
            return "";
        }
        long accepted = produced.get();
        long end = endedAt > 0 ? endedAt : System.currentTimeMillis();
        double seconds = Math.max(1.0, (end - startedAt) / 1000.0);
        double achieved = accepted / seconds;
        double target = command.resolvedTargetRate();
        double failRatio = totalFailed * 100.0 / Math.max(1, accepted + totalFailed);
        if (failRatio >= 1.0) {
            List<StressStatus.Error> errors = mergedErrors();
            String top = errors.isEmpty() ? "" : "，最常见原因：" + errors.getFirst().message();
            return "已被冲垮：%d 条被拒绝/超时（占 %.1f%%）%s".formatted(totalFailed, failRatio, top);
        }
        if (running) {
            // 运行中不给结论：平均值会被开头几秒拉低，误导成「被限流」
            return "实验进行中：已完成 %s 条，暂无失败".formatted(humanRate(accepted));
        }
        String loss = totalFailed == 0 ? "" : "，另有 %d 条在停止时仍未确认".formatted(totalFailed);
        if (achieved < target * 0.85) {
            return "被限流：目标 %s 条/秒，实际只有 %s 条/秒（%.0f%%）%s。没有大批异常，但发送线程被客户端缓冲区阻塞，broker 的背压已经把流量压了下来"
                    .formatted(humanRate(target), humanRate(achieved), achieved * 100.0 / target, loss);
        }
        if (totalFailed > 0) {
            return "勉强扛住：%d 条失败（%.2f%%），实际速率 %s 条/秒达标"
                    .formatted(totalFailed, failRatio, humanRate(achieved));
        }
        return "未被冲垮：全程 %d 条全部被 broker 接受，实际速率 %s 条/秒达标"
                .formatted(accepted, humanRate(achieved));
    }

    private static String humanRate(double rate) {
        return Math.round(rate) >= 1000 ? "%,d".formatted(Math.round(rate)) : String.valueOf(Math.round(rate));
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
