package com.example.clustermanager.infrastructure.common;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

/**
 * JVM 进程级指标读取工具。
 *
 * <p>嵌入式 Kafka / Qpid 节点与平台运行在同一 JVM 内，CPU 与内存只能取进程值，
 * 各节点共享同一读数（与 RocketMQ 嵌入式运行时的口径一致）。
 */
public final class JvmProcessMetrics {

    private JvmProcessMetrics() {
    }

    /**
     * 进程 CPU 占用率百分比（0~100），未就绪时返回 0。
     *
     * @return CPU 占用率
     */
    public static double processCpuPercent() {
        try {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean bean) {
                double load = bean.getProcessCpuLoad();
                return load < 0 ? 0.0 : Math.min(100.0, load * 100.0);
            }
        } catch (Exception ignored) {
            // 读取失败按 0 处理
        }
        return 0.0;
    }

    /**
     * JVM 堆使用率百分比（0~100）。
     *
     * @return 堆使用率
     */
    public static double heapUsagePercent() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long max = heap.getMax();
        return max <= 0 ? 0.0 : Math.min(100.0, (double) heap.getUsed() / max * 100.0);
    }
}
