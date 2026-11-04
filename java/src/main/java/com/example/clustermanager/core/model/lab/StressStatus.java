package com.example.clustermanager.core.model.lab;

import java.util.List;

/**
 * 压力实验的实时状态与结果。
 *
 * <p>由压测引擎每秒采样一次；{@link #samples()} 是完整的时间序列，前端画速率曲线，
 * {@link #errors()} 汇总被 broker 拒绝/超时的真实异常（按消息聚合计数）。
 *
 * @param running       是否正在运行
 * @param phase         IDLE / RUNNING / DONE
 * @param elapsedMs     已运行毫秒数
 * @param targetRate    目标速率（条/秒）
 * @param produced      已成功交给客户端的消息数
 * @param failed        失败消息数（含客户端拒绝与 broker 拒绝）
 * @param produceTps    最近一个采样窗口的实际发送速率
 * @param failTps       最近一个采样窗口的失败速率
 * @param p99LatencyMs  最近窗口内单次发送的 p99 耗时
 * @param samples       每秒采样序列（最多保留 300 个点）
 * @param errors        失败原因聚合（最多 8 条，按次数倒序）
 * @param verdict       结论：是否被流量冲垮、依据是什么
 */
public record StressStatus(
        boolean running,
        String phase,
        long elapsedMs,
        int targetRate,
        long produced,
        long failed,
        double produceTps,
        double failTps,
        double p99LatencyMs,
        List<Sample> samples,
        List<Error> errors,
        String verdict
) {

    /**
     * 一个采样点。
     *
     * @param elapsedMs   相对实验开始的毫秒数
     * @param produced    累计成功数
     * @param failed      累计失败数
     * @param produceTps  窗口内发送速率
     * @param failTps     窗口内失败速率
     * @param p95LatencyMs 窗口内 p95 单次发送耗时
     * @param p99LatencyMs 窗口内 p99 单次发送耗时
     */
    public record Sample(long elapsedMs, long produced, long failed, double produceTps, double failTps,
                         double p95LatencyMs, double p99LatencyMs) {
    }

    /**
     * 失败原因聚合。
     *
     * @param message 真实异常类型与首行信息
     * @param count   出现次数
     */
    public record Error(String message, long count) {
    }

    /** 空状态，用于尚未运行过实验的集群。 */
    public static StressStatus idle() {
        return new StressStatus(false, "IDLE", 0, 0, 0, 0, 0, 0, 0, List.of(), List.of(), "");
    }
}
