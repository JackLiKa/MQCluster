package com.example.clustermanager.core.model.lab;

import java.util.List;

/**
 * 中间件内部机制快照——把 broker 此刻的真实内部状态翻译成可直接绘制的数据。
 *
 * <p>字段来源全部是真实运行时数据（磁盘段文件、CommitLog 写指针、Qpid 对象模型），
 * 不含任何前端模拟。前端只负责展示，不做二次推算。
 *
 * @param headline 一句话现状（含真实计数）
 * @param pipeline 消息在 broker 内部经过的阶段，每个阶段带真实数值
 * @param segments 底层存储单元（CommitLog 文件 / Kafka 段文件 / 队列对象）
 * @param metrics  关键内部计数器
 * @param notes    机制说明与注意事项（例如 Qpid 与真实 RabbitMQ 的差异）
 */
public record InternalsSnapshot(
        String headline,
        List<Stage> pipeline,
        List<Segment> segments,
        List<Metric> metrics,
        List<String> notes
) {

    /**
     * 内部流水线的一个阶段。
     *
     * @param name   阶段名称（如「写入 CommitLog」）
     * @param value  该阶段的真实数值（已格式化，便于直接展示）
     * @param detail 说明文字，解释这个数值意味着什么
     */
    public record Stage(String name, String value, String detail) {
    }

    /**
     * 一个底层存储单元。
     *
     * @param group  所属节点/分组（如 broker 节点 ID）
     * @param name   单元名称（文件名或队列名）
     * @param size   当前大小，配合 {@code unit} 解释
     * @param unit   单位：{@code B} 表示字节，{@code 条} 表示消息条数
     * @param detail 附加说明（写指针、偏移范围等）
     */
    public record Segment(String group, String name, long size, String unit, String detail) {
    }

    /**
     * 一个内部计数器。
     *
     * @param label 指标名
     * @param value 已格式化的值
     * @param hint  解释
     */
    public record Metric(String label, String value, String hint) {
    }
}
