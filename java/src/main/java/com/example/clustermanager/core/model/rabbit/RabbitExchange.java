package com.example.clustermanager.core.model.rabbit;

import java.util.Map;

/**
 * RabbitMQ 交换机。
 *
 * @param name      交换机名称（默认交换机为空字符串）
 * @param type      类型：direct / fanout / topic / headers
 * @param durable   是否持久化
 * @param builtIn   是否为内置交换机（默认交换机与 {@code amq.*}），内置交换机不可删除
 * @param arguments 声明参数
 */
public record RabbitExchange(String name, String type, boolean durable, boolean builtIn, Map<String, String> arguments) {

    public RabbitExchange {
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
