package com.example.clustermanager.infrastructure.rabbitmq.routing;

import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AMQP 0-9-1 路由匹配（纯函数）：用于路由预览与消息模拟的结果归因。
 *
 * <ul>
 *   <li>direct：routing key 与 binding key 完全相等</li>
 *   <li>fanout：所有绑定</li>
 *   <li>topic：按 {@code .} 分词，{@code *} 匹配一个词，{@code #} 匹配零个或多个词</li>
 *   <li>headers：{@code x-match=all}（默认）要求所有非 {@code x-} 参数相等，{@code any} 只需其一</li>
 * </ul>
 */
public final class RabbitRouteMatcher {

    private RabbitRouteMatcher() {
    }

    /**
     * 计算命中的绑定。
     *
     * @param exchangeType 交换机类型
     * @param bindings     该交换机的绑定
     * @param routingKey   routing key
     * @param headers      消息头
     * @return 命中的绑定（保持输入顺序）
     */
    public static List<RabbitBinding> match(String exchangeType, List<RabbitBinding> bindings, String routingKey, Map<String, String> headers) {
        String key = routingKey == null ? "" : routingKey;
        Map<String, String> messageHeaders = headers == null ? Map.of() : headers;
        List<RabbitBinding> matched = new ArrayList<>();
        for (RabbitBinding binding : bindings) {
            boolean hit = switch (exchangeType) {
                case "fanout" -> true;
                case "direct" -> binding.routingKey().equals(key);
                case "topic" -> topicMatches(binding.routingKey(), key);
                case "headers" -> headersMatch(binding.headers(), messageHeaders);
                default -> false;
            };
            if (hit) {
                matched.add(binding);
            }
        }
        return matched;
    }

    /** topic 通配匹配。 */
    public static boolean topicMatches(String pattern, String routingKey) {
        String[] patternWords = pattern.isEmpty() ? new String[0] : pattern.split("\\.", -1);
        String[] keyWords = routingKey.isEmpty() ? new String[0] : routingKey.split("\\.", -1);
        return topicMatches(patternWords, 0, keyWords, 0);
    }

    private static boolean topicMatches(String[] pattern, int p, String[] key, int k) {
        if (p == pattern.length) {
            return k == key.length;
        }
        String word = pattern[p];
        if ("#".equals(word)) {
            for (int skip = k; skip <= key.length; skip++) {
                if (topicMatches(pattern, p + 1, key, skip)) {
                    return true;
                }
            }
            return false;
        }
        if (k == key.length) {
            return false;
        }
        return ("*".equals(word) || word.equals(key[k])) && topicMatches(pattern, p + 1, key, k + 1);
    }

    /** headers 匹配。 */
    public static boolean headersMatch(Map<String, String> bindingArgs, Map<String, String> messageHeaders) {
        boolean any = "any".equalsIgnoreCase(bindingArgs.getOrDefault("x-match", "all"));
        int candidates = 0;
        int hits = 0;
        for (Map.Entry<String, String> entry : bindingArgs.entrySet()) {
            if (entry.getKey().startsWith("x-")) {
                continue;
            }
            candidates++;
            if (entry.getValue().equals(messageHeaders.get(entry.getKey()))) {
                hits++;
            }
        }
        if (candidates == 0) {
            return true;
        }
        return any ? hits > 0 : hits == candidates;
    }
}
