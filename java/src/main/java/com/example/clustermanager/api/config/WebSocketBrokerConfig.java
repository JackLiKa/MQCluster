package com.example.clustermanager.api.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket STOMP 消息代理配置。
 *
 * <p>API 层角色：为 {@link com.example.clustermanager.api.controller.ClusterTelemetryPushService}
 * 提供实时推送通道。后端通过 {@code SimpMessagingTemplate} 向 STOMP 主题推送指标与日志，
 * 前端通过 SockJS + STOMP 订阅消费（见前端 hooks/use-cluster-data.ts）。
 *
 * <p>关键配置说明：
 * <ul>
 *   <li>STOMP 端点：{@code /ws}——前端建立 WebSocket 连接的入口，同时提供原生 WebSocket
 *       与 SockJS 回退两种方式，CORS 来源复用 {@link CorsProperties}。</li>
 *   <li>消息代理前缀：{@code /topic}——服务端推送使用的主题前缀，如
 *       {@code /topic/clusters/{clusterId}/metrics} 与 {@code /topic/clusters/{clusterId}/logs}。</li>
 *   <li>应用前缀：{@code /app}——客户端发送消息到服务端 @MessageMapping 方法的前缀（当前未使用）。</li>
 * </ul>
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketBrokerConfig implements WebSocketMessageBrokerConfigurer {

    /** CORS 属性来源，用于限制 STOMP 端点的允许来源。 */
    private final CorsProperties corsProperties;

    /**
     * 构造器注入 CORS 属性。
     *
     * @param corsProperties 来自 {@code cluster.cors.allowed-origins} 的允许来源列表
     */
    public WebSocketBrokerConfig(CorsProperties corsProperties) {
        this.corsProperties = corsProperties;
    }

    /**
     * 配置消息代理：启用内存简单代理处理 {@code /topic} 前缀的主题，
     * 并设置 {@code /app} 为客户端到服务端消息的应用目的地前缀。
     *
     * @param registry STOMP 消息代理注册表
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    /**
     * 注册 STOMP 端点 {@code /ws}，同时提供原生 WebSocket 与 SockJS 回退两种连接方式。
     * 允许来源复用 {@link CorsProperties}，与 REST 端点的 CORS 策略保持一致。
     *
     * @param registry STOMP 端点注册表
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOrigins(corsProperties.allowedOrigins().toArray(String[]::new));
        registry.addEndpoint("/ws")
                .setAllowedOrigins(corsProperties.allowedOrigins().toArray(String[]::new))
                .withSockJS();
    }
}
