package com.example.clustermanager.api.controller;

import com.example.clustermanager.api.dto.RabbitBindingRequest;
import com.example.clustermanager.api.dto.RabbitConsumerRequest;
import com.example.clustermanager.api.dto.RabbitExchangeRequest;
import com.example.clustermanager.api.dto.RabbitQueueRequest;
import com.example.clustermanager.api.dto.RabbitRoutePreviewRequest;
import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.rabbit.RabbitBinding;
import com.example.clustermanager.core.model.rabbit.RabbitConsumer;
import com.example.clustermanager.core.model.rabbit.RabbitExchange;
import com.example.clustermanager.core.model.rabbit.RabbitQueue;
import com.example.clustermanager.core.model.rabbit.RabbitRoutePreview;
import com.example.clustermanager.core.model.rabbit.RabbitRoutingTopology;
import com.example.clustermanager.core.port.IRabbitInspector;
import jakarta.validation.Valid;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * RabbitMQ 专属 API：交换机 / 队列 / 绑定 / 路由预览 / 常驻消费者。
 *
 * <p>通过 {@link ClusterFacadeService#capability} 取得 {@link IRabbitInspector}；
 * RabbitMQ 被 {@code cluster.rabbitmq.enabled=false} 关闭时 provider 不存在，返回 400。
 */
@Validated
@RestController
@RequestMapping("/api/clusters/pseudo/rabbitmq/{clusterId}")
public class RabbitController {

    private final ClusterFacadeService clusterFacadeService;

    public RabbitController(ClusterFacadeService clusterFacadeService) {
        this.clusterFacadeService = clusterFacadeService;
    }

    /** 完整路由拓扑：交换机、队列（含 ready/unacked/速率）、绑定、常驻消费者。 */
    @GetMapping("/routing")
    public RabbitRoutingTopology routing(@PathVariable String clusterId) {
        return inspector(clusterId).routing(ref(clusterId));
    }

    /** 路由预览：给定 exchange / routing key / headers，返回会命中的队列（不发送消息）。 */
    @PostMapping("/routing/preview")
    public RabbitRoutePreview preview(@PathVariable String clusterId, @RequestBody RabbitRoutePreviewRequest request) {
        return inspector(clusterId).previewRoute(ref(clusterId), request.exchange(), request.routingKey(), request.headers());
    }

    /** 声明交换机。 */
    @PostMapping("/exchanges")
    public RabbitExchange declareExchange(@PathVariable String clusterId, @Valid @RequestBody RabbitExchangeRequest request) {
        return inspector(clusterId).declareExchange(ref(clusterId), request.name().trim(), request.type(),
                request.durable() == null || request.durable());
    }

    /** 删除交换机。 */
    @DeleteMapping("/exchanges/{name}")
    public OperationResult deleteExchange(@PathVariable String clusterId, @PathVariable String name) {
        return inspector(clusterId).deleteExchange(ref(clusterId), name);
    }

    /** 声明队列（支持死信交换机与 TTL）。 */
    @PostMapping("/queues")
    public RabbitQueue declareQueue(@PathVariable String clusterId, @Valid @RequestBody RabbitQueueRequest request) {
        return inspector(clusterId).declareQueue(ref(clusterId), request.name().trim(), request.deadLetterExchange(), request.messageTtlMs());
    }

    /** 删除队列。 */
    @DeleteMapping("/queues/{name}")
    public OperationResult deleteQueue(@PathVariable String clusterId, @PathVariable String name) {
        return inspector(clusterId).deleteQueue(ref(clusterId), name);
    }

    /** 清空队列中的 ready 消息。 */
    @PostMapping("/queues/{name}/purge")
    public OperationResult purgeQueue(@PathVariable String clusterId, @PathVariable String name) {
        return inspector(clusterId).purgeQueue(ref(clusterId), name);
    }

    /** 创建绑定。 */
    @PostMapping("/bindings")
    public RabbitBinding bind(@PathVariable String clusterId, @Valid @RequestBody RabbitBindingRequest request) {
        return inspector(clusterId).bind(ref(clusterId), request.exchange(), request.queue(), request.routingKey(), request.headers());
    }

    /** 删除绑定（请求体与创建时相同）。 */
    @DeleteMapping("/bindings")
    public OperationResult unbind(@PathVariable String clusterId, @Valid @RequestBody RabbitBindingRequest request) {
        return inspector(clusterId).unbind(ref(clusterId), request.exchange(), request.queue(), request.routingKey());
    }

    /** 增加常驻消费者。 */
    @PostMapping("/consumers")
    public RabbitConsumer addConsumer(@PathVariable String clusterId, @Valid @RequestBody RabbitConsumerRequest request) {
        return inspector(clusterId).addConsumer(ref(clusterId), request.queue().trim(),
                request.prefetch() == null ? 10 : request.prefetch(),
                request.ackMode() == null ? "manual" : request.ackMode(),
                request.processingDelayMs() == null ? 200 : request.processingDelayMs(),
                request.nackRatio() == null ? 0 : request.nackRatio(),
                request.requeue() != null && request.requeue());
    }

    /** 移除常驻消费者。 */
    @DeleteMapping("/consumers/{consumerId}")
    public OperationResult removeConsumer(@PathVariable String clusterId, @PathVariable String consumerId) {
        return inspector(clusterId).removeConsumer(ref(clusterId), consumerId);
    }

    private IRabbitInspector inspector(String clusterId) {
        return clusterFacadeService.capability(new ClusterSelection(clusterId, ClusterMode.PSEUDO, MiddlewareType.RABBITMQ), IRabbitInspector.class);
    }

    private static ClusterRef ref(String clusterId) {
        return new ClusterRef(clusterId, ClusterMode.PSEUDO, MiddlewareType.RABBITMQ);
    }
}
