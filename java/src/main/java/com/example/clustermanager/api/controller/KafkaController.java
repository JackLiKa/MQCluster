package com.example.clustermanager.api.controller;

import com.example.clustermanager.api.dto.KafkaConsumerMemberRequest;
import com.example.clustermanager.api.dto.KafkaConsumerRateRequest;
import com.example.clustermanager.api.dto.KafkaTopicCreateRequest;
import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.OperationResult;
import com.example.clustermanager.core.model.kafka.KafkaConsumerGroupDetail;
import com.example.clustermanager.core.model.kafka.KafkaGroupMember;
import com.example.clustermanager.core.model.kafka.KafkaLeaderChange;
import com.example.clustermanager.core.model.kafka.KafkaTopicDetail;
import com.example.clustermanager.core.port.IKafkaInspector;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka 专属 API：topic / 分区 / 消费者组 / leader 切换。
 *
 * <p>通过 {@link ClusterFacadeService#capability} 取得 {@link IKafkaInspector}；
 * Kafka 被 {@code cluster.kafka.enabled=false} 关闭时 provider 不存在，返回 400。
 */
@Validated
@RestController
@RequestMapping("/api/clusters/pseudo/kafka/{clusterId}")
public class KafkaController {

    private final ClusterFacadeService clusterFacadeService;

    public KafkaController(ClusterFacadeService clusterFacadeService) {
        this.clusterFacadeService = clusterFacadeService;
    }

    /** 列出 topic（默认不含内部 topic）。 */
    @GetMapping("/topics")
    public List<KafkaTopicDetail> listTopics(@PathVariable String clusterId,
                                             @RequestParam(defaultValue = "false") boolean includeInternal) {
        return inspector(clusterId).listTopics(ref(clusterId), includeInternal);
    }

    /** 创建 topic。 */
    @PostMapping("/topics")
    public KafkaTopicDetail createTopic(@PathVariable String clusterId, @Valid @RequestBody KafkaTopicCreateRequest request) {
        return inspector(clusterId).createTopic(ref(clusterId), request.name().trim(),
                request.partitions() == null ? 3 : request.partitions(),
                request.replicationFactor() == null ? 3 : request.replicationFactor(),
                request.configs());
    }

    /** 查询单个 topic（含分区 leader/副本/ISR 与关键配置）。 */
    @GetMapping("/topics/{topic}")
    public KafkaTopicDetail describeTopic(@PathVariable String clusterId, @PathVariable String topic) {
        return inspector(clusterId).describeTopic(ref(clusterId), topic);
    }

    /** 删除 topic。 */
    @DeleteMapping("/topics/{topic}")
    public OperationResult deleteTopic(@PathVariable String clusterId, @PathVariable String topic) {
        return inspector(clusterId).deleteTopic(ref(clusterId), topic);
    }

    /** 列出消费者组。 */
    @GetMapping("/consumer-groups")
    public List<KafkaConsumerGroupDetail> listConsumerGroups(@PathVariable String clusterId) {
        return inspector(clusterId).listConsumerGroups(ref(clusterId));
    }

    /** 查询消费者组详情（成员、分配、每分区 lag）。 */
    @GetMapping("/consumer-groups/{group}")
    public KafkaConsumerGroupDetail describeConsumerGroup(@PathVariable String clusterId, @PathVariable String group) {
        return inspector(clusterId).describeConsumerGroup(ref(clusterId), group);
    }

    /** 新增常驻消费者成员（可设置每秒处理条数以制造 lag）。 */
    @PostMapping("/consumer-groups/{group}/members")
    public KafkaGroupMember addMember(@PathVariable String clusterId, @PathVariable String group,
                                      @Valid @RequestBody KafkaConsumerMemberRequest request) {
        return inspector(clusterId).addConsumer(ref(clusterId), group, request.topic().trim(),
                request.ratePerSecond() == null ? 5 : request.ratePerSecond());
    }

    /** 调整常驻消费者速率。 */
    @PatchMapping("/consumer-groups/{group}/members/{memberId}")
    public KafkaGroupMember updateMember(@PathVariable String clusterId, @PathVariable String group, @PathVariable String memberId,
                                         @Valid @RequestBody KafkaConsumerRateRequest request) {
        return inspector(clusterId).updateConsumerRate(ref(clusterId), group, memberId, request.ratePerSecond());
    }

    /** 移除常驻消费者成员，触发 rebalance。 */
    @DeleteMapping("/consumer-groups/{group}/members/{memberId}")
    public OperationResult removeMember(@PathVariable String clusterId, @PathVariable String group, @PathVariable String memberId) {
        return inspector(clusterId).removeConsumer(ref(clusterId), group, memberId);
    }

    /** 最近的分区 leader 切换事件。 */
    @GetMapping("/leader-changes")
    public List<KafkaLeaderChange> leaderChanges(@PathVariable String clusterId, @RequestParam(defaultValue = "50") int limit) {
        return inspector(clusterId).leaderChanges(ref(clusterId), limit);
    }

    private IKafkaInspector inspector(String clusterId) {
        return clusterFacadeService.capability(new ClusterSelection(clusterId, ClusterMode.PSEUDO, MiddlewareType.KAFKA), IKafkaInspector.class);
    }

    private static ClusterRef ref(String clusterId) {
        return new ClusterRef(clusterId, ClusterMode.PSEUDO, MiddlewareType.KAFKA);
    }
}
