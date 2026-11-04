package com.example.clustermanager.api.controller;

import com.example.clustermanager.api.dto.StressStartRequest;
import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.application.service.ClusterFacadeService;
import com.example.clustermanager.core.model.ClusterMode;
import com.example.clustermanager.core.model.ClusterRef;
import com.example.clustermanager.core.model.MiddlewareType;
import com.example.clustermanager.core.model.lab.InternalsSnapshot;
import com.example.clustermanager.core.model.lab.StressCommand;
import com.example.clustermanager.core.model.lab.StressStatus;
import com.example.clustermanager.core.port.IInternalsInspector;
import com.example.clustermanager.core.port.IStressLab;
import jakarta.validation.Valid;
import java.util.Locale;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 实验室 API：压力实验与内部机制可视化，三种 MQ 共用一套路径。
 *
 * <p>路径：{@code /api/clusters/{mode}/{middleware}/{clusterId}/lab}，
 * 通过 {@link ClusterFacadeService#capability} 取得 provider 实现的
 * {@link IStressLab} 与 {@link IInternalsInspector}；provider 未实现时返回 400。
 *
 * <p>压测是后台异步任务：{@code POST /stress} 立即返回，前端轮询 {@code GET /stress} 取实时曲线，
 * {@code DELETE /stress} 提前终止。
 */
@Validated
@RestController
@RequestMapping("/api/clusters/{mode}/{middleware}/{clusterId}/lab")
public class LabController {

    private final ClusterFacadeService clusterFacadeService;

    public LabController(ClusterFacadeService clusterFacadeService) {
        this.clusterFacadeService = clusterFacadeService;
    }

    /** 读取一次内部机制快照（真实存储/对象模型，不做模拟）。 */
    @GetMapping("/internals")
    public InternalsSnapshot internals(@PathVariable String mode, @PathVariable String middleware,
                                       @PathVariable String clusterId) {
        return clusterFacadeService.capability(selection(mode, middleware, clusterId), IInternalsInspector.class)
                .inspect(ref(mode, middleware, clusterId));
    }

    /** 启动压力实验。 */
    @PostMapping("/stress")
    public StressStatus start(@PathVariable String mode, @PathVariable String middleware,
                              @PathVariable String clusterId, @Valid @RequestBody StressStartRequest request) {
        StressCommand command = new StressCommand(request.topic().trim(), request.targetRate(), request.threads(),
                request.durationSeconds(), request.payloadBytes(), request.acks(), request.options());
        return clusterFacadeService.capability(selection(mode, middleware, clusterId), IStressLab.class)
                .start(ref(mode, middleware, clusterId), command);
    }

    /** 读取当前/最近一次实验状态。 */
    @GetMapping("/stress")
    public StressStatus status(@PathVariable String mode, @PathVariable String middleware,
                               @PathVariable String clusterId) {
        return clusterFacadeService.capability(selection(mode, middleware, clusterId), IStressLab.class)
                .status(ref(mode, middleware, clusterId));
    }

    /** 停止实验并返回最终结果。 */
    @DeleteMapping("/stress")
    public StressStatus stop(@PathVariable String mode, @PathVariable String middleware,
                             @PathVariable String clusterId) {
        return clusterFacadeService.capability(selection(mode, middleware, clusterId), IStressLab.class)
                .stop(ref(mode, middleware, clusterId));
    }

    private static ClusterSelection selection(String mode, String middleware, String clusterId) {
        return new ClusterSelection(clusterId,
                ClusterMode.valueOf(mode.toUpperCase(Locale.ROOT)),
                MiddlewareType.valueOf(middleware.toUpperCase(Locale.ROOT)));
    }

    private static ClusterRef ref(String mode, String middleware, String clusterId) {
        return new ClusterRef(clusterId,
                ClusterMode.valueOf(mode.toUpperCase(Locale.ROOT)),
                MiddlewareType.valueOf(middleware.toUpperCase(Locale.ROOT)));
    }
}
