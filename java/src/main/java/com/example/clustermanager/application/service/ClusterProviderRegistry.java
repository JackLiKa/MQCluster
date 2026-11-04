package com.example.clustermanager.application.service;

import com.example.clustermanager.application.model.ClusterSelection;
import com.example.clustermanager.core.model.ProviderDescriptor;
import com.example.clustermanager.core.port.IClusterProvider;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Provider 註冊表 —— application 層的適配器解析器，按 {@code (ClusterMode, MiddlewareType)} 二元組解析對應 provider。
 *
 * <p>Spring 啟動時自動注入所有實現 {@link IClusterProvider} 的 bean（如
 * {@code PseudoClusterProvider}、{@code RocketMqClusterProvider}），本註冊表在調用
 * {@link #resolve(ClusterSelection)} 時按 {@code (mode, middleware)} 二元組篩選唯一適配器。
 *
 * <p>與各層關係：
 * <ul>
 *   <li>application 層：{@link ClusterFacadeService} 依賴本註冊表解析 provider。</li>
 *   <li>core 層：依賴 {@code IClusterProvider} 端口接口與 {@code ProviderDescriptor} 描述符。</li>
 *   <li>infrastructure 層：各 provider 實現 {@code IClusterProvider} 並標 {@code @Component}，由 Spring 收集注入。</li>
 * </ul>
 *
 * <p>新增中間件（如 Kafka）時，只需在 {@code infrastructure/<name>} 包新增實現
 * {@code IClusterProvider} 的 {@code @Component}，本註冊表自動收錄，無需改動。
 */
@Component
public class ClusterProviderRegistry {

    /** 所有已註冊的 provider 適配器，由 Spring 在構造時注入。 */
    private final List<IClusterProvider> providers;

    /**
     * 構造註冊表，注入所有 {@code IClusterProvider} bean。
     *
     * @param providers Spring 容器中所有 {@code IClusterProvider} 實現（按類型聚合注入）
     */
    public ClusterProviderRegistry(List<IClusterProvider> providers) {
        this.providers = providers;
    }

    /**
     * 按 {@code (mode, middleware)} 二元組解析唯一的 provider 適配器。
     *
     * <p>從已注入的 provider 列表中篩選 {@code descriptor().mode()} 與 {@code descriptor().middleware()}
     * 均匹配的適配器，取第一個。若找不到任何匹配項，拋出 {@link IllegalArgumentException}，
     * 由 {@code ApiExceptionHandler} 映射為 HTTP 400。
     *
     * @param selection 集群選擇，攜帶 {@code mode} 與 {@code middleware} 用於匹配
     * @return 匹配的 provider 適配器
     * @throws IllegalArgumentException 當不存在 {@code (mode, middleware)} 對應的 provider 時拋出
     */
    public IClusterProvider resolve(ClusterSelection selection) {
        return providers.stream()
                .filter(provider -> provider.descriptor().mode() == selection.mode())
                .filter(provider -> provider.descriptor().middleware() == selection.middleware())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No cluster provider for mode=%s, middleware=%s".formatted(
                                selection.mode(),
                                selection.middleware()
                        )));
    }

    /**
     * 解析 provider 的專屬能力端口（如 {@code IKafkaInspector}、{@code IRabbitInspector}）。
     *
     * @param selection  集群選擇
     * @param capability 能力接口類型
     * @param <T>        能力類型
     * @return 已轉型的 provider
     * @throws IllegalArgumentException provider 不存在或未實現該能力時拋出（映射為 HTTP 400）
     */
    public <T> T resolveCapability(ClusterSelection selection, Class<T> capability) {
        IClusterProvider provider = resolve(selection);
        if (!capability.isInstance(provider)) {
            throw new IllegalArgumentException("Cluster provider %s does not support %s".formatted(
                    provider.descriptor().providerId(),
                    capability.getSimpleName()
            ));
        }
        return capability.cast(provider);
    }

    /**
     * 列出所有已註冊 provider 的描述符，供前端展示可選集群類型。
     *
     * @return provider 描述符列表，每項包含 mode、middleware、clusterId 等元信息
     */
    public List<ProviderDescriptor> listProviders() {
        return providers.stream()
                .map(IClusterProvider::descriptor)
                .toList();
    }
}
