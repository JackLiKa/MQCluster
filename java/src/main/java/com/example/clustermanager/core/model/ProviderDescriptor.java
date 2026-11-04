package com.example.clustermanager.core.model;

import java.util.Set;

/**
 * Provider 描述符，领域核心层的不可变值对象。
 *
 * <p>描述一个 {@code IClusterProvider} 适配器的元信息：标识、展示名称、
 * 支持的集群模式与中间件类型、默认集群 ID 以及能力集合。由 {@code IClusterProvider#descriptor} 返回，
 * 供 {@code ClusterProviderRegistry} 注册与按 {@link ClusterRef} 解析适配器使用。
 * 前端通过 {@code clusterId} 定位集群，通过 {@code capabilities} 决定展示哪些标签页与按钮，
 * 不再按 providerId 硬编码。
 */
public record ProviderDescriptor(
        /** Provider 唯一标识，如 {@code pseudo-rocketmq}。 */
        String providerId,
        /** Provider 展示名称，用于前端选择列表显示。 */
        String displayName,
        /** 该 Provider 支持的集群模式，取值见 {@link ClusterMode}。 */
        ClusterMode mode,
        /** 该 Provider 支持的中间件类型，取值见 {@link MiddlewareType}。 */
        MiddlewareType middleware,
        /** 该 Provider 承载的集群 ID，如 {@code local-lab}、{@code kafka-lab}。 */
        String clusterId,
        /** 能力集合，取值见 {@link ProviderCapabilities}；不为 {@code null}。 */
        Set<String> capabilities
) {

    public ProviderDescriptor {
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }

    /**
     * 判断是否具备某项能力。
     *
     * @param capability 能力名，见 {@link ProviderCapabilities}
     * @return 具备时返回 {@code true}
     */
    public boolean supports(String capability) {
        return capabilities.contains(capability);
    }
}
