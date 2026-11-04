package com.example.clustermanager.infrastructure.pseudo.runtime;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * 端口分配池——從操作系統動態分配空閒端口，節點停止時釋放。
 *
 * <p>使用 {@code ServerSocket(0)} 讓 OS 自動分配可用端口，確保不衝突。
 * 作為 Spring 單例 {@code @Component}，被 {@link EmbeddedRocketMqRuntime} 持有。
 *
 * <p><b>線程安全</b>：allocate/release 方法使用 {@code synchronized} 保護，
 * {@code allocated} 使用 {@code ConcurrentHashMap.newKeySet()} 支持並發讀。
 *
 * <p><b>生命週期</b>：隨應用啟動創建，端口在節點啟動時分配、停止時釋放。
 */
@Component
public class PortPool {

    /** 連續分配重試上限——正常情況下第二次就能拿到新端口 */
    private static final int MAX_ATTEMPTS = 50;

    /** 已分配端口集合——線程安全，用於追蹤和釋放 */
    private final Set<Integer> allocated = ConcurrentHashMap.newKeySet();

    /**
     * Broker 端口對：listenPort → 預留的 fast 端口（{@code listenPort - 2}）。
     *
     * <p>RocketMQ 的 {@code BrokerController.initializeRemotingServer()} 會複製監聽配置並把端口減 2，
     * 在該端口上再起一個 fast remoting server（VIP 通道）。這個端口不受配置控制，
     * 必須由 PortPool 一并預留，否則會撞上其他節點（實測會撞上 NameServer）的端口。
     * 值為 -1 表示該 listenPort 因 fast 端口不可用而被保留佔用、但不可用於 Broker。
     */
    private final Map<Integer, Integer> brokerFastPorts = new ConcurrentHashMap<>();

    /**
     * 分配一個空閒端口。優先嘗試 OS 自動分配（port=0），確保不衝突。
     *
     * <p>{@code ServerSocket(0)} 關閉後該端口立刻變回「空閒」，連續兩次分配可能拿到同一個號碼
     * （節點尚未真正 bind 時尤其明顯）。因此這裡對已發放過的端口重試，直到拿到未被佔用的號碼。
     *
     * @return 分配到的可用端口號
     * @throws IllegalStateException 無法分配端口時拋出（如系統資源耗盡）
     */
    public synchronized int allocate() {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (allocated.add(port)) {
                    return port;
                }
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to allocate free port", exception);
            }
        }
        throw new IllegalStateException("Failed to allocate a free port after " + MAX_ATTEMPTS + " attempts");
    }

    /**
     * 為 Broker 分配監聽端口，並預留 RocketMQ 隱式啟動的 fast remoting 端口（{@code listenPort - 2}）。
     *
     * <p>若候選端口的 fast 端口已被佔用（可能已分配給其他節點，或被別的進程監聽），
     * 則丟棄該候選並保留佔用記錄，避免 OS 再次返回同一個號碼造成死循環。
     *
     * @return 可安全用於 Broker 的監聽端口
     * @throws IllegalStateException 重試上限內無法找到可用端口對時拋出
     */
    public synchronized int allocateBrokerListenPort() {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            int listenPort = allocate();
            int fastPort = listenPort - 2;
            if (fastPort > 1024 && !allocated.contains(fastPort) && isBindable(fastPort)) {
                allocated.add(fastPort);
                brokerFastPorts.put(listenPort, fastPort);
                return listenPort;
            }
            // fast 端口不可用：保留 listenPort 不放回，避免 OS 反复返回同一候選端口
            brokerFastPorts.put(listenPort, -1);
        }
        throw new IllegalStateException(
                "Failed to allocate a broker port pair after " + MAX_ATTEMPTS + " attempts");
    }

    /**
     * 釋放 Broker 端口對（監聽端口 + 預留的 fast 端口）。
     *
     * @param listenPort {@link #allocateBrokerListenPort()} 返回的監聽端口
     */
    public synchronized void releaseBrokerListenPort(int listenPort) {
        Integer fastPort = brokerFastPorts.remove(listenPort);
        if (fastPort != null && fastPort > 0) {
            allocated.remove(fastPort);
        }
        allocated.remove(listenPort);
    }

    /**
     * 判斷某端口當前是否可被 bind（關閉探測用的 socket 後立即返回）。
     *
     * @param port 待檢測端口
     * @return 可綁定時返回 true
     */
    private boolean isBindable(int port) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException exception) {
            return false;
        }
    }

    /**
     * 釋放端口回池。節點停止時調用，允許端口被後續節點重用。
     *
     * @param port 待釋放的端口號
     */
    public synchronized void release(int port) {
        allocated.remove(port);
    }

    /**
     * 當前已分配端口數。
     *
     * @return 已分配端口數量
     */
    public int allocatedCount() {
        return allocated.size();
    }
}
