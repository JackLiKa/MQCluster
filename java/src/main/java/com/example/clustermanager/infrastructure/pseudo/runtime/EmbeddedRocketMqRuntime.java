package com.example.clustermanager.infrastructure.pseudo.runtime;

import com.example.clustermanager.core.model.LogEntry;
import com.example.clustermanager.core.model.NodeMetrics;
import com.example.clustermanager.core.model.NodeStatus;
import com.example.clustermanager.infrastructure.pseudo.node.ManagedNode;
import com.example.clustermanager.infrastructure.pseudo.node.NodeRegistry;
import com.example.clustermanager.infrastructure.pseudo.node.NodeRole;
import jakarta.annotation.PreDestroy;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 嵌入式 RocketMQ 運行時——取代舊的 LocalPseudoNodeRuntime。
 *
 * <p>管理進程內嵌入式 NameServer + Broker 的完整生命週期，是 PSEUDO 模式的核心運行時組件。
 * 作為 Spring 單例 {@code @Component}，被 {@link com.example.clustermanager.infrastructure.pseudo.PseudoClusterProvider}
 * 委託執行節點啟停操作。
 *
 * <p><b>啟動順序（關鍵）</b>：必須先啟動 NameServer，再啟動 Broker。
 * 因為 Broker 啟動時需要向 NameServer 註冊，若 NameServer 未運行則拋出
 * {@code IllegalStateException("Cannot start broker before NameServer is running")}。
 * {@link com.example.clustermanager.infrastructure.pseudo.PseudoClusterProvider#startAllVirtualNodes}
 * 已保證此順序。
 *
 * <p><b>端口動態分配</b>：每個節點的監聽端口和 HA 端口由 {@link PortPool} 動態分配，
 * 不使用 ManagedNode 中的邏輯端口。這避免了多節點端口衝突。
 *
 * <p><b>Spike 修復的關鍵坑</b>：
 * <ul>
 *   <li>Broker 啟動後需等待 2 秒讓註冊完成（{@code Thread.sleep(2000)}），
 *       否則首次消息發送會報 "No route info of this topic"</li>
 *   <li>每節點獨立存儲目錄（baseStorePath/{nodeId}），避免多 broker commitlog 衝突</li>
 *   <li>停止時釋放端口回 PortPool，允許端口重用</li>
 * </ul>
 *
 * <p><b>線程安全</b>：start/stop/restart 方法使用 {@code synchronized} 保護，
 * 防止並發啟停導致的端口洩漏和狀態不一致。{@code runningNodes} 使用 {@link ConcurrentHashMap}。
 * {@code namesrvAddr} 使用 {@code volatile} 保證可見性。
 *
 * <p><b>生命週期</b>：隨應用啟動創建，節點按需啟動/停止。應用關閉時通過 {@link PreDestroy}
 * 回調 {@link #shutdown()} 停止所有運行中的節點。
 *
 * <p>被以下組件依賴：
 * <ul>
 *   <li>{@link com.example.clustermanager.infrastructure.pseudo.PseudoClusterProvider}——節點啟停/重啟</li>
 *   <li>{@link com.example.clustermanager.infrastructure.pseudo.messaging.EmbeddedMessageWorkbench}——獲取 NameServer 地址和 Broker 控制器</li>
 * </ul>
 */
@Component
public class EmbeddedRocketMqRuntime {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedRocketMqRuntime.class);

    /** 端口分配池——動態分配/釋放節點監聽端口和 HA 端口 */
    private final PortPool portPool;
    /** 節點註冊表——查詢節點元數據（角色、ID 等） */
    private final NodeRegistry nodeRegistry;
    /** 嵌入式存儲根路徑——所有節點的存儲目錄的父目錄（workDir/embedded） */
    private final Path baseStorePath;
    /** 是否在節點啟動時清理舊 store 目錄——可配置，默認 true */
    private final boolean cleanStoreOnStart;
    /** 運行中節點表——nodeId → EmbeddedRocketMqNode，線程安全 */
    private final Map<String, EmbeddedRocketMqNode> runningNodes = new ConcurrentHashMap<>();
    /** 當前運行中的 NameServer 地址——volatile 保證跨線程可見性，供 Broker 註冊和消息工作台使用 */
    private volatile String namesrvAddr;

    /**
     * 構造器注入端口池、節點註冊表和配置屬性。
     *
     * @param portPool    端口分配池
     * @param nodeRegistry 節點註冊表
     * @param properties  偽集群配置（讀取 workDir 作為存儲根路徑、cleanStoreOnStart 開關）
     */
    public EmbeddedRocketMqRuntime(PortPool portPool, NodeRegistry nodeRegistry,
                                   com.example.clustermanager.infrastructure.pseudo.PseudoClusterProperties properties) {
        this.portPool = portPool;
        this.nodeRegistry = nodeRegistry;
        this.baseStorePath = Path.of(properties.resolvedWorkDir()).resolve("embedded").toAbsolutePath().normalize();
        this.cleanStoreOnStart = properties.resolvedCleanStoreOnStart();
    }

    /**
     * 啟動一個嵌入式節點。NameServer 必須先於 Broker 啟動。
     *
     * <p>流程：
     * <ol>
     *   <li>若節點已在運行則直接返回（冪等）</li>
     *   <li>創建存儲根目錄</li>
     *   <li>通過 {@link #buildSpec} 構建節點規格（含動態端口分配）</li>
     *   <li>創建並初始化嵌入式節點（NameServer 或 Broker）</li>
     *   <li>啟動節點並存入 runningNodes</li>
     *   <li>NameServer 啟動後記錄地址；Broker 啟動後等待 2 秒讓註冊完成（Spike 修復）</li>
     * </ol>
     *
     * @param node 待啟動的受管理節點
     * @throws IllegalStateException Broker 在 NameServer 之前啟動、或節點初始化/啟動失敗時拋出
     */
    public synchronized void start(ManagedNode node) {
        if (runningNodes.containsKey(node.nodeId())) {
            return;
        }
        try {
            Files.createDirectories(baseStorePath);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to create base store path: " + baseStorePath, exception);
        }

        EmbeddedNodeSpec spec = buildSpec(node);

        // 清理節點的舊 store 目錄——避免上次運行殘留的 abort/commitlog/consumequeue 導致
        // broker 載入舊 topic 配置和 stale offset，進而引發 "No route info" 和 "Offset not matched" 問題。
        // 可通過 cluster.pseudo.clean-store-on-start=false 關閉，以保留歷史消息用於調試。
        if (cleanStoreOnStart) {
            cleanNodeStoreDir(spec.storePath());
        }

        EmbeddedRocketMqNode embeddedNode = spec.role() == NodeRole.NAMESERVER
                ? EmbeddedRocketMqNode.nameserver(spec)
                : EmbeddedRocketMqNode.broker(spec);

        embeddedNode.start();
        runningNodes.put(node.nodeId(), embeddedNode);

        if (spec.role() == NodeRole.NAMESERVER) {
            namesrvAddr = embeddedNode.namesrvAddr();
            log.info("Embedded NameServer ready at {}", namesrvAddr);
        }

        // Spike 修復: broker 啟動後等待註冊到 NameServer
        if (spec.role().isBroker()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 停止一個嵌入式節點。釋放端口回 PortPool，清理 NameServer 地址引用。
     *
     * @param nodeId 待停止的節點 ID（不存在時安全返回，冪等）
     */
    public synchronized void stop(String nodeId) {
        EmbeddedRocketMqNode node = runningNodes.remove(nodeId);
        if (node == null) {
            return;
        }
        node.stop();
        if (node.spec().role().isBroker()) {
            // Broker 會額外佔用 listenPort - 2（fast remoting），必須成對釋放
            portPool.releaseBrokerListenPort(node.spec().listenPort());
        } else {
            portPool.release(node.spec().listenPort());
        }
        if (node.spec().haPort() > 0) {
            portPool.release(node.spec().haPort());
        }

        if (node.isNameserver()) {
            namesrvAddr = null;
        }
    }

    /**
     * 重啟節點——先停止再啟動。端口會被釋放後重新分配（可能獲得不同端口）。
     *
     * @param node 待重啟的受管理節點
     * @throws IllegalStateException 重啟過程中啟動失敗時拋出
     */
    public synchronized void restart(ManagedNode node) {
        stop(node.nodeId());
        // 等待 Broker 完全關閉——shutdown() 是異步的，端口釋放和線程池終止需要時間
        // 不等待會導致重啟時端口衝突或 "Address already in use" 錯誤
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        start(node);
    }

    /**
     * 查詢節點狀態。未在 runningNodes 中的節點返回 STOPPED。
     *
     * @param nodeId 節點 ID
     * @return 節點當前狀態
     */
    public NodeStatus status(String nodeId) {
        EmbeddedRocketMqNode node = runningNodes.get(nodeId);
        return node == null ? NodeStatus.STOPPED : node.status();
    }

    /**
     * 獲取節點監控指標。未運行的節點返回零值。
     *
     * <p>指標來源（真實數據，非估算）：
     * <ul>
     *   <li><b>CPU 使用率</b>：進程 CPU 占用率，從 {@code com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()} 讀取，
     *       反映嵌入式節點所在 JVM 進程的 CPU 占用。所有節點共享同一 JVM，故 CPU 值相同。</li>
     *   <li><b>內存使用率</b>：JVM heap 使用率，從 {@code ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()} 計算。
     *       同樣為進程級指標，所有節點共享。</li>
     *   <li><b>網絡入站速率（networkInBytesPerSecond）</b>：Broker 的消息 put TPS（消息數/秒），
     *       從 {@code BrokerStatsManager.getStatsItem(BROKER_PUT_NUMS, brokerClusterName)} 讀取。
     *       語義映射為消息進站速率。NameServer 無消息吞吐，返回 0。</li>
     *   <li><b>網絡出站速率（networkOutBytesPerSecond）</b>：Broker 的消息 get TPS（消息數/秒），
     *       從 {@code BrokerStatsManager.getStatsItem(BROKER_GET_NUMS, brokerClusterName)} 讀取。
     *       語義映射為消息出站速率。NameServer 無消息吞吐，返回 0。</li>
     * </ul>
     *
     * <p><b>注意</b>：CPU/內存為 JVM 進程級指標（嵌入式節點共享同一 JVM），非節點級隔離。
     * 網絡 IO 的單位為消息數/秒（TPS），映射到 NodeMetrics 的 bytes/s 字段——這是教學平台的語義映射，
     * 前端顯示為「入站/出站」速率，反映 broker 消息投遞活動。
     *
     * @param nodeId 節點 ID
     * @return 節點指標（運行中返回真實值，否則零值）
     */
    public NodeMetrics metrics(String nodeId) {
        EmbeddedRocketMqNode node = runningNodes.get(nodeId);
        if (node == null || node.status() != NodeStatus.RUNNING) {
            return new NodeMetrics(nodeId, 0, 0, 0, 0);
        }
        double cpuUsage = readProcessCpuUsage();
        double memoryUsage = readJvmHeapUsage();
        if (node.isNameserver()) {
            // NameServer 無消息吞吐，網絡 IO 為 0
            return new NodeMetrics(nodeId, cpuUsage, memoryUsage, 0, 0);
        }
        // Broker：從 BrokerStatsManager 讀取真實 put/get TPS
        double putTps = readBrokerTps(node, BrokerStatsManager.BROKER_PUT_NUMS);
        double getTps = readBrokerTps(node, BrokerStatsManager.BROKER_GET_NUMS);
        return new NodeMetrics(nodeId, cpuUsage, memoryUsage, putTps, getTps);
    }

    /**
     * 讀取 JVM 進程 CPU 占用率。
     *
     * <p>使用 {@code com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()}，
     * 返回 0.0~1.0 的進程 CPU 占用率（相對於所有 CPU 核）。乘 100 轉為百分比。
     * 首次調用可能返回 -1（未準備好），此時返回 0。
     *
     * @return CPU 使用率百分比（0.0~100.0），未準備好時返回 0
     */
    private double readProcessCpuUsage() {
        try {
            java.lang.management.OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
                double cpuLoad = sunOsBean.getProcessCpuLoad();
                if (cpuLoad < 0) {
                    return 0.0;
                }
                return Math.min(100.0, cpuLoad * 100.0);
            }
        } catch (Exception exception) {
            log.warn("Failed to read process CPU usage: {}", exception.getMessage());
        }
        return 0.0;
    }

    /**
     * 讀取 JVM heap 內存使用率。
     *
     * <p>從 {@code ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()} 獲取 used/max，
     * 計算百分比。max 為 0 或負數時返回 0（避免除零）。
     *
     * @return heap 使用率百分比（0.0~100.0）
     */
    private double readJvmHeapUsage() {
        try {
            MemoryUsage heapUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long max = heapUsage.getMax();
            if (max <= 0) {
                return 0.0;
            }
            long used = heapUsage.getUsed();
            return Math.min(100.0, (double) used / max * 100.0);
        } catch (Exception exception) {
            log.warn("Failed to read JVM heap usage: {}", exception.getMessage());
        }
        return 0.0;
    }

    /**
     * 從 Broker 的統計管理器讀取指定統計項的 TPS（消息數/秒）。
     *
     * <p>通過 {@code BrokerStatsManager.getStatsItem(statName, brokerClusterName)} 獲取統計項，
     * 再從 {@code StatsItem.getStatsDataInMinute()} 讀取一分鐘窗口的 TPS。
     * 統計項不存在（無數據）或讀取異常時返回 0。
     *
     * @param node     運行中的 Broker 節點
     * @param statName 統計項名稱（如 {@link BrokerStatsManager#BROKER_PUT_NUMS}）
     * @return TPS（消息數/秒），無數據時返回 0
     */
    private double readBrokerTps(EmbeddedRocketMqNode node, String statName) {
        try {
            BrokerController broker = node.brokerController();
            BrokerStatsManager statsManager = broker.getBrokerStatsManager();
            if (statsManager == null) {
                return 0.0;
            }
            // RocketMQ 5.3.3 的 BROKER_PUT_NUMS/BROKER_GET_NUMS 統計項以 brokerClusterName 為 key
            //（實測日誌：[BROKER_PUT_NUMS] [embedded-cluster]），而非 brokerName。
            String statsKey = node.spec().brokerClusterName();
            if (statsKey == null) {
                return 0.0;
            }
            var statsItem = statsManager.getStatsItem(statName, statsKey);
            if (statsItem == null) {
                return 0.0;
            }
            StatsSnapshot snapshot = statsItem.getStatsDataInMinute();
            if (snapshot == null) {
                return 0.0;
            }
            return Math.max(0.0, snapshot.getTps());
        } catch (Exception exception) {
            log.debug("Failed to read broker TPS for stat={}: {}", statName, exception.getMessage());
            return 0.0;
        }
    }

    /**
     * 獲取節點運行時日誌。未運行的節點返回一條 WARN 日誌。
     *
     * @param nodeId 節點 ID
     * @param limit  最大返回條數（當前實現僅返回 1 條摘要）
     * @return 日誌條目列表
     */
    public List<LogEntry> logs(String nodeId, int limit) {
        EmbeddedRocketMqNode node = runningNodes.get(nodeId);
        if (node == null) {
            return List.of(new LogEntry(Instant.now(), nodeId, "WARN", "Embedded node not running"));
        }
        return List.of(new LogEntry(
                Instant.now(),
                nodeId,
                "INFO",
                "Embedded %s on port %d, status=%s".formatted(node.spec().role(), node.spec().listenPort(), node.status())
        ));
    }

    /**
     * 獲取當前運行中的 NameServer 地址（供消息工作台使用）。
     *
     * @return NameServer 地址（如 "127.0.0.1:12345"），無 NameServer 運行時返回 null
     */
    public String namesrvAddr() {
        return namesrvAddr;
    }

    /**
     * 獲取 Broker 控制器（供消息工作台創建 topic）。
     *
     * @param nodeId Broker 節點 ID
     * @return BrokerController 實例
     * @throws IllegalStateException 節點未運行或不是 Broker 時拋出
     */
    public BrokerController brokerController(String nodeId) {
        EmbeddedRocketMqNode node = runningNodes.get(nodeId);
        if (node == null) {
            throw new IllegalStateException("Node not running: " + nodeId);
        }
        return node.brokerController();
    }

    /**
     * 是否有節點正在運行。
     *
     * @return runningNodes 非空時返回 true
     */
    public boolean hasRunningNodes() {
        return !runningNodes.isEmpty();
    }

    /**
     * 應用關閉回調——停止所有運行中的節點，釋放端口。
     * 通過 {@link PreDestroy} 註解在 Spring 容器關閉時自動調用。
     */
    @PreDestroy
    public void shutdown() {
        List.copyOf(runningNodes.keySet()).forEach(this::stop);
    }

    /**
     * 清理節點的舊 store 目錄。
     *
     * <p>刪除指定路徑下的所有文件和子目錄（包括 abort、commitlog、consumequeue、config 等），
     * 確保 broker 啟動時為乾淨狀態。目錄不存在時安全返回（冪等）。
     *
     * <p><b>為何需要清理</b>：嵌入式 RocketMQ 的 BrokerController 啟動時會從 store 目錄載入
     * 上次的 topics.json、consumerOffset.json 和 commitlog。如果上次運行未乾淨關閉（abort 文件存在），
     * 會載入舊 topic 配置和 stale offset，導致：
     * <ul>
     *   <li>"No route info of this topic"——舊 topic 配置干擾新 topic 注冊</li>
     *   <li>"Offset not matched"——舊 commitlog 的 mapped file offset 與當前不匹配</li>
     * </ul>
     * 學習平台場景下每次啟動應為乾淨狀態，無需持久化歷史消息。
     *
     * @param storePath 節點存儲路徑（baseStorePath/{nodeId}）
     */
    private void cleanNodeStoreDir(Path storePath) {
        if (!Files.exists(storePath)) {
            return;
        }
        try (var paths = Files.walk(storePath)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (Exception exception) {
                            log.warn("Failed to delete stale store file {}: {}", p, exception.getMessage());
                        }
                    });
        } catch (Exception exception) {
            log.warn("Failed to clean stale store dir {}: {}", storePath, exception.getMessage());
        }
    }

    /**
     * 從 ManagedNode 構建 EmbeddedNodeSpec。
     *
     * <p>動態分配監聽端口和 HA 端口（僅 Broker 需要 HA）。
     * 若為 Broker 且 NameServer 未運行，釋放已分配的端口並拋出異常。
     *
     * <p>Broker 名稱從 nodeId 中提取（移除 "rmq-broker-" 前綴和 "-01" 後綴），
     * Broker ID 由角色決定（主=0，從=1）。
     *
     * @param node 受管理節點
     * @return 嵌入式節點規格
     * @throws IllegalStateException Broker 在 NameServer 之前啟動時拋出
     */
    private EmbeddedNodeSpec buildSpec(ManagedNode node) {
        boolean broker = node.nodeRole().isBroker();
        // Broker 的 listenPort 需要成對分配：BrokerController 會在 listenPort - 2 上
        // 隱式啟動 fast remoting server，只發放 listenPort 會與其他節點端口衝突。
        int listenPort = broker ? portPool.allocateBrokerListenPort() : portPool.allocate();
        int haPort = broker ? portPool.allocate() : -1;

        if (node.nodeRole() == NodeRole.NAMESERVER) {
            return EmbeddedNodeSpec.nameserver(node.nodeId(), listenPort, baseStorePath);
        }

        if (namesrvAddr == null) {
            portPool.releaseBrokerListenPort(listenPort);
            if (haPort > 0) {
                portPool.release(haPort);
            }
            throw new IllegalStateException("Cannot start broker before NameServer is running");
        }

        String brokerName = node.nodeId().replace("rmq-broker-", "").replace("-01", "");
        int brokerId = node.nodeRole() == NodeRole.BROKER_MASTER ? 0 : 1;

        return EmbeddedNodeSpec.broker(
                node.nodeId(),
                node.nodeRole(),
                listenPort,
                haPort,
                baseStorePath,
                "embedded-cluster",
                brokerName,
                brokerId,
                namesrvAddr
        );
    }
}
