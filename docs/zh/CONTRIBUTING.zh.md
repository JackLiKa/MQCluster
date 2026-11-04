# 貢獻指南

首先——**感謝**你抽出時間貢獻！🎉

MQCluster 是一個社區驅動的學習平台，每一份貢獻——bug 報告、功能想法、文檔改進或代碼——都讓它對所有學習者更好。

本文檔涵蓋你開始有效貢獻所需的一切。

---

## 目錄

- [行為準則](#行為準則)
- [報告 Bug](#報告-bug)
- [請求功能](#請求功能)
- [開發環境搭建](#開發環境搭建)
- [代碼風格](#代碼風格)
- [添加一種新的 MQ](#添加一種新的-mq或新增一個跨-mq-的標籤頁)
- [提交規範（Conventional Commits）](#提交規範conventional-commits)
- [Pull Request 流程](#pull-request-流程)

---

## 行為準則

保持友善、尊重和建設性。我們期望所有貢獻者無論背景或經驗水平，都為每個人維持一個受歡迎的環境。任何形式的騷擾或歧視都不會被容忍。

---

## 報告 Bug

發現 bug？幫我們修復！

1. **搜索現有 issue**——查看[開放 issue](https://github.com/JackLiKa/MQCluster/issues)避免重複。
2. **使用 Bug 報告模板開新 issue**（如有）或包含：
   - **標題**——問題的簡潔摘要。
   - **環境**——操作系統、Java 版本、Node 版本、瀏覽器。
   - **重現步驟**——編號、最小化、具體。
   - **預期行為**——你認為會發生什麼。
   - **實際行為**——實際發生了什麼。
   - **日誌/截圖**——後端控制台輸出、瀏覽器控制台錯誤或截圖。
3. **保持響應**——維護者可能追問；請及時回覆。

> **安全相關 bug**：**不要**開公開 issue。見 [SECURITY.md](../../SECURITY.md)。

---

## 請求功能

有讓 MQCluster 更好的想法？

1. **搜索現有 issue**看是否已有人提出。
2. **用 Feature Request 標籤開新 issue**並包含：
   - **用例**——解決什麼問題？誰受益？
   - **提議方案**——描述你設想它如何工作。
   - **考慮過的替代方案**——任何變通或其他方法。
3. **等待討論**——維護者會在實現開始前分類並提供反饋。

---

## 開發環境搭建

### 前置條件

| 工具 | 版本 |
| --- | --- |
| JDK | 21+ |
| Maven | 3.9+（或使用內置 wrapper） |
| Node.js | 20+ |
| npm | 10+ |
| Git | 任何近期版本 |

### 克隆並運行

```bash
git clone https://github.com/JackLiKa/MQCluster.git
cd MQCluster

# 後端
cd java
./mvnw spring-boot:run          # Windows: .\mvnw.cmd spring-boot:run
# → http://localhost:8088

# 前端（新終端）
cd next
npm install
npm run dev
# → http://localhost:3000
```

### 推薦 IDE 配置

- **後端**：IntelliJ IDEA（含 Spring Boot 插件）或 VS Code + Java 擴展包。
- **前端**：VS Code + ESLint、Prettier 和 Tailwind CSS IntelliSense 擴展。

---

## 代碼風格

### Java（後端）

- 以 **Google Java Style Guide** 為基線。
- 使用 **4 空格縮進**。
- 包名小寫：`com.example.clustermanager.*`。
- 類名 `PascalCase`；方法和字段 `camelCase`。
- 常量 `UPPER_SNAKE_CASE`。
- 嚴格保持六邊形分層：
  - `core` 必須**零**外部依賴。
  - `application` 僅依賴 `core`。
  - `infrastructure` 實現 `core` 端口。
  - `api` 是唯一與外界對話的層。
- 優先構造器注入而非字段注入。
- 為新領域邏輯和適配器編寫測試。

### TypeScript / React（前端）

- **嚴格模式** TypeScript——無 `any` 除非有註釋說明理由。
- 僅使用**函數組件**和 hooks。
- 2 空格縮進。
- 優先命名導出；僅頁面組件用默認導出。
- 保持組件小而可組合；將共享邏輯提取到 hooks。
- 使用 Tailwind CSS 工具類樣式；除非必要避免自定義 CSS。
- 提交前運行 `npx tsc --noEmit` 和 `npm run lint`——兩者都必須通過。

---

## 添加一種新的 MQ（或新增一個跨 MQ 的標籤頁）

六邊形分層的好處是：新增一種中間件，每層只需動一次。建議按這個順序：

1. **Core** — 加 `MiddlewareType` 常量、領域模型與端口（`IClusterProvider`，以及可選的
   `IKafkaInspector` / `IStressLab` / `IInternalsInspector`）。`core` 必須保持零外部依賴。
2. **Infrastructure** — 新建 `infrastructure/<middleware>`，放 provider 與運行時；若需要壓力實驗，
   實現 `IStressLab` 並提供 `StressSender`，直接複用 `infrastructure/lab/StressRunner`，不要另寫一套引擎。
3. **能力聲明** — 在 `ProviderDescriptor` 裡聲明 `ProviderCapabilities`，前端靠它決定顯示哪些標籤頁。
4. **API** — 只有 MQ 專屬路由才需要新控制器；`/lab/stress` 與 `/lab/internals` 由 `LabController` 共用。
5. **前端** — 在 `lib/mq-registry.ts` 註冊（名稱、強調色、角色 → 形狀、帶 `requires` 的標籤頁），
   組件放 `components/<middleware>` 或直接複用 `components/lab`。
6. **測試** — 生命週期行為的 provider 測試 + 黑盒 API 測試，然後跑 `./mvnw clean verify`。
7. **文檔** — README 表格、`AGENTS.md`、以及成對的 `docs/ARCHITECTURE.md` 與
   `docs/zh/ARCHITECTURE.zh.md`（兩份文件結構保持一一對應）。

---

## 提交規範（Conventional Commits）

我們遵循 [Conventional Commits](https://www.conventionalcommits.org/) 規範。

### 格式

```
<type>(<scope>): <description>

[可選 body]

[可選 footer]
```

### 類型

| 類型 | 描述 |
| --- | --- |
| `feat` | 新功能 |
| `fix` | Bug 修復 |
| `docs` | 僅文檔變更 |
| `style` | 代碼風格變更（格式化，無邏輯變化） |
| `refactor` | 既不修 bug 也不加功能的代碼變更 |
| `perf` | 性能改進 |
| `test` | 添加或修正測試 |
| `chore` | 構建、工具或依賴變更 |
| `ci` | CI 流水線變更 |

### Scope

使用模塊或層名，如 `core`、`api`、`infra`、`frontend`、`docs`。

### 示例

```
feat(frontend): 為節點卡片添加實時 CPU 走勢圖
fix(infra): 修正 broker 重啟競態條件
docs: 更新快速開始前置條件表
test(core): 添加集群狀態轉換單元測試
```

> `#` issue 號在標題中可選，但在 PR footer 中鼓勵用於關閉 issue：
>
> ```
> fix(api): 處理拓撲端點中的 null cluster id
>
> Closes #42
> ```

---

## Pull Request 流程

1. **Fork** 倉庫並從 `main` 創建功能分支：
   ```bash
   git checkout -b feat/my-awesome-feature
   ```
2. **按上述代碼風格修改**。
3. **編寫或更新測試**。
4. **本地運行檢查**：
   ```bash
   # 後端
   cd java && ./mvnw clean verify

   # 前端
   cd next && npx tsc --noEmit && npm run lint && npm run build
   ```
5. **使用 Conventional Commits 提交**（見上）。
6. **推送**分支並對 `main` 開 Pull Request。
7. **填寫 PR 模板**——描述改了什麼、為什麼、如何測試。
8. **關聯相關 issue**（如 `Closes #42`）。
9. **回應 review 反饋**——向同一分支推送額外提交（合併前不要 squash）。
10. **維護者會在 CI 通過且 review 批准後合併**。

### PR 檢查清單

- [ ] 分支與 `main` 同步。
- [ ] 提交遵循 Conventional Commits。
- [ ] 後端測試通過（`./mvnw clean verify`）。
- [ ] 前端 typecheck、lint 與 build 通過。
- [ ] 行為變更時文檔已更新——`docs/ARCHITECTURE.md` 有改動時，同步更新 `docs/zh/ARCHITECTURE.zh.md`。
- [ ] 相對連結仍可解析（`python3 scripts/doc-lint.py`）。
- [ ] 無 secret 或憑證提交。

---

## 有疑問？

歡迎[開啟討論](https://github.com/JackLiKa/MQCluster/discussions)或 issue——我們很樂意幫你入門。

快樂編程！🚀
