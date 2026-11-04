# PySpark ＋ 工程化四週計畫（wafer-spark-pipeline）

> 前提（已確認）：每週 8–10 小時｜沿用現有 8 表晶圓資料並生成放大到百萬列｜本機 Docker 跑 PySpark standalone｜工程化＝Docker Compose ＋ 排程 ＋ 自製監控｜以「面試可講的完整 pipeline」為主，原理點到為止。

---

## 為什麼是這個專案（面試敘事）

你的 SQL 專案已經證明「會寫產線報表」，這個專案要證明的是下一段：

> **「資料量從 132 筆變成 200 萬筆、從人工執行變成每天自動跑、從跑完就好變成有監控與資料品質攔阻」——同一組晶圓問題，換一個量級的解法。**

面試時可以這樣講一句話：

> 我把一個 8 張表的晶圓廠資料模型，做成 bronze／silver／gold 三層的 Spark 管線；產出的良率與事件摘要數字，和我先前用純 SQL 驗證過的結果完全一致；管線有資料品質閘門，去年（模擬）有 3 次因為孤兒外鍵被擋下而沒有污染報表。

最後那句「被擋下」比任何漂亮數字都更像真的做過工程。

---

## 環境架構

```
┌──────────────────── Docker Compose ─────────────────────┐
│                                                          │
│  oracle (沿用 oracle-sql-lab)   spark-master             │
│     8 表 / 132 筆基線         ├── spark-worker           │
│          │                    └── spark-client（submit）  │
│          │ JDBC ingest                                    │
│          ▼                                                │
│   ┌──────────────────────────────────────────────┐        │
│   │  /data/lake                                  │        │
│   │  bronze/  原始落地（Parquet，分區）          │        │
│   │  silver/  清洗＋JOIN 後的寬表（Parquet）     │        │
│   │  gold/    報表（Parquet + 每日快照）         │        │
│   │  quarantine/ 被 DQ 擋下的壞資料              │        │
│   └──────────────────────────────────────────────┘        │
│          ▼                                                │
│  scheduler（cron 風格的排程容器）                          │
│    └─ run history (JSON/Parquet) ＋ DQ gate ＋ 報表 HTML    │
└──────────────────────────────────────────────────────────┘
```

- 映像：`apache/spark:3.5.x`（master / worker / client 各一）
- 資源：worker `4g`、2–3 cores；Docker Desktop（WSL2）分配 8 GB 給 Docker
- 工具腳本沿用你的習慣：`run-spark.cmd`（包 `spark-submit`，純 ASCII、`-ExecutionPolicy Bypass`）

## 資料規模與生成策略

| 層 | 表 | 列數 | 來源 |
|---|---|---:|---|
| 真實 | 8 張基線表 | 132 | **從 Oracle 用 JDBC 讀進來**（證明會做真實 ingest） |
| 放大 | `process_event` | **200 萬** | 由基線參數生成（lot／wafer／step／tool／時間規律） |
| 放大 | `metrology` | **100 萬** | 依 metric 的規格上下限生成，含固定比例超規 |
| 放大 | `wafer_bin` | **50 萬** | 每片晶圓 1–3 個 bin，`pass_fail` 分佈可控 |
| 放大 | `wafer` / `lot` | 8 萬 / 6,000 | 由 product、priority 分佈生成 |

**刻意保留的三個特徵**（和 SQL 專案同源，可用來對照驗收）：

1. 固定比例（0.5%）的 `out_ts IS NULL` → 練 `still_in_process`
2. 固定比例（0.3%）的超規量測 → 練 Spec 判定與 DQ
3. 少量孤兒外鍵（每天約 20 筆，藏在「壞資料」生成器裡）→ 練 DQ gate 攔阻

> 規模控制在 **Parquet 合計 < 1.5 GB**，筆電跑得動；真正的 Spark 價值（shuffle、分區裁剪）在這個量級就看得出來。

---

## 四週里程碑（8 個 M，每個約 4–5 小時）

### 第 1 週：場地與資料（M1–M2）

**M1｜Spark 叢集跑起來（4h）**
- `docker-compose.yml`：spark-master / spark-worker / spark-client（+ 沿用 oracle）
- 寫第一支 job `jobs/00_hello.py`：讀 `range(100000)` 做 groupBy 計數，寫 Parquet
- **驗收**：`spark-submit` 成功、Spark UI（`:8080` master、`:4040` job）看得到 app、輸出 Parquet 列數 = 100,000
- **原理（只講這些）**：`local[*]` vs `spark://` 的差異、lazy evaluation（為什麼 `show()` 才真的跑）

**M2｜從 Oracle 進資料 ＋ 生成百萬列（5h）**
- `jobs/10_ingest_bronze.py`：JDBC 讀 8 張基線表 → `bronze/seed/*.parquet`
- `jobs/11_generate_scale.py`：由 seed 參數生成放大資料 → `bronze/events/`、`bronze/metrology/`、`bronze/bins/`（按 `dt` 分區）
- **驗收**：bronze 各表列數 = 132 真實 + 上表放大值；`events` 的 `step_name` 分佈為 1/3、1/3、1/3；`out_ts IS NULL` 比例 = 0.5% ± 0.02%
- **要交出的檔案**：`docs/01_data_contract.md`（每張表的 schema、筆數、允許值範圍）

### 第 2 週：轉換與報表（M3–M4）

**M3｜bronze → silver：JOIN 與清洗（5h）**
- 三表 JOIN（event → wafer → lot）成寬表，補 `lot_id`、`priority`、`product_id`
- 去重規則、時間欄位正規化、加入 `dur_sec`（TIMESTAMP 相減 → 秒）
- **驗收（可與 SQL 對照）**：用**同樣的 132 筆基線**跑 silver，良率與事件摘要必須和 SQL 專案一模一樣——L-2601-01 = 100%、L-2605 `still_in_process` = 2、每站 12 筆
- **原理**：shuffle 是什麼、`broadcast` 為什麼能省掉 shuffle（dimension 表很小）

**M4｜silver → gold：三個產線報表（5h）**
- `gold/wafer_yield_daily`：每片良率（對應你的 `v_wafer_yield`）
- `gold/lot_event_summary`：每批事件數／首進站／末出站／未完成數（對應 `v_lot_event_summary`）
- `gold/tool_load_hourly`：每小時機台負載（對應 M11 的時間分桶）
- **驗收**：三張報表的數字與 SQL 版一致（基線資料範圍內）；`tool_load_hourly` 的總事件數 = 200 萬
- **要交出的檔案**：`docs/02_metrics_dictionary.md`（每個指標的定義、口徑、SQL 對照）

### 第 3 週：工程化（M5–M6）

**M5｜排程、執行歷史與 DQ gate（5h）**
- `scheduler` 容器：每天固定時間跑整條管線（`01_ingest → 11_generate → 20_silver → 30_gold`）
- `run history`：每次執行寫一筆（`run_id`、開始/結束、各層筆數、耗時、狀態）
- **DQ gate（重點）**：4 條規則，任一不過就中止並寫入 `quarantine/`
  1. 孤兒外鍵 = 0（event.wafer_id 必須存在於 wafer）
  2. `out_ts >= in_ts`
  3. `metric_value` 落在 `[spec_lower, spec_upper]` 的比例 ≥ 99%
  4. 每日事件數與前一天的變動 < 20%
- **冪等**：同一天重跑兩次，gold 的列數與數字必須完全相同（分區覆寫）
- **驗收**：故意塞 20 筆孤兒外鍵 → 管線**失敗**、狀態記為 `FAILED_DQ`、`quarantine` 有那 20 筆、gold 沒有被更新

**M6｜監控儀表板與告警（4h）**
- 每跑完一次產生 `reports/daily_<date>.html`（Plotly 深色主題：良率趨勢、機台負載、未完成數）
- 告警：DQ 失敗或耗時超過門檻 → 寫 `alerts.log`（可選：接你的 n8n webhook 發 Telegram）
- **驗收**：故意讓一次執行失敗，`alerts.log` 有紀錄、HTML 報表顯示上一次成功的資料並標註「上次更新時間」

### 第 4 週：效能、文件與收官（M7–M8）

**M7｜效能實驗（4h）**
- 三個對照，各量時間並記在 `docs/03_performance.md`：
  1. `broadcast(small_dim)` vs 一般 join（看 shuffle bytes）
  2. 分區數：`repartition(8)` vs `coalesce(1)` 寫檔耗時與檔案數
  3. `explain(True)` 看 physical plan 的 `Exchange`（shuffle 節點）
- **驗收**：表格有前後數字（例如 shuffle 從 1.2 GB 降到 40 MB），並且你能用一句話解釋為什麼

**M8｜文件與面試講稿（4h）**
- `README.md`：架構圖、資料契約、怎麼跑、有哪些報表、DQ 觸發過幾次
- `docs/04_interview_story.md`：3 分鐘版／30 秒版，各含「量化成果＋一次失敗經驗」
- 收尾：`git push`、repo 設為 public

---

## 每週節奏（約 9 小時）

| 時段 | 內容 |
|---|---|
| 平日 4 天 × 1.5h | 教學 20 分鐘 → 你實作 → 我批改（沿用 SQL 專案的節奏） |
| 週末 1 天 × 3h | 該週里程碑收尾、驗收、commit、寫文件 |

## 驗收原則（沿用 SQL 專案，不打折）

1. **每個 M 都有可驗收的預期值**（筆數、比例、良率、耗時），不是「看起來對」
2. **與 SQL 專案對照**：能對照的一定對照，這是這個專案最強的說服力
3. **每個 M 一個 commit**，訊息寫清楚做了什麼與為什麼
4. 遇到錯誤先抄錯誤訊息再修（你的習慣，已經很有效）

## 風險與對策

| 風險 | 對策 |
|---|---|
| Windows + Docker 跑 Spark 記憶體不足 | worker 固定 `4g`、只用 2–3 cores；資料量先跑 20 萬列，成功後再放大 |
| `spark-submit` 路徑與權限問題（你踩過 PowerShell 的 `@`） | 一律用 `run-spark.cmd` 包裝，參數走檔案不走命令列 |
| JDBC jar 版本 | 用 `ojdbc11.jar`，放在 `jars/` 並在 compose 掛載，不從網路臨時下載 |
| 生成資料寫太慢 | 先寫 10 萬列驗證程式，再改參數放大；寫檔用 `partitionBy('dt')` |
| 四週做不完 | **M7（效能）可延後**，M1–M6＋M8 就是完整可講的作品 |

## 這個專案結束後你會有的能力（面試可講的句子）

- 「我用 Spark 做過 bronze/silver/gold 分層，並用 Parquet 分區支援增量重跑」
- 「我知道 join 會觸發 shuffle，所以把 dimension 表 broadcast 掉，shuffle 量降了一個數量級」
- 「我的管線有資料品質閘門，孤兒外鍵會讓 job 失敗而不是讓報表變錯」
- 「同一組指標我用 SQL 與 Spark 各實作一次，數字一致——所以我知道兩邊的差異在哪裡」
