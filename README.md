# wafer-spark-pipeline

以 Oracle 半導體資料模型為來源，用 **PySpark + Docker Compose** 建出的三層資料湖管線：
bronze（原始落地）→ silver（清理與標準化）→ **DQ 閘門** → gold（分析指標）→ 每日 HTML 報表，
含排程、執行歷史、失敗告警與冪等性驗證。

> 目的不是跑一個 demo，而是回答三個實際會被問到的問題：
> 資料壞掉誰會知道？重跑會不會算出不一樣的結果？慢在哪裡、怎麼證明？

## 架構

```
Oracle 23ai (8 tables)
      │  JDBC (ojdbc11)                      統一產生器：固定 seed、固定時間基準
      ▼
  bronze/  ── 原樣落地（seed 資料 + 放大資料：events 202 萬列）
      │       每日寫入 /data/ops/run_history.jsonl（每次執行一列 JSON）
      ▼
  silver/  ── 清理、型別轉換、LEFT JOIN、去重、衍生欄位（dur_sec / dt）
      │       rows 守恆檢查：輸出列數必須等於輸入列數（孤兒列不得被默默丟掉）
      ▼
  ★ DQ gate ── R1 孤兒外鍵 / R2 時間順序 / R3 規格內比例 / R4 缺日
      │        失敗 → 隔離到 quarantine、exit code 2（與程式錯誤區分）
      ▼
  gold/    ── wafer_yield、lot_event_summary、tool_load_hourly
      ▼
  reports/ ── daily_YYYYMMDD.html（Plotly 深色儀表板）

scheduler 容器：固定間隔呼叫 run_pipeline.sh；失敗寫 alerts.log（可選 POST 到 n8n）
```

## 專案結構

```
docker-compose.yml           spark-master / spark-worker / spark-client / oracle / scheduler
.env.example                 連線與排程參數範例（實際 .env 不進版控）
.gitattributes               *.sh 強制 LF（Windows autocrlf 會讓容器內 shell 壞掉）
run-spark.cmd|.ps1           執行單一 Spark job（自動帶 ojdbc11.jar）
run-pipeline.cmd|.ps1        執行整條管線（ingest → silver → dq → gold → report）
run-experiments.cmd|.ps1     執行效能實驗（唯讀，不動 gold）

jobs/
  00_hello.py                叢集煙霧測試（partition 與 bucket 分佈）
  10_ingest_bronze.py        Oracle → bronze（JDBC）
  11_generate_scale.py       確定性放大資料（2.9 天 → 71 天、200 萬事件）
  12_verify_bronze.py        bronze 驗收（基線 8 表 + 放大後筆數 + 注入缺陷比例）
  20_build_silver.py         bronze → silver（清理、LEFT JOIN、守恆檢查）
  21_verify_silver.py        silver 驗收 + 與 SQL 基線對照
  30_build_gold.py           silver → gold（三張指標表）
  31_verify_gold.py          gold 驗收 + 12 片晶圓良率逐筆對照 SQL
  40_dq_gate.py              4 條硬規則，失敗 exit 2 並寫 quarantine
  41_fingerprint.py          gold 指紋（驗冪等：重跑後內容必須完全相同）
  50_build_report.py         每日 HTML 報表（Plotly.js CDN，無額外套件）
  60-67_exp_*.py             M7 效能實驗（join 策略、分區數、實體計畫、threshold 掃描）

pipeline/
  run_pipeline.sh            管線執行器（計時、寫 run_history、失敗即停）
  scheduler.sh               最簡排程（sleep → run → repeat）
  run_experiments.sh         效能實驗批次

oracle/
  db/001_load_schema.sh      以 APP_USER 身分載入 schema/seed（含 < /dev/null 避免卡 stdin）
  schema/002_schema.sql      8 張表 DDL、具名約束、註解
  schema/003_seed.sql        132 筆基線資料

docs/
  DATA_CONTRACT.md           bronze 的欄位契約與型別
  METRICS.md                 三張 gold 表的指標定義與口徑
  OPERATIONS.md              維運手冊（三態、DQ 規則、隔離區、冪等測試）
  PERFORMANCE.md             效能實驗完整紀錄（含三次猜錯的檢討）

jars/.gitkeep                ojdbc11.jar 自行下載（不進版控）
wafer-spark-pipeline-plan.md 四週八里程碑的原始計畫
```

## 快速開始

前置：Docker Desktop（建議 ≥ 8 GB）、Windows PowerShell。

```powershell
git clone <repo> && cd wafer-spark-pipeline
Copy-Item .env.example .env
# 下載 JDBC driver（Oracle 官方 Maven）
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.26.4.0.0/ojdbc11-23.26.4.0.0.jar" -OutFile jars\ojdbc11.jar

docker compose up -d                     # 等 oracle healthy（首次約 1-2 分鐘）
.\run-spark.cmd jobs\00_hello.py        # 叢集煙霧測試
.\run-pipeline.cmd 1                     # 第一次要帶 1（含放大資料產生，約 8-10 分鐘）
.\run-pipeline.cmd                       # 之後正常跑（約 4-5 分鐘）
Start-Process (Get-ChildItem data\reports\daily_*.html | Select-Object -Last 1).FullName
```

Spark UI：`http://localhost:8080`（cluster）、`http://localhost:4040`（job，執行期間可見）。

## 里程碑與驗收數字

| 里程碑 | 內容 | 驗收結果（實測） |
|---|---|---|
| M1 | Spark standalone 叢集 + 煙霧測試 | Alive Workers 1、Cores 2、100,000 列、10 個 bucket 各 10,000 |
| M2 | Oracle JDBC → bronze + 確定性放大 | 基線 8 表 3/6/8/5/12/36/36/26；放大後 lot 6,000、wafer 78,000、events **2,028,000**、metrology 1,014,000、bins 468,000 |
| M3 | silver 清理層 | 2,028,000 列、主鍵全唯一、`dur_sec` 全為 8,100 秒、`out_ts` NULL 10,143（0.5001%） |
| M4 | gold 三張指標表 | wafer_yield 78,000、lot_event_summary 6,000、tool_load_hourly 5,007；良率 mean 95.0 / min 15.24 / max 100；**12 片逐筆對照 SQL 全數相符** |
| M5 | 排程、執行歷史、DQ 閘門、冪等 | 四步驟 18/105/74/70 秒（共 4 分 28 秒）；注入 20 筆孤兒 → `FAILED_DQ` exit 2、gold 時間戳未變；重跑指紋完全相同 |
| M6 | 每日 HTML 報表 + 告警 | 5 張 KPI + 4 張圖；失敗寫 `alerts.log`（可 POST 到 n8n） |
| M7 | 效能實驗 | 廣播 shuffle 0 MB vs 排序合併 9.8 MB；分區 2→8 快 20%；大 shuffle 下 32 最佳（1.36 秒） |

## 關鍵設計決策

1. **bronze/silver/gold 三層**：來源變了或程式改了，都能從 bronze 重跑，不必重新打 Oracle。
2. **silver 用 LEFT JOIN 而非 INNER JOIN**：INNER JOIN 會把孤兒列默默丟掉，讓列數守恆檢查失敗卻看不出原因；LEFT JOIN 讓列活下來，交由 DQ 判定。
3. **DQ 在 gold 之前**：壞資料不可以先變成人人看得到的報表。
4. **失敗分三態**：`SUCCESS` / `FAILED_DQ`（資料問題）/ `FAILED`（程式問題）。混在一起會讓值班的人修錯東西。
5. **冪等的定義是「重跑後內容相同」**，不是「重跑不報錯」；用 `41_fingerprint.py` 的 hash 總和驗證。
6. **良率採 `SUM(pass die) / SUM(all die)`**，不採「各 bin 比例取平均」，避免大小 bin 權重錯誤。
7. **在製品必須顯式計算**：`still_in_process = COUNT(*) - COUNT(out_ts)`，`MAX(out_ts)` 會忽略 NULL 而給出「全部已出站」的假象。
8. **生成資料確定性**：固定 seed、固定 2026-01-01、`xxhash64` 產生事件 id，使同日重跑結果一致。

## 踩雷筆記（每一條都實際發生過）

| 現象 | 原因 | 解法 |
|---|---|---|
| `UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS` | `explode()` 不能寫在 `CAST()` 裡面 | 拆成兩個 `withColumn`：先 explode、再 cast |
| `parquet` schema 推斷失敗 | `F.lit(None)` 沒有型別 | `F.lit(None).cast("timestamp")` |
| `TypeError: fromisoformat: argument must be str` | Spark 從分區目錄名 `dt=2026-01-01` **反推型別**，字串讀回來變 `date` | 讀取後 `F.date_format("dt", "yyyy-MM-dd")` 正規化 |
| `Cannot modify the value of a static config` | `spark.sql.warehouse.dir` 屬靜態設定 | 改用 `SparkSession.builder.config(...)` |
| 報表兩張圖空白 | Plotly 會把解析後的軸設定**寫回傳入的 layout 物件**，四張圖共用同一個 `LAYOUT.yaxis`，分類軸污染了後面兩張 | 每張圖用 factory 產生獨立 layout |
| 分類軸長條圖互相疊 | 步驟名稱跨 run 重複，分類軸合併同名類別 | x 用唯一索引、`ticktext` 才顯示名稱 |
| 隔離資料夾名為 `__manual` | `RUN_ID` 沒 export 給子行程 | `export RUN_ID` |
| ShuffledHashJoin 怎麼設都不出現 | `autoBroadcastJoinThreshold = -1` 同時讓 `canBuildLocalHashMapBySize` 恆假 | 門檻設正值（如 512 KB） |
| `forceApplyShuffledHashJoin` 無效 | 原始碼寫死 `Utils.isTesting`，只對 Spark 自家測試有效 | 放棄這個開關 |
| 一個 join 用 SHJ、另一個用 SMJ | 優化器用的是**估計列寬**（2.1–3.6 MiB），不是磁碟上的 375 KB | 用 `EXPLAIN COST` 對照，不要用 `ANALYZE` 的位元組數推算 |

## 效能實測摘要

| 實驗 | 結論 |
|---|---|
| join 策略 | 自動廣播門檻內 Catalyst 就選 BroadcastHashJoin，shuffle 0 MB；關掉廣播變 9.8 MB，但耗時差在誤差內 → **這個資料量下 join 不是瓶頸** |
| shuffle 分區 | 2→8 快 20%（15.43s → 12.29s）；8/32/200 差距 0.16 秒＝雜訊 |
| 大 shuffle（23.4 萬 key） | 8: 1.74s、32: 1.36s、200: 1.40s → 膝點在核心數的 2–4 倍 |
| 實體計畫 | `PartitionFilters` 與 `ReadSchema` 兩項下推都成立 |

完整過程與方法論見 `docs/PERFORMANCE.md`。

## 已知限制

- 單機 Docker（2 cores / 4 GB executor），沒有分散式儲存，資料量上限約千萬列。
- 沒有 streaming：管線是批次排程，不是即時。
- Parquet 尚無 schema evolution 與時間旅行（可換 Delta Lake，但會增加映像體積）。
- 告警只寫本機 log／可選 webhook，沒有 retry 與 backoff。
- 報表用 CDN 的 Plotly.js，離線環境需自行內嵌。
