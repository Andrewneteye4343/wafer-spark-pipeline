# 效能實驗（M7）

三個實驗，目的都是把「這條管線為什麼這樣寫」變成可以講數字的句子。

執行：

```powershell
.\run-experiments.cmd
```

產出：
- `data/ops/experiments.jsonl`（每次一行，可累積比較）
- `data/ops/experiments_<run_id>.log`（完整 transcript）
- `data/ops/plans/physical_plans.txt`（實體計畫）

---

## 實驗一：join 策略對決（`jobs/60_exp_joins.py`）

同一條查詢、四種實體計畫：

| variant | 設定 | 預期實體計畫 |
|---|---|---|
| `auto` | 預設（門檻 10 MB） | BroadcastHashJoin（wafer 只有 78k 列、遠小於門檻） |
| `broadcast` | 加 `/*+ BROADCAST(w) */` | BroadcastHashJoin |
| `sort_merge` | 關閉自動廣播 | SortMergeJoin + Exchange ×2 |
| `shuffle_hash` | 關閉自動廣播、偏好的 hash | **實測仍是 SortMergeJoin**（見下方結果） |

**要看的三件事**（不只是時間）：
1. `plan` 欄位：Catalyst 到底選了哪一個（不是你說要哪個就一定是哪個）
2. `shuffle_write_mb`：`broadcast` 應該是 **0 MB**（不用搬資料），`sort_merge` 會把兩邊都重分區
3. `fingerprint`：四個 variant 必須完全相同——**先確定答案一樣，才比較快慢**

小表廣播的價值就在這裡：把「搬 200 萬列」換成「複製 7.8 萬列到每台 executor」。

---

## 實驗二：shuffle partition 數量（`jobs/61_exp_partitions.py`）

同一條聚合，`spark.sql.shuffle.partitions` 依序 2 / 8 / 32 / 200。

**為什麼不是越多越好**：
- 分區太少 → 2 個 task 餵 2 個 core，看起來剛好，但每個 task 要處理 100 萬列，中間還要 spill
- 分區太多 → 200 個 task 各自只處理 1 萬列，排程與開啟 task 的固定成本佔比變高，而且輸出變成 200 個小檔案

判斷準則：**2–4 個 task / core**，2 cores 就是 8–16。落點在哪由你的機器實測決定。

---

## 實驗三：讀實體計畫（`jobs/62_explain.py`）

三條查詢對應三個要看得懂的地方：

1. **`Q1a` vs `Q1b`**：加了 `dt` 過濾後，`FileScan parquet` 的 `PartitionFilters` 會只列出那一天的分割區。
   計畫裡若看不到 `PartitionFilters`，代表過濾沒有下推，整份資料都會被讀。
2. **`Q2`**：小表 join 出現 `BroadcastHashJoin`，左邊不會有 `Exchange`（不 shuffle）。
3. **`Q3`**：聚合會看到兩層 `HashAggregate`（partial → final），中間夾一個 `Exchange`；
   這是 map-side 預聚合，能大幅減少 shuffle 量。

`[exp]` 印出的「一整份 vs 單一天」列數比例，就是**分區裁剪的實際收益**。

---

## 量測的誠實說明

- 每個 variant **只跑一次**，時間會有 ±10% 的 run 間變異（JVM 暖機、快取、排程抖動）。
  要下結論前至少跑兩次，兩次趨勢一致再寫進結論。
- 第一次執行會比較慢（讀 Parquet 到 OS page cache）；比較時看清楚是不是同一輪的數字。
- shuffle bytes 來自 driver 自己的 Spark UI REST API（`spark.uiWebUrl`），是累計值相減，不是估算。

## 面試可以講的一句

> 這條管線在 2 core 的環境上，我把 join 從 sort-merge 改成 broadcast，shuffle 寫入從 X MB 降到 0，
> 端到端從 A 秒降到 B 秒；聚合的 shuffle partitions 從 200 調到 16 後再快 C%。
> 每次調整都用 hash fingerprint 驗證結果完全相同，避免「變快但算錯」。

---

# 實測結果（2026-11-05，RTX 3050 Ti 筆電／Docker，worker 2 cores）

## 實驗一：join（資料量 200 萬 events ⋈ 7.8 萬 wafer ⋈ 6 千 lot）

| variant | 實體計畫 | 耗時 | shuffle 寫入 | 正確性 |
|---|---|---|---|---|
| `auto` | BroadcastHashJoin | 14.63s | 0.0 MB | ✓ |
| `broadcast` | BroadcastHashJoin | 13.00s | 0.0 MB | ✓ |
| `sort_merge` | SortMergeJoin | 14.72s | 9.8 MB | ✓ |
| `shuffle_hash` | **SortMergeJoin** | 13.54s | 9.8 MB | ✓ |

四個 variant 的 `fp=1144590852228642` **完全相同** —— 先證明答案一致，才有資格談快慢。

**三個結論**：

1. **`auto` 與 `broadcast` 一樣好，因為 Catalyst 本來就會廣播。**
   自動廣播門檻是 10 MB，wafer 只有約 2 MB，本來就落在門檻內。hint 只在「優化器原本會選別的」時才有價值——
   13.00s vs 14.63s 的差距是 run 間變異，不是 hint 的功勞。

2. **不廣播的代價是 9.8 MB shuffle，但時間沒有變慢。**
   9.8 MB 在 gigabit 級的本地 shuffle 上不足以造成延遲。也就是說：**在這個資料量下，join 不是瓶頸**。
   要看到 broadcast 的價值，資料量必須再大一個數量級，或 executor 較多（廣播把「搬資料」換成「複製小表」，
   搬的量越大、節點越多，差距才出現）。

3. **config 不是命令。** `spark.sql.join.preferSortMergeJoin=false` 要求偏好 ShuffledHashJoin，
   Catalyst 仍然選了 SortMergeJoin（見實驗四）。

## 實驗二：shuffle partitions（小聚合，輸出 5,007 列）

| partitions | 耗時 | shuffle 寫入 |
|---|---|---|
| 2 | 15.43s | 0.1 MB |
| 8 | 12.29s | 0.1 MB |
| 32 | 12.14s | 0.1 MB |
| 200 | 12.13s | 0.2 MB |

**誠實解讀**（這裡很容易講錯）：

- **2 → 8 是真實改善**：−3.1 秒（−20%）。1 個分區的 task 扛 200 萬列，另外的 core 在等。
- **8 / 32 / 200 之間都是雜訊**：差距只有 0.16 秒，而單次量測的變異就有 ±10%。
  「200 最快」不是結論，是捨入誤差。
- **為什麼差別這麼小**：這個聚合的 shuffle 只有 0.1 MB（輸出 5,007 列）。沒有東西可以平行化，
  分區數再多也只是換個方式跑同樣的工作。

補充：日誌顯示 `executor cores=1`——worker 有 2 cores，但每個 executor 只用 1 core，
所以真正的並行上限是「executor 數量 × 1」。要看分區數的影響，得先讓核心數對齊。

## 實驗三：實體計畫（Q1a）

```
FileScan parquet [event_id, step_name, dt] ...
  PartitionFilters: [isnotnull(dt#15), (dt#15 = 2026-02-10)]
  ReadSchema: struct<event_id:bigint, step_name:string>
```

兩個下推都成立：

- `PartitionFilters` 有值 → **分區裁剪生效**，只會讀 `dt=2026-02-10` 那個資料夾
- `ReadSchema` 只有 2 個欄位 → **欄位裁剪生效**（`dt` 僅用於過濾，不進 output）

對照 Q1b（無過濾）會是全部分割區 + 全欄位。這就是「分區寫得好，查詢就快」的實體證據。

---

# 實驗四（補做）：統計資訊與大 shuffle

`jobs/63_exp_stats.py` 回答實驗一與實驗二留下的兩個疑問。

**(a) 為什麼 `preferSortMergeJoin=false` 沒用？**
Catalyst 的決策依賴統計資訊。Parquet 讀進來時只有檔案大小，沒有列數與欄位分佈（NDV），
成本模型是半盲的。這個實驗把表註冊進 catalog、`ANALYZE TABLE ... COMPUTE STATISTICS`、
打開 `spark.sql.cbo.enabled`，再看一次同樣的三個 variant。計畫若改變，
結論就是「**計畫跟著統計走，不是跟著 config 走**」。

**(b) 分區數在大 shuffle 下才有意義。**
改成 `group by wafer_id x step_name`（23.4 萬個 key、真正的 shuffle），
比較 8 / 32 / 200。這才是能拿來說「分區數怎麼調」的資料。

## 量測的誠實說明（保留）

- 每個設定只跑一次，±10% 是常態；差距小於 0.5 秒的都不要當結論。
- shuffle bytes 來自 driver 自己的 Spark UI REST API，是累計值相減。
- 覺得「變快」之前，先確認 `fp` 沒變——**快但是錯，比慢更糟**。

## 面試可以講的版本（用實測數字，不用形容詞）

> 我在 200 萬列的管線上比較了四種 join 實體計畫。自動廣播門檻 10 MB 時 Catalyst 本來就選
> BroadcastHashJoin，shuffle 寫入 0 MB；關掉自動廣播後變成 SortMergeJoin、多出 9.8 MB shuffle。
> 但我同時量到耗時差在誤差內，所以我沒有說「broadcast 快兩倍」——我的結論是
> **在這個資料量下 join 不是瓶頸**，真正的成本在別處。
> 分區數我從 2 調到 8 得到 20% 改善，8 到 200 之間是雜訊，我就照實寫雜訊。
> 每次調整都用 hash fingerprint 驗證結果完全相同。

---

# 實驗五／六：join 策略為什麼不聽話（已對照 Spark 3.5 原始碼）

## 原始碼怎麼選 join（`sql/core/.../SparkStrategies.scala`，`object JoinSelection`）

沒有 hint 時，規則**依序**判斷，先命中者勝：

1. **Broadcast hash join** — 有一邊小到可以廣播（`sizeInBytes <= autoBroadcastJoinThreshold`）
2. **Shuffle hash join** — 有一邊小到可以建本地 hash map、且遠小於另一邊、且 `preferSortMergeJoin=false`
3. **Sort merge join** — 兩邊 key 可排序
4. Cartesian product
5. Broadcast nested loop join

## 三個判斷式的實際定義（`optimizer/joins.scala`）

```scala
// SHJ 的尺寸條件
def canBuildLocalHashMapBySize(plan, conf) =
  plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions

// 「遠小於」：建 hash map 比排序貴，所以要求小 3 倍以上
def muchSmaller(a, b, conf) =
  a.stats.sizeInBytes * conf.getConf(SQLConf.SHUFFLE_HASH_JOIN_FACTOR) <= b.stats.sizeInBytes
```

## 於是實驗四／五的結果全部有解釋了

| 觀察 | 原因 |
|---|---|
| `threshold=-1` 時 SHJ 永不出現 | `sizeInBytes < -1 × 8` 恆假。**關掉廣播的設定，同時鎖死了 SHJ 的尺寸條件** |
| `threshold=100KB` 時出現 SHJ | wafer 375,918 B ≥ 100KB → 不可廣播；但 375,918 < 100KB×8 = 819,200 → 可 hash；且 375,918×3 ≤ 22,307,186 → 遠小於 → **兩個條件都成立** |
| `forceApplyShuffledHashJoin=true` 完全無效 | 原始碼寫死：`Utils.isTesting && ...`，註解明講「testing only and should not be exposed」。一般 `spark-submit` 執行時 `isTesting=false`，**這個開關對我們是死的** |
| v4 出現 SHJ **+ SMJ**、沒有 BSH | 待驗：AQE 可能在執行期把 broadcast 降級為 shuffle（門檻只有 100 KB 很容易被降級） |

## 已量測（Spark 3.5.9，200 萬列 events ⋈ 7.8 萬 wafer ⋈ 6 千 lot）

| variant | 觀察到的策略 | Exchange | 耗時 | shuffle 寫入 |
|---|---|---|---|---|
| v1 預設 | BroadcastHashJoin | 2 | 2.12s | 0.0 MB |
| v2 門檻−1 + 偏好SMJ | SortMergeJoin | 4 | 3.84s | 9.8 MB |
| v3 門檻−1 + 不偏好SMJ | SortMergeJoin | 4 | 2.91s | 9.8 MB |
| v4 門檻100KB + 不偏好SMJ | ShuffledHashJoin+SortMergeJoin | 4 | 2.64s | 9.8 MB |
| v5 門檻−1 + force | SortMergeJoin | 4 | 2.74s | 9.8 MB |

注意這批數字因為表已 materialize 到 warehouse、被 page cache 記住，**不能和實驗一的 13–14 秒直接比較**；
同一輪內的相對關係才有效。`Exchange` 數量則是可以直接比較的結構性指標：
廣播 2 個、任何需要 shuffle 的做法都是 4 個。

## 這一段面試可以怎麼講

> 我原本以為把 `autoBroadcastJoinThreshold` 設成 −1 就只是「關掉廣播」，
> 結果 ShuffledHashJoin 也一起消失。去讀 `JoinSelection` 才發現它判斷 hash join 可不可行時
> 重用了同一個門檻（`build.size < threshold × numShufflePartitions`），所以 −1 讓那個不等式恆假。
> 我把門檻改成 100 KB（讓 375 KB 的 wafer 不可廣播、但仍在 800 KB 的 hash 上限內），
> ShuffledHashJoin 就出現了。**這件事讓我學會：調參數之前先看那個參數被哪些判斷式共用。**
---

# 結案：為什麼 wafer 那一邊的 join 沒有變成 ShuffledHashJoin

## 關鍵發現：`ANALYZE` 的位元組數 **不是** 優化器用的數字

| 來源 | `exp_wafer` 大小 |
|---|---|
| `ANALYZE TABLE`（磁碟上壓縮後） | **375,918 bytes** |
| `EXPLAIN COST`（優化器實際使用） | **2.1 MiB**（scan 3.6 MiB、投影後 2.1 MiB） |

比值約 10 倍。原因是 Spark 在沒有欄位級統計時，用**列數 × 估計列寬**
（字串預設每個 20 bytes、不考慮壓縮）來估大小；Parquet 實體檔案是壓縮過的，
所以「磁碟大小」和「優化器眼中的大小」是兩件事。

## 於是把判斷式代進去，答案就出來了

```scala
canBuildLocalHashMapBySize(side) = side.sizeInBytes < autoBroadcastJoinThreshold * numShufflePartitions
```

門檻 100 KB、8 個 shuffle 分區 → 上限 = **819,200 bytes ≈ 0.78 MiB**

| join | build side 的估計大小 | 上限 0.78 MiB | 結果 |
|---|---|---|---|
| `events ⋈ wafer` | wafer 投影後 **2.1 MiB** | 2.1 > 0.78 ✗ | **SortMergeJoin** |
| `wafer ⋈ lot` | lot 約 **0.34 MiB** | 0.34 < 0.78 ✓ | **ShuffledHashJoin** |

**這就是完整解釋**：不是 AQE、不是 CBO 沒生效、也不是 config 打錯——
是我拿「磁碟上的 375 KB」去算，而優化器拿的是「估計的 2.1 MiB」。
同一組設定下，`lot` 剛好在上限內、`wafer` 超出，才會出現一個 SHJ 一個 SMJ。

## 教訓（這才是這一段的真正價值）

1. **算優化器的條件時，只能用 `EXPLAIN COST` 的數字**，不能用 `ANALYZE` 的檔案大小。
2. 壓縮率會讓「看起來很小的表」在優化器眼中變大 10 倍；文字欄位特別明顯。
3. 我在這一段連續猜錯三次（AQE、CBO、config），每一次都是因為**用推論代替讀数字**。
   `EXPLAIN COST` 一印出來，答案就在那裡。

## 推論出來的預測（實驗八驗證）

| 門檻 | SHJ 上限 | wafer（2.1 MiB）可否 hash | 預期策略 |
|---|---|---|---|
| 100 KB | 0.78 MiB | ✗ | SortMergeJoin |
| 512 KB | 4.00 MiB | ✓ | **ShuffledHashJoin** |
| 1 MB | 8.00 MiB | ✓ 但 wafer 2.1 MiB ≤ 1 MB？否 → 仍 SHJ；lot 0.34 MiB ≤ 1 MB → 廣播 | ShuffledHashJoin + BroadcastHashJoin |

## 驗證結果（實驗八）：機制成立

| 門檻 | SHJ 上限 | 優化器看到的 wafer | 實際選的策略 |
|---|---|---|---|
| 100 KB | 0.78 MiB | 3.6 MiB（投影後 2.1 MiB） | **SortMergeJoin** |
| 512 KB | 4.00 MiB | 同上 | **ShuffledHashJoin** ✓ |
| 1 MB | 8.00 MiB | 同上 | **ShuffledHashJoin** ✓ |

零界點落在 0.78 MiB 與 4.00 MiB 之間，正好涵蓋 2.1–3.6 MiB 的估計大小——
**預測與實驗一致，機制成立**：SHJ 出不出的唯一關鍵，是「優化器眼中的大小 < 門檻 × 分區數」。

一個要照實記錄的失誤：我對 1 MB 那行預測「ShuffledHashJoin + BroadcastHashJoin」是錯的，
因為那次掃描的查詢**只有 events ⋈ wafer 兩張表，根本沒有 lot**。預測寫錯，不是實驗錯。

## M7 總結：這條管線到底學到什麼

| 現象 | 原因 | 可驗證證據 |
|---|---|---|
| 廣播 vs 排序合併 | 小表廣播可省去 shuffle | shuffle 寫入 0.0 MB vs 9.8 MB |
| 時間差很小 | 9.8 MB 在這台機器上不是瓶頸 | 14.63s / 13.00s / 14.72s 全在誤差內 |
| 分區 2→8 有改善 | 單一 task 扛 200 萬列 | 15.43s → 12.29s |
| 分區 8→200 無感 | shuffle 只有 0.1 MB，沒有東西可切 | 12.29 / 12.14 / 12.13s |
| 大 shuffle 下 32 最快 | 膝點在核心數的 2–4 倍 | 8: 1.74s / 32: 1.36s / 200: 1.40s |
| SHJ 不出現 | 關閉廣播的 −1 門檻同時鎖死 SHJ 尺寸條件 | `sizeInBytes < -1 × 8` 恆假 |
| force 開關無效 | 原始碼寫死 `Utils.isTesting` | 只對 Spark 自家測試有效 |
| 一個 SHJ 一個 SMJ | 優化器用估計列寬（2.1–3.6 MiB），不是磁碟 375 KB | `EXPLAIN COST` 對照 `ANALYZE` |
| 分區裁剪生效 | `PartitionFilters` 有值 | Q1a 計畫 |

## 30 秒面試版本

> 我在 200 萬列的三表 join 管線上做效能實驗。自動廣播門檻內 Catalyst 就選 BroadcastHashJoin、
> shuffle 寫入 0 MB；關掉廣播後變 9.8 MB，但耗時差在誤差內，所以我照實說「這個資料量下 join 不是瓶頸」。
> 分區數 2→8 快 20%，8→200 是雜訊，我沒有把雜訊寫成成果。
> 過程中我原本以為 SHA 跟 config 有關，實際讀 `JoinSelection` 原始碼才發現
> 判斷式重用廣播門檻、而且優化器用的是估計列寬而不是磁碟大小——
> 最後用 `EXPLAIN COST` 把兩個數字並列，機制才確立。**我的結論都是量出來的，不是猜的。**
