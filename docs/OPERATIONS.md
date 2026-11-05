# 維運手冊（M5）

## 一次執行包含什麼

```
run_pipeline.sh
  ├─ [可選] generate  11_generate_scale.py   RUN_GENERATE=1 才跑（重建放大資料）
  ├─ ingest           10_ingest_bronze.py    Oracle → bronze/seed
  ├─ silver           20_build_silver.py     bronze → silver/event_wide
  ├─ dq               40_dq_gate.py          ★ 資料品質閘門（失敗即中止）
  └─ gold             30_build_gold.py       silver → gold 三張報表
```

**順序是刻意的**：DQ 在 gold 之前。壞資料不可以先變成人人看得到的報表。

## 執行歷史

每次執行追加一行 JSON 到 `data/ops/run_history.jsonl`：

```json
{"run_id":"20261105T021500Z","step":"dq","job":"40_dq_gate.py","status":"FAILED_DQ","exit_code":2,"duration_sec":48,"ts":"2026-11-05T02:16:03Z"}
```

`status` 只有三種：

| status | 意義 | 誰要處理 |
|---|---|---|
| `SUCCESS` | 通過 | — |
| `FAILED_DQ` | 資料品質不合格（exit 2） | 製程／資料擁有者 |
| `FAILED` | 程式或環境錯誤（exit 1 或其他） | 工程師 |

**把「資料不好」與「程式壞掉」分成兩種狀態**，是維運最基本的設計：前者不該叫人去改程式，後者不該叫人去修資料。

詳細日誌：`data/ops/run_<run_id>.log`

## DQ 規則（硬性，任一失敗即中止）

| 規則 | 檢查 | 為什麼 |
|---|---|---|
| R1 orphan | 每筆 event 的 `wafer_id` 必須存在於 `wafer` | 孤兒列會讓所有 JOIN 靜默少資料 |
| R2 time order | `out_ts >= in_ts` | 時間倒流代表資料來源或時鐘有問題 |
| R3 spec ratio | 落在規格內的比例 ≥ 99% | 低於此代表量測或製程失控，不是正常波動 |
| R4 missing days | 首末事件之間的每一天都要有資料 | 缺日＝管線漏抓，比錯資料更難發現 |

被擋下的資料列會寫進 `data/lake/quarantine/<規則>__<run_id>/`，用 Parquet 保存，可以直接讀出來看。

## 手動執行

```powershell
.\run-pipeline.cmd            # 正常執行（不動 bronze 的放大資料）
.\run-pipeline.cmd 1          # 連 generate 一起重跑（約 5 分鐘）
```

## 排程

`scheduler` 容器以固定間隔執行（預設 86400 秒 = 每天一次）：

```powershell
docker compose up -d scheduler
docker compose logs -f scheduler
```

想快速驗證排程行為，把間隔改小：

```powershell
$env:RUN_EVERY_SECONDS = "300"; docker compose up -d scheduler
```

## DQ gate 驗收（故意弄壞資料）

```powershell
# 1. 在 bronze 塞 20 筆孤兒事件（wafer_id = GHOST-*）
docker compose exec -T -e ORPHAN_ROWS=20 spark-client bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-client /opt/jobs/11_generate_scale.py"

# 2. 跑管線，DQ 應該在 gold 之前就擋下來
.\run-pipeline.cmd

# 3. 看到什麼
#    - 結束碼 2、狀態 FAILED_DQ
#    - data/lake/quarantine/R1_orphan__<run_id>/ 有 20 筆
#    - gold 完全沒有被更新（時間戳沒變）
```

修復方式：把 `11_generate_scale.py` 不加 `ORPHAN_ROWS` 重跑一次，再跑管線就會回到 PASS。

## 冪等性（重跑安全）

```powershell
.\run-pipeline.cmd
docker compose exec -T spark-client bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-client --conf spark.driver.bindAddress=0.0.0.0 /opt/jobs/41_fingerprint.py"
.\run-pipeline.cmd
docker compose exec -T spark-client bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-client --conf spark.driver.bindAddress=0.0.0.0 /opt/jobs/41_fingerprint.py"
```

兩次的 fingerprint 必須**完全相同**（`rows`、`sum_*` 全部一致）。這就是「可以安全重試」的定義：
不是「重跑不會報錯」，而是**重跑後每一張表的內容都一樣**。

`data/` 是 `.gitignore` 的一部分，所以執行歷史與隔離區不會進版控。
