# 資料契約（bronze 層）

## 1. 真實基線（從 Oracle JDBC 讀入 → `bronze/seed/`）

| 表 | 列數 | 備註 |
|---|---:|---|
| product | 3 | |
| tool | 6 | |
| chamber | 8 | |
| lot | 5 | |
| wafer | 12 | |
| process_event | 36 | 其中 2 筆 `out_ts` 為 NULL |
| metrology | 36 | 其中 1 筆超規 |
| wafer_bin | 26 | |

**驗收**：這 8 個數字必須與 `oracle-sql-lab` 的 `verify_m1.sql` 完全一致。

## 2. 放大資料（由 `11_generate_scale.py` 生成 → `bronze/`）

| 資料集 | 列數 | 生成規則 |
|---|---:|---|
| `lot` | 6,000 | `lot_id = L-9xxxxx`、`qty_wafers = id % 25 + 1`、priority NORMAL 90% / HOT 7% / SUPER 3% |
| `wafer` | **78,000** | ＝ Σ`qty_wafers`（6,000 × 平均 13，精確值） |
| `events` | **2,028,000** | 78,000 × 26 站；`step_no` = 10,20,…,260；LITHO／ETCH／CVD 循環；按 `dt` 分區 |
| `metrology` | **1,014,000** | 78,000 × 13 筆量測；三種 metric 循環；按 `dt` 分區 |
| `bins` | **468,000** | 78,000 × 6 個 bin；`pass_fail` P 95% / F 5% |

時間範圍：`2026-01-01` 起 **60 天**（各 lot 的投片日以 `id % 60` 天分散）。

## 3. 刻意植入的瑕疵（供 DQ gate 使用）

| 瑕疵 | 比例 | 預期筆數 |
|---|---|---|
| `out_ts IS NULL`（還在機台上） | 0.5% | ≈ 10,140 |
| 量測超出規格 | 0.3% | ≈ 3,042 |
| 孤兒外鍵（`wafer_id` 不存在於 wafer） | 預設 0，設定 `ORPHAN_ROWS=20` 時注入 | 20 |

## 4. 可重現性

- 所有隨機值使用固定 seed（`rand(11)`、`rand(21)`、`rand(31)`…），**同日重跑結果相同**
- 時間基準固定為 `2026-01-01T00:00:00Z`（epoch 1767225600），不依賴執行當下時間
- `event_id` / `meas_id` 用 `xxhash64(鍵值)`，跨次執行穩定
