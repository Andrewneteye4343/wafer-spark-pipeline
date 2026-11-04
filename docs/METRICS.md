# 指標定義（gold 層）

每一個指標都要有：**grain（一列代表什麼）、分子、分母、口徑、SQL 對照**。
沒有這份文件的報表，三個月後沒有人敢用。

## 1. `gold/wafer_yield` — 一列一片晶圓

| 欄位 | 定義 |
|---|---|
| `wafer_id` / `lot_id` | 晶圓與批次 |
| `pass_die` | `SUM(die_count)` where `pass_fail = 'P'` |
| `fail_die` | `SUM(die_count)` where `pass_fail = 'F'` |
| `total_die` | `SUM(die_count)`（全部 bin） |
| `yield_pct` | `ROUND(pass_die * 100 / total_die, 2)` |

**口徑要點**：先加總 die 數再相除，**不可**對每片晶圓的平均 bin 良率再取平均。
一個大 bin 與六個小 bin 在「平均比例」下權重相同，會算錯。

**SQL 對照**：`v_wafer_yield`（oracle-sql-lab M10）

## 2. `gold/lot_event_summary` — 一列一個批次

| 欄位 | 定義 |
|---|---|
| `event_cnt` | 事件筆數 |
| `wafer_cnt` | `COUNT(DISTINCT wafer_id)`，該批次實際有幾片晶圓在跑 |
| `first_in` / `last_out` | `MIN(in_ts)` / `MAX(out_ts)` |
| `still_in_process` | `COUNT(*) - COUNT(out_ts)`：**還在機台上的片數** |
| `avg_dur_sec` | 平均站內停留秒數（排除未出站） |

**口徑要點**：`MAX(out_ts)` 會忽略 NULL，所以單獨看它會誤判「全部已出站」。
必須同時輸出 `still_in_process`。

**SQL 對照**：`v_lot_event_summary`（M10）

## 3. `gold/tool_load_hourly` — 一列一個（機台, 小時）

| 欄位 | 定義 |
|---|---|
| `tool_id` / `step_name` | 機台與站別 |
| `hour_ts` | `in_ts` 截到小時（`yyyy-MM-dd HH:00`） |
| `event_cnt` | 該機台該小時的進站次數 |
| `wafer_cnt` | 該機台該小時處理的不同晶圓片數 |

**口徑要點**：這是「負載曲線」的分子。要做利用率要再除以機台可用時間（本專案資料沒有停機紀錄，故不計算）。

## 一致性保證

`31_verify_gold.py` 會用**完全相同的 transform** 跑一次 132 筆基線，並與 SQL 專案的答案逐項比對：

- 12 片晶圓良率：`100.00 / 98.03 / 96.01 / 90.67 / 99.81 / 97.55 / 96.05 / 94.09 / 96.05 / 97.51 / 95.33 / 93.09`
- 每批事件數：`9 / 6 / 9 / 6 / 6`，`still_in_process` 只有 L-2605 = 2
- (小時, 站別)：`06 LITHO = 12`、`12 ETCH = 12`、`18 CVD = 12`
