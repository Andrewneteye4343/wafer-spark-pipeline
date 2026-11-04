-- ============================================================
-- Oracle SQL Lab | M1 基線資料
-- 刻意包含：out_ts IS NULL（還在機台上）、1 筆超出規格的量測值、
--           沒有生產紀錄的機台與腔體（後面 outer join / anti-join 練習用）
-- 語法刻意用「一列一個 INSERT」，符合 1Z0-071（19c 以前）的 DML 寫法
-- ============================================================
SET DEFINE OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO ON

-- ---------- product（3 列） ----------
INSERT INTO product (product_id, product_name, technology_node, product_family) VALUES ('P-ALPHA', 'Alpha SoC', 'N5', 'MOBILE');
INSERT INTO product (product_id, product_name, technology_node, product_family) VALUES ('P-BRAVO', 'Bravo GPU', 'N3', 'HPC');
INSERT INTO product (product_id, product_name, technology_node, product_family) VALUES ('P-CHARLIE', 'Charlie MCU', '40nm', 'EMBEDDED');

-- ---------- tool（6 列） ----------
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-LITHO-01', 'LITHO', '黃光', 'ASML-NXE3600', DATE '2023-03-01');
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-LITHO-02', 'LITHO', '黃光', 'ASML-NXE3600', DATE '2024-06-15');
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-ETCH-01', 'ETCH', '蝕刻', 'LAM-Kiyo45', DATE '2022-11-20');
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-ETCH-02', 'ETCH', '蝕刻', 'TEL-Tactras', DATE '2025-01-08');
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-CVD-01', 'CVD', '薄膜', 'AMAT-Producer', DATE '2023-09-12');
INSERT INTO tool (tool_id, tool_type, fab_area, vendor_model, installed_on) VALUES ('T-CMP-01', 'CMP', 'CMP', 'EBARA-F300', DATE '2021-07-30');

-- ---------- chamber（8 列） ----------
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (1, 'T-LITHO-01', 'A');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (2, 'T-LITHO-01', 'B');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (3, 'T-LITHO-02', 'A');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (4, 'T-ETCH-01', 'A');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (5, 'T-ETCH-01', 'B');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (6, 'T-ETCH-01', 'C');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (7, 'T-ETCH-02', 'A');
INSERT INTO chamber (chamber_id, tool_id, chamber_name) VALUES (8, 'T-CVD-01', 'A');

-- ---------- lot（5 列） ----------
INSERT INTO lot (lot_id, product_id, wafer_start_ts, priority, qty_wafers) VALUES ('L-2601', 'P-ALPHA', TIMESTAMP '2026-08-03 08:15:00', 'NORMAL', 3);
INSERT INTO lot (lot_id, product_id, wafer_start_ts, priority, qty_wafers) VALUES ('L-2602', 'P-ALPHA', TIMESTAMP '2026-08-05 21:40:00', 'HOT', 2);
INSERT INTO lot (lot_id, product_id, wafer_start_ts, priority, qty_wafers) VALUES ('L-2603', 'P-BRAVO', TIMESTAMP '2026-08-06 10:05:00', 'NORMAL', 3);
INSERT INTO lot (lot_id, product_id, wafer_start_ts, priority, qty_wafers) VALUES ('L-2604', 'P-BRAVO', TIMESTAMP '2026-08-11 07:25:00', 'SUPER', 2);
INSERT INTO lot (lot_id, product_id, wafer_start_ts, priority, qty_wafers) VALUES ('L-2605', 'P-CHARLIE', TIMESTAMP '2026-08-14 15:50:00', 'NORMAL', 2);

-- ---------- wafer（12 列） ----------
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2601-01', 'L-2601', 1);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2601-02', 'L-2601', 2);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2601-03', 'L-2601', 3);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2602-01', 'L-2602', 1);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2602-02', 'L-2602', 2);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2603-01', 'L-2603', 1);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2603-02', 'L-2603', 2);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2603-03', 'L-2603', 3);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2604-01', 'L-2604', 1);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2604-02', 'L-2604', 2);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2605-01', 'L-2605', 1);
INSERT INTO wafer (wafer_id, lot_id, slot_no) VALUES ('L-2605-02', 'L-2605', 2);

-- ---------- process_event（晶圓履歷）（36 列） ----------
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1001, 'L-2601-01', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-1', 1, TIMESTAMP '2026-08-20 06:00:00', TIMESTAMP '2026-08-20 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1002, 'L-2601-01', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-1', 1, TIMESTAMP '2026-08-20 12:00:00', TIMESTAMP '2026-08-20 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1003, 'L-2601-01', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-1', 1, TIMESTAMP '2026-08-20 18:00:00', TIMESTAMP '2026-08-20 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1004, 'L-2601-02', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-2', 1, TIMESTAMP '2026-08-21 06:00:00', TIMESTAMP '2026-08-21 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1005, 'L-2601-02', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-2', 1, TIMESTAMP '2026-08-21 12:00:00', TIMESTAMP '2026-08-21 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1006, 'L-2601-02', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-2', 1, TIMESTAMP '2026-08-21 18:00:00', TIMESTAMP '2026-08-21 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1007, 'L-2601-03', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-3', 1, TIMESTAMP '2026-08-22 06:00:00', TIMESTAMP '2026-08-22 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1008, 'L-2601-03', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-3', 1, TIMESTAMP '2026-08-22 12:00:00', TIMESTAMP '2026-08-22 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1009, 'L-2601-03', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-3', 1, TIMESTAMP '2026-08-22 18:00:00', TIMESTAMP '2026-08-22 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1010, 'L-2602-01', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-1', 1, TIMESTAMP '2026-08-23 06:00:00', TIMESTAMP '2026-08-23 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1011, 'L-2602-01', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-1', 1, TIMESTAMP '2026-08-23 12:00:00', TIMESTAMP '2026-08-23 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1012, 'L-2602-01', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-1', 1, TIMESTAMP '2026-08-23 18:00:00', TIMESTAMP '2026-08-23 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1013, 'L-2602-02', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-2', 1, TIMESTAMP '2026-08-24 06:00:00', TIMESTAMP '2026-08-24 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1014, 'L-2602-02', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-2', 1, TIMESTAMP '2026-08-24 12:00:00', TIMESTAMP '2026-08-24 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1015, 'L-2602-02', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-2', 1, TIMESTAMP '2026-08-24 18:00:00', TIMESTAMP '2026-08-24 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1016, 'L-2603-01', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-3', 1, TIMESTAMP '2026-08-25 06:00:00', TIMESTAMP '2026-08-25 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1017, 'L-2603-01', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-3', 1, TIMESTAMP '2026-08-25 12:00:00', TIMESTAMP '2026-08-25 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1018, 'L-2603-01', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-3', 1, TIMESTAMP '2026-08-25 18:00:00', TIMESTAMP '2026-08-25 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1019, 'L-2603-02', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-1', 2, TIMESTAMP '2026-08-26 06:00:00', TIMESTAMP '2026-08-26 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1020, 'L-2603-02', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-1', 2, TIMESTAMP '2026-08-26 12:00:00', TIMESTAMP '2026-08-26 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1021, 'L-2603-02', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-1', 2, TIMESTAMP '2026-08-26 18:00:00', TIMESTAMP '2026-08-26 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1022, 'L-2603-03', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-2', 2, TIMESTAMP '2026-08-27 06:00:00', TIMESTAMP '2026-08-27 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1023, 'L-2603-03', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-2', 2, TIMESTAMP '2026-08-27 12:00:00', TIMESTAMP '2026-08-27 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1024, 'L-2603-03', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-2', 2, TIMESTAMP '2026-08-27 18:00:00', TIMESTAMP '2026-08-27 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1025, 'L-2604-01', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-3', 2, TIMESTAMP '2026-08-28 06:00:00', TIMESTAMP '2026-08-28 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1026, 'L-2604-01', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-3', 2, TIMESTAMP '2026-08-28 12:00:00', TIMESTAMP '2026-08-28 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1027, 'L-2604-01', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-3', 2, TIMESTAMP '2026-08-28 18:00:00', TIMESTAMP '2026-08-28 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1028, 'L-2604-02', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-1', 2, TIMESTAMP '2026-08-29 06:00:00', TIMESTAMP '2026-08-29 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1029, 'L-2604-02', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-1', 2, TIMESTAMP '2026-08-29 12:00:00', TIMESTAMP '2026-08-29 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1030, 'L-2604-02', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-1', 2, TIMESTAMP '2026-08-29 18:00:00', TIMESTAMP '2026-08-29 20:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1031, 'L-2605-01', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-2', 2, TIMESTAMP '2026-08-30 06:00:00', TIMESTAMP '2026-08-30 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1032, 'L-2605-01', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-2', 2, TIMESTAMP '2026-08-30 12:00:00', TIMESTAMP '2026-08-30 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1033, 'L-2605-01', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-2', 2, TIMESTAMP '2026-08-30 18:00:00', NULL);
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1034, 'L-2605-02', 10, 'LITHO', 'T-LITHO-01', 1, 'RCP-LITHO-3', 2, TIMESTAMP '2026-08-31 06:00:00', TIMESTAMP '2026-08-31 08:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1035, 'L-2605-02', 20, 'ETCH', 'T-ETCH-01', 4, 'RCP-ETCH-3', 2, TIMESTAMP '2026-08-31 12:00:00', TIMESTAMP '2026-08-31 14:15:00');
INSERT INTO process_event (event_id, wafer_id, step_no, step_name, tool_id, chamber_id, recipe_id, recipe_ver, in_ts, out_ts) VALUES (1036, 'L-2605-02', 30, 'CVD', 'T-CVD-01', 8, 'RCP-CVD-3', 2, TIMESTAMP '2026-08-31 18:00:00', NULL);

-- ---------- metrology（36 列） ----------
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5001, 'L-2601-01', 10, 'THICKNESS_A', 105.0, 95.0, 110.0, TIMESTAMP '2026-08-20 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5002, 'L-2601-01', 20, 'CD_NM', 12.436, 11.0, 13.0, TIMESTAMP '2026-08-20 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5003, 'L-2601-01', 30, 'OVERLAY_NM', 1.95, 0.0, 3.0, TIMESTAMP '2026-08-20 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5004, 'L-2601-02', 10, 'THICKNESS_A', 105.45, 95.0, 110.0, TIMESTAMP '2026-08-21 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5005, 'L-2601-02', 20, 'CD_NM', 12.388, 11.0, 13.0, TIMESTAMP '2026-08-21 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5006, 'L-2601-02', 30, 'OVERLAY_NM', 2.4, 0.0, 3.0, TIMESTAMP '2026-08-21 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5007, 'L-2601-03', 10, 'THICKNESS_A', 104.85, 95.0, 110.0, TIMESTAMP '2026-08-22 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5008, 'L-2601-03', 20, 'CD_NM', 12.424, 11.0, 13.0, TIMESTAMP '2026-08-22 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5009, 'L-2601-03', 30, 'OVERLAY_NM', 1.8, 0.0, 3.0, TIMESTAMP '2026-08-22 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5010, 'L-2602-01', 10, 'THICKNESS_A', 105.3, 95.0, 110.0, TIMESTAMP '2026-08-23 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5011, 'L-2602-01', 20, 'CD_NM', 12.376, 11.0, 13.0, TIMESTAMP '2026-08-23 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5012, 'L-2602-01', 30, 'OVERLAY_NM', 2.25, 0.0, 3.0, TIMESTAMP '2026-08-23 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5013, 'L-2602-02', 10, 'THICKNESS_A', 104.7, 95.0, 110.0, TIMESTAMP '2026-08-24 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5014, 'L-2602-02', 20, 'CD_NM', 12.412, 11.0, 13.0, TIMESTAMP '2026-08-24 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5015, 'L-2602-02', 30, 'OVERLAY_NM', 1.65, 0.0, 3.0, TIMESTAMP '2026-08-24 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5016, 'L-2603-01', 10, 'THICKNESS_A', 105.15, 95.0, 110.0, TIMESTAMP '2026-08-25 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5017, 'L-2603-01', 20, 'CD_NM', 12.364, 11.0, 13.0, TIMESTAMP '2026-08-25 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5018, 'L-2603-01', 30, 'OVERLAY_NM', 2.1, 0.0, 3.0, TIMESTAMP '2026-08-25 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5019, 'L-2603-02', 10, 'THICKNESS_A', 104.55, 95.0, 110.0, TIMESTAMP '2026-08-26 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5020, 'L-2603-02', 20, 'CD_NM', 13.42, 11.0, 13.0, TIMESTAMP '2026-08-26 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5021, 'L-2603-02', 30, 'OVERLAY_NM', 2.55, 0.0, 3.0, TIMESTAMP '2026-08-26 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5022, 'L-2603-03', 10, 'THICKNESS_A', 105.0, 95.0, 110.0, TIMESTAMP '2026-08-27 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5023, 'L-2603-03', 20, 'CD_NM', 12.436, 11.0, 13.0, TIMESTAMP '2026-08-27 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5024, 'L-2603-03', 30, 'OVERLAY_NM', 1.95, 0.0, 3.0, TIMESTAMP '2026-08-27 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5025, 'L-2604-01', 10, 'THICKNESS_A', 105.45, 95.0, 110.0, TIMESTAMP '2026-08-28 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5026, 'L-2604-01', 20, 'CD_NM', 12.388, 11.0, 13.0, TIMESTAMP '2026-08-28 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5027, 'L-2604-01', 30, 'OVERLAY_NM', 2.4, 0.0, 3.0, TIMESTAMP '2026-08-28 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5028, 'L-2604-02', 10, 'THICKNESS_A', 104.85, 95.0, 110.0, TIMESTAMP '2026-08-29 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5029, 'L-2604-02', 20, 'CD_NM', 12.424, 11.0, 13.0, TIMESTAMP '2026-08-29 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5030, 'L-2604-02', 30, 'OVERLAY_NM', 1.8, 0.0, 3.0, TIMESTAMP '2026-08-29 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5031, 'L-2605-01', 10, 'THICKNESS_A', 105.3, 95.0, 110.0, TIMESTAMP '2026-08-30 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5032, 'L-2605-01', 20, 'CD_NM', 12.376, 11.0, 13.0, TIMESTAMP '2026-08-30 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5033, 'L-2605-01', 30, 'OVERLAY_NM', 2.25, 0.0, 3.0, TIMESTAMP '2026-08-30 09:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5034, 'L-2605-02', 10, 'THICKNESS_A', 104.7, 95.0, 110.0, TIMESTAMP '2026-08-31 07:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5035, 'L-2605-02', 20, 'CD_NM', 12.412, 11.0, 13.0, TIMESTAMP '2026-08-31 08:00:00');
INSERT INTO metrology (meas_id, wafer_id, step_no, metric_name, metric_value, spec_lower, spec_upper, meas_ts) VALUES (5036, 'L-2605-02', 30, 'OVERLAY_NM', 1.65, 0.0, 3.0, TIMESTAMP '2026-08-31 09:00:00');

-- ---------- wafer_bin（CP 結果）（26 列） ----------
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-01', 'BIN001', 1180, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-01', 'BIN007', 0, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-02', 'BIN001', 1143, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-02', 'BIN007', 23, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-03', 'BIN001', 1106, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2601-03', 'BIN007', 46, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2602-01', 'BIN001', 1069, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2602-01', 'BIN007', 69, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2602-01', 'BIN013', 41, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2602-02', 'BIN001', 1032, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2602-02', 'BIN007', 2, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-01', 'BIN001', 995, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-01', 'BIN007', 25, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-02', 'BIN001', 1168, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-02', 'BIN007', 48, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-03', 'BIN001', 1131, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2603-03', 'BIN007', 71, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2604-01', 'BIN001', 1094, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2604-01', 'BIN007', 4, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2604-01', 'BIN013', 41, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2604-02', 'BIN001', 1057, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2604-02', 'BIN007', 27, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2605-01', 'BIN001', 1020, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2605-01', 'BIN007', 50, 'F');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2605-02', 'BIN001', 983, 'P');
INSERT INTO wafer_bin (wafer_id, bin_code, die_count, pass_fail) VALUES ('L-2605-02', 'BIN007', 73, 'F');

COMMIT;

-- sqlplus 明確離開（避免讀完檔案還停在 stdin 等輸入）
EXIT
