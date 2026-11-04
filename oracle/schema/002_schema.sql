-- ============================================================
-- Oracle SQL Lab | 半導體製造資料模型（M1 基線）
-- 執行身分：APP_USER（schema = 你的使用者），資料庫：FREEPDB1
-- 全部約束都具名，方便後面的資料字典章節查詢
-- ============================================================
SET DEFINE OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO ON

CREATE TABLE product (
  product_id      VARCHAR2(20)  CONSTRAINT pk_product PRIMARY KEY,
  product_name    VARCHAR2(80)  CONSTRAINT nn_product_name NOT NULL,
  technology_node VARCHAR2(10),
  product_family  VARCHAR2(20)
);

CREATE TABLE tool (
  tool_id      VARCHAR2(12) CONSTRAINT pk_tool PRIMARY KEY,
  tool_type    VARCHAR2(12) CONSTRAINT nn_tool_type NOT NULL,
  fab_area     VARCHAR2(20),
  vendor_model VARCHAR2(40),
  installed_on DATE
);

CREATE TABLE chamber (
  chamber_id   NUMBER(6)     CONSTRAINT pk_chamber PRIMARY KEY,
  tool_id      VARCHAR2(12)  CONSTRAINT nn_chamber_tool NOT NULL,
  chamber_name VARCHAR2(10)  CONSTRAINT nn_chamber_name NOT NULL,
  CONSTRAINT fk_chamber_tool FOREIGN KEY (tool_id) REFERENCES tool (tool_id),
  CONSTRAINT uq_chamber_name UNIQUE (tool_id, chamber_name)
);

CREATE TABLE lot (
  lot_id         VARCHAR2(12) CONSTRAINT pk_lot PRIMARY KEY,
  product_id     VARCHAR2(20) CONSTRAINT nn_lot_product NOT NULL,
  wafer_start_ts TIMESTAMP    CONSTRAINT nn_lot_start NOT NULL,
  priority       VARCHAR2(8)  DEFAULT 'NORMAL' CONSTRAINT nn_lot_priority NOT NULL,
  qty_wafers     NUMBER(3)    CONSTRAINT nn_lot_qty NOT NULL,
  CONSTRAINT fk_lot_product  FOREIGN KEY (product_id) REFERENCES product (product_id),
  CONSTRAINT ck_lot_priority CHECK (priority IN ('NORMAL','HOT','SUPER')),
  CONSTRAINT ck_lot_qty      CHECK (qty_wafers BETWEEN 1 AND 25)
);

CREATE TABLE wafer (
  wafer_id VARCHAR2(16) CONSTRAINT pk_wafer PRIMARY KEY,
  lot_id   VARCHAR2(12) CONSTRAINT nn_wafer_lot NOT NULL,
  slot_no  NUMBER(3)    CONSTRAINT nn_wafer_slot NOT NULL,
  CONSTRAINT fk_wafer_lot  FOREIGN KEY (lot_id) REFERENCES lot (lot_id),
  CONSTRAINT uq_wafer_slot UNIQUE (lot_id, slot_no)
);

CREATE TABLE process_event (
  event_id   NUMBER(10)    CONSTRAINT pk_process_event PRIMARY KEY,
  wafer_id   VARCHAR2(16)  CONSTRAINT nn_pe_wafer NOT NULL,
  step_no    NUMBER(4)     CONSTRAINT nn_pe_step NOT NULL,
  step_name  VARCHAR2(40)  CONSTRAINT nn_pe_step_name NOT NULL,
  tool_id    VARCHAR2(12)  CONSTRAINT nn_pe_tool NOT NULL,
  chamber_id NUMBER(6)     CONSTRAINT nn_pe_chamber NOT NULL,
  recipe_id  VARCHAR2(20)  CONSTRAINT nn_pe_recipe NOT NULL,
  recipe_ver NUMBER(3)     CONSTRAINT nn_pe_recipe_ver NOT NULL,
  in_ts      TIMESTAMP     CONSTRAINT nn_pe_in_ts NOT NULL,
  out_ts     TIMESTAMP,
  CONSTRAINT fk_pe_wafer   FOREIGN KEY (wafer_id)   REFERENCES wafer (wafer_id),
  CONSTRAINT fk_pe_tool    FOREIGN KEY (tool_id)    REFERENCES tool (tool_id),
  CONSTRAINT fk_pe_chamber FOREIGN KEY (chamber_id) REFERENCES chamber (chamber_id),
  CONSTRAINT ck_pe_period  CHECK (out_ts IS NULL OR out_ts > in_ts)
);

CREATE TABLE metrology (
  meas_id      NUMBER(10)     CONSTRAINT pk_metrology PRIMARY KEY,
  wafer_id     VARCHAR2(16)   CONSTRAINT nn_meas_wafer NOT NULL,
  step_no      NUMBER(4)      CONSTRAINT nn_meas_step NOT NULL,
  metric_name  VARCHAR2(30)   CONSTRAINT nn_meas_metric NOT NULL,
  metric_value NUMBER(12,4)   CONSTRAINT nn_meas_value NOT NULL,
  spec_lower   NUMBER(12,4),
  spec_upper   NUMBER(12,4),
  meas_ts      TIMESTAMP      CONSTRAINT nn_meas_ts NOT NULL,
  CONSTRAINT fk_meas_wafer FOREIGN KEY (wafer_id) REFERENCES wafer (wafer_id)
);

CREATE TABLE wafer_bin (
  wafer_id  VARCHAR2(16) CONSTRAINT nn_bin_wafer NOT NULL,
  bin_code  VARCHAR2(8)  CONSTRAINT nn_bin_code NOT NULL,
  die_count NUMBER(7)    CONSTRAINT nn_bin_die NOT NULL,
  pass_fail CHAR(1)      CONSTRAINT nn_bin_pf NOT NULL,
  CONSTRAINT pk_wafer_bin PRIMARY KEY (wafer_id, bin_code),
  CONSTRAINT fk_bin_wafer FOREIGN KEY (wafer_id) REFERENCES wafer (wafer_id),
  CONSTRAINT ck_bin_pf    CHECK (pass_fail IN ('P','F')),
  CONSTRAINT ck_bin_die   CHECK (die_count >= 0)
);

COMMENT ON TABLE  process_event IS '晶圓履歷：每一次上機（wafer × step × tool/chamber/recipe）';
COMMENT ON TABLE  lot           IS '批次：半導體廠的基本生產單位，一箱最多 25 片';
COMMENT ON COLUMN lot.priority  IS 'NORMAL / HOT（急件）/ SUPER（超級急件）';
COMMENT ON COLUMN process_event.out_ts IS 'NULL 代表尚未出站（WIP 仍在機台上）';

-- sqlplus 明確離開（避免讀完檔案還停在 stdin 等輸入）
EXIT
