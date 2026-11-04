"""M2 step 2: scale the tiny baseline up to a realistic volume.

Design rules
------------
* deterministic: every random value comes from a fixed seed, so two runs
  produce the same numbers (needed for idempotency tests in M5)
* 6,000 lots * qty_wafers(id % 25 + 1) = exactly 78,000 wafers
* 78,000 wafers * 26 steps       = 2,028,000 events
* 78,000 wafers * 13 measurements = 1,014,000 metrology rows
* 78,000 wafers * 6 bins          =   468,000 bin rows
* three deliberate defects, same spirit as the SQL project:
    - 0.5% of events have out_ts IS NULL   (wafer still in the tool)
    - 0.3% of measurements are out of spec
    - optional orphan rows (ORPHAN_ROWS env) for the data quality gate in M5

Everything is written as Parquet; events and metrology are partitioned by dt.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SEED = 42
N_LOTS = 6_000
N_STEPS = 26
N_METRIC_ROWS = 13
N_BINS = 6
DAYS = 60
EPOCH_2026_01_01 = 1_767_225_600  # 2026-01-01T00:00:00Z

ORPHANS = int(os.environ.get("ORPHAN_ROWS", "0"))
BRONZE = "/data/lake/bronze"


def build_lots(spark: SparkSession) -> DataFrame:
    return (
        spark.range(N_LOTS)
        .select(
            F.concat(
                F.lit("L-"),
                F.lpad((F.col("id") + 900001).cast("string"), 6, "0"),
            ).alias("lot_id"),
            F.when(F.col("id") % 3 == 0, "P-ALPHA")
            .when(F.col("id") % 3 == 1, "P-BRAVO")
            .otherwise("P-CHARLIE")
            .alias("product_id"),
            F.when(F.rand(11) < 0.07, "HOT")
            .when(F.rand(12) < 0.03, "SUPER")
            .otherwise("NORMAL")
            .alias("priority"),
            (F.col("id") % 25 + 1).cast("int").alias("qty_wafers"),
            F.to_timestamp(
                F.from_unixtime(
                    F.lit(EPOCH_2026_01_01) + F.col("id") % F.lit(DAYS * 86400)
                )
            ).alias("wafer_start_ts"),
        )
    )


def build_wafers(lots: DataFrame) -> DataFrame:
    return (
        lots.select("lot_id", "qty_wafers", "wafer_start_ts")
        # explode() is a generator: it must be a top-level expression, so the
        # cast has to happen in a separate withColumn.
        .withColumn("slot_no", F.explode(F.sequence(F.lit(1), F.col("qty_wafers"))))
        .withColumn("slot_no", F.col("slot_no").cast("int"))
        .withColumn(
            "wafer_id",
            F.concat(
                F.col("lot_id"), F.lit("-"),
                F.lpad(F.col("slot_no").cast("string"), 2, "0"),
            ),
        )
        .select("wafer_id", "lot_id", "slot_no", "wafer_start_ts")
    )


def build_steps(spark: SparkSession) -> DataFrame:
    names = F.array(F.lit("LITHO"), F.lit("ETCH"), F.lit("CVD"))
    tools = F.array(F.lit("T-LITHO-01"), F.lit("T-ETCH-01"), F.lit("T-CVD-01"))
    chambers = F.array(F.lit(1), F.lit(4), F.lit(8))
    idx = (F.col("id") % 3 + 1).cast("int")
    return spark.range(N_STEPS).select(
        ((F.col("id") + 1) * 10).cast("int").alias("step_no"),
        F.element_at(names, idx).alias("step_name"),
        F.element_at(tools, idx).alias("tool_id"),
        F.element_at(chambers, idx).alias("chamber_id"),
        F.concat(F.lit("RCP-"), F.element_at(names, idx), F.lit("-1")).alias("recipe_id"),
        F.lit(1).cast("int").alias("recipe_ver"),
    )


def build_events(wafers: DataFrame, steps: DataFrame) -> DataFrame:
    ev = wafers.crossJoin(steps)  # 78,000 * 26 = 2,028,000 rows
    in_ts = F.to_timestamp(
        F.unix_timestamp(F.col("wafer_start_ts")) + F.col("step_no") * 3600
    )
    out_ts = F.to_timestamp(
        F.unix_timestamp(F.col("wafer_start_ts")) + F.col("step_no") * 3600 + 8100
    )
    return (
        ev.withColumn("in_ts", in_ts)
        .withColumn(
            "out_ts",
            F.when(F.rand(21) < 0.005, F.lit(None).cast("timestamp")).otherwise(out_ts),
        )
        .withColumn("event_id", F.xxhash64("wafer_id", "step_no"))
        .withColumn("dt", F.date_format(F.col("in_ts"), "yyyy-MM-dd"))
        .select(
            "event_id", "wafer_id", "step_no", "step_name", "tool_id",
            "chamber_id", "recipe_id", "recipe_ver", "in_ts", "out_ts", "dt",
        )
    )


def build_metrology(wafers: DataFrame) -> DataFrame:
    names = F.array(F.lit("THICKNESS_A"), F.lit("CD_NM"), F.lit("OVERLAY_NM"))
    idx = (F.col("id") % 3 + 1).cast("int")
    mid = F.element_at(F.array(F.lit(102.5), F.lit(12.0), F.lit(2.15)), idx)
    span = F.element_at(F.array(F.lit(15.0), F.lit(2.0), F.lit(1.3)), idx)
    lower = F.element_at(F.array(F.lit(95.0), F.lit(11.0), F.lit(1.5)), idx)
    upper = F.element_at(F.array(F.lit(110.0), F.lit(13.0), F.lit(2.8)), idx)

    dim = spark_range_metrics(wafers)
    dim = dim.select(
        ((F.col("id") + 1) * 10).cast("int").alias("step_no"),
        F.element_at(names, idx).alias("metric_name"),
        mid.alias("mid"),
        span.alias("span"),
        lower.alias("spec_lower"),
        upper.alias("spec_upper"),
    )
    mo = wafers.crossJoin(dim)  # 78,000 * 13 = 1,014,000 rows
    value = F.col("mid") + (F.rand(31) - 0.5) * F.col("span") * 0.9
    out_of_spec = F.col("mid") + F.col("span") * 0.75
    return (
        mo.withColumn("metric_value", F.when(F.rand(32) < 0.003, out_of_spec).otherwise(value))
        .withColumn("meas_ts", F.col("wafer_start_ts") + F.expr("INTERVAL 1 DAY"))
        .withColumn("meas_id", F.xxhash64("wafer_id", "step_no", "metric_name"))
        .withColumn("dt", F.date_format(F.col("meas_ts"), "yyyy-MM-dd"))
        .select(
            "meas_id", "wafer_id", "step_no", "metric_name", "metric_value",
            "spec_lower", "spec_upper", "meas_ts", "dt",
        )
    )


def spark_range_metrics(wafers: DataFrame) -> DataFrame:
    """13-row dimension so each wafer gets 13 measurements."""
    spark = wafers.sparkSession
    return spark.range(N_METRIC_ROWS)


def build_bins(wafers: DataFrame) -> DataFrame:
    spark = wafers.sparkSession
    dim = spark.range(N_BINS).select(
        F.concat(F.lit("BIN"), F.lpad((F.col("id") + 1).cast("string"), 3, "0")).alias("bin_code")
    )
    wb = wafers.crossJoin(dim)  # 78,000 * 6 = 468,000 rows
    return wb.select(
        F.col("wafer_id"),
        F.col("bin_code"),
        ((F.rand(41) * 80) + 180).cast("int").alias("die_count"),
        F.when(F.rand(42) < 0.05, "F").otherwise("P").alias("pass_fail"),
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("11_generate_scale")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    lots = build_lots(spark)
    wafers = build_wafers(lots)
    steps = build_steps(spark)
    events = build_events(wafers, steps)
    metrology = build_metrology(wafers)
    bins = build_bins(wafers)

    if ORPHANS > 0:
        orphans = (
            spark.range(ORPHANS)
            .withColumn("event_id", F.lit(-1) - F.col("id"))
            .withColumn("wafer_id", F.concat(F.lit("GHOST-"), F.col("id").cast("string")))
            .withColumn("step_no", F.lit(999).cast("int"))
            .withColumn("step_name", F.lit("ORPHAN"))
            .withColumn("tool_id", F.lit("T-LITHO-01"))
            .withColumn("chamber_id", F.lit(1).cast("int"))
            .withColumn("recipe_id", F.lit("RCP-ORPHAN"))
            .withColumn("recipe_ver", F.lit(1).cast("int"))
            .withColumn("in_ts", F.current_timestamp())
            .withColumn("out_ts", F.current_timestamp())
            .withColumn("dt", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
            .select(events.columns)
        )
        events = events.unionByName(orphans)
        print(f"[gen] injected {ORPHANS} orphan events (for the DQ gate)")

    lots.write.mode("overwrite").parquet(f"{BRONZE}/lot")
    wafers.select("wafer_id", "lot_id", "slot_no").write.mode("overwrite").parquet(f"{BRONZE}/wafer")
    events.write.mode("overwrite").partitionBy("dt").parquet(f"{BRONZE}/events")
    metrology.write.mode("overwrite").partitionBy("dt").parquet(f"{BRONZE}/metrology")
    bins.write.mode("overwrite").parquet(f"{BRONZE}/bins")

    rows = {
        "lot": lots.count(),
        "wafer": wafers.count(),
        "events": events.count(),
        "metrology": metrology.count(),
        "bins": bins.count(),
    }
    for k, v in rows.items():
        print(f"[gen] {k:10s} rows={v:,}")
    spark.stop()


if __name__ == "__main__":
    main()
