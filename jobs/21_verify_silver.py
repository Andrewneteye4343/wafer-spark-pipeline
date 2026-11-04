"""M3 step 2: verify silver.

Two kinds of checks:

A. internal consistency of the generated data
   - row count did not change through the joins (no fanout)
   - (wafer_id, step_no) is unique
   - every non-null duration is exactly 8,100 s (2h15m, as generated)

B. parity with the SQL project
   the same transform is applied to the 132-row baseline that came out of
   Oracle, and the numbers are compared against what oracle-sql-lab produced
   with plain SQL. If these match, the Spark implementation is faithful.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "/data/lake/bronze"
SILVER = "/data/lake/silver"

SQL_EVENTS_PER_LOT = {"L-2601": 9, "L-2602": 6, "L-2603": 9, "L-2604": 6, "L-2605": 6}
SQL_EVENTS_PER_STEP = {"LITHO": 12, "ETCH": 12, "CVD": 12}
SQL_STILL_IN_PROCESS = {"L-2605": 2}
SQL_DUR_SEC = {"non_null": 34, "value": 8100}


def lower_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def silver_from(events, wafer, lot) -> DataFrame:
    return (
        events.join(F.broadcast(wafer), "wafer_id", "inner")
        .join(F.broadcast(lot), "lot_id", "inner")
        .withColumn(
            "dur_sec",
            F.when(F.col("out_ts").isNull(), F.lit(None).cast("long"))
            .otherwise(F.unix_timestamp("out_ts") - F.unix_timestamp("in_ts")),
        )
    )


def main() -> None:
    spark = SparkSession.builder.appName("21_verify_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ok = True

    print("== A. generated silver ==")
    wide = spark.read.parquet(f"{SILVER}/event_wide")
    rows = wide.count()
    events = lower_columns(spark.read.parquet(f"{BRONZE}/events")).count()
    print(f"  rows           : {rows:,} (bronze events {events:,})")
    ok &= rows == events

    uniq = wide.select("wafer_id", "step_no").distinct().count()
    print(f"  unique keys    : {uniq:,} ({'OK' if uniq == rows else 'BAD: duplicates'})")
    ok &= uniq == rows

    durs = wide.filter(F.col("dur_sec").isNotNull()).select("dur_sec").distinct().collect()
    values = sorted(r["dur_sec"] for r in durs)
    print(f"  distinct dur_sec: {values[:5]} (expect [8100])")
    ok &= values == [8100]

    null_dur = wide.filter(F.col("dur_sec").isNull()).count()
    print(f"  null dur_sec   : {null_dur:,} = {null_dur / rows:.4%} (expect ~0.5%)")

    print("== B. parity with oracle-sql-lab (same transform on the 132-row baseline) ==")
    se = lower_columns(spark.read.parquet(f"{BRONZE}/seed/process_event"))
    sw = lower_columns(spark.read.parquet(f"{BRONZE}/seed/wafer"))
    sl = lower_columns(spark.read.parquet(f"{BRONZE}/seed/lot"))
    seed_wide = silver_from(se, sw, sl)

    per_lot = {
        r["lot_id"]: r["cnt"]
        for r in seed_wide.groupBy("lot_id").agg(F.count("*").alias("cnt")).collect()
    }
    print(f"  events per lot : {dict(sorted(per_lot.items()))}")
    print(f"  SQL expected   : {SQL_EVENTS_PER_LOT}")
    ok &= per_lot == SQL_EVENTS_PER_LOT

    per_step = {
        r["step_name"]: r["cnt"]
        for r in seed_wide.groupBy("step_name").agg(F.count("*").alias("cnt")).collect()
    }
    print(f"  events per step: {dict(sorted(per_step.items()))} (SQL: {SQL_EVENTS_PER_STEP})")
    ok &= per_step == SQL_EVENTS_PER_STEP

    still = {
        r["lot_id"]: r["n"]
        for r in seed_wide.groupBy("lot_id")
        .agg((F.count("*") - F.count("out_ts")).alias("n")).collect()
        if r["n"] > 0
    }
    print(f"  still_in_process: {still} (SQL: {SQL_STILL_IN_PROCESS})")
    ok &= still == SQL_STILL_IN_PROCESS

    nn = seed_wide.filter(F.col("dur_sec").isNotNull())
    nn_count = nn.count()
    nn_vals = sorted({r["dur_sec"] for r in nn.select("dur_sec").distinct().collect()})
    print(f"  dur_sec        : {nn_count} non-null rows, distinct values {nn_vals} (SQL: 34 rows @ 8100)")
    ok &= nn_count == SQL_DUR_SEC["non_null"] and nn_vals == [SQL_DUR_SEC["value"]]

    print("RESULT:", "PASS" if ok else "FAIL")
    if not ok:
        raise SystemExit(1)
    spark.stop()


if __name__ == "__main__":
    main()
