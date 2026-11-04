"""M4 step 2: verify gold - and prove parity with the SQL project.

Part A checks the generated volume and the shape of the numbers.
Part B recomputes the same three reports on the 132-row baseline and compares
them against the numbers oracle-sql-lab produced with plain SQL. If these match,
the Spark pipeline and the SQL pipeline agree on every metric we publish.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "/data/lake/bronze"
SILVER = "/data/lake/silver"
GOLD = "/data/lake/gold"

# oracle-sql-lab results, verified by hand in the SQL project
SQL_YIELD = {
    "L-2601-01": 100.00, "L-2601-02": 98.03, "L-2601-03": 96.01,
    "L-2602-01": 90.67, "L-2602-02": 99.81, "L-2603-01": 97.55,
    "L-2603-02": 96.05, "L-2603-03": 94.09, "L-2604-01": 96.05,
    "L-2604-02": 97.51, "L-2605-01": 95.33, "L-2605-02": 93.09,
}
SQL_LOT_EVENTS = {"L-2601": 9, "L-2602": 6, "L-2603": 9, "L-2604": 6, "L-2605": 6}
SQL_STILL_IN = {"L-2605": 2}
SQL_HOUR_STEP = {("06", "LITHO"): 12, ("12", "ETCH"): 12, ("18", "CVD"): 12}
SQL_DUR_NONNULL = 34


def lower_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def wafer_yield(bins: DataFrame, wafer: DataFrame) -> DataFrame:
    agg = bins.groupBy("wafer_id").agg(
        F.sum(F.when(F.col("pass_fail") == "P", F.col("die_count")).otherwise(0)).alias("pass_die"),
        F.sum(F.when(F.col("pass_fail") == "F", F.col("die_count")).otherwise(0)).alias("fail_die"),
        F.sum("die_count").alias("total_die"),
    )
    return (
        agg.join(F.broadcast(wafer.select("wafer_id", "lot_id")), "wafer_id", "inner")
        .withColumn("yield_pct", F.round(F.col("pass_die") * 100.0 / F.col("total_die"), 2))
    )


def lot_event_summary(events: DataFrame) -> DataFrame:
    return events.groupBy("lot_id").agg(
        F.count("*").alias("event_cnt"),
        (F.count("*") - F.count("out_ts")).alias("still_in_process"),
    )


def main() -> None:
    spark = SparkSession.builder.appName("31_verify_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ok = True

    print("== A. generated gold ==")
    y = spark.read.parquet(f"{GOLD}/wafer_yield")
    s = spark.read.parquet(f"{GOLD}/lot_event_summary")
    t = spark.read.parquet(f"{GOLD}/tool_load_hourly")
    ny, ns, nt = y.count(), s.count(), t.count()
    print(f"  wafer_yield       rows={ny:,} (expect 78,000)")
    print(f"  lot_event_summary rows={ns:,} (expect 6,000)")
    print(f"  tool_load_hourly  rows={nt:,}")
    ok &= ny == 78_000 and ns == 6_000

    total_events = t.select(F.sum("event_cnt")).first()[0]
    print(f"  sum(tool_load.event_cnt) = {total_events:,} (expect 2,028,000)")
    ok &= total_events == 2_028_000

    stats = y.select(
        F.round(F.min("yield_pct"), 2).alias("min"),
        F.round(F.max("yield_pct"), 2).alias("max"),
        F.round(F.avg("yield_pct"), 2).alias("mean"),
        F.round(F.avg(F.col("total_die")), 1).alias("avg_die"),
    ).first()
    print(f"  yield min/mean/max = {stats['min']} / {stats['mean']} / {stats['max']}, avg die={stats['avg_die']}")
    ok &= 93.0 <= stats["mean"] <= 97.0

    still = s.select(F.sum("still_in_process")).first()[0]
    print(f"  sum(still_in_process) = {still:,} (expect 10,143)")
    ok &= still == 10_143

    print("== B. parity with oracle-sql-lab (132-row baseline) ==")
    se = lower_columns(spark.read.parquet(f"{BRONZE}/seed/process_event"))
    sw = lower_columns(spark.read.parquet(f"{BRONZE}/seed/wafer"))
    sb = lower_columns(spark.read.parquet(f"{BRONZE}/seed/wafer_bin"))

    wide = se.join(F.broadcast(sw.select("wafer_id", "lot_id")), "wafer_id", "inner")

    yields = {r["wafer_id"]: float(r["yield_pct"]) for r in wafer_yield(sb, sw).collect()}
    diff = {k: (yields.get(k), v) for k, v in SQL_YIELD.items() if yields.get(k) != v}
    print(f"  yield parity: {len(SQL_YIELD) - len(diff)}/{len(SQL_YIELD)} wafers match SQL")
    if diff:
        print(f"    MISMATCH: {diff}")
    ok &= not diff
    for k in ["L-2601-01", "L-2602-01", "L-2605-02"]:
        print(f"    {k}: spark={yields.get(k)} sql={SQL_YIELD[k]}")

    lots = {r["lot_id"]: r["event_cnt"] for r in lot_event_summary(wide).collect()}
    print(f"  lot event_cnt = {dict(sorted(lots.items()))} (SQL {SQL_LOT_EVENTS})")
    ok &= lots == SQL_LOT_EVENTS

    st = {
        r["lot_id"]: r["still_in_process"]
        for r in lot_event_summary(wide).collect()
        if r["still_in_process"] > 0
    }
    print(f"  still_in_process = {st} (SQL {SQL_STILL_IN})")
    ok &= st == SQL_STILL_IN

    tmp = wide.withColumn("hh", F.date_format("in_ts", "HH"))
    pairs = {
        (r["hh"], r["step_name"]): r["c"]
        for r in tmp.groupBy("hh", "step_name").agg(F.count("*").alias("c")).collect()
    }
    print(f"  hour x step = {dict(sorted(pairs.items()))} (SQL {dict(sorted(SQL_HOUR_STEP.items()))})")
    ok &= pairs == SQL_HOUR_STEP

    print("RESULT:", "PASS" if ok else "FAIL")
    if not ok:
        raise SystemExit(1)
    spark.stop()


if __name__ == "__main__":
    main()
