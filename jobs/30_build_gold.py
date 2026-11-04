"""M4 step 1: build the gold layer - the three tables a human would read.

Grain matters
-------------
Every gold table must answer "what does one row mean":
  wafer_yield        one row per wafer
  lot_event_summary  one row per lot
  tool_load_hourly   one row per (tool, hour)

Why sums before division
------------------------
Yield must be SUM(pass die) / SUM(all die), never AVG(pass ratio per bin).
Averaging ratios silently weights a wafer with one big bin the same as a wafer
with six small ones. The SQL project made the same decision.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "/data/lake/bronze"
SILVER = "/data/lake/silver"
GOLD = "/data/lake/gold"


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
        .select("wafer_id", "lot_id", "pass_die", "fail_die", "total_die", "yield_pct")
    )


def lot_event_summary(events: DataFrame) -> DataFrame:
    return events.groupBy("lot_id").agg(
        F.count("*").alias("event_cnt"),
        F.countDistinct("wafer_id").alias("wafer_cnt"),
        F.min("in_ts").alias("first_in"),
        F.max("out_ts").alias("last_out"),
        (F.count("*") - F.count("out_ts")).alias("still_in_process"),
        F.round(F.avg("dur_sec"), 2).alias("avg_dur_sec"),
    )


def tool_load_hourly(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("hour_ts", F.to_timestamp(F.date_format("in_ts", "yyyy-MM-dd HH:00")))
        .groupBy("tool_id", "step_name", "hour_ts")
        .agg(
            F.count("*").alias("event_cnt"),
            F.countDistinct("wafer_id").alias("wafer_cnt"),
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("30_build_gold")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(f"{SILVER}/event_wide")
    bins = lower_columns(spark.read.parquet(f"{BRONZE}/bins"))
    wafer = lower_columns(spark.read.parquet(f"{BRONZE}/wafer"))

    y = wafer_yield(bins, wafer)
    s = lot_event_summary(events)
    t = tool_load_hourly(events)

    y.write.mode("overwrite").parquet(f"{GOLD}/wafer_yield")
    s.write.mode("overwrite").parquet(f"{GOLD}/lot_event_summary")
    t.write.mode("overwrite").parquet(f"{GOLD}/tool_load_hourly")

    print(f"[gold] wafer_yield       rows={y.count():,}")
    print(f"[gold] lot_event_summary rows={s.count():,}")
    print(f"[gold] tool_load_hourly  rows={t.count():,}")
    y.orderBy("yield_pct").select("wafer_id", "lot_id", "yield_pct").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
