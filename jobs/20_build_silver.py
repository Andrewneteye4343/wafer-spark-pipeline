"""M3 step 1: build the silver layer (cleaned, joined, partitioned).

bronze keeps the source shape; silver is the first layer that knows about the
business: every event row carries its lot, product, priority and duration.

Row-count guard
---------------
A join must never create rows here. 2,028,000 events with exactly one matching
wafer and one matching lot per row has to stay 2,028,000. If it grows, the join
key is not unique -> stop instead of writing bad data.

Broadcast hint
--------------
`wafer` (78k rows) and `lot` (6k rows) are tiny next to 2M events, so we mark
them for broadcast: the executor keeps a copy in memory and the 2M-row side is
never shuffled. Same idea as the function-based index in the SQL project, but
the cost we save is network, not I/O.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "/data/lake/bronze"
SILVER = "/data/lake/silver"

COLUMNS = [
    "event_id", "wafer_id", "lot_id", "slot_no", "product_id", "priority",
    "step_no", "step_name", "tool_id", "chamber_id", "recipe_id", "recipe_ver",
    "in_ts", "out_ts", "dur_sec", "dt",
]


def lower_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def build_silver(events: DataFrame, wafer: DataFrame, lot: DataFrame) -> DataFrame:
    return (
        events.join(F.broadcast(wafer), "wafer_id", "inner")
        .join(F.broadcast(lot), "lot_id", "inner")
        .withColumn(
            "dur_sec",
            F.when(F.col("out_ts").isNull(), F.lit(None).cast("long"))
            .otherwise(F.unix_timestamp("out_ts") - F.unix_timestamp("in_ts")),
        )
        .select(*COLUMNS)
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("20_build_silver")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = lower_columns(spark.read.parquet(f"{BRONZE}/events"))
    wafer = lower_columns(spark.read.parquet(f"{BRONZE}/wafer"))
    lot = lower_columns(spark.read.parquet(f"{BRONZE}/lot"))

    before = events.count()
    deduped = events.dropDuplicates(["wafer_id", "step_no"])
    if deduped.count() != before:
        print(f"[silver] WARNING: duplicate (wafer_id, step_no) found, deduped")
        events = deduped

    wide = build_silver(events, wafer, lot)
    rows = wide.count()
    print(f"[silver] events in ={before:,} rows out={rows:,}")
    assert rows == before, f"join created rows: {rows} != {before} (fanout!)"

    wide.write.mode("overwrite").partitionBy("dt").parquet(f"{SILVER}/event_wide")
    print(f"[silver] wrote {SILVER}/event_wide rows={rows:,}")
    spark.stop()


if __name__ == "__main__":
    main()
