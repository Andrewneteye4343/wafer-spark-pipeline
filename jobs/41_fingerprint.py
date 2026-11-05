"""M5: fingerprint the gold layer so two runs can be compared without eyeballing.

Run  ->  pipeline  ->  fingerprint
then compare the two lines: identical fingerprint means the re-run produced
exactly the same tables (idempotent), which is what "safe to retry" means.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD = "/data/lake/gold"
TABLES = {
    "wafer_yield": ["wafer_id", "total_die", "pass_die", "yield_pct"],
    "lot_event_summary": ["lot_id", "event_cnt", "still_in_process"],
    "tool_load_hourly": ["tool_id", "event_cnt"],
}


def main() -> None:
    spark = SparkSession.builder.appName("41_fingerprint").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for table, cols in TABLES.items():
        df = spark.read.parquet(f"{GOLD}/{table}")
        rows = df.count()
        parts = [F.count("*").alias("rows")]
        for c in cols[1:]:
            parts.append(F.sum(F.col(c).cast("double")).alias(f"sum_{c}"))
        agg = df.agg(*parts).first()
        digest = "|".join(f"{k}={agg[k]}" for k in agg.asDict())
        print(f"[fp] {table:20s} {digest}")
    spark.stop()


if __name__ == "__main__":
    main()
