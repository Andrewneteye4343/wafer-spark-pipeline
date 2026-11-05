"""M7 / experiment 8: the number that decided it.

EXPLAIN COST (experiment 7) showed the optimiser does NOT use the byte figures
ANALYZE reported:

    ANALYZE (on disk)        -> exp_wafer 375,918 bytes
    EXPLAIN COST (optimiser) -> exp_wafer Project sizeInBytes = 2.1 MiB

The optimiser estimates size as rowCount x estimated row width (default 20 bytes
per string, no compression), so for these narrow text columns it sees roughly
10x the compressed file size. The ShuffledHashJoin test is

    build.sizeInBytes < autoBroadcastJoinThreshold * numShufflePartitions

At threshold 100 KB that ceiling is 0.78 MiB, and the wafer side is 2.1 MiB, so
the test fails and Catalyst falls through to SortMergeJoin. The lot side is only
~0.34 MiB, which is why the lot join DID become ShuffledHashJoin.

This job sweeps the threshold and prints, for each one, the ceiling, the wafer
size the optimiser uses, and the join operator it actually chose - so the
mechanism is shown, not argued.

Predictions (written before running):
  threshold  100 KB  ceiling 0.78 MiB  -> wafer 2.1 MiB fails  -> SortMergeJoin  + lot broadcast?
  threshold  512 KB  ceiling 4.00 MiB  -> wafer 2.1 MiB ok     -> ShuffledHashJoin + lot broadcast
  threshold    1 MB  ceiling 8.00 MiB  -> wafer 2.1 MiB <= 1 MB -> both broadcast
"""
import os as _os

from pyspark.sql import SparkSession

SILVER = "/data/lake/silver"
BRONZE = "/data/lake/bronze"
OUT = "/data/ops/plans"
PARTITIONS = 8
THRESHOLDS = [(100 * 1024, "100 KB"), (512 * 1024, "512 KB"), (1024 * 1024, "1 MB")]

SQL_JOIN = ("SELECT count(*) FROM exp_events e "
            "JOIN exp_wafer w ON e.wafer_id = w.wafer_id")


def main() -> None:
    spark = (
        SparkSession.builder.appName("67_exp_threshold_sweep")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(PARTITIONS))
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    _os.makedirs(OUT, exist_ok=True)

    print("[exp] rebuilding catalog tables + statistics")
    spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec").write.mode("overwrite").saveAsTable("exp_events")
    spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id").write.mode(
        "overwrite").saveAsTable("exp_wafer")
    spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority").write.mode(
        "overwrite").saveAsTable("exp_lot")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS")

    with open(f"{OUT}/threshold_sweep.txt", "w", encoding="utf-8") as fh:
        for thr, label in THRESHOLDS:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(thr))
            ceiling = thr * PARTITIONS

            cost = "\n".join(r[0] for r in spark.sql(f"EXPLAIN COST {SQL_JOIN}").collect())
            phys = "\n".join(r[0] for r in spark.sql(f"EXPLAIN FORMATTED {SQL_JOIN}").collect())
            fh.write(f"\n===== threshold {label} =====\n{cost}\n{phys}\n")

            wafer_line = next((l.strip() for l in cost.splitlines()
                               if "exp_wafer" in l), "?")
            joins = sorted({w for w in ("BroadcastHashJoin", "ShuffledHashJoin",
                                        "SortMergeJoin", "BroadcastNestedLoopJoin")
                            if w in phys})

            print(f"\n[exp] threshold={label:7s} SHJ ceiling={ceiling / 1024 / 1024:5.2f} MiB")
            print(f"[exp]   optimiser sees wafer as: {wafer_line[:110]}")
            print(f"[exp]   chosen: {'+'.join(joins) if joins else 'none'}")

    print(f"\n[exp] full plans in {OUT}/threshold_sweep.txt")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.stop()


if __name__ == "__main__":
    main()
