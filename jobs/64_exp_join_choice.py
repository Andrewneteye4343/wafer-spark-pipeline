"""M7 / experiment 5: why ShuffledHashJoin never showed up (and how to make it).

Experiment 4 asked for `preferSortMergeJoin=false` and still got SortMergeJoin.
Reading Spark's JoinSelection rule, shuffled hash join needs the build side to
be small enough to fit in memory, and the check reuses the broadcast threshold:

    canBuildLocalHashMap(build) = build.stats.sizeInBytes
                                  < autoBroadcastJoinThreshold * numShufflePartitions

So setting the threshold to -1 (the usual way of "turning broadcast off") also
makes that inequality impossible - we disabled the very thing we wanted.

This job tests that reading of the code by predicting each outcome first:

  v1 default                    -> BroadcastHashJoin   (wafer 375 KB < 10 MB)
  v2 thr=-1, smj preferred      -> SortMergeJoin
  v3 thr=-1, smj not preferred  -> SortMergeJoin       (reproduces experiment 4)
  v4 thr=100KB, smj off         -> ShuffledHashJoin    (375 KB >= 100 KB, and
                                    375 KB < 100 KB * 8 partitions = 800 KB)
  v5 thr=-1, force SHJ          -> ShuffledHashJoin    (spark.sql.join.forceApplyShuffledHashJoin)

It also counts EVERY join operator and Exchange node in the plan. Experiment 4
reported only the first operator it found, which hid the fact that a query with
two joins can execute them with two different strategies.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "/data/lake/silver"
BRONZE = "/data/lake/bronze"
OPS = "/data/ops"


def shuffle_bytes(sc) -> int:
    base = sc.uiWebUrl
    if not base:
        return 0
    try:
        with urllib.request.urlopen(f"{base}/api/v1/applications", timeout=5) as r:
            app_id = json.load(r)[0]["id"]
        with urllib.request.urlopen(
            f"{base}/api/v1/applications/{app_id}/stages", timeout=5
        ) as r:
            return sum(s.get("shuffleWriteBytes", 0) for s in json.load(r))
    except Exception:
        return 0


def plan_summary(df) -> dict:
    """Count every operator, not just the first one."""
    txt = df._jdf.queryExecution().executedPlan().toString()
    ops = {}
    for op in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
               "BroadcastNestedLoopJoin", "CartesianProduct", "Exchange",
               "HashAggregate"):
        n = txt.count(op)
        if n:
            ops[op] = n
    joins = [k for k in ops if k.endswith("Join") or k == "CartesianProduct"]
    ops["join_strategy"] = "+".join(sorted(joins)) if joins else "none"
    return ops


def main() -> None:
    spark = (
        SparkSession.builder.appName("64_exp_join_choice")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.cbo.enabled", "true")

    print("[exp] materialising catalog tables + statistics (needed for any cost-based choice)")
    spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec").write.mode("overwrite").saveAsTable("exp_events")
    spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id").write.mode(
        "overwrite").saveAsTable("exp_wafer")
    spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority").write.mode(
        "overwrite").saveAsTable("exp_lot")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS")
    for r in spark.sql("DESCRIBE EXTENDED exp_wafer").collect():
        if str(r["col_name"]).lower() in ("statistics", "location"):
            print(f"[exp]   exp_wafer {r['col_name']}={r['data_type']}")

    variants = [
        ("v1_default", "BroadcastHashJoin", {}),
        ("v2_smj", "SortMergeJoin",
         {"spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.join.preferSortMergeJoin": "true"}),
        ("v3_hash_neg", "SortMergeJoin (reproduces exp4)",
         {"spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.join.preferSortMergeJoin": "false"}),
        ("v4_hash_pos", "ShuffledHashJoin",
         {"spark.sql.autoBroadcastJoinThreshold": str(100 * 1024),
          "spark.sql.join.preferSortMergeJoin": "false"}),
        ("v5_force", "ShuffledHashJoin",
         {"spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.join.forceApplyShuffledHashJoin": "true"}),
    ]

    e, w, l = spark.table("exp_events"), spark.table("exp_wafer"), spark.table("exp_lot")
    results = []
    for name, predicted, confs in variants:
        for k, v in confs.items():
            spark.conf.set(k, v)
        df = e.join(w, "wafer_id", "inner").join(l, "lot_id", "inner").select(
            "event_id", "lot_id", "product_id", "priority")

        ops = plan_summary(df)
        before = shuffle_bytes(spark.sparkContext)
        t0 = time.perf_counter()
        got = df.agg(F.count("*").alias("n"),
                     F.sum(F.hash("product_id", "priority").cast("long")).alias("fp")).first()
        elapsed = time.perf_counter() - t0
        wrote = (shuffle_bytes(spark.sparkContext) - before) / 1024 / 1024

        for k in confs:
            spark.conf.unset(k)

        rec = {"exp": "join_choice", "variant": name, "predicted": predicted,
               "observed": ops["join_strategy"], "exchanges": ops.get("Exchange", 0),
               "rows": got["n"], "fingerprint": got["fp"],
               "elapsed_sec": round(elapsed, 2), "shuffle_write_mb": round(wrote, 1),
               "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        results.append(rec)
        print(f"[exp] {name:14s} observed={ops['join_strategy']:34s} "
              f"Exchange={ops.get('Exchange', 0)} rows={got['n']:,} "
              f"{rec['elapsed_sec']:5.2f}s shuffle_w={rec['shuffle_write_mb']:7.1f}MB")
        print(f"[exp]   predicted was: {predicted}")

    with open(f"{OPS}/experiments.jsonl", "a", encoding="utf-8") as fh:
        for rec in results:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.stop()


if __name__ == "__main__":
    main()
