"""M7 / experiment 6: the two things experiment 5 could not explain.

Experiment 5 produced two surprises, and Spark 3.5's source explains both:

SURPRISE 1 - `forceApplyShuffledHashJoin=true` did nothing.
  sql/catalyst/.../optimizer/joins.scala:
      def forceApplyShuffledHashJoin(conf: SQLConf): Boolean = {
        Utils.isTesting &&
          conf.getConfString("spark.sql.join.forceApplyShuffledHashJoin", "false") == "true"
      }
  "The config key is hard-coded because it's testing only and should not be
   exposed." In a real spark-submit app Utils.isTesting is false, so the switch
  is inert. It exists for Spark's own test suites, not for us.

SURPRISE 2 - with threshold=100KB we got ShuffledHashJoin + SortMergeJoin,
  and no BroadcastHashJoin at all, even though the 30 KB lot table should have
  qualified for broadcast (broadcast is rule #1 in JoinSelection).
  Prime suspect: adaptive query execution. AQE re-checks the broadcast decision
  at runtime and can demote a broadcast join to a shuffle join when the side
  turns out bigger than the (here very small, 100 KB) threshold.

So this job runs the same two configurations with AQE off and on, and dumps the
formatted plans so the join strategies can be read side by side.

Predictions, written before running:
  v4 threshold=100KB, AQE off -> BroadcastHashJoin + ShuffledHashJoin
  v4 threshold=100KB, AQE on  -> ShuffledHashJoin + SortMergeJoin (as observed)
  v5 force=true,      AQE off -> SortMergeJoin (switch is inert outside tests)
  v5 force=true,      AQE on  -> SortMergeJoin
"""
import json
import os as _os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "/data/lake/silver"
BRONZE = "/data/lake/bronze"
OPS = "/data/ops"
PLANS = "/data/ops/plans"

SMALL_THRESHOLD = str(100 * 1024)   # 100 KB: wafer (375 KB) no, lot (30 KB) yes


def plan_summary(df) -> dict:
    txt = df._jdf.queryExecution().executedPlan().toString()
    ops = {}
    for op in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
               "BroadcastNestedLoopJoin", "Exchange", "HashAggregate"):
        if txt.count(op):
            ops[op] = txt.count(op)
    joins = [k for k in ops if k.endswith("Join")]
    ops["join_strategy"] = "+".join(sorted(joins)) if joins else "none"
    return ops


def main() -> None:
    spark = (
        SparkSession.builder.appName("65_exp_aqe")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.cbo.enabled", "true")
    _os.makedirs(PLANS, exist_ok=True)

    print("[exp] rebuilding catalog tables + statistics")
    spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec").write.mode("overwrite").saveAsTable("exp_events")
    spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id").write.mode(
        "overwrite").saveAsTable("exp_wafer")
    spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority").write.mode(
        "overwrite").saveAsTable("exp_lot")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS")

    e, w, l = spark.table("exp_events"), spark.table("exp_wafer"), spark.table("exp_lot")

    cases = [
        ("v4_thr100k_aqe_off", "BroadcastHashJoin+ShuffledHashJoin",
         {"spark.sql.autoBroadcastJoinThreshold": SMALL_THRESHOLD,
          "spark.sql.join.preferSortMergeJoin": "false",
          "spark.sql.adaptive.enabled": "false"}),
        ("v4_thr100k_aqe_on", "ShuffledHashJoin+SortMergeJoin",
         {"spark.sql.autoBroadcastJoinThreshold": SMALL_THRESHOLD,
          "spark.sql.join.preferSortMergeJoin": "false",
          "spark.sql.adaptive.enabled": "true"}),
        ("v5_force_aqe_off", "SortMergeJoin",
         {"spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.join.forceApplyShuffledHashJoin": "true",
          "spark.sql.adaptive.enabled": "false"}),
        ("v5_force_aqe_on", "SortMergeJoin",
         {"spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.join.forceApplyShuffledHashJoin": "true",
          "spark.sql.adaptive.enabled": "true"}),
    ]

    results = []
    with open(f"{PLANS}/join_choice.txt", "w", encoding="utf-8") as fh:
        for name, predicted, confs in cases:
            for k, v in confs.items():
                spark.conf.set(k, v)
            df = e.join(w, "wafer_id", "inner").join(l, "lot_id", "inner").select(
                "event_id", "lot_id", "product_id", "priority")

            ops = plan_summary(df)
            plan_txt = df._jdf.queryExecution().executedPlan().toString()
            fh.write(f"\n========== {name} ==========\n{predicted=}\n{plan_txt}\n")

            t0 = time.perf_counter()
            got = df.agg(F.count("*").alias("n"),
                         F.sum(F.hash("product_id", "priority").cast("long")).alias("fp")).first()
            elapsed = time.perf_counter() - t0

            for k in confs:
                spark.conf.unset(k)

            rec = {"exp": "aqe_join", "variant": name, "predicted": predicted,
                   "observed": ops["join_strategy"], "exchanges": ops.get("Exchange", 0),
                   "rows": got["n"], "fingerprint": got["fp"],
                   "elapsed_sec": round(elapsed, 2),
                   "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            results.append(rec)
            print(f"[exp] {name:22s} observed={ops['join_strategy']:34s} "
                  f"Exchange={ops.get('Exchange', 0)} {elapsed:5.2f}s")
            print(f"[exp]   predicted: {predicted}")

    with open(f"{OPS}/experiments.jsonl", "a", encoding="utf-8") as fh:
        for rec in results:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"[exp] full plans written to {PLANS}/join_choice.txt")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.stop()


if __name__ == "__main__":
    main()
