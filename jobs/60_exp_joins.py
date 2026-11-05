"""M7 / experiment 1: join strategy shoot-out.

Same logical query, four physical plans. We measure wall clock AND shuffle bytes
(read straight from the driver's own Spark UI REST API - no extra tooling), and
we check that every variant returns the same row count, because a fast wrong
answer is not a result.

Variants
  auto          let Catalyst decide (autoBroadcastJoinThreshold = 10MB default)
  broadcast     explicit /*+ BROADCAST(w) */ hint
  sort_merge    auto-broadcast off, sort-merge preferred
  shuffle_hash  auto-broadcast off, shuffle-hash preferred

Output: /data/ops/experiments.jsonl (one line per variant)
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


def ui_snapshot(sc) -> dict:
    """Cumulative shuffle bytes from the driver's Spark UI REST API."""
    base = sc.uiWebUrl
    if not base:
        return {"shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "stages": 0}
    try:
        with urllib.request.urlopen(f"{base}/api/v1/applications", timeout=5) as r:
            apps = json.load(r)
        if not apps:
            return {"shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "stages": 0}
        app_id = apps[0]["id"]
        with urllib.request.urlopen(
            f"{base}/api/v1/applications/{app_id}/stages", timeout=5
        ) as r:
            stages = json.load(r)
    except Exception as exc:  # UI disabled or race - do not fail the experiment
        print(f"[exp] (ui metrics unavailable: {exc})")
        return {"shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "stages": 0}
    return {
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "stages": len(stages),
    }


def diff(after: dict, before: dict) -> dict:
    return {k: after[k] - before[k] for k in after}


def join_plan_line(df) -> str:
    """First join operator in the physical plan - that is the answer we care about."""
    txt = df._jdf.queryExecution().executedPlan().toString()
    for op in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
               "BroadcastNestedLoopJoin", "CartesianProduct"):
        if op in txt:
            return op
    return "unknown"


def main() -> None:
    spark = SparkSession.builder.appName("60_exp_joins").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    events = spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec"
    )
    wafer = spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id")
    lot = spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority")

    n_events = events.count()
    print(f"[exp] events={n_events:,} wafer={wafer.count():,} lot={lot.count():,}")

    variants = [
        ("auto", {}),
        ("broadcast", {"hint": True}),
        ("sort_merge", {"sql": {"spark.sql.autoBroadcastJoinThreshold": "-1",
                                "spark.sql.join.preferSortMergeJoin": "true"}}),
        ("shuffle_hash", {"sql": {"spark.sql.autoBroadcastJoinThreshold": "-1",
                                  "spark.sql.join.preferSortMergeJoin": "false"}}),
    ]

    baseline = None
    results = []
    for name, opts in variants:
        for k, v in opts.get("sql", {}).items():
            spark.conf.set(k, v)

        if opts.get("hint"):
            joined = events.join(F.broadcast(wafer), "wafer_id", "inner")
        else:
            joined = events.join(wafer, "wafer_id", "inner")
        joined = joined.join(lot, "lot_id", "inner").select(
            "event_id", "lot_id", "product_id", "priority", "step_name"
        )

        plan = join_plan_line(joined)
        before = ui_snapshot(spark.sparkContext)
        t0 = time.perf_counter()
        # a hash sum verifies the content without adding a shuffle of its own
        rows = joined.agg(
            F.count("*").alias("n"),
            F.sum(F.hash("product_id", "priority").cast("long")).alias("fp"),
        ).first()
        elapsed = time.perf_counter() - t0
        spent = diff(ui_snapshot(spark.sparkContext), before)

        # restore defaults before the next variant
        for k in opts.get("sql", {}):
            spark.conf.unset(k)

        ok = baseline is None or (rows["n"] == baseline[0] and rows["fp"] == baseline[1])
        if baseline is None:
            baseline = (rows["n"], rows["fp"])

        rec = {
            "exp": "join", "variant": name, "plan": plan,
            "rows": rows["n"], "fingerprint": rows["fp"],
            "correct": ok,
            "elapsed_sec": round(elapsed, 2),
            "shuffle_read_mb": round(spent["shuffle_read_bytes"] / 1024 / 1024, 1),
            "shuffle_write_mb": round(spent["shuffle_write_bytes"] / 1024 / 1024, 1),
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        results.append(rec)
        print(f"[exp] {name:13s} plan={plan:18s} rows={rec['rows']:,} "
              f"fp={rec['fingerprint']} {rec['elapsed_sec']:6.2f}s "
              f"shuffle_w={rec['shuffle_write_mb']:8.1f}MB correct={ok}")

    fastest = min(results, key=lambda r: r["elapsed_sec"])
    print(f"[exp] fastest variant: {fastest['variant']} ({fastest['elapsed_sec']}s)")

    with open(f"{OPS}/experiments.jsonl", "a", encoding="utf-8") as fh:
        for rec in results:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
