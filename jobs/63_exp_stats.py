"""M7 / experiment 4: statistics, and a shuffle big enough to matter.

Two honest follow-ups to experiments 1-2:

(a) Why did `preferSortMergeJoin=false` still give SortMergeJoin?
    Because Catalyst had no row/column statistics, so its cost model was blind.
    Here we write the tables into the catalog, ANALYZE them (collect statistics)
    and turn CBO on, then ask for the same variants again. If the plan changes,
    the lesson is "plan choice follows statistics", not "the config was ignored".

(b) Why did partitions=200 look fastest in experiment 2?
    Because that aggregation only produced 5,007 rows - the shuffle was tiny
    (0.1 MB), so all four settings were within noise. Here we force a wide
    shuffle (group by wafer_id x step, ~234k keys) so partition count has
    something to actually change.

Everything is appended to /data/ops/experiments.jsonl.
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


def join_plan(df) -> str:
    txt = df._jdf.queryExecution().executedPlan().toString()
    for op in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
               "BroadcastNestedLoopJoin", "CartesianProduct"):
        if op in txt:
            return op
    return "unknown"


def append(rec: dict) -> None:
    with open(f"{OPS}/experiments.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main() -> None:
    # spark.sql.warehouse.dir is a STATIC config: it must be set when the
    # session is created, not afterwards (spark.conf.set would fail with
    # "Cannot modify the value of a static config").
    spark = (
        SparkSession.builder.appName("63_exp_stats")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    events = spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec")
    wafer = spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id")
    lot = spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority")

    print("[exp] registering tables in the catalog and collecting statistics")
    for name, df in (("exp_events", events), ("exp_wafer", wafer), ("exp_lot", lot)):
        df.write.mode("overwrite").saveAsTable(name)
    spark.sql("ANALYZE TABLE exp_events COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE exp_wafer COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE exp_lot COMPUTE STATISTICS")
    for name in ("exp_events", "exp_wafer", "exp_lot"):
        rows = spark.sql(f"DESCRIBE EXTENDED {name}").collect()
        kept = [(r["col_name"], r["data_type"]) for r in rows
                if str(r["col_name"]).lower() in
                ("statistics", "numrows", "sizeinbytes", "location", "provider")]
        print(f"[exp]   {name}: " + "; ".join(f"{k}={v}" for k, v in kept))

    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

    e = spark.table("exp_events")
    w = spark.table("exp_wafer")
    l = spark.table("exp_lot")

    print("\n[exp] (a) join variants WITH statistics and CBO on")
    variants = [
        ("stats_auto", {}),
        ("stats_no_broadcast_smj", {"spark.sql.autoBroadcastJoinThreshold": "-1",
                                    "spark.sql.join.preferSortMergeJoin": "true"}),
        ("stats_no_broadcast_hash", {"spark.sql.autoBroadcastJoinThreshold": "-1",
                                     "spark.sql.join.preferSortMergeJoin": "false"}),
    ]
    for name, confs in variants:
        for k, v in confs.items():
            spark.conf.set(k, v)
        df = e.join(w, "wafer_id", "inner").join(l, "lot_id", "inner").select(
            "event_id", "lot_id", "product_id", "priority")
        plan = join_plan(df)
        before = shuffle_bytes(spark.sparkContext)
        t0 = time.perf_counter()
        rows = df.agg(F.count("*").alias("n"),
                      F.sum(F.hash("product_id", "priority").cast("long")).alias("fp")).first()
        elapsed = time.perf_counter() - t0
        wrote = (shuffle_bytes(spark.sparkContext) - before) / 1024 / 1024
        rec = {"exp": "join_stats", "variant": name, "plan": plan, "rows": rows["n"],
               "fingerprint": rows["fp"], "elapsed_sec": round(elapsed, 2),
               "shuffle_write_mb": round(wrote, 1),
               "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        append(rec)
        print(f"[exp] {name:24s} plan={plan:18s} rows={rec['rows']:,} "
              f"{rec['elapsed_sec']:6.2f}s shuffle_w={rec['shuffle_write_mb']:8.1f}MB")
        for k in confs:
            spark.conf.unset(k)

    print("\n[exp] (b) wide shuffle: group by wafer_id x step_name (234,000 keys)")
    wide = e.withColumn("wafer_step", F.concat_ws("|", "wafer_id", "step_name"))
    for n in (8, 32, 200):
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        before = shuffle_bytes(spark.sparkContext)
        t0 = time.perf_counter()
        out = wide.groupBy("wafer_step").agg(F.count("*").alias("events"),
                                             F.avg("dur_sec").alias("avg_dur"))
        rows = out.count()
        elapsed = time.perf_counter() - t0
        wrote = (shuffle_bytes(spark.sparkContext) - before) / 1024 / 1024
        rec = {"exp": "wide_partitions", "partitions": n, "rows": rows,
               "elapsed_sec": round(elapsed, 2), "shuffle_write_mb": round(wrote, 1),
               "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        append(rec)
        print(f"[exp] partitions={n:4d} rows={rows:,} {rec['elapsed_sec']:6.2f}s "
              f"shuffle_w={rec['shuffle_write_mb']:8.1f}MB")

    spark.sql("DROP TABLE IF EXISTS exp_events")
    spark.sql("DROP TABLE IF EXISTS exp_wafer")
    spark.sql("DROP TABLE IF EXISTS exp_lot")
    spark.stop()


if __name__ == "__main__":
    main()
