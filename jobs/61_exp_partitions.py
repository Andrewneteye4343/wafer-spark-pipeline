"""M7 / experiment 2: how many shuffle partitions should this box use?

One aggregation (the same shape as gold/tool_load_hourly) run with
spark.sql.shuffle.partitions = 2 / 8 / 32 / 200 on a 2-core worker.

The point is not "more is better". Too few leaves cores idle; too many spends
more time scheduling 200 tiny tasks than doing work. The number that wins on
your machine is the answer - this job only produces the evidence.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "/data/lake/silver"
OPS = "/data/ops"


def ui_shuffle(sc) -> int:
    base = sc.uiWebUrl
    if not base:
        return 0
    try:
        with urllib.request.urlopen(f"{base}/api/v1/applications", timeout=5) as r:
            apps = json.load(r)
        app_id = apps[0]["id"]
        with urllib.request.urlopen(f"{base}/api/v1/applications/{app_id}/stages", timeout=5) as r:
            return sum(s.get("shuffleWriteBytes", 0) for s in json.load(r))
    except Exception:
        return 0


def main() -> None:
    spark = SparkSession.builder.appName("61_exp_partitions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(f"{SILVER}/event_wide").select(
        "tool_id", "step_name", "in_ts", "dur_sec"
    )
    events = events.withColumn("hour_ts", F.date_format("in_ts", "yyyy-MM-dd HH:00"))

    cores = spark.conf.get("spark.executor.cores", "?")
    print(f"[exp] executor cores={cores} (this is the ceiling on real parallelism)")

    results = []
    baseline = None
    for n in (2, 8, 32, 200):
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        before = ui_shuffle(spark.sparkContext)
        t0 = time.perf_counter()
        out = (events.groupBy("tool_id", "step_name", "hour_ts")
                     .agg(F.count("*").alias("event_cnt")))
        rows = out.count()
        elapsed = time.perf_counter() - t0
        wrote = (ui_shuffle(spark.sparkContext) - before) / 1024 / 1024

        ok = baseline is None or rows == baseline
        if baseline is None:
            baseline = rows

        rec = {"exp": "partitions", "partitions": n, "rows": rows, "correct": ok,
               "elapsed_sec": round(elapsed, 2), "shuffle_write_mb": round(wrote, 1),
               "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        results.append(rec)
        print(f"[exp] partitions={n:4d} rows={rows:,} {rec['elapsed_sec']:6.2f}s "
              f"shuffle_w={rec['shuffle_write_mb']:8.1f}MB correct={ok}")

    best = min(results, key=lambda r: r["elapsed_sec"])
    print(f"[exp] fastest setting: shuffle.partitions={best['partitions']}")
    print("[exp] rule of thumb: 2-4 tasks per core, so ~8-16 on a 2-core worker")

    with open(f"{OPS}/experiments.jsonl", "a", encoding="utf-8") as fh:
        for rec in results:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
