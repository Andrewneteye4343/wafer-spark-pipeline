"""M5: data quality gate.

Hard rules (the job exits 2 when any of them fails, so the pipeline stops
before gold is published to anyone):

  R1 orphan foreign key : every process_event.wafer_id must exist in wafer
  R2 time order         : out_ts must be >= in_ts
  R3 spec ratio         : at least 99% of measurements are inside their spec
  R4 missing days       : no calendar day between the first and last event is empty

Failing rows are written to /data/lake/quarantine/<rule>__<run_id>/ so the next
person can look at exactly what was rejected instead of re-deriving it.

Exit codes: 0 = pass, 2 = data quality failure (distinct from 1 = crash).
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "/data/lake/bronze"
SILVER = "/data/lake/silver"
QUAR = "/data/lake/quarantine"
MIN_IN_SPEC = 0.99


def lower_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def dump(df: DataFrame, run_id: str, rule: str) -> None:
    path = f"{QUAR}/{rule}__{run_id}"
    df.write.mode("overwrite").parquet(path)
    print(f"[dq]   rejected rows written to {path}")


def main() -> int:
    run_id = os.environ.get("RUN_ID", "manual")
    spark = SparkSession.builder.appName("40_dq_gate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(f"{SILVER}/event_wide")
    # Spark infers a partition column type from the directory name, so dt
    # (written as a string) reads back as DateType. Normalise it to string
    # so date.fromisoformat() keeps working.
    events = events.withColumn("dt", F.date_format("dt", "yyyy-MM-dd"))
    wafer = lower_columns(spark.read.parquet(f"{BRONZE}/wafer")).select("wafer_id")
    mo = lower_columns(spark.read.parquet(f"{BRONZE}/metrology"))

    failures = []

    # R1 - orphan foreign keys
    orphans = events.join(F.broadcast(wafer), "wafer_id", "left_anti")
    n_orphan = orphans.count()
    print(f"[dq] R1 orphan events        : {n_orphan}")
    if n_orphan:
        dump(orphans, run_id, "R1_orphan")
        failures.append(f"R1: {n_orphan} events reference a wafer that does not exist")

    # R2 - time order
    bad_time = events.filter(F.col("out_ts").isNotNull() & (F.col("out_ts") < F.col("in_ts")))
    n_bad_time = bad_time.count()
    print(f"[dq] R2 out_ts < in_ts       : {n_bad_time}")
    if n_bad_time:
        dump(bad_time, run_id, "R2_time_order")
        failures.append(f"R2: {n_bad_time} events finish before they start")

    # R3 - measurement spec ratio
    total = mo.count()
    in_spec = mo.filter(
        F.col("metric_value").between(F.col("spec_lower"), F.col("spec_upper"))
    ).count() if total else 0
    ratio = (in_spec / total) if total else 0.0
    print(f"[dq] R3 in-spec ratio        : {in_spec:,}/{total:,} = {ratio:.4%} (min {MIN_IN_SPEC:.0%})")
    if ratio < MIN_IN_SPEC:
        failures.append(f"R3: in-spec ratio {ratio:.4%} below {MIN_IN_SPEC:.0%}")

    # R4 - missing calendar days
    days = [r["dt"] for r in events.select("dt").distinct().collect()]
    days = sorted(d for d in days if d)
    missing = []
    if days:
        from datetime import date, timedelta
        start = date.fromisoformat(days[0])
        end = date.fromisoformat(days[-1])
        have = set(days)
        cur = start
        while cur <= end:
            if cur.isoformat() not in have:
                missing.append(cur.isoformat())
            cur += timedelta(days=1)
    print(f"[dq] R4 missing days         : {len(missing)} ({missing[:5]}{'...' if len(missing) > 5 else ''})")
    if missing:
        failures.append(f"R4: {len(missing)} calendar days have no events")

    if failures:
        print("[dq] RESULT: FAIL")
        for f in failures:
            print(f"[dq]   - {f}")
        spark.stop()
        return 2

    print("[dq] RESULT: PASS (all 4 rules)")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
