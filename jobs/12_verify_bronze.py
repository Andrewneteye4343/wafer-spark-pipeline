"""M2 step 3: accept or reject the bronze layer.

Everything printed here is a number you can check by hand:
the seed tables must match the SQL project, the generated tables must match the
documented volume, and the three injected defects must land in the expected
proportion.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SEED_EXPECTED = {
    "product": 3, "tool": 6, "chamber": 8, "lot": 5,
    "wafer": 12, "process_event": 36, "metrology": 36, "wafer_bin": 26,
}
GEN_EXPECTED = {
    "lot": 6_000, "wafer": 78_000,
    "events": 2_028_000, "metrology": 1_014_000, "bins": 468_000,
}
B = "/data/lake/bronze"


def main() -> None:
    spark = SparkSession.builder.appName("12_verify_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("== 1. seed tables (must equal the SQL baseline) ==")
    ok = True
    for t, n in SEED_EXPECTED.items():
        got = spark.read.parquet(f"{B}/seed/{t}").count()
        flag = "OK " if got == n else "BAD"
        ok &= got == n
        print(f"  [{flag}] {t:15s} {got:>6} (expected {n})")

    print("== 2. generated volume ==")
    for t, n in GEN_EXPECTED.items():
        got = spark.read.parquet(f"{B}/{t}").count()
        flag = "OK " if got == n else "BAD"
        ok &= got == n
        print(f"  [{flag}] {t:15s} {got:>10,} (expected {n:,})")

    events = spark.read.parquet(f"{B}/events")
    total = events.count()
    nulls = events.filter(F.col("out_ts").isNull()).count()
    ratio = nulls / total
    print("== 3. injected defects ==")
    print(f"  out_ts IS NULL     : {nulls:,} / {total:,} = {ratio:.4%} (expect ~0.5%)")
    ok &= 0.003 <= ratio <= 0.007

    dist = events.groupBy("step_name").count().orderBy("step_name").collect()
    print("  step distribution  : " + ", ".join(f"{r['step_name']}={r['count']:,}" for r in dist))
    ok &= len(dist) == 3

    mo = spark.read.parquet(f"{B}/metrology")
    mo_total = mo.count()
    oos = mo.filter((F.col("metric_value") < F.col("spec_lower")) | (F.col("metric_value") > F.col("spec_upper"))).count()
    print(f"  out of spec        : {oos:,} / {mo_total:,} = {oos / mo_total:.4%} (expect ~0.3%)")
    ok &= 0.001 <= oos / mo_total <= 0.006

    wafers = spark.read.parquet(f"{B}/wafer").select("wafer_id")
    orphans = events.join(wafers, "wafer_id", "left_anti").count()
    print(f"  orphan events      : {orphans:,} (expect 0 unless ORPHAN_ROWS was set)")

    print("== 4. spot check: one baseline wafer ==")
    spark.read.parquet(f"{B}/seed/process_event").filter(
        F.col("wafer_id") == "L-2601-01"
    ).select("event_id", "step_no", "step_name", "in_ts").orderBy("step_no").show(truncate=False)

    print("RESULT:", "PASS" if ok else "FAIL")
    if not ok:
        raise SystemExit(1)
    spark.stop()


if __name__ == "__main__":
    main()
