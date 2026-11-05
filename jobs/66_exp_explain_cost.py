"""M7 / experiment 7: EXPLAIN COST - print the numbers the optimiser used.

Experiment 6 showed a join that should have satisfied every documented condition
for ShuffledHashJoin, yet Catalyst picked SortMergeJoin. Instead of guessing
which size it saw, this job asks Spark to print it.

  EXPLAIN COST      -> optimised logical plan annotated with sizeInBytes/rowCount
  EXPLAIN FORMATTED -> the physical plan, i.e. which join operator was chosen

Three queries under one fixed configuration:
  A  events JOIN wafer only     (the join that refused to become SHJ)
  B  wafer  JOIN lot only       (the join that did become SHJ)
  C  all three joins            (the real shape)

Configuration held constant for all three:
  spark.sql.autoBroadcastJoinThreshold = 102400   (100 KB)
  spark.sql.join.preferSortMergeJoin   = false
  spark.sql.adaptive.enabled           = false    (AQE out of the picture)

Output: /data/ops/plans/explain_cost.txt
"""
import os as _os

from pyspark.sql import SparkSession

OUT = "/data/ops/plans"
SILVER = "/data/lake/silver"
BRONZE = "/data/lake/bronze"
THRESHOLD = str(100 * 1024)

QUERIES = [
    ("A. events JOIN wafer only",
     "SELECT count(*) FROM exp_events e JOIN exp_wafer w ON e.wafer_id = w.wafer_id"),
    ("B. wafer JOIN lot only",
     "SELECT count(*) FROM exp_wafer w JOIN exp_lot l ON w.lot_id = l.lot_id"),
    ("C. all three joins",
     "SELECT count(*) FROM exp_events e "
     "JOIN exp_wafer w ON e.wafer_id = w.wafer_id "
     "JOIN exp_lot l ON w.lot_id = l.lot_id"),
]


def emit(fh, title: str, body: str) -> None:
    print(f"\n===== {title} =====")
    print(body)
    fh.write(f"\n===== {title} =====\n{body}\n")


def main() -> None:
    spark = (
        SparkSession.builder.appName("66_exp_explain_cost")
        .config("spark.sql.warehouse.dir", "/data/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", THRESHOLD)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

    print("[exp] configuration in force:")
    for k in ("spark.sql.autoBroadcastJoinThreshold", "spark.sql.join.preferSortMergeJoin",
              "spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
              "spark.sql.cbo.enabled"):
        print(f"[exp]   {k} = {spark.conf.get(k)}")
    print(f"[exp] => SHJ size ceiling = {int(THRESHOLD) * 8:,} bytes "
          f"(threshold x shuffle partitions)")

    _os.makedirs(OUT, exist_ok=True)
    print("\n[exp] rebuilding catalog tables + statistics")
    spark.read.parquet(f"{SILVER}/event_wide").select(
        "event_id", "wafer_id", "step_name", "dur_sec").write.mode("overwrite").saveAsTable("exp_events")
    spark.read.parquet(f"{BRONZE}/wafer").select("wafer_id", "lot_id").write.mode(
        "overwrite").saveAsTable("exp_wafer")
    spark.read.parquet(f"{BRONZE}/lot").select("lot_id", "product_id", "priority").write.mode(
        "overwrite").saveAsTable("exp_lot")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS")

    with open(f"{OUT}/explain_cost.txt", "w", encoding="utf-8") as fh:
        emit(fh, "table statistics", "")
        for t in ("exp_events", "exp_wafer", "exp_lot"):
            stats = [(r["col_name"], r["data_type"])
                     for r in spark.sql(f"DESCRIBE EXTENDED {t}").collect()
                     if str(r["col_name"]).lower() in ("statistics", "location")]
            line = "; ".join(f"{k}={v}" for k, v in stats)
            print(f"[exp]   {t}: {line}")
            fh.write(f"{t}: {line}\n")

        for title, sql in QUERIES:
            cost = "\n".join(r[0] for r in spark.sql(f"EXPLAIN COST {sql}").collect())
            phys = "\n".join(r[0] for r in spark.sql(f"EXPLAIN FORMATTED {sql}").collect())
            emit(fh, f"{title} - EXPLAIN COST (the numbers Catalyst used)", cost)
            emit(fh, f"{title} - EXPLAIN FORMATTED (the operator it chose)", phys)

    print(f"\n[exp] written to {OUT}/explain_cost.txt")
    for t in ("exp_events", "exp_wafer", "exp_lot"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.stop()


if __name__ == "__main__":
    main()
