"""M2 step 1: pull the 8 baseline tables out of Oracle into the bronze layer.

Why JDBC and not "export CSV and load": a real pipeline talks to the source
system. The 8 tables are tiny (132 rows total) so this is safe to run against
the container every time.

Prereq: jars/ojdbc11.jar must exist (see README) and be mounted at /opt/jars.
"""
import os

from pyspark.sql import SparkSession

URL_TPL = "jdbc:oracle:thin:@//{host}:{port}/{service}"
TABLES = [
    "product", "tool", "chamber", "lot",
    "wafer", "process_event", "metrology", "wafer_bin",
]
EXPECTED = {
    "product": 3, "tool": 6, "chamber": 8, "lot": 5,
    "wafer": 12, "process_event": 36, "metrology": 36, "wafer_bin": 26,
}
OUT = "/data/lake/bronze/seed"


def main() -> None:
    host = os.environ.get("ORACLE_HOST", "oracle")
    port = os.environ.get("ORACLE_PORT", "1521")
    service = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
    user = os.environ.get("ORACLE_USER", "sqluser")
    password = os.environ.get("ORACLE_PASSWORD", "SqlDev#2026")
    url = URL_TPL.format(host=host, port=port, service=service)

    spark = SparkSession.builder.appName("10_ingest_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[ingest] url={url} user={user}")
    seen = {}
    for table in TABLES:
        df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", table.upper())
            .option("user", user)
            .option("password", password)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("fetchsize", "500")
            .load()
        )
        # Oracle returns UPPERCASE column names over JDBC; normalise to lowercase
        # so every layer (bronze/silver/gold) uses the same names.
        df = df.toDF(*[c.lower() for c in df.columns])
        rows = df.count()
        seen[table] = rows
        df.write.mode("overwrite").parquet(f"{OUT}/{table}")
        print(f"[ingest] {table:15s} rows={rows:>6}  (expected {EXPECTED[table]})")

    bad = {t: (n, EXPECTED[t]) for t, n in seen.items() if n != EXPECTED[t]}
    assert not bad, f"row count mismatch vs SQL baseline: {bad}"
    print("[ingest] OK - bronze/seed matches the oracle-sql-lab baseline exactly")
    spark.stop()


if __name__ == "__main__":
    main()
