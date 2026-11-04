"""M1 smoke test: does the cluster actually compute anything?

What to look at in the output:
  1. bucket 0..9 each counting 10,000 -> the shuffle actually happened
  2. the partition count -> it equals the number of cores the cluster gave us
  3. the Parquet row count -> proves the write really landed in /data
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N = 100_000
OUT = "/data/lake/_smoke/hello"


def main() -> None:
    spark = (
        SparkSession.builder.appName("00_hello")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # range() is lazy: nothing is computed until an action is called
    df = spark.range(N).withColumn("bucket", (F.col("id") % 10).cast("int"))

    print(f"[hello] partitions={df.rdd.getNumPartitions()}")

    # groupBy triggers a shuffle; show() is the action that runs it
    counts = df.groupBy("bucket").count().orderBy("bucket")
    counts.show(10, truncate=False)

    df.write.mode("overwrite").parquet(OUT)
    written = spark.read.parquet(OUT).count()
    print(f"[hello] wrote {OUT} rows={written}")
    assert written == N, f"expected {N} rows, got {written}"

    spark.stop()


if __name__ == "__main__":
    main()
