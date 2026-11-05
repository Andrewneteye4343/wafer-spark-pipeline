#!/bin/bash
# M7: run the three performance experiments and keep the transcript.
set -u
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG="/data/ops/experiments_$RUN_ID.log"
mkdir -p /data/ops/plans /data/ops

SPARK="/opt/spark/bin/spark-submit --master spark://spark-master:7077"
SPARK="$SPARK --conf spark.driver.host=spark-client --conf spark.driver.bindAddress=0.0.0.0"
SPARK="$SPARK --conf spark.driver.memory=1g --conf spark.executor.memory=2g"
SPARK="$SPARK --conf spark.executor.cores=1 --conf spark.sql.shuffle.partitions=8"

echo "--- experiments $RUN_ID" | tee "$LOG"
for job in 60_exp_joins.py 61_exp_partitions.py 63_exp_stats.py 62_explain.py; do
  echo "" | tee -a "$LOG"
  echo "########## $job ##########" | tee -a "$LOG"
  $SPARK "/opt/jobs/$job" 2>&1 | tee -a "$LOG"
done
echo "" | tee -a "$LOG"
echo "--- done, transcript: $LOG" | tee -a "$LOG"
