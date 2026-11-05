#!/bin/bash
# M5: pipeline runner. Lives inside spark-client and is called either by hand
# (run-pipeline.cmd) or by the scheduler container.
#
# Every step appends one JSON line to /data/ops/run_history.jsonl, so the run
# history is machine readable without a database.
#
# Exit codes: 0 = success, 2 = data quality gate failed, other = crash.
set -u

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OPS=/data/ops
LOG="$OPS/run_$RUN_ID.log"
HIST="$OPS/run_history.jsonl"
QUAR=/data/lake/quarantine
mkdir -p "$OPS" "$QUAR"
export RUN_ID   # so jobs can name their quarantine folders after the run

SPARK="/opt/spark/bin/spark-submit --master spark://spark-master:7077"
SPARK="$SPARK --conf spark.driver.host=spark-client --conf spark.driver.bindAddress=0.0.0.0"
SPARK="$SPARK --conf spark.driver.memory=1g --conf spark.executor.memory=2g"
SPARK="$SPARK --conf spark.executor.cores=1 --conf spark.sql.shuffle.partitions=8"
if [ -f /opt/jars/ojdbc11.jar ]; then SPARK="$SPARK --jars /opt/jars/ojdbc11.jar"; fi

alert() {  # status detail
  local msg="[$1] run=$RUN_ID $2"
  echo "[alert] $msg $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OPS/alerts.log"
  if [ -n "${N8N_WEBHOOK_URL:-}" ] && command -v curl >/dev/null 2>&1; then
    curl -s -X POST -H 'Content-Type: application/json' \
      -d "{\"text\":\"$msg\"}" "$N8N_WEBHOOK_URL" >/dev/null || true
  fi
}

log_json() {  # step job status exit_code duration_sec
  printf '{"run_id":"%s","step":"%s","job":"%s","status":"%s","exit_code":%s,"duration_sec":%s,"ts":"%s"}\n' \
    "$RUN_ID" "$1" "$2" "$3" "$4" "$5" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$HIST"
}

run_step() {  # name job
  local name="$1" job="$2"
  echo "=== $name ($job) start $(date -u +%H:%M:%S)" | tee -a "$LOG"
  local t0 t1 dur rc
  t0=$(date +%s)
  $SPARK "/opt/jobs/$job" >> "$LOG" 2>&1
  rc=$?
  t1=$(date +%s); dur=$((t1 - t0))
  if [ "$rc" -eq 0 ]; then
    echo "=== $name OK (${dur}s)" | tee -a "$LOG"
    log_json "$name" "$job" "SUCCESS" "$rc" "$dur"
  elif [ "$rc" -eq 2 ]; then
    echo "=== $name FAILED_DQ (${dur}s) - see $QUAR" | tee -a "$LOG"
    log_json "$name" "$job" "FAILED_DQ" "$rc" "$dur"
    alert FAILED_DQ "step=$name job=$job exit=2 durations=${dur}s"
    exit 2
  else
    echo "=== $name FAILED rc=$rc (${dur}s)" | tee -a "$LOG"
    log_json "$name" "$job" "FAILED" "$rc" "$dur"
    alert FAILED "step=$name job=$job exit=$rc duration=${dur}s"
    exit "$rc"
  fi
}

echo "--- pipeline run $RUN_ID" | tee -a "$LOG"

if [ "${RUN_GENERATE:-0}" = "1" ]; then
  run_step generate 11_generate_scale.py
fi

run_step ingest 10_ingest_bronze.py
run_step silver 20_build_silver.py
run_step dq     40_dq_gate.py
run_step gold   30_build_gold.py
run_step report 50_build_report.py

echo "--- pipeline run $RUN_ID finished OK" | tee -a "$LOG"
exit 0
