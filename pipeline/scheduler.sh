#!/bin/bash
# M5: minimal scheduler. No cron dependency: sleep, run, repeat.
# RUN_EVERY_SECONDS=0 -> run once and exit (used for manual tests).
set -u
INTERVAL="${RUN_EVERY_SECONDS:-86400}"
echo "[scheduler] interval=${INTERVAL}s"
while true; do
  echo "[scheduler] $(date -u +%Y-%m-%dT%H:%M:%SZ) starting run"
  bash /opt/pipeline/run_pipeline.sh || echo "[scheduler] run failed with $?"
  if [ "$INTERVAL" -le 0 ]; then
    echo "[scheduler] single-shot mode, exiting"
    exit 0
  fi
  sleep "$INTERVAL"
done
