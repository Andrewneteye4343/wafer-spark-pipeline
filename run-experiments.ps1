# run-experiments.ps1 - M7 performance experiments (read-only: no gold is rewritten)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)
docker compose exec -T spark-client bash -lc "bash /opt/pipeline/run_experiments.sh"
if ($LASTEXITCODE -ne 0) { Write-Host "experiments exited with code $LASTEXITCODE" }
