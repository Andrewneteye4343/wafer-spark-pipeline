# run-pipeline.ps1 - trigger one full pipeline run inside spark-client
# ASCII only: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI.
param(
    [string]$Generate = "0"
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)
$env:RUN_GENERATE = $Generate
$inner = "RUN_GENERATE=$Generate bash /opt/pipeline/run_pipeline.sh"
docker compose exec -T -e RUN_GENERATE=$Generate spark-client bash -lc $inner
if ($LASTEXITCODE -ne 0) { Write-Host "pipeline exited with code $LASTEXITCODE" }
