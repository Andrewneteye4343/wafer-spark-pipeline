# run-spark.ps1 - submit a PySpark job to the local Spark cluster
# ASCII only: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI.
#
# Usage:
#   .\run-spark.ps1 jobs\10_ingest_bronze.py
#   .\run-spark.ps1 jobs\11_generate_scale.py
#
# If jars\ojdbc11.jar exists on the host it is shipped to the cluster with
# --jars, which is what the Oracle JDBC reader in 10_ingest_bronze.py needs.
# Both jars\ and jobs\ are mounted into spark-client AND spark-worker, because
# the JDBC read happens on the executors, not on the driver.
param(
    [Parameter(Position = 0)][string]$Job = "",
    [string]$Master = "spark://spark-master:7077",
    [string]$ExecutorMem = "2g",
    [string]$DriverMem = "1g"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)
$jobsRoot = Join-Path (Get-Location).Path "jobs"
$jarPath = Join-Path (Get-Location).Path "jars\ojdbc11.jar"

if ($Job -eq "") {
    Write-Host 'Usage: .\run-spark.ps1 jobs\10_ingest_bronze.py'
    exit 1
}
if (-not (Test-Path $Job)) {
    Write-Host "Job not found: $Job"
    exit 1
}

$full = (Resolve-Path $Job).Path
if (-not $full.StartsWith($jobsRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    Write-Host "Job must live under .\jobs\ so the containers can see it."
    exit 1
}
$rel = $full.Substring($jobsRoot.Length).Replace('\', '/').TrimStart('/')

$parts = @(
    "/opt/spark/bin/spark-submit"
    "--master $Master"
    "--conf spark.driver.host=spark-client"
    "--conf spark.driver.bindAddress=0.0.0.0"
    "--conf spark.driver.memory=$DriverMem"
    "--conf spark.executor.memory=$ExecutorMem"
    "--conf spark.executor.cores=1"
    "--conf spark.sql.shuffle.partitions=8"
)
if (Test-Path $jarPath) {
    $parts += "--jars /opt/jars/ojdbc11.jar"
    Write-Host "[run-spark] shipping ojdbc11.jar"
}
$parts += "/opt/jobs/$rel"
$inner = $parts -join " "

docker compose exec -T spark-client bash -lc $inner
if ($LASTEXITCODE -ne 0) { Write-Host "spark-submit exited with code $LASTEXITCODE" }
