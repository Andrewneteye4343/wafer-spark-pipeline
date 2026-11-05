@echo off
rem run-pipeline.cmd - run the pipeline once (wrapper for run-pipeline.ps1)
rem Usage:  .\run-pipeline.cmd            (ingest, silver, dq, gold)
rem         .\run-pipeline.cmd 1          (also regenerate bronze first)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-pipeline.ps1" %*
