@echo off
rem run-experiments.cmd - wrapper for run-experiments.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-experiments.ps1" %*
