@echo off
rem run-spark.cmd - wrapper so the script runs even when the execution policy blocks .ps1
rem Usage:  .\run-spark.cmd jobs\00_hello.py
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-spark.ps1" %*
