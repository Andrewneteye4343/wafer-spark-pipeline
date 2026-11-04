#!/bin/sh
# 這個檔案會被 container entrypoint「source」，所以可用 compose 傳進來的 APP_USER / APP_USER_PASSWORD。
# schema/*.sql 故意放 /schema（不放 /container-entrypoint-initdb.d），避免 entrypoint 再用 SYS 跑一次。
# 重要：每個 sqlplus 都加 < /dev/null。容器有 TTY 時，sqlplus 讀完 @script 可能停在 stdin
#       等輸入而下不來（實測：schema 建完就卡住、下一階段永遠不開始）。
set -e
echo "[init] 建立 schema 物件（身分：${APP_USER}，資料庫：FREEPDB1）"
sqlplus -s -L "${APP_USER}/${APP_USER_PASSWORD}@//localhost:1521/FREEPDB1" @/schema/002_schema.sql < /dev/null
echo "[init] schema 完成，開始載入基線資料"
sqlplus -s -L "${APP_USER}/${APP_USER_PASSWORD}@//localhost:1521/FREEPDB1" @/schema/003_seed.sql < /dev/null
echo "[init] 完成：schema + seed 已載入"
