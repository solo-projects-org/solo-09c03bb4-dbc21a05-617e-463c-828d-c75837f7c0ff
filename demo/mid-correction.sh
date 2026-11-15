#!/usr/bin/env bash
# 中途更正手动样例：用 curl 完整走一遍"采集途中县站更正"的流程。
# 依赖：docker compose 启动后的默认端口（station 8001 / web 8011）。
set -euo pipefail

STATION="${STATION:-http://localhost:8001}"
WEB="${WEB:-http://localhost:8011}"
SECTION="SECTION-A"
DATE="2026-11-14"

j() { python3 -m json.tool; }

echo "== 1. 县站回到修订号 1 =="
curl -sS -X POST "$STATION/admin/reset" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}" | j

echo "== 2. 记下修订号 1 时签发的游标 =="
CURSOR=$(curl -sS "$STATION/observations?section=$SECTION&date=$DATE" | python3 -c "import sys,json;print(json.load(sys.stdin)['next_cursor'])")
echo "cursor=$CURSOR"

echo "== 3. Web 端启动采集（后台任务） =="
RUN=$(curl -sS -X POST "$WEB/api/collect" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}")
echo "$RUN" | j
RUN_ID=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")

echo "== 4. 采集进行中，县站更正 08:00 的氨氮值，修订号递增 =="
sleep 1
curl -sS -X POST "$STATION/admin/correct" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\",\"time\":\"08:00\",\"nh3n\":9.999}" | j

echo "== 5. 旧游标已失效 =="
curl -sS -o /dev/null -w "旧游标读取 HTTP 状态: %{http_code} (期望 409)\n" \
  "$STATION/observations?section=$SECTION&date=$DATE&cursor=$CURSOR"

echo "== 6. 本次采集失败，已发布数据不受影响（不公布半成品） =="
for _ in $(seq 1 30); do
  STATUS=$(curl -sS "$WEB/api/runs/$RUN_ID" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  [ "$STATUS" != "running" ] && break
  sleep 0.5
done
curl -sS "$WEB/api/runs/$RUN_ID" | j

echo "== 7. 重新采集，整日按修订号 2 发布 =="
RUN2=$(curl -sS -X POST "$WEB/api/collect" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}")
RUN2_ID=$(echo "$RUN2" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
for _ in $(seq 1 30); do
  STATUS=$(curl -sS "$WEB/api/runs/$RUN2_ID" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  [ "$STATUS" != "running" ] && break
  sleep 0.5
done
curl -sS "$WEB/api/published?section=$SECTION&date=$DATE" | j
