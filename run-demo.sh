#!/usr/bin/env bash
# Build/start the stack and run the full demonstration.
set -euo pipefail

cd "$(dirname "$0")"

docker compose up -d --build

echo "Waiting for services to become healthy..."
for port in 8001 8011 8012; do
  for _ in $(seq 1 30); do
    if curl -fsS "http://localhost:${port}/health" >/dev/null 2>&1; then break; fi
    sleep 1
  done
  curl -fsS "http://localhost:${port}/health" >/dev/null
done

python3 -I demo/demo.py
