#!/usr/bin/env bash
# Start (or restart) the recording proxy for conformance experiments.
# Writes captures to $GOLDEN_DIR (default /tmp/conf_golden). Idempotent.
set -uo pipefail
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"

pkill -9 -f "target/debug/record-proxy" 2>/dev/null || true
sleep 1
mkdir -p "$GOLDEN_DIR"
cd "$REPO"
GOLDEN_DIR="$GOLDEN_DIR" PROXY_LISTEN="127.0.0.1:$PORT" RUST_LOG=warn \
  nohup cargo run -q -p dbt-state-harness --features fuzz --bin record-proxy \
  > /tmp/conf_proxy.log 2>&1 &
echo "proxy pid $!"
for i in $(seq 1 40); do
  if (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    echo "LISTENING on 127.0.0.1:$PORT (golden dir $GOLDEN_DIR)"; exit 0
  fi
  sleep 1
done
echo "FAILED to come up"; cat /tmp/conf_proxy.log; exit 1
