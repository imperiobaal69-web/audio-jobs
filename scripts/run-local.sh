#!/usr/bin/env bash
# Run the API and N workers as separate processes against the local Redis and
# Postgres (scripts/dev-up.sh). Extra environment (ANALYZER, CHAOS_*, ...) is
# passed through to the workers.
#   scripts/run-local.sh start [workers]   scripts/run-local.sh stop
#   scripts/run-local.sh worker <n>        (start one more worker, used by chaos.sh)
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=.local/run
API_PORT="${API_PORT:-8088}"
export DATABASE_URL="${DATABASE_URL:-postgres://postgres@127.0.0.1:5440/audiojobs?sslmode=disable}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6390}"
export BLOB_DIR="${BLOB_DIR:-$PWD/.local/blobs}"
mkdir -p "$RUN" bin

start_worker() {
  local n="$1"
  METRICS_ADDR="127.0.0.1:$((9100 + n))" CONSUMER_NAME="worker-$n-$RANDOM" \
    nohup bin/worker >>"$RUN/worker-$n.log" 2>&1 &
  echo $! >"$RUN/worker-$n.pid"
}

case "${1:-}" in
start)
  go build -o bin/ ./cmd/...
  bin/migrate >/dev/null
  HTTP_ADDR="127.0.0.1:$API_PORT" nohup bin/api >>"$RUN/api.log" 2>&1 &
  echo $! >"$RUN/api.pid"
  for n in $(seq 1 "${2:-4}"); do start_worker "$n"; done
  for _ in $(seq 1 50); do curl -fs "http://127.0.0.1:$API_PORT/readyz" >/dev/null && break; sleep 0.1; done
  echo "api on http://127.0.0.1:$API_PORT with ${2:-4} workers"
  ;;
worker)
  start_worker "$2"
  ;;
stop)
  for f in "$RUN"/*.pid; do
    [ -e "$f" ] || continue
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  # wait for graceful shutdown
  for _ in $(seq 1 100); do pgrep -f "bin/(api|worker)$" >/dev/null || break; sleep 0.1; done
  echo stopped
  ;;
*)
  echo "usage: $0 start [workers] | worker <n> | stop" >&2
  exit 2
  ;;
esac
