#!/usr/bin/env bash
# Start throwaway Redis and Postgres for local development and tests, inside
# .local/ on non-default ports, so nothing global is touched.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD/.local"
REDIS_PORT="${REDIS_PORT:-6390}"
PG_PORT="${PG_PORT:-5440}"
PG_BIN="${PG_BIN:-$(dirname "$(command -v pg_ctl || echo /opt/homebrew/opt/postgresql@17/bin/pg_ctl)")}"
mkdir -p "$ROOT/redis" "$ROOT/blobs"

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  redis-server --port "$REDIS_PORT" --dir "$ROOT/redis" --save "" --appendonly no \
    --daemonize yes --logfile "$ROOT/redis/redis.log"
fi

if [ ! -d "$ROOT/pg" ]; then
  LC_ALL=C "$PG_BIN/initdb" -D "$ROOT/pg" -U postgres --auth=trust -E UTF8 --locale=C >/dev/null
fi
if ! "$PG_BIN/pg_isready" -h 127.0.0.1 -p "$PG_PORT" >/dev/null 2>&1; then
  LC_ALL=C "$PG_BIN/pg_ctl" -D "$ROOT/pg" -o "-p $PG_PORT -k /tmp" -l "$ROOT/pg.log" start >/dev/null
  for _ in $(seq 1 50); do "$PG_BIN/pg_isready" -h 127.0.0.1 -p "$PG_PORT" >/dev/null 2>&1 && break; sleep 0.1; done
fi
"$PG_BIN/psql" -h 127.0.0.1 -p "$PG_PORT" -U postgres -tAc "select 1 from pg_database where datname='audiojobs'" | grep -q 1 \
  || "$PG_BIN/createdb" -h 127.0.0.1 -p "$PG_PORT" -U postgres audiojobs

echo "REDIS_ADDR=127.0.0.1:$REDIS_PORT"
echo "DATABASE_URL=postgres://postgres@127.0.0.1:$PG_PORT/audiojobs?sslmode=disable"
