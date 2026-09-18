#!/usr/bin/env bash
# MANUAL seeder — run ONCE on each engine VM (before traffic) to seed its shard of the 5M
# population into the collapsed Redis store. The 5 engine VMs are idle pre-traffic, so they
# double as the seeder pool (design: reuse engine VMs to seed).
#
# On engine VM i (i = 0..SHARDS-1):
#     cd <engine app dir> && SHARD=i SHARDS=5 bash deploy/seed.sh
# VM 0 also loads reference data + cfg (rules incl. blunt device rule + cfg:bands) and the
# flagged high-risk customers (cust_1..FLAGGED0). Knobs: TOTAL(5000000) DAYS(30) FLAGGED0(50000).
set -euo pipefail
cd "$(dirname "$0")/.."                          # engine app dir (test_data/, redis.env)
[ -f redis.env ] && source redis.env

: "${REDIS_HOST:?run from the engine app dir where boot wrote redis.env, or export REDIS_HOST/REDIS_PORT}"
REDIS_PORT="${REDIS_PORT:-6379}"
SHARD="${SHARD:-0}"; SHARDS="${SHARDS:-5}"; TOTAL="${TOTAL:-5000000}"; DAYS="${DAYS:-30}"
PER=$(( TOTAL / SHARDS )); START=$(( SHARD * PER ))
FLAGGED=0; [ "$SHARD" = "0" ] && FLAGGED="${FLAGGED0:-50000}"

python3 -m pip install --quiet --user redis

# VM 0 loads shared config + reference fixtures once.
if [ "$SHARD" = "0" ]; then
  python3 test_data/load_redis.py --redis-url "redis://${REDIS_HOST}:${REDIS_PORT}/0"
fi

echo "[seed] shard $SHARD/$SHARDS -> customers [$START, $((START+PER))) on ${REDIS_HOST}:${REDIS_PORT}"
python3 test_data/seed_feature_store.py --host "$REDIS_HOST" --port "$REDIS_PORT" \
  --customer-start "$START" --customers "$PER" --total-customers "$TOTAL" --days "$DAYS" --procs 8
python3 test_data/seed_signal_store.py  --host "$REDIS_HOST" --port "$REDIS_PORT" \
  --customer-start "$START" --customers "$PER" --total-customers "$TOTAL" --flagged "$FLAGGED" --procs 8
