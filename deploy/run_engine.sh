#!/usr/bin/env bash
# Engine launcher — runs the pre-built app.jar bundled in engine.tar.gz (built locally; no VM
# build). Also records the Redis endpoint to redis.env so the seeders (deploy/seed.sh) can be
# run manually on this VM before traffic. Injected env: REDIS_FRAUD_ENDPOINT, GCS_FRAUD_DATA_*.
# Tarball layout:  ./app.jar  ./test_data/*  ./deploy/{run_engine.sh,seed.sh}
set -euo pipefail
cd "$(dirname "$0")/.."                         # tarball root (app.jar, test_data/, deploy/)

: "${REDIS_FRAUD_ENDPOINT:?injected by connectDatabases=[fraud]}"
STORE="redis://${REDIS_FRAUD_ENDPOINT}"

# Persist the endpoint so deploy/seed.sh (manual) can reach Redis without the injected env.
{ echo "export REDIS_FRAUD_ENDPOINT=${REDIS_FRAUD_ENDPOINT}"
  echo "export REDIS_HOST=${REDIS_FRAUD_ENDPOINT%%:*}"
  echo "export REDIS_PORT=${REDIS_FRAUD_ENDPOINT##*:}"; } > redis.env

# Enable ONNX iff a model already exists on the shared mount; else boot rules-only.
SHARED="${SHARED_DATA:-/data}"; MODEL="$SHARED/models/fraud-model.onnx"
MODEL_FLAGS="--fraud.model.enabled=false"
[ -f "$MODEL" ] && MODEL_FLAGS="--fraud.model.enabled=true --fraud.model.type=onnx --fraud.model.path=$MODEL"

exec java -Xms4g -Xmx4g -XX:+UseZGC -jar app.jar \
  --server.port=8080 \
  --fraud.feature-store.uri="$STORE" \
  --fraud.signal-store.uri="$STORE" \
  --fraud.audit-sink.type=redis-stream \
  --fraud.redis.log-calls=false \
  $MODEL_FLAGS
