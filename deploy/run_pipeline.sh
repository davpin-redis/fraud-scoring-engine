#!/usr/bin/env bash
# Start command for the wizard "pipeline" app (1 VM). Ships INSIDE pipeline.tar.gz.
# Runs the whole feedback-loop data plane: Parquet writer, fast loop, chargeback label feed,
# retrain loop, and the aggregator + live dashboard (SSE on :8090).
#
# Wizard injects: REDIS_FRAUD_ENDPOINT, GCS_FRAUD_DATA_BUCKET / GCS_FRAUD_DATA_URL.
# Tarball layout expected:  ./pipeline/*  ./deploy/run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")/.."                       # tarball root (contains pipeline/)

: "${REDIS_FRAUD_ENDPOINT:?injected by connectDatabases=[fraud]}"

# Shared POSIX path for the Parquet system-of-record + model artifact. The Parquet writer uses
# atomic temp+rename, so this must be a real filesystem (Filestore NFS, or gcsfuse with
# --implicit-dirs) — not a raw gs:// URL. The operator mounts it (see deploy/README.md).
SHARED="${SHARED_DATA:-/data}"
mkdir -p "$SHARED/parquet" "$SHARED/models"

pip3 install -q -r pipeline/requirements.txt

export REDIS_URL="redis://${REDIS_FRAUD_ENDPOINT}"
export PIPELINE_DATA="$SHARED/parquet"

python3 pipeline/parquet_writer.py &                                   # txn:events -> Parquet (system of record)
python3 pipeline/fast_loop.py &                                        # confirmed fraud -> bl:* (live containment)
python3 pipeline/chargeback_feed.py &                                  # ground-truth labels -> Redis + labels Parquet
python3 pipeline/retrain_loop.py --audit "$SHARED/parquet/transactions" \
  --labels "$SHARED/parquet/labels" --model-out "$SHARED/models" --interval 900 &   # retrain -> ONNX -> model:invalidate

# Aggregator + dashboard in the foreground (keeps the VM's service alive). Internal only —
# reach it over an IAP tunnel; do not expose :8090 publicly.
exec python3 -m uvicorn --app-dir pipeline aggregator:create_app --factory --host 0.0.0.0 --port 8090
