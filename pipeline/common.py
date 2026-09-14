"""Shared config, Redis connection, and the audit/label record schema for the
feedback-loop data pipeline (design doc §8.4.1). Pure helpers live here so the
Parquet writer and label store share one definition and can be unit-tested
without a live Redis."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pyarrow as pa

# --- config (env-overridable) ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
DATA_DIR = os.environ.get("PIPELINE_DATA", os.path.join(os.path.dirname(__file__), "data"))
TXN_STREAM = os.environ.get("TXN_STREAM", "txn:events")          # engine XADDs scored txns here (§8.4.1)
TXN_PARQUET = os.path.join(DATA_DIR, "transactions")             # system of record (partitioned by dt)
LABELS_STREAM = os.environ.get("LABELS_STREAM", "labels:events")  # online label feed
LABELS_PARQUET = os.path.join(DATA_DIR, "labels")
LABEL_KEY_PREFIX = "label:"                                      # SET label:{txn_id} -> json (online copy)

# Consumer-group names (each is an independent §8.4.1 consumer).
GROUP_PARQUET = "parquet-writer"

# --- scored-transaction schema (matches the engine's snake_case ScoredTransaction JSON) ---
TXN_SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("customer_id", pa.string()),
    ("receiver_account", pa.string()),
    ("decision", pa.string()),
    ("model_version", pa.string()),
    ("final_score", pa.float64()),
    ("model_score", pa.float64()),          # nullable (null when model disabled)
    ("amount", pa.float64()),
    ("amount_base", pa.float64()),
    ("currency", pa.string()),
    ("customer_portfolio_country", pa.string()),
    ("timestamp_epoch_ms", pa.int64()),
    ("device_fingerprint", pa.string()),
    ("tpp_name_ud", pa.string()),
    ("rules_fired", pa.list_(pa.string())),
    ("feature_snapshot_json", pa.string()),
    ("dt", pa.string()),                     # partition column: UTC date of the transaction
])

TXN_FIELDS = [f.name for f in TXN_SCHEMA]

# --- label schema ---
LABEL_SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("label", pa.string()),                  # confirmed_fraud | confirmed_legit
    ("source", pa.string()),                 # chargeback | manual_review | simulation
    ("labeled_at_ms", pa.int64()),
    ("txn_ts_ms", pa.int64()),               # original scoring time (for maturation of legit-by-timeout)
])
LABEL_FIELDS = [f.name for f in LABEL_SCHEMA]


def dt_of(epoch_ms: int) -> str:
    """UTC date string used as the Parquet partition key."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def redis_client():
    """Lazily create a redis client (imported here so pure helpers don't require redis)."""
    import redis
    return redis.from_url(REDIS_URL, decode_responses=True)
