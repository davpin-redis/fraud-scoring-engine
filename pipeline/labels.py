"""A4 — Label store + ingestion (design doc §8.4.1).

Confirmed outcomes attach to the original transaction by id. Labels are kept in
two places: an **online copy in Redis** (`SET label:{txn_id}` + `XADD labels:events`)
that the fast reactive loop and the metrics aggregator consume, and an **append to
Parquet** for the training join. A maturation window governs when an
un-charged-back transaction may be treated as legit.

Pure helpers (`label_record`, `is_matured`, `matured`) are unit-tested without a
live Redis; `submit_label` / `persist_labels` are the runtime I/O.
"""
from __future__ import annotations

import json
import os
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from common import (
    LABEL_FIELDS, LABEL_KEY_PREFIX, LABEL_SCHEMA, LABELS_PARQUET, LABELS_STREAM, redis_client,
)

CONFIRMED_FRAUD = "confirmed_fraud"
CONFIRMED_LEGIT = "confirmed_legit"


def label_record(txn_id: str, label: str, source: str, txn_ts_ms: int, labeled_at_ms: int | None = None) -> dict:
    """Build a normalised label record."""
    return {
        "transaction_id": txn_id,
        "label": label,
        "source": source,
        "labeled_at_ms": int(labeled_at_ms if labeled_at_ms is not None else time.time() * 1000),
        "txn_ts_ms": int(txn_ts_ms),
    }


def is_matured(rec: dict, now_ms: int, maturation_ms: int) -> bool:
    """When is a label usable for training?

    - confirmed_fraud: as soon as it is labelled (a chargeback/analyst confirmation).
    - confirmed_legit: only once the maturation window has elapsed since the
      transaction, so we never call a recent transaction 'legit' prematurely.
    """
    if rec["label"] == CONFIRMED_FRAUD:
        return rec["labeled_at_ms"] <= now_ms
    if rec["label"] == CONFIRMED_LEGIT:
        return rec["txn_ts_ms"] + maturation_ms <= now_ms
    return False


def matured(records: list[dict], now_ms: int, maturation_ms: int) -> list[dict]:
    """Filter to labels usable for a training round as of `now_ms`."""
    return [r for r in records if is_matured(r, now_ms, maturation_ms)]


# --- runtime I/O ---

def submit_label(r, txn_id: str, label: str, source: str, txn_ts_ms: int, labeled_at_ms: int | None = None) -> dict:
    """Write the online copy (Redis) for the fast loop + aggregator. Returns the record."""
    rec = label_record(txn_id, label, source, txn_ts_ms, labeled_at_ms)
    r.set(LABEL_KEY_PREFIX + txn_id, json.dumps(rec))
    r.xadd(LABELS_STREAM, {"v": json.dumps(rec)})
    return rec


def persist_labels(records: list[dict], base_dir: str = LABELS_PARQUET) -> int:
    """Append labels to Parquet for the training join. Returns rows written."""
    records = list(records)
    if not records:
        return 0
    cols = {name: [r.get(name) for r in records] for name in LABEL_FIELDS}
    table = pa.table(cols, schema=LABEL_SCHEMA)
    os.makedirs(base_dir, exist_ok=True)
    pq.write_table(table, os.path.join(base_dir, f"part-{uuid.uuid4().hex}.parquet"))
    return len(records)
