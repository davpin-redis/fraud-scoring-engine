"""A3 — Parquet writer (design doc §8.4.1).

Consumes the bounded `txn:events` Redis Stream as a consumer group and appends
each scored transaction to date-partitioned Parquet, the durable **system of
record**. The Redis Stream is only transport (trimmed by the engine's approximate
MAXLEN); this writer is the single writer to the Parquet store.

Pure transforms (`decode_message`, `records_to_table`) are unit-tested without a
live Redis; `run()` is the runtime consume loop.
"""
from __future__ import annotations

import json
import os
import uuid
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from common import (
    GROUP_PARQUET, TXN_FIELDS, TXN_PARQUET, TXN_SCHEMA, TXN_STREAM, dt_of, redis_client,
)


def decode_message(fields: dict) -> dict:
    """Stream entry body -> a normalised record matching TXN_SCHEMA.

    The engine writes the ScoredTransaction JSON under field 'v'; we also accept a
    flat body for flexibility. Missing columns are filled with None; the partition
    column `dt` is derived from the transaction timestamp.
    """
    obj = json.loads(fields["v"]) if "v" in fields else dict(fields)
    rec = {k: obj.get(k) for k in TXN_FIELDS if k != "dt"}
    ts = rec.get("timestamp_epoch_ms")
    rec["dt"] = dt_of(int(ts)) if ts is not None else "unknown"
    if rec.get("rules_fired") is None:
        rec["rules_fired"] = []
    return rec


def records_to_table(records: list[dict]) -> pa.Table:
    """Normalise decoded records into a single Arrow table with the fixed schema."""
    cols = {name: [] for name in TXN_FIELDS}
    for r in records:
        for name in TXN_FIELDS:
            cols[name].append(r.get(name))
    return pa.table(cols, schema=TXN_SCHEMA)


def write_batch(records: Iterable[dict], base_dir: str = TXN_PARQUET) -> int:
    """Append a batch to the partitioned Parquet dataset. Returns rows written."""
    records = list(records)
    if not records:
        return 0
    table = records_to_table(records)
    keep = [c for c in table.column_names if c != "dt"]   # dt lives in the partition dir, not the file
    # one file per (dt) per batch, unique name -> append-only, no rewrite
    for dt in set(table.column("dt").to_pylist()):
        part = table.filter(pc.equal(table.column("dt"), dt)).select(keep)
        out_dir = os.path.join(base_dir, f"dt={dt}")
        os.makedirs(out_dir, exist_ok=True)
        pq.write_table(part, os.path.join(out_dir, f"part-{uuid.uuid4().hex}.parquet"))
    return len(records)


def run(batch_size: int = 500, block_ms: int = 5000, consumer: str = "w1"):  # pragma: no cover
    """Runtime consume loop: consumer-group read -> Parquet -> XACK."""
    r = redis_client()
    try:
        r.xgroup_create(TXN_STREAM, GROUP_PARQUET, id="0", mkstream=True)
    except Exception:
        pass  # group already exists
    print(f"[parquet_writer] consuming {TXN_STREAM} -> {TXN_PARQUET}")
    while True:
        resp = r.xreadgroup(GROUP_PARQUET, consumer, {TXN_STREAM: ">"}, count=batch_size, block=block_ms)
        if not resp:
            continue
        ids, records = [], []
        for _stream, entries in resp:
            for msg_id, fields in entries:
                ids.append(msg_id)
                records.append(decode_message(fields))
        n = write_batch(records)
        if ids:
            r.xack(TXN_STREAM, GROUP_PARQUET, *ids)
        print(f"[parquet_writer] wrote {n} rows, acked {len(ids)}")


if __name__ == "__main__":  # pragma: no cover
    run()
