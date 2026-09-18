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


# file rolling: flush when the buffer hits either threshold (design doc §8.4.1, runbook).
MAX_ROWS = 200_000          # ~100–150 MB/file at typical snapshot sizes -> few large files, not many tiny ones
MAX_INTERVAL_SEC = 60


def write_batch(records: Iterable[dict], base_dir: str = TXN_PARQUET) -> int:
    """Write a (large, buffered) batch to the partitioned dataset, one atomic file per dt.
    Atomic = temp file + os.replace, so a reader/DuckDB never sees a half-written file."""
    records = list(records)
    if not records:
        return 0
    table = records_to_table(records)
    keep = [c for c in table.column_names if c != "dt"]   # dt lives in the partition dir, not the file
    for dt in set(table.column("dt").to_pylist()):
        part = table.filter(pc.equal(table.column("dt"), dt)).select(keep)
        out_dir = os.path.join(base_dir, f"dt={dt}")
        os.makedirs(out_dir, exist_ok=True)
        final = os.path.join(out_dir, f"part-{uuid.uuid4().hex}.parquet")
        pq.write_table(part, final + ".tmp")
        os.replace(final + ".tmp", final)                 # atomic publish
    return len(records)


def should_flush(n_buffered: int, seconds_since_flush: float,
                 max_rows: int = MAX_ROWS, max_interval_sec: float = MAX_INTERVAL_SEC) -> bool:
    """Roll a file when the buffer is large enough OR has aged past the interval (and is non-empty)."""
    if n_buffered == 0:
        return False
    return n_buffered >= max_rows or seconds_since_flush >= max_interval_sec


def run(read_count: int = 5000, block_ms: int = 2000, consumer: str = "w1",
        max_rows: int = MAX_ROWS, max_interval_sec: float = MAX_INTERVAL_SEC):  # pragma: no cover
    """Runtime loop: buffer across reads, roll one large atomic file per flush, XACK only after a
    durable flush (so a crash re-delivers un-persisted records — at-least-once)."""
    import time
    r = redis_client()
    try:
        r.xgroup_create(TXN_STREAM, GROUP_PARQUET, id="0", mkstream=True)
    except Exception:
        pass
    print(f"[parquet_writer] consuming {TXN_STREAM} -> {TXN_PARQUET} (roll {max_rows} rows / {max_interval_sec}s)")
    buf, pending_ids, last_flush = [], [], time.monotonic()

    def flush():
        nonlocal buf, pending_ids, last_flush
        if buf:
            n = write_batch(buf)
            r.xack(TXN_STREAM, GROUP_PARQUET, *pending_ids)
            print(f"[parquet_writer] rolled {n} rows -> 1 file/partition, acked {len(pending_ids)}")
            buf, pending_ids, last_flush = [], [], time.monotonic()

    while True:
        resp = r.xreadgroup(GROUP_PARQUET, consumer, {TXN_STREAM: ">"}, count=read_count, block=block_ms)
        for _stream, entries in (resp or []):
            for msg_id, fields in entries:
                pending_ids.append(msg_id)
                buf.append(decode_message(fields))
        if should_flush(len(buf), time.monotonic() - last_flush, max_rows, max_interval_sec):
            flush()


if __name__ == "__main__":  # pragma: no cover
    run()
