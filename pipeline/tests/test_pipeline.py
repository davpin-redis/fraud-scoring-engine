"""Phase 0 component tests (design doc §8.4.1) — pure transforms, no live Redis."""
import json
import os
import sys

import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common import TXN_FIELDS, dt_of  # noqa: E402
import parquet_writer as pw  # noqa: E402
import labels as lb  # noqa: E402

SCORED = {
    "transaction_id": "t1", "customer_id": "cust_001", "receiver_account": "bene_010",
    "decision": "approve", "model_version": "seed-v0", "final_score": 0.12, "model_score": None,
    "amount": 100.0, "amount_base": 100.0, "currency": "GBP", "customer_portfolio_country": "GB",
    "timestamp_epoch_ms": 1_760_000_000_000, "device_fingerprint": "device_x",
    "tpp_name_ud": "tpp_Alpha", "rules_fired": ["R017_velocity_burst_5m"],
    "feature_snapshot_json": "{\"customer_txn_rate_5m\":2}",
}


def test_decode_message_from_v_field():
    rec = pw.decode_message({"v": json.dumps(SCORED)})
    assert rec["transaction_id"] == "t1"
    assert rec["decision"] == "approve"
    assert rec["rules_fired"] == ["R017_velocity_burst_5m"]
    assert rec["dt"] == dt_of(SCORED["timestamp_epoch_ms"])   # partition derived from txn time
    assert set(rec.keys()) == set(TXN_FIELDS)


def test_decode_message_fills_missing_and_null_rules():
    partial = {k: SCORED[k] for k in ("transaction_id", "decision", "timestamp_epoch_ms")}
    rec = pw.decode_message({"v": json.dumps(partial)})
    assert rec["customer_id"] is None          # missing filled with None
    assert rec["rules_fired"] == []            # null rules -> empty list


def test_records_to_table_schema_and_partition_write(tmp_path):
    recs = [pw.decode_message({"v": json.dumps(SCORED)}) for _ in range(3)]
    table = pw.records_to_table(recs)
    assert table.num_rows == 3
    assert table.schema.field("rules_fired").type.value_type.equals(table.schema.field("rules_fired").type.value_type)
    n = pw.write_batch(recs, base_dir=str(tmp_path))
    assert n == 3
    dt = dt_of(SCORED["timestamp_epoch_ms"])
    part_dir = tmp_path / f"dt={dt}"
    assert part_dir.exists()
    # one buffered flush -> exactly one file for the partition (not one per record), and no .tmp left
    files = list(part_dir.glob("*.parquet"))
    assert len(files) == 1
    assert not list(part_dir.glob("*.tmp"))
    back = pq.read_table(str(part_dir))
    assert back.num_rows == 3
    assert back.column("transaction_id").to_pylist() == ["t1", "t1", "t1"]


def test_should_flush_thresholds():
    assert pw.should_flush(0, 999, max_rows=100, max_interval_sec=60) is False        # empty never flushes
    assert pw.should_flush(100, 1, max_rows=100, max_interval_sec=60) is True          # row cap hit
    assert pw.should_flush(5, 61, max_rows=100, max_interval_sec=60) is True           # aged out (non-empty)
    assert pw.should_flush(5, 1, max_rows=100, max_interval_sec=60) is False           # below both


def test_label_maturation_fraud_immediate_legit_gated():
    now = 1_000_000_000_000
    mat = 60 * 24 * 3600 * 1000   # 60 days
    fraud = lb.label_record("t1", lb.CONFIRMED_FRAUD, "chargeback", txn_ts_ms=now - 1000, labeled_at_ms=now - 500)
    legit_recent = lb.label_record("t2", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=now - 1000, labeled_at_ms=now - 500)
    legit_old = lb.label_record("t3", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=now - mat - 1000, labeled_at_ms=now - 500)

    assert lb.is_matured(fraud, now, mat) is True             # fraud usable immediately
    assert lb.is_matured(legit_recent, now, mat) is False     # legit not yet matured
    assert lb.is_matured(legit_old, now, mat) is True         # legit past the window

    out = lb.matured([fraud, legit_recent, legit_old], now, mat)
    assert {r["transaction_id"] for r in out} == {"t1", "t3"}


def test_persist_labels_roundtrip(tmp_path):
    recs = [
        lb.label_record("t1", lb.CONFIRMED_FRAUD, "chargeback", txn_ts_ms=1, labeled_at_ms=2),
        lb.label_record("t2", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=1, labeled_at_ms=2),
    ]
    n = lb.persist_labels(recs, base_dir=str(tmp_path))
    assert n == 2
    back = pq.read_table(str(tmp_path))
    assert set(back.column("label").to_pylist()) == {lb.CONFIRMED_FRAUD, lb.CONFIRMED_LEGIT}
