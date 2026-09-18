"""Phase 5 — slow-loop retrain driver tests (design doc §8.4.2): the engine's audit snapshots
+ matured labels train a model and export a fresh ONNX artifact. No Redis/engine required."""
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import labels as lb  # noqa: E402
import parquet_writer as pw  # noqa: E402
import retrain_loop as rl  # noqa: E402
from generator import FEATURES  # noqa: E402


def _audit_record(tid, snapshot):
    rec = {f: None for f in __import__("common").TXN_FIELDS}
    rec.update(transaction_id=tid, decision="approve", model_version="rules-v0",
               timestamp_epoch_ms=1_760_000_000_000, amount_base=100.0,
               feature_snapshot_json=json.dumps(snapshot), rules_fired=[])
    rec["dt"] = "2025-10-09"
    return rec


def test_features_from_snapshot_orders_and_defaults():
    snap = {"velocity_ratio_1h": 3.0, "device_surge": 5.0, "unused_key": 9}
    vec = rl.features_from_snapshot(json.dumps(snap))
    assert len(vec) == len(FEATURES)
    assert vec[FEATURES.index("velocity_ratio_1h")] == 3.0
    assert vec[FEATURES.index("device_surge")] == 5.0
    assert vec[FEATURES.index("amount_base")] == 0.0     # missing -> 0


def _fraud_snap(i):
    return {"velocity_ratio_1h": 6 + i % 3, "device_surge": 8, "bene_surge": 8,
            "bene_distinct_senders_90d": 40, "new_payee": 1, "device_distinct_customers": 20,
            "amount_zscore_90d": 3, "customer_txn_rate_5m": 6, "amount_base": 800}


def _legit_snap():
    return {"velocity_ratio_1h": 1, "device_surge": 1, "bene_surge": 1,
            "bene_distinct_senders_90d": 3, "new_payee": 0, "device_distinct_customers": 1,
            "amount_zscore_90d": 0, "amount_base": 100, "account_age_days": 1500, "local_hour": 12}


def _write_join_fixture(tmp_path):
    audit = str(tmp_path / "audit")
    labels = str(tmp_path / "labels")
    recs, labrecs = [], []
    for i in range(60):
        recs.append(_audit_record(f"f{i}", _fraud_snap(i)))
        labrecs.append(lb.label_record(f"f{i}", lb.CONFIRMED_FRAUD, "chargeback", txn_ts_ms=1, labeled_at_ms=1))
    for i in range(140):
        recs.append(_audit_record(f"g{i}", _legit_snap()))
        labrecs.append(lb.label_record(f"g{i}", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=1, labeled_at_ms=1))
    pw.write_batch(recs, base_dir=audit)
    lb.persist_labels(labrecs, base_dir=labels)
    return audit, labels


def test_build_training_set_duckdb_joins_labels_and_snapshots(tmp_path):
    audit, labels = _write_join_fixture(tmp_path)
    X, y = rl.build_training_set(audit, labels, now_ms=10**13, maturation_ms=0)  # duckdb path
    assert X.shape == (200, len(FEATURES))
    assert int(y.sum()) == 60
    # a fraud row's velocity_ratio_1h feature survives the JSON extraction
    vr = X[y == 1][:, FEATURES.index("velocity_ratio_1h")]
    assert vr.min() >= 6.0


def test_duckdb_matches_pyarrow(tmp_path):
    audit, labels = _write_join_fixture(tmp_path)
    Xd, yd = rl.build_training_set(audit, labels, 10**13, 0, use_duckdb=True)
    Xp, yp = rl.build_training_set(audit, labels, 10**13, 0, use_duckdb=False)
    assert int(yd.sum()) == int(yp.sum()) == 60
    assert Xd.shape == Xp.shape
    # same rows (order differs) -> equal column sums
    assert np.allclose(np.sort(Xd.sum(axis=0)), np.sort(Xp.sum(axis=0)))


def test_duckdb_sample_cap_bounds_rows(tmp_path):
    audit, labels = _write_join_fixture(tmp_path)
    X, y = rl.build_training_set(audit, labels, 10**13, 0, sample_cap=50)
    assert len(y) == 50 and X.shape == (50, len(FEATURES))


def test_maturation_filter_excludes_unmatured_legit(tmp_path):
    audit, labels = _write_join_fixture(tmp_path)
    # legit labels have txn_ts_ms=1; with a huge maturation window and now just past it,
    # legit are excluded but fraud (immediate) remain.
    X, y = rl.build_training_set(audit, labels, now_ms=100, maturation_ms=10**9)
    assert int(y.sum()) == 60 and len(y) == 60   # only the 60 fraud are matured


def test_retrain_once_exports_onnx_and_skips_when_sparse(tmp_path):
    audit = str(tmp_path / "audit")
    labels = str(tmp_path / "labels")
    out = str(tmp_path / "model")
    recs, labrecs = [], []
    for i in range(60):
        recs.append(_audit_record(f"f{i}", _fraud_snap(i)))
        labrecs.append(lb.label_record(f"f{i}", lb.CONFIRMED_FRAUD, "chargeback", txn_ts_ms=1, labeled_at_ms=1))
    for i in range(140):
        recs.append(_audit_record(f"g{i}", _legit_snap()))
        labrecs.append(lb.label_record(f"g{i}", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=1, labeled_at_ms=1))
    pw.write_batch(recs, base_dir=audit)
    lb.persist_labels(labrecs, base_dir=labels)

    res = rl.retrain_once(audit, labels, out, "fraud-model", "fb-r1", r=None, now_ms=10**13)
    assert res["skipped"] is False and res["pos"] == 60
    assert os.path.exists(os.path.join(out, "fraud-model.onnx"))
    manifest = json.load(open(os.path.join(out, "fraud-model.manifest.json")))
    assert manifest["version"] == "fb-r1" and manifest["features"] == FEATURES

    # too few positives -> skip (don't ship a degenerate model)
    res2 = rl.retrain_once(audit, labels, out, "fraud-model", "fb-r2", r=None, now_ms=10**13, min_positives=1000)
    assert res2["skipped"] is True
