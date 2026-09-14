"""Slow-loop retrain driver (design doc §8.4.2, Phase 5).

The production feedback loop: periodically join the engine's audit Parquet (each row carries
the point-in-time `feature_snapshot_json` the engine scored) with the matured labels, train a
fresh Track-A model, export it to ONNX at the shared model path, and publish `model:invalidate`
so every engine hot-reloads it — no redeploy. This is what replaces the demo orchestrator's
pre-baked promotion at scale: the engine learns from its *own* scored traffic + confirmed
outcomes.

Pure helpers (`features_from_snapshot`, `build_training_set`) are unit-tested; `retrain_once`
/ `run` are the runtime.
"""
from __future__ import annotations

import json
import os
import sys
import time

import glob

import numpy as np
import pyarrow.dataset as ds

sys.path.insert(0, os.path.dirname(__file__))
from common import LABELS_PARQUET, TXN_PARQUET, redis_client
from export_onnx import export_trained
from generator import FEATURES
from labels import CONFIRMED_FRAUD, matured
from model import train

MODEL_INVALIDATE = "model:invalidate"
DEFAULT_MATURATION_MS = 0          # demo: labels usable immediately; prod: 60–90 days (§8.4.1)
MIN_POSITIVES = 20                 # don't retrain until enough confirmed fraud has accrued
SAMPLE_SEED = 42


def features_from_snapshot(js: str) -> list[float]:
    """Extract the model's ordered feature vector from the engine's feature_snapshot JSON."""
    d = json.loads(js) if js else {}
    out = []
    for f in FEATURES:
        v = d.get(f)
        out.append(float(v) if isinstance(v, (int, float)) else 0.0)
    return out


def _has_parquet(d: str) -> bool:
    return bool(glob.glob(os.path.join(d, "**", "*.parquet"), recursive=True))


def build_training_set(audit_dir: str, labels_dir: str, now_ms: int,
                       maturation_ms: int = DEFAULT_MATURATION_MS,
                       sample_cap: int | None = None, use_duckdb: bool = True):
    """Join matured labels with the audit feature snapshots into (X, y). Point-in-time: the
    snapshot is exactly what the engine scored, so no train/serve skew and no leakage.

    Default path pushes the join + maturation filter + feature extraction + reservoir sample
    into DuckDB (streams from Parquet/GCS, out-of-core, bounded RAM); the pyarrow path is kept
    for parity/fallback.
    """
    if not _has_parquet(audit_dir) or not _has_parquet(labels_dir):
        return np.empty((0, len(FEATURES))), np.empty((0,), dtype=int)
    if use_duckdb:
        return _build_duckdb(audit_dir, labels_dir, now_ms, maturation_ms, sample_cap)
    return _build_pyarrow(audit_dir, labels_dir, now_ms, maturation_ms)


def _build_duckdb(audit_dir, labels_dir, now_ms, maturation_ms, sample_cap):
    import duckdb
    feat_cols = ",\n  ".join(
        f"COALESCE(TRY_CAST(json_extract_string(a.feature_snapshot_json, '$.{f}') AS DOUBLE), 0.0) AS \"{f}\""
        for f in FEATURES)
    audit_glob = os.path.join(audit_dir, "dt=*", "*.parquet")
    labels_glob = os.path.join(labels_dir, "*.parquet")
    sample = f"USING SAMPLE {int(sample_cap)} ROWS (reservoir, {SAMPLE_SEED})" if sample_cap else ""
    sql = f"""
        SELECT {feat_cols},
               CASE WHEN l.label = 'confirmed_fraud' THEN 1 ELSE 0 END AS y
        FROM read_parquet('{audit_glob}', hive_partitioning=1) a
        JOIN read_parquet('{labels_glob}') l USING (transaction_id)
        WHERE (l.label = 'confirmed_fraud' AND l.labeled_at_ms <= {int(now_ms)})
           OR (l.label = 'confirmed_legit' AND l.txn_ts_ms + {int(maturation_ms)} <= {int(now_ms)})
        {sample}
    """
    tbl = duckdb.sql(sql).fetch_arrow_table()
    if tbl.num_rows == 0:
        return np.empty((0, len(FEATURES))), np.empty((0,), dtype=int)
    X = np.column_stack([tbl.column(f).to_numpy(zero_copy_only=False) for f in FEATURES]).astype(float)
    y = tbl.column("y").to_numpy(zero_copy_only=False).astype(int)
    return X, y


def _build_pyarrow(audit_dir, labels_dir, now_ms, maturation_ms):
    labs = ds.dataset(labels_dir).to_table().to_pylist()
    label_of = {r["transaction_id"]: (1 if r["label"] == CONFIRMED_FRAUD else 0)
                for r in matured(labs, now_ms, maturation_ms)}
    if not label_of:
        return np.empty((0, len(FEATURES))), np.empty((0,), dtype=int)
    aud = ds.dataset(audit_dir, partitioning="hive").to_table(
        columns=["transaction_id", "feature_snapshot_json"]).to_pylist()
    X, y = [], []
    for row in aud:
        tid = row["transaction_id"]
        if tid in label_of:
            X.append(features_from_snapshot(row["feature_snapshot_json"]))
            y.append(label_of[tid])
    return np.array(X, dtype=float), np.array(y, dtype=int)


def retrain_once(audit_dir: str, labels_dir: str, model_out_dir: str, name: str, version: str,
                 r=None, now_ms: int | None = None, maturation_ms: int = DEFAULT_MATURATION_MS,
                 min_positives: int = MIN_POSITIVES, sample_cap: int | None = 1_000_000) -> dict:
    now_ms = now_ms or int(time.time() * 1000)
    X, y = build_training_set(audit_dir, labels_dir, now_ms, maturation_ms, sample_cap=sample_cap)
    pos = int(y.sum()) if len(y) else 0
    if pos < min_positives:
        return {"skipped": True, "n": int(len(y)), "pos": pos}
    export_trained(train(X, y), model_out_dir, name, version,
                   extra={"n_train": int(len(y)), "pos": pos, "trained_at_ms": now_ms})
    if r is not None:
        r.publish(MODEL_INVALIDATE, "reload")     # every engine hot-reloads (§8.4.3)
    return {"skipped": False, "n": int(len(y)), "pos": pos, "version": version}


def run(audit_dir: str = TXN_PARQUET, labels_dir: str = LABELS_PARQUET,
        model_out_dir: str = "model", name: str = "fraud-model",
        interval_sec: int = 600, maturation_ms: int = DEFAULT_MATURATION_MS):  # pragma: no cover
    r = redis_client()
    n = 0
    while True:
        n += 1
        res = retrain_once(audit_dir, labels_dir, model_out_dir, name, f"fb-r{n}", r,
                           maturation_ms=maturation_ms)
        print(f"[retrain_loop] round {n}: {res}")
        time.sleep(interval_sec)


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", default=TXN_PARQUET)
    ap.add_argument("--labels", default=LABELS_PARQUET)
    ap.add_argument("--model-out", default="model")
    ap.add_argument("--interval", type=int, default=600)
    a = ap.parse_args()
    run(a.audit, a.labels, a.model_out, interval_sec=a.interval)
