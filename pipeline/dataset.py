"""B1 — point-in-time dataset assembly (design doc §8.4 / §13.6).

Loads the generated feature-vector + label Parquet into (X, y, amount) arrays. In the
offline simulation the label is co-located with the feature snapshot, so the
"point-in-time join" is a direct load; the real join of the txn:events feature
snapshots against the label store happens in the engine-in-the-loop path (Phase 4).
Training sets are assembled in round order (train past → test future, never shuffled).
"""
from __future__ import annotations

import os

import numpy as np
import pyarrow.parquet as pq

from generator import FEATURES

AMOUNT_IDX = FEATURES.index("amount_base")


def _load(path: str):
    t = pq.read_table(path)
    X = np.column_stack([t.column(f).to_numpy() for f in FEATURES]).astype(float)
    y = t.column("label").to_numpy().astype(int)
    amount = t.column("amount_base").to_numpy().astype(float)
    return X, y, amount


ENTITY_COLS = ("customer_id", "receiver_account", "device_fingerprint")


def load_entities(path: str) -> dict:
    """Entity id columns aligned row-for-row with _load() (same file, same order)."""
    t = pq.read_table(path)
    return {c: t.column(c).to_pylist() for c in ENTITY_COLS}


def holdout_entities(sim_dir: str) -> dict:
    return load_entities(os.path.join(sim_dir, "holdout.parquet"))


def warmup(sim_dir: str):
    return _load(os.path.join(sim_dir, "warmup.parquet"))


def holdout(sim_dir: str):
    return _load(os.path.join(sim_dir, "holdout.parquet"))


def rounds_upto(sim_dir: str, r: int, include_warmup: bool = True):
    """Cumulative training set through round r (optionally including the warm-start backfill)."""
    parts = [os.path.join(sim_dir, "warmup.parquet")] if include_warmup else []
    parts += [os.path.join(sim_dir, "rounds", f"round_{i:02d}.parquet") for i in range(1, r + 1)]
    Xs, ys, ams = zip(*[_load(p) for p in parts])
    return np.vstack(Xs), np.concatenate(ys), np.concatenate(ams)
