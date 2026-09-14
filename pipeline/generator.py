"""D1/D2 — Parameterised feedback-loop simulation generator (design doc §13.6).

Produces the labelled datasets that prove the loop *improves over time*: a warm-start
round, K streaming rounds, and a fixed holdout, drawn from a stationary mixture of
three processes:

  * normal legit        — low on every fraud signal;
  * look-alike legit    — trips the BLUNT rule (shared device) but normal on the joint
                          signature → these are the false positives a naive rule makes;
  * fraud ring          — a JOINT signature (device fan-out × surge × velocity × new-payee
                          × mule fan-in), reusing farm devices / mule payees across rounds.

The joint signature is learnable (high full-feature AUC) while the blunt feature alone
is weak (lower AUC, poor precision) — so a model trained on enough labels can raise recall
AND cut false positives (§8.4.4). Emits per-transaction FEATURE VECTORS + ground-truth
labels (the offline Track-A path trains on these; the engine mapping comes in Phase 4).

Deterministic given --seed. `verify()` asserts the required data properties (D2).
"""
from __future__ import annotations

import argparse
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Feature names mirror the engine's SignalNames / rule inputs so the Phase-4 mapping is direct.
FEATURES = [
    "velocity_ratio_1h", "customer_txn_rate_5m", "customer_declines_90d",
    "customer_distinct_bene_90d", "bene_distinct_senders_90d", "amount_zscore_90d",
    "device_distinct_customers", "dormancy_days", "device_surge", "bene_surge",
    "new_payee", "amount_base", "local_hour", "account_age_days",
]
BLUNT_FEATURE = "device_distinct_customers"   # the crude rule `device_distinct_customers > 2`
BLUNT_THRESHOLD = 2

PRESETS = {
    # laptop / CI — small but enough fraud per round for stable metrics
    "small": dict(customers=5_000, benes=1_000, mules=50, devices=4_000, farm=15, tpps=5,
                  warmup_txns=20_000, rounds=10, txns_per_round=10_000, holdout_txns=20_000,
                  fraud_rate=0.01, lookalike_rate=0.08),
    # distributed / prod-scale — file generator sizes population+backfill+holdout; the
    # 1,000 tx/s streaming is driven by Gatling (Phase 5), not this file emitter.
    "large": dict(customers=5_000_000, benes=1_000_000, mules=20_000, devices=7_500_000, farm=3_000, tpps=200,
                  warmup_txns=2_000_000, rounds=10, txns_per_round=200_000, holdout_txns=500_000,
                  fraud_rate=0.01, lookalike_rate=0.08),
}

DAY_MS = 86_400_000
BASE_TS = 1_760_000_000_000   # arbitrary fixed epoch so runs are reproducible


# ---------------------------------------------------------------------------
# per-process feature samplers (vectorised)
# ---------------------------------------------------------------------------
def _legit(rng: np.random.Generator, n: int) -> dict:
    return dict(
        velocity_ratio_1h=rng.lognormal(0.0, 0.35, n),
        customer_txn_rate_5m=rng.poisson(0.5, n).astype(float),
        customer_declines_90d=rng.poisson(0.3, n).astype(float),
        customer_distinct_bene_90d=rng.poisson(4, n).astype(float) + 1,
        bene_distinct_senders_90d=rng.poisson(3, n).astype(float) + 1,
        amount_zscore_90d=rng.normal(0, 1, n),
        device_distinct_customers=rng.choice([1, 2], n, p=[0.85, 0.15]).astype(float),
        dormancy_days=rng.exponential(5, n),
        device_surge=rng.lognormal(0.0, 0.3, n),
        bene_surge=rng.lognormal(0.0, 0.3, n),
        new_payee=(rng.random(n) < 0.10).astype(float),
        amount_base=rng.lognormal(4.8, 0.6, n),
        local_hour=rng.integers(7, 23, n).astype(float),
        account_age_days=rng.integers(200, 3000, n).astype(float),
    )


def _lookalike(rng: np.random.Generator, n: int) -> dict:
    """Legit, but on a shared family device → trips the blunt rule; normal on the joint."""
    f = _legit(rng, n)
    f["device_distinct_customers"] = rng.integers(3, 7, n).astype(float)   # 3–6 shared device
    return f


def _fraud(rng: np.random.Generator, n: int) -> dict:
    """Fraud via a JOINT signature, deliberately overlapping legit so it is learnable but
    not trivial. ~70% is STEALTH (low device-sharing) so the blunt device rule misses it —
    the recall the model has to earn; ~30% is device-farm (trips the blunt rule)."""
    stealth = rng.random(n) < 0.70
    device = np.where(
        stealth,
        rng.choice([1, 2, 3], n, p=[0.55, 0.30, 0.15]).astype(float),  # stealth: mostly ≤2 → blunt rule misses
        rng.integers(6, 40, n).astype(float),                          # farm: trips the blunt rule
    )
    fan_in = np.where(stealth, rng.integers(3, 12, n), rng.integers(15, 45, n)).astype(float)
    # ~35% of fraud is "hard": drawn almost like legit (only faintly shifted), giving irreducible
    # overlap so the Bayes-optimal AUC is <1 and the model must earn recall with more labels.
    hard = rng.random(n) < 0.35
    shift = np.where(hard, 0.30, 1.0)   # hard fraud barely shifted
    return dict(
        velocity_ratio_1h=rng.lognormal(0.35 * shift, 0.45, n),        # overlaps legit lognormal(0,0.35)
        customer_txn_rate_5m=rng.poisson(1.2, n).astype(float),
        customer_declines_90d=rng.poisson(0.6, n).astype(float),
        customer_distinct_bene_90d=rng.poisson(6, n).astype(float) + 1,
        bene_distinct_senders_90d=fan_in,
        amount_zscore_90d=rng.normal(0.6 * shift, 1.1, n),             # overlaps legit Normal(0,1)
        device_distinct_customers=device,
        dormancy_days=np.where(rng.random(n) < 0.2, rng.uniform(70, 200, n), rng.exponential(5, n)),
        device_surge=rng.lognormal(0.5 * shift, 0.4, n),               # overlaps legit lognormal(0,0.3)
        bene_surge=rng.lognormal(0.5 * shift, 0.4, n),
        new_payee=(rng.random(n) < np.where(hard, 0.12, 0.35)).astype(float),
        amount_base=rng.lognormal(4.8 + 0.4 * shift, 0.65, n),
        local_hour=rng.integers(0, 24, n).astype(float),
        account_age_days=rng.integers(10, 3000, n).astype(float),
    )


# ---------------------------------------------------------------------------
# assembly
# ---------------------------------------------------------------------------
def _mix(rng, n, fraud_rate, lookalike_rate):
    n_fraud = int(round(n * fraud_rate))
    n_look = int(round(n * lookalike_rate))
    n_legit = n - n_fraud - n_look
    parts, labels, kinds = [], [], []
    for sampler, cnt, lab, kind in [
        (_legit, n_legit, 0, "legit"), (_lookalike, n_look, 0, "lookalike"), (_fraud, n_fraud, 1, "fraud")
    ]:
        if cnt <= 0:
            continue
        parts.append(sampler(rng, cnt))
        labels.append(np.full(cnt, lab, dtype=np.int8))
        kinds.append(np.full(cnt, kind))
    feats = {k: np.concatenate([p[k] for p in parts]) for k in FEATURES}
    label = np.concatenate(labels)
    kind = np.concatenate(kinds)
    order = rng.permutation(len(label))
    return {k: v[order] for k, v in feats.items()}, label[order], kind[order]


def _assign_entities(rng, kind, cfg):
    """Assign customer/bene/device/tpp ids; fraud reuses a rotating ring of farm devices + mules."""
    n = len(kind)
    cust = np.array([f"cust_{i:07d}" for i in rng.integers(0, cfg["customers"], n)])
    tpp = np.array([f"tpp_{i}" for i in rng.integers(0, cfg["tpps"], n)])
    bene = np.array([f"bene_{i:07d}" for i in rng.integers(cfg["mules"], cfg["benes"], n)])
    dev = np.array([f"device_{i:07d}" for i in rng.integers(cfg["farm"], cfg["devices"], n)])
    fmask = kind == "fraud"
    bene[fmask] = [f"mule_{i:05d}" for i in rng.integers(0, cfg["mules"], fmask.sum())]
    dev[fmask] = [f"farm_{i:05d}" for i in rng.integers(0, cfg["farm"], fmask.sum())]
    lmask = kind == "lookalike"
    dev[lmask] = [f"shared_{i:05d}" for i in rng.integers(0, max(1, cfg["farm"]), lmask.sum())]
    return cust, bene, dev, tpp


def _table(feats, label, kind, cust, bene, dev, tpp, ts, round_idx):
    cols = {f: pa.array(feats[f], type=pa.float64()) for f in FEATURES}
    cols.update(
        transaction_id=pa.array([f"sim_{round_idx}_{i}" for i in range(len(label))]),
        customer_id=pa.array(cust), receiver_account=pa.array(bene),
        device_fingerprint=pa.array(dev), tpp_name_ud=pa.array(tpp),
        timestamp_epoch_ms=pa.array(ts, type=pa.int64()),
        label=pa.array(label, type=pa.int8()), kind=pa.array(kind),
        round=pa.array(np.full(len(label), round_idx), type=pa.int32()),
    )
    return pa.table(cols)


def generate(scale: str, seed: int, out_dir: str) -> dict:
    cfg = PRESETS[scale]
    rng = np.random.default_rng(seed)
    os.makedirs(out_dir, exist_ok=True)
    manifest = dict(scale=scale, seed=seed, features=FEATURES, blunt_feature=BLUNT_FEATURE,
                    blunt_threshold=BLUNT_THRESHOLD, params=cfg, counts={})

    def emit(name, n, round_idx, ts_base):
        feats, label, kind = _mix(rng, n, cfg["fraud_rate"], cfg["lookalike_rate"])
        cust, bene, dev, tpp = _assign_entities(rng, kind, cfg)
        ts = ts_base + rng.integers(0, DAY_MS, n)
        pq.write_table(_table(feats, label, kind, cust, bene, dev, tpp, ts, round_idx),
                       os.path.join(out_dir, f"{name}.parquet"))
        manifest["counts"][name] = dict(rows=int(n), fraud=int(label.sum()))

    emit("warmup", cfg["warmup_txns"], 0, BASE_TS)
    os.makedirs(os.path.join(out_dir, "rounds"), exist_ok=True)
    for r in range(1, cfg["rounds"] + 1):
        emit(os.path.join("rounds", f"round_{r:02d}"), cfg["txns_per_round"], r, BASE_TS + r * DAY_MS)
    # holdout timestamped AFTER all training rounds (respects train-past / test-future)
    emit("holdout", cfg["holdout_txns"], cfg["rounds"] + 1, BASE_TS + (cfg["rounds"] + 2) * DAY_MS)

    with open(os.path.join(out_dir, "manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2, default=str)
    return manifest


# ---------------------------------------------------------------------------
# D2 — self-checks on the required data properties
# ---------------------------------------------------------------------------
def _auc(scores: np.ndarray, labels: np.ndarray) -> float:
    """Rank-based ROC-AUC (Mann–Whitney), no sklearn dependency."""
    order = np.argsort(scores, kind="mergesort")
    ranks = np.empty(len(scores), dtype=float)
    ranks[order] = np.arange(1, len(scores) + 1)
    pos = labels == 1
    n_pos, n_neg = int(pos.sum()), int((~pos).sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    return (ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def _zscore(a):
    s = a.std()
    return (a - a.mean()) / s if s > 0 else a * 0.0


def verify(out_dir: str) -> dict:
    """Assert the properties that make an improving learning curve possible (D2)."""
    hold = pq.read_table(os.path.join(out_dir, "holdout.parquet"))
    label = hold.column("label").to_numpy()
    f = {name: hold.column(name).to_numpy() for name in FEATURES}
    kind = np.array(hold.column("kind").to_pylist())

    # joint signature (what a well-trained model can use) vs the blunt feature alone
    joint = sum(_zscore(f[k]) for k in
                ["velocity_ratio_1h", "device_surge", "bene_surge",
                 "bene_distinct_senders_90d", "new_payee", "device_distinct_customers"])
    joint_auc = float(_auc(joint, label))
    blunt_auc = float(_auc(f[BLUNT_FEATURE], label))

    # blunt rule quality: fires on look-alikes (false positives) and misses fraud
    fires = f[BLUNT_FEATURE] > BLUNT_THRESHOLD
    blunt_precision = float(label[fires].mean()) if fires.any() else float("nan")
    blunt_recall = float(fires[label == 1].mean())
    lookalike_frac_of_legit = float((kind == "lookalike").sum() / max(1, (label == 0).sum()))

    checks = {
        "fraud_rate": float(label.mean()),
        "joint_auc": joint_auc,
        "blunt_only_auc": blunt_auc,
        "blunt_rule_precision": blunt_precision,
        "blunt_rule_recall": blunt_recall,
        "lookalike_frac_of_legit": lookalike_frac_of_legit,
    }
    checks["assertions"] = {
        "imbalance_realistic": bool(0.003 <= checks["fraud_rate"] <= 0.03),
        "joint_learnable": bool(0.80 <= joint_auc <= 0.985),       # present, but not trivially separable (room to learn)
        "joint_beats_blunt": bool(joint_auc - blunt_auc > 0.08),   # the joint is materially better than the blunt feature
        "blunt_rule_imprecise": bool(blunt_precision < 0.5),       # blunt rule false-positives (look-alikes)
        "blunt_rule_incomplete": bool(blunt_recall < 0.85),        # blunt rule MISSES a real chunk of fraud (stealth)
        "lookalikes_exist": bool(lookalike_frac_of_legit > 0.02),
    }
    checks["ok"] = bool(all(checks["assertions"].values()))
    return checks


def main():  # pragma: no cover
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=list(PRESETS), default="small")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None)
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    out = a.out or os.path.join(os.path.dirname(__file__), "data", "sim", a.scale)
    m = generate(a.scale, a.seed, out)
    print(f"[generator] scale={a.scale} seed={a.seed} -> {out}")
    print(json.dumps(m["counts"], indent=2))
    if a.verify:
        v = verify(out)
        print(json.dumps(v, indent=2))
        if not v["ok"]:
            raise SystemExit("data self-checks FAILED")


if __name__ == "__main__":  # pragma: no cover
    main()
