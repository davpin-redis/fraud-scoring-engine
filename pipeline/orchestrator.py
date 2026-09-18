"""D4-small — round orchestrator (design doc §13.6, §10.3).

Stands in for the engine fleet on the laptop: streams transactions, submits their
(ground-truth) labels to the label store, scores each under two model versions —
`rules-v0` (the pre-ML blunt device rule = champion) and `fb-model` (the Track-A
feedback model = challenger) — and XADDs a decision event per version to `txn:events`.
The metrics aggregator consumes those + the labels and serves the live dashboard.

On `apply-feedback` (dashboard button → aggregator sets the feedback key), the challenger
model is promoted from the label-starved initial round to the fully-accrued round, so the
dashboard shows recall step up and false positives step down. In Phase 4 the engine
replaces this orchestrator; the aggregator and dashboard are unchanged.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
import aggregator as agg
import dataset
import fast_loop as fl
from common import LABEL_KEY_PREFIX, LABELS_STREAM, TXN_STREAM, redis_client
from labels import CONFIRMED_FRAUD, CONFIRMED_LEGIT
from model import predict, train

BUDGET = 0.02
CHAMP_VER = "rules-v0"      # pre-ML blunt rule
CHALL_VER = "fb-model"      # Track-A feedback model


def _train_challenger(sim_dir, upto_round, Xh, seed=0):
    X, y, _ = dataset.rounds_upto(sim_dir, upto_round, include_warmup=False)
    m = train(X, y, seed=seed)
    thr = float(np.quantile(predict(m, Xh), 1.0 - BUDGET))
    return m, thr


def setup(r, sim_dir, initial_round=1):
    Xh, yh, amh = dataset.holdout(sim_dir)
    ent = dataset.holdout_entities(sim_dir)
    dev = Xh[:, dataset.FEATURES.index("device_distinct_customers")]
    r.set(agg.CHAMPION_KEY, CHAMP_VER)
    r.set(agg.CHALLENGER_KEY, CHALL_VER)
    r.delete(agg.FEEDBACK_KEY)
    inst = f"orch-{uuid.uuid4().hex[:6]}"
    r.sadd(agg.INSTANCES_KEY, inst)
    model, thr = _train_challenger(sim_dir, initial_round, Xh)
    return dict(sim_dir=sim_dir, Xh=Xh, yh=yh, amh=amh, dev=dev,
                recv=ent["receiver_account"], devfp=ent["device_fingerprint"],
                model=model, thr=thr, promoted=False, rng=np.random.default_rng(0))


def apply_blacklist(decision, receiver, device, bl_accts, bl_devs):
    """Hard-block overlay simulating engine rules R001/R002: a blacklisted payee or device
    is declined regardless of the model/rule score. This is how the fast loop's newly-listed
    entities contain repeat offenders live."""
    if receiver in bl_accts or device in bl_devs:
        return "decline"
    return decision


def maybe_promote(r, st, promoted_round=10):
    if not st["promoted"] and r.get(agg.FEEDBACK_KEY):
        st["model"], st["thr"] = _train_challenger(st["sim_dir"], promoted_round, st["Xh"])
        st["promoted"] = True
        print(f"[orchestrator] feedback applied -> challenger promoted to round {promoted_round}")


def one_tick(r, st, batch=200):
    idx = st["rng"].integers(0, len(st["yh"]), batch)
    scores = predict(st["model"], st["Xh"][idx])
    # snapshot the (small) blacklists once per tick; the fast loop grows them out of band
    bl_accts = r.smembers(fl.BL_ACCOUNTS)
    bl_devs = r.smembers(fl.BL_DEVICES)
    pipe = r.pipeline()
    for j, i in enumerate(idx):
        tid = f"live_{uuid.uuid4().hex[:12]}"
        is_fraud = st["yh"][i] == 1
        recv, device = st["recv"][i], st["devfp"][i]
        pipe.set(LABEL_KEY_PREFIX + tid, json.dumps(
            {"transaction_id": tid, "label": CONFIRMED_FRAUD if is_fraud else CONFIRMED_LEGIT}))
        # feed the fast loop: confirmed fraud + implicated entities -> labels:events
        if is_fraud:
            pipe.xadd(LABELS_STREAM, {"v": json.dumps(
                {"transaction_id": tid, "label": CONFIRMED_FRAUD, "source": "simulation",
                 "receiver_account": recv, "device_fingerprint": device})},
                maxlen=200_000, approximate=True)
        amt = float(st["amh"][i])
        champ = "decline" if st["dev"][i] > 2 else "approve"          # blunt rule
        chall = agg.decision_at(float(scores[j]), st["thr"])          # feedback model @ fixed budget
        # engine rules R001/R002 (blacklist) apply to both paths
        champ = apply_blacklist(champ, recv, device, bl_accts, bl_devs)
        chall = apply_blacklist(chall, recv, device, bl_accts, bl_devs)
        for ver, dec in ((CHAMP_VER, champ), (CHALL_VER, chall)):
            pipe.xadd(TXN_STREAM, {"v": json.dumps(
                {"transaction_id": tid, "model_version": ver, "decision": dec, "amount_base": amt})},
                maxlen=1_000_000, approximate=True)
    pipe.execute()
    return batch


def run(sim_dir, tps=400, tick=0.5, initial_round=1, promoted_round=10, max_ticks=None):  # pragma: no cover
    r = redis_client()
    st = setup(r, sim_dir, initial_round)
    print(f"[orchestrator] streaming ~{tps} tx/s; challenger=round {initial_round} (promotes to {promoted_round} on feedback)")
    batch = max(1, int(tps * tick))
    t = 0
    while max_ticks is None or t < max_ticks:
        maybe_promote(r, st, promoted_round)
        one_tick(r, st, batch)
        time.sleep(tick)
        t += 1


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--sim", required=True)
    ap.add_argument("--tps", type=int, default=400)
    a = ap.parse_args()
    run(a.sim, tps=a.tps)
