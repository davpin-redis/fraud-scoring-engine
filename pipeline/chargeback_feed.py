"""Chargeback / label feed for the large-scale run (design doc §8.4.1, Phase 5).

Stands in for the real chargeback + manual-review label sources: consumes the audit stream
and submits matured labels to the label store, which the retrain loop (slow) and fast loop
consume. Ground truth is inferred from the feedback-loop feeder's entity scheme — fraud rings
reuse `mule_*` payees / `farm_*` devices — so no ground-truth leaks through the scoring API.

A configurable fraction of fraud is "charged back" (with a maturation lag in prod) and a
sample of legit is confirmed, giving the trainer both classes. Pure `classify` is unit-tested.
"""
from __future__ import annotations

import json
import os
import random
import sys
import threading

sys.path.insert(0, os.path.dirname(__file__))
from common import TXN_STREAM, redis_client
from labels import CONFIRMED_FRAUD, CONFIRMED_LEGIT, submit_label

GROUP = "chargeback-feed"
CHARGEBACK_PROB = 0.85      # fraction of true fraud that gets charged back / confirmed
LEGIT_SAMPLE_RATE = 0.05    # sample of legit confirmed (no-chargeback-after-window), for negatives


def is_fraud_entity(receiver_account: str | None, device: str | None) -> bool:
    """Feedback-loop feeder marks fraud rings with mule_* payees and farm_* devices."""
    return bool((receiver_account and receiver_account.startswith("mule_"))
                or (device and device.startswith("farm_")))


def classify(event: dict, rng: random.Random,
             chargeback_prob: float = CHARGEBACK_PROB,
             legit_rate: float = LEGIT_SAMPLE_RATE) -> tuple[str, str] | None:
    """(label, source) to submit for an audit event, or None to skip (unlabeled)."""
    recv, dev = event.get("receiver_account"), event.get("device_fingerprint")
    if is_fraud_entity(recv, dev):
        return (CONFIRMED_FRAUD, "chargeback") if rng.random() < chargeback_prob else None
    return (CONFIRMED_LEGIT, "manual_review") if rng.random() < legit_rate else None


def react_once(r, rng: random.Random, count: int = 1000, block_ms: int = 1000) -> dict:
    resp = r.xreadgroup(GROUP, "cb1", {TXN_STREAM: ">"}, count=count, block=block_ms)
    if not resp:
        return {"fraud": 0, "legit": 0}
    ids, seen, out = [], set(), {"fraud": 0, "legit": 0}
    for _stream, entries in resp:
        for msg_id, fields in entries:
            ids.append(msg_id)
            ev = json.loads(fields["v"]) if "v" in fields else fields
            tid = ev.get("transaction_id")
            if not tid or tid in seen:      # dedupe champion/challenger duplicate events per txn
                continue
            seen.add(tid)
            decided = classify(ev, rng)
            if not decided:
                continue
            label, source = decided
            submit_label(r, tid, label, source, txn_ts_ms=int(ev.get("timestamp_epoch_ms", 0)),
                         receiver_account=ev.get("receiver_account"),
                         device_fingerprint=ev.get("device_fingerprint"))
            out["fraud" if label == CONFIRMED_FRAUD else "legit"] += 1
    if ids:
        r.xack(TXN_STREAM, GROUP, *ids)
    return out


def run(stop: threading.Event | None = None, seed: int = 0):  # pragma: no cover
    r = redis_client()
    rng = random.Random(seed)
    try:
        r.xgroup_create(TXN_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    print(f"[chargeback_feed] labelling {TXN_STREAM} -> label store")
    while stop is None or not stop.is_set():
        n = react_once(r, rng)
        if n["fraud"] or n["legit"]:
            print(f"[chargeback_feed] +{n['fraud']} fraud, +{n['legit']} legit labels")


if __name__ == "__main__":  # pragma: no cover
    run()
