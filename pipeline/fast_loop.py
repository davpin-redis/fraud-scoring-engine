"""B6 — fast reactive loop (design doc §8.4.2).

The slow loop retrains the model; this fast loop contains *repeats* immediately. It consumes
the label feed and, on a confirmed fraud, blacklists the implicated payee account and device
so the engine declines the next transaction that reuses them — via the existing hard-block
rules R001/R002, which read `bl:accounts` / `bl:devices` live with `SISMEMBER` (no config
reload needed). Guarded against poisoning: only high-confidence labels act, additions per
batch are capped, and already-listed entities are skipped.

Pure helper (`blacklist_targets`) is unit-tested; `react_once` / `run` are the runtime.
"""
from __future__ import annotations

import json
import threading

from common import LABELS_STREAM, redis_client
from labels import CONFIRMED_FRAUD

BL_ACCOUNTS = "bl:accounts"
BL_DEVICES = "bl:devices"
GROUP = "fast-loop"
MAX_PER_BATCH = 500                 # cap blast radius per batch (poisoning guard)
TRUSTED_SOURCES = {"chargeback", "manual_review", "simulation"}


def blacklist_targets(rec: dict) -> tuple[list[str], list[str]]:
    """(accounts, devices) to blacklist for a label record — only for trusted confirmed fraud."""
    if rec.get("label") != CONFIRMED_FRAUD or rec.get("source") not in TRUSTED_SOURCES:
        return [], []
    accts = [rec["receiver_account"]] if rec.get("receiver_account") else []
    devs = [rec["device_fingerprint"]] if rec.get("device_fingerprint") else []
    return accts, devs


def react_once(r, count: int = 1000, block_ms: int = 1000) -> dict:
    """Read one batch of label events and blacklist implicated entities. Returns counts added."""
    resp = r.xreadgroup(GROUP, "fl1", {LABELS_STREAM: ">"}, count=count, block=block_ms)
    if not resp:
        return {"accounts": 0, "devices": 0}
    ids, accts, devs = [], set(), set()
    for _stream, entries in resp:
        for msg_id, fields in entries:
            ids.append(msg_id)
            rec = json.loads(fields["v"]) if "v" in fields else fields
            a, d = blacklist_targets(rec)
            accts.update(a)
            devs.update(d)
    accts, devs = list(accts)[:MAX_PER_BATCH], list(devs)[:MAX_PER_BATCH]
    added_a = int(r.sadd(BL_ACCOUNTS, *accts)) if accts else 0   # SADD returns # newly added
    added_d = int(r.sadd(BL_DEVICES, *devs)) if devs else 0
    if ids:
        r.xack(LABELS_STREAM, GROUP, *ids)
    return {"accounts": added_a, "devices": added_d}


def run(stop: threading.Event | None = None):  # pragma: no cover
    r = redis_client()
    try:
        r.xgroup_create(LABELS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    print(f"[fast_loop] reacting to {LABELS_STREAM} -> {BL_ACCOUNTS}/{BL_DEVICES}")
    while stop is None or not stop.is_set():
        n = react_once(r)
        if n["accounts"] or n["devices"]:
            print(f"[fast_loop] blacklisted +{n['accounts']} accounts, +{n['devices']} devices")


if __name__ == "__main__":  # pragma: no cover
    run()
