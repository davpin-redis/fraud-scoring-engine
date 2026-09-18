"""C1 — metrics aggregator (design doc §10.3).

Consumes the shared `txn:events` stream (every engine/producer's decisions) and joins each
against the label store, maintaining **global confusion counters in Redis**
(`metrics:{version}:{bucket}` → tp/fp/fn/tn + £), so the figures span all instances and
survive restarts. Serves an SSE frame (`/metrics/fraud/stream`) the dashboard renders, and
a `POST /metrics/fraud/apply-feedback` that promotes the challenger. The pure helpers
(`rates`, `classify`, `decision_at`, `read_counters`) are unit-tested without Redis/HTTP.
"""
from __future__ import annotations

import json
import os
import threading
import time

from common import LABEL_KEY_PREFIX, TXN_STREAM, redis_client

CONFIRMED_FRAUD = "confirmed_fraud"
BUCKET_SEC = 2                      # confusion counter bucket width
WINDOW_SEC = 20                     # rolling window the SSE frame reports over
GROUP = "aggregator"
FEEDBACK_KEY = "metrics:feedback_ts"
INSTANCES_KEY = "metrics:instances"
CHAMPION_KEY = "metrics:champion_version"
CHALLENGER_KEY = "metrics:challenger_version"
FIELDS = ("tp", "fp", "fn", "tn")


# ---------------- pure helpers ----------------
def decision_at(score: float, threshold: float) -> str:
    return "decline" if score >= threshold else "approve"


def classify(decision: str, label: int) -> str:
    """Map (decision, ground-truth) to a confusion cell. Positive prediction = 'decline'."""
    flagged = decision == "decline"
    if flagged and label == 1:
        return "tp"
    if flagged and label == 0:
        return "fp"
    if not flagged and label == 1:
        return "fn"
    return "tn"


def rates(c: dict) -> dict:
    tp, fp, fn, tn = c.get("tp", 0), c.get("fp", 0), c.get("fn", 0), c.get("tn", 0)
    return dict(
        recall=tp / (tp + fn) if tp + fn else 0.0,
        precision=tp / (tp + fp) if tp + fp else 0.0,
        fpr=fp / (fp + tn) if fp + tn else 0.0,
        tp=tp, fp=fp, fn=fn, tn=tn,
    )


def _bucket(now_ms: int) -> int:
    return int(now_ms / 1000 // BUCKET_SEC)


def read_counters(r, version: str, now_ms: int, window_sec: int = WINDOW_SEC) -> dict:
    """Sum the confusion buckets for a model version over the rolling window."""
    b_now = _bucket(now_ms)
    n = max(1, window_sec // BUCKET_SEC)
    total = {f: 0 for f in FIELDS}
    caught = fd = 0.0
    pipe = r.pipeline()
    keys = [f"metrics:{version}:{b_now - i}" for i in range(n)]
    for k in keys:
        pipe.hgetall(k)
    for h in pipe.execute():
        if not h:
            continue
        for f in FIELDS:
            total[f] += int(h.get(f, 0))
        caught += float(h.get("gbp_caught", 0))
        fd += float(h.get("gbp_false_decline", 0))
    total["gbp_caught"] = caught
    total["gbp_false_decline"] = fd
    return total


# ---------------- runtime: consume stream + join labels ----------------
def _apply(r, version: str, decision: str, label: int, amount: float, now_ms: int):
    cell = classify(decision, label)
    key = f"metrics:{version}:{_bucket(now_ms)}"
    pipe = r.pipeline()
    pipe.hincrby(key, cell, 1)
    if cell == "tp":
        pipe.hincrbyfloat(key, "gbp_caught", amount)
    elif cell == "fp":
        pipe.hincrbyfloat(key, "gbp_false_decline", amount)
    pipe.expire(key, WINDOW_SEC * 3)
    pipe.execute()


def ensure_group(r):
    try:
        r.xgroup_create(TXN_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass


def drain_once(r, count: int = 1000, block_ms: int = 1000) -> int:
    """Read one batch: decision event -> join label store -> increment counters. Returns processed."""
    resp = r.xreadgroup(GROUP, "agg1", {TXN_STREAM: ">"}, count=count, block=block_ms)
    if not resp:
        return 0
    ids, processed = [], 0
    for _stream, entries in resp:
        for msg_id, fields in entries:
            ids.append(msg_id)
            obj = json.loads(fields["v"]) if "v" in fields else fields
            txn_id = obj.get("transaction_id")
            lab = r.get(LABEL_KEY_PREFIX + txn_id) if txn_id else None
            if lab is None:
                continue  # label not yet available (lag); skip for now
            label = 1 if json.loads(lab)["label"] == CONFIRMED_FRAUD else 0
            _apply(r, obj.get("model_version", "unknown"), obj.get("decision", "approve"),
                   label, float(obj.get("amount_base", 0.0)), int(time.time() * 1000))
            processed += 1
    if ids:
        r.xack(TXN_STREAM, GROUP, *ids)
    return processed


def consume(r, stop: threading.Event):
    """Consumer-group loop (runtime)."""
    ensure_group(r)
    while not stop.is_set():
        drain_once(r)


def build_frame(r, now_ms: int | None = None) -> dict:
    now_ms = now_ms or int(time.time() * 1000)
    champ = (r.get(CHAMPION_KEY) or "champion")
    chall = (r.get(CHALLENGER_KEY) or "challenger")
    cc = read_counters(r, champ, now_ms)
    hc = read_counters(r, chall, now_ms)
    cr, hr = rates(cc), rates(hc)
    fb = r.get(FEEDBACK_KEY)
    total_events = sum(cc[f] for f in FIELDS) + sum(hc[f] for f in FIELDS)
    return dict(
        ts=now_ms, window="%ds" % WINDOW_SEC,
        instances=r.scard(INSTANCES_KEY) or 1,
        champion=dict(version=champ, recall=cr["recall"], fpr=cr["fpr"], precision=cr["precision"]),
        challenger=dict(version=chall, recall=hr["recall"], fpr=hr["fpr"], precision=hr["precision"],
                        active=fb is not None),
        gbp=dict(fraudCaught=hc["gbp_caught"], falseDeclineCost=hc["gbp_false_decline"]),
        tps=round(total_events / WINDOW_SEC, 1),
        feedbackAppliedTs=int(fb) if fb else None,
    )


# ---------------- FastAPI app ----------------
def create_app():  # pragma: no cover
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, StreamingResponse

    app = FastAPI(title="fraud feedback metrics aggregator")
    r = redis_client()
    stop = threading.Event()
    threading.Thread(target=consume, args=(r, stop), daemon=True).start()

    @app.get("/metrics/fraud/stream")
    def stream():
        def gen():
            while True:
                yield f"data: {json.dumps(build_frame(r))}\n\n"
                time.sleep(1)
        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/metrics/fraud/apply-feedback")
    def apply_feedback():
        r.set(FEEDBACK_KEY, int(time.time() * 1000))
        return {"status": "feedback applied"}

    @app.get("/", response_class=HTMLResponse)
    def dashboard():
        path = os.path.join(os.path.dirname(__file__), "..", "feedback_dashboard_mockup.html")
        html = open(path).read().replace("const USE_SIMULATION = true;", "const USE_SIMULATION = false;")
        return HTMLResponse(html)

    return app


if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=8090)
