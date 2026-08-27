#!/usr/bin/env python3
"""
Offline training for the fraud model (design doc §8.2), laptop-fixture edition.

The laptop backfill has no ground-truth `outcome_label`, so this derives WEAK
labels from the injected fraud patterns (§13.1): a transaction is labelled fraud
if its beneficiary is a mule/blacklisted account, its device is blacklisted, or
the customer shows a velocity burst (>5 txns in the prior hour). Features are
computed the same way the engine assembles them at scoring time (§7.4-7.5), so
the exported weights line up with `LinearModelScorer`.

Zero third-party deps (pure-Python logistic regression) so it runs anywhere.
Output: engine/src/main/resources/model/model.json.

NOTE: with labels derived from features there is deliberate leakage — this model
is illustrative of the *pipeline*, not a production-quality classifier. A real
model needs the labelled seed data from §13.1 (open item, §12).
"""
import json
import math
import os
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_DATA = os.path.join(HERE, "..", "test_data")
OUT = os.path.join(HERE, "..", "engine", "src", "main", "resources", "model", "model.json")

FEATURE_ORDER = [
    "amount_base",
    "local_hour",
    "customer_txn_count_1h",
    "bene_distinct_senders_24h",
    "pair_txn_count_90d",
    "cross_border",
]
VERSION = "logreg-v1"


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def ts(s):
    return datetime.fromisoformat(s)


def build_dataset():
    txns = load_jsonl(os.path.join(TEST_DATA, "transactions", "historical_backfill.jsonl"))
    benes = {b["receiver_transaction_bank_account_number"]: b
             for b in load_json(os.path.join(TEST_DATA, "reference", "beneficiaries.json"))}
    bl = load_json(os.path.join(TEST_DATA, "reference", "blacklists.json"))
    bad_accounts = set(bl["bl_accounts"])
    bad_devices = set(bl["bl_devices"])

    for t in txns:
        t["_ts"] = ts(t["timestamp"])
    txns.sort(key=lambda t: t["_ts"])

    rows, labels = [], []
    for i, t in enumerate(txns):
        cid = t["customer_id"]
        bene = t["receiver_transaction_bank_account_number"]
        device = t["device_fingerprint"]
        now = t["_ts"]

        prior = txns[:i]
        count_1h = sum(1 for p in prior
                       if p["customer_id"] == cid and 0 <= (now - p["_ts"]).total_seconds() <= 3600)
        distinct_24h = len({p["customer_id"] for p in prior
                            if p["receiver_transaction_bank_account_number"] == bene
                            and 0 <= (now - p["_ts"]).total_seconds() <= 86400})
        pair_90d = sum(1 for p in prior
                       if p["customer_id"] == cid and p["receiver_transaction_bank_account_number"] == bene)

        bene_country = benes.get(bene, {}).get("country")
        cross_border = 1.0 if (bene_country and bene_country != t["customer_portfolio_country"]) else 0.0

        features = [
            float(t["amount_base"]),
            float(now.hour),
            float(count_1h),
            float(distinct_24h),
            float(pair_90d),
            cross_border,
        ]

        is_mule = benes.get(bene, {}).get("is_mule_pattern", False)
        label = 1 if (is_mule or bene in bad_accounts or device in bad_devices or count_1h > 5) else 0

        rows.append(features)
        labels.append(label)
    return rows, labels


def standardize(rows):
    n, d = len(rows), len(rows[0])
    means = [sum(r[j] for r in rows) / n for j in range(d)]
    stds = []
    for j in range(d):
        var = sum((r[j] - means[j]) ** 2 for r in rows) / n
        stds.append(math.sqrt(var) if var > 1e-12 else 1.0)
    std_rows = [[(r[j] - means[j]) / stds[j] for j in range(d)] for r in rows]
    return std_rows, means, stds


def sigmoid(z):
    if z < -60:
        return 0.0
    if z > 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-z))


def train(std_rows, labels, iters=4000, lr=0.2):
    n, d = len(std_rows), len(std_rows[0])
    pos = sum(labels) or 1
    neg = n - pos or 1
    w_pos, w_neg = n / (2.0 * pos), n / (2.0 * neg)  # class balancing
    w = [0.0] * d
    b = 0.0
    for _ in range(iters):
        gw = [0.0] * d
        gb = 0.0
        for x, y in zip(std_rows, labels):
            p = sigmoid(b + sum(w[j] * x[j] for j in range(d)))
            weight = w_pos if y == 1 else w_neg
            err = weight * (p - y)
            for j in range(d):
                gw[j] += err * x[j]
            gb += err
        for j in range(d):
            w[j] -= lr * gw[j] / n
        b -= lr * gb / n
    return w, b


def average_precision(scores, labels):
    order = sorted(range(len(scores)), key=lambda i: -scores[i])
    total_pos = sum(labels) or 1
    tp = 0
    ap = 0.0
    for rank, i in enumerate(order, start=1):
        if labels[i] == 1:
            tp += 1
            ap += tp / rank
    return ap / total_pos


def main():
    rows, labels = build_dataset()
    std_rows, means, stds = standardize(rows)
    w, b = train(std_rows, labels)

    scores = [sigmoid(b + sum(w[j] * x[j] for j in range(len(w)))) for x in std_rows]
    ap = average_precision(scores, labels)

    model = {
        "version": VERSION,
        "feature_order": FEATURE_ORDER,
        "means": means,
        "stds": stds,
        "weights": w,
        "intercept": b,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(model, f, indent=2)

    print(f"Trained {VERSION} on {len(rows)} txns "
          f"({sum(labels)} fraud / {len(labels) - sum(labels)} legit).")
    print(f"Train PR-AUC (average precision): {ap:.3f}  "
          f"[illustrative — labels derived from features, §13.1]")
    print(f"Wrote {os.path.relpath(OUT, os.path.join(HERE, '..'))}")


if __name__ == "__main__":
    main()
