"""B3 — champion/challenger evaluation + learning curve (design doc §8.4.4 / §13.6.7).

Everything is measured on a FIXED labelled holdout at a FIXED operating point (a fixed
decline budget — the top-`budget` fraction by score are flagged), so a rise in recall
alongside a fall in false-positive rate is a genuine discrimination gain, not a
threshold trade. The learning curve trains the model on the labels accrued through each
round and scores the same holdout, so the metric-vs-round series shows the loop
improving as labels accumulate.
"""
from __future__ import annotations

import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score

import dataset
from model import predict, train

BLUNT_IDX = dataset.FEATURES.index("device_distinct_customers")
BLUNT_THRESHOLD = 2


def metrics_at_budget(y: np.ndarray, scores: np.ndarray, amount: np.ndarray, budget: float) -> dict:
    """Confusion + rate + £ metrics with the top-`budget` fraction of scores flagged."""
    thr = float(np.quantile(scores, 1.0 - budget))
    pred = scores >= thr
    tp = int((pred & (y == 1)).sum()); fp = int((pred & (y == 0)).sum())
    fn = int((~pred & (y == 1)).sum()); tn = int((~pred & (y == 0)).sum())
    return dict(
        recall=tp / (tp + fn) if tp + fn else 0.0,
        precision=tp / (tp + fp) if tp + fp else 0.0,
        fpr=fp / (fp + tn) if fp + tn else 0.0,
        pr_auc=float(average_precision_score(y, scores)),
        roc_auc=float(roc_auc_score(y, scores)),
        gbp_caught=float(amount[pred & (y == 1)].sum()),
        gbp_false_decline=float(amount[pred & (y == 0)].sum()),
        threshold=thr, flagged=int(pred.sum()),
    )


def blunt_rule_metrics(X: np.ndarray, y: np.ndarray, amount: np.ndarray) -> dict:
    """The pre-ML baseline: flag device_distinct_customers > 2 (a fixed rule, no budget)."""
    pred = X[:, BLUNT_IDX] > BLUNT_THRESHOLD
    tp = int((pred & (y == 1)).sum()); fp = int((pred & (y == 0)).sum())
    fn = int((~pred & (y == 1)).sum()); tn = int((~pred & (y == 0)).sum())
    return dict(
        recall=tp / (tp + fn) if tp + fn else 0.0,
        precision=tp / (tp + fp) if tp + fp else 0.0,
        fpr=fp / (fp + tn) if fp + tn else 0.0,
        gbp_caught=float(amount[pred & (y == 1)].sum()),
        gbp_false_decline=float(amount[pred & (y == 0)].sum()),
        flagged=int(pred.sum()),
    )


def learning_curve(sim_dir: str, rounds: int, budget: float = 0.02, seed: int = 0,
                   include_warmup: bool = True) -> dict:
    """Champion (round-0, warm-start only) vs challenger retrained each round, plus the
    blunt-rule baseline, all scored on the fixed holdout at a fixed decline budget."""
    Xh, yh, amh = dataset.holdout(sim_dir)

    Xw, yw, _ = dataset.warmup(sim_dir)
    champion = metrics_at_budget(yh, predict(train(Xw, yw, seed), Xh), amh, budget)
    champion["n_fraud"] = int(yw.sum())

    curve = []
    for r in range(1, rounds + 1):
        Xr, yr, _ = dataset.rounds_upto(sim_dir, r, include_warmup)
        m = metrics_at_budget(yh, predict(train(Xr, yr, seed), Xh), amh, budget)
        m["round"] = r
        m["n_fraud"] = int(yr.sum())
        curve.append(m)

    return {"champion": champion, "curve": curve,
            "blunt": blunt_rule_metrics(Xh, yh, amh), "budget": budget}


if __name__ == "__main__":  # pragma: no cover
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Print the champion/challenger learning curve.")
    ap.add_argument("--sim", required=True, help="generated dataset dir (from generator.py)")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--budget", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--include-warmup", action="store_true",
                    help="train the challenger on warm-start backfill + rounds (default: feedback rounds only)")
    a = ap.parse_args()
    print(json.dumps(learning_curve(a.sim, a.rounds, a.budget, a.seed, a.include_warmup), indent=2))
