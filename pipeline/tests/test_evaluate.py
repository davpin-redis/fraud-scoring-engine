"""E1 — component tests for the eval harness + model (design doc §13.6.7)."""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import evaluate  # noqa: E402
import model as m  # noqa: E402


def test_metrics_at_budget_perfect_ranking():
    # 2 fraud with the highest scores; flag top 20% of 10 => top 2 => both fraud
    y = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0])
    scores = np.array([0.95, 0.90, 0.50, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.10])
    amount = np.ones(10) * 100
    met = evaluate.metrics_at_budget(y, scores, amount, budget=0.2)
    assert met["flagged"] == 2
    assert met["recall"] == 1.0 and met["precision"] == 1.0 and met["fpr"] == 0.0
    assert met["gbp_caught"] == 200.0 and met["gbp_false_decline"] == 0.0


def test_metrics_at_budget_mixed_top():
    # top-2 are [legit, fraud]; other fraud is ranked lower -> recall .5, precision .5, fpr 1/8
    y = np.array([0, 1, 1, 0, 0, 0, 0, 0, 0, 0])
    scores = np.array([0.99, 0.90, 0.50, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.10])
    amount = np.ones(10) * 100
    met = evaluate.metrics_at_budget(y, scores, amount, budget=0.2)
    assert met["flagged"] == 2
    assert met["recall"] == 0.5 and met["precision"] == 0.5
    assert abs(met["fpr"] - 1 / 8) < 1e-9


def test_blunt_rule_metrics():
    X = np.zeros((4, len(evaluate.dataset.FEATURES)))
    X[:, evaluate.BLUNT_IDX] = [5, 1, 3, 5]   # >2 fires on rows 0,2,3
    y = np.array([1, 0, 1, 0])
    amount = np.ones(4) * 10
    met = evaluate.blunt_rule_metrics(X, y, amount)
    assert met["flagged"] == 3
    assert met["recall"] == 1.0            # both fraud (device 5,3) flagged
    assert abs(met["precision"] - 2 / 3) < 1e-9   # one legit (device 5) false-positive
    assert met["fpr"] == 0.5


def test_model_trains_and_separates():
    rng = np.random.default_rng(0)
    n = 2000
    X = rng.normal(size=(n, 6))
    y = (X[:, 0] + X[:, 1] + rng.normal(0, 0.5, n) > 1.5).astype(int)
    mdl = m.train(X, y, seed=0)
    p = m.predict(mdl, X)
    assert p.min() >= 0.0 and p.max() <= 1.0
    from sklearn.metrics import roc_auc_score
    assert roc_auc_score(y, p) > 0.9
