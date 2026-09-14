"""E2 — the headline assertion (design doc §13.6.7): as the feedback loop accrues labels
over rounds, fraud detection rises and false positives fall, at a fixed operating point —
and the learned model beats the pre-ML blunt baseline on every axis. Deterministic."""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import evaluate  # noqa: E402
import generator as g  # noqa: E402

BUDGET = 0.02


def _curve(tmp_path):
    out = str(tmp_path / "sim")
    g.generate("small", seed=42, out_dir=out)
    return evaluate.learning_curve(out, rounds=10, budget=BUDGET, seed=0, include_warmup=False)


def test_feedback_loop_improves_over_rounds(tmp_path):
    r = _curve(tmp_path)
    curve = r["curve"]
    assert len(curve) == 10
    first, last = curve[0], curve[-1]
    recalls = [m["recall"] for m in curve]
    fprs = [m["fpr"] for m in curve]
    pr_aucs = [m["pr_auc"] for m in curve]

    # fraud detection rises materially from the first to the last round
    assert last["recall"] - first["recall"] >= 0.06
    # ... without a large backslide on the way (monotone within tolerance)
    assert min(np.diff(recalls)) >= -0.03
    # false positives do NOT worsen; precision improves
    assert last["fpr"] <= first["fpr"] + 0.001
    assert last["precision"] - first["precision"] >= 0.02
    # ranking quality (PR-AUC) rises clearly and near-monotonically
    assert last["pr_auc"] - first["pr_auc"] >= 0.05
    assert min(np.diff(pr_aucs)) >= -0.03


def test_model_beats_blunt_baseline_on_every_axis(tmp_path):
    r = _curve(tmp_path)
    blunt = r["blunt"]
    for met in r["curve"]:
        assert met["recall"] > blunt["recall"] + 0.20      # catches far more fraud
        assert met["precision"] > 3 * blunt["precision"]   # dramatically fewer false alarms
        assert met["fpr"] < blunt["fpr"]                   # lower false-positive rate


def test_challenger_surpasses_initial_champion(tmp_path):
    r = _curve(tmp_path)
    # by the last round the feedback-trained model matches/beats the round-0 (warm-start) model
    assert r["curve"][-1]["recall"] >= r["champion"]["recall"] - 0.01


def test_learning_curve_deterministic(tmp_path):
    out = str(tmp_path / "sim")
    g.generate("small", seed=42, out_dir=out)
    a = evaluate.learning_curve(out, rounds=10, budget=BUDGET, seed=0, include_warmup=False)
    b = evaluate.learning_curve(out, rounds=10, budget=BUDGET, seed=0, include_warmup=False)
    assert [m["recall"] for m in a["curve"]] == [m["recall"] for m in b["curve"]]
