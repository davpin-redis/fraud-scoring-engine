"""C1 component tests — the aggregator's pure confusion/rate helpers (no Redis/HTTP)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import aggregator as agg  # noqa: E402


def test_decision_at():
    assert agg.decision_at(0.9, 0.5) == "decline"
    assert agg.decision_at(0.4, 0.5) == "approve"


def test_classify_confusion_cells():
    assert agg.classify("decline", 1) == "tp"
    assert agg.classify("decline", 0) == "fp"
    assert agg.classify("approve", 1) == "fn"
    assert agg.classify("approve", 0) == "tn"


def test_rates():
    r = agg.rates({"tp": 80, "fp": 20, "fn": 20, "tn": 880})
    assert abs(r["recall"] - 0.8) < 1e-9        # 80 / (80+20)
    assert abs(r["precision"] - 0.8) < 1e-9     # 80 / (80+20)
    assert abs(r["fpr"] - (20 / 900)) < 1e-9


def test_rates_zero_guards():
    r = agg.rates({})
    assert r["recall"] == 0.0 and r["precision"] == 0.0 and r["fpr"] == 0.0
