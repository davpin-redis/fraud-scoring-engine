"""Chargeback/label feed classification tests (design doc §8.4.1)."""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import chargeback_feed as cb  # noqa: E402
from labels import CONFIRMED_FRAUD, CONFIRMED_LEGIT  # noqa: E402


def test_fraud_entities_detected():
    assert cb.is_fraud_entity("mule_00007", "device_5") is True
    assert cb.is_fraud_entity("bene_9", "farm_0003") is True
    assert cb.is_fraud_entity("bene_9", "device_5") is False


def test_classify_labels_all_fraud_and_samples_legit():
    rng = random.Random(0)
    fraud_ev = {"receiver_account": "mule_00007", "device_fingerprint": "d"}
    legit_ev = {"receiver_account": "bene_9", "device_fingerprint": "device_5"}
    # chargeback_prob=1 -> every fraud confirmed; legit_rate=1 -> every legit confirmed
    assert cb.classify(fraud_ev, rng, chargeback_prob=1.0, legit_rate=1.0)[0] == CONFIRMED_FRAUD
    assert cb.classify(legit_ev, rng, chargeback_prob=1.0, legit_rate=1.0)[0] == CONFIRMED_LEGIT
    # legit_rate=0 -> legit skipped
    assert cb.classify(legit_ev, rng, chargeback_prob=1.0, legit_rate=0.0) is None


def test_classify_fraud_source_and_rates():
    rng = random.Random(1)
    n_fraud = sum(1 for _ in range(2000)
                  if cb.classify({"receiver_account": "mule_1"}, rng, chargeback_prob=0.85, legit_rate=0.0))
    assert 1500 < n_fraud < 1900   # ~85% of 2000
