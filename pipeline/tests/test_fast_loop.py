"""B6 component tests — the fast reactive loop's blacklist decision (design doc §8.4.2)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import fast_loop as fl  # noqa: E402
import labels as lb  # noqa: E402


def test_confirmed_fraud_blacklists_entities():
    rec = lb.label_record("t1", lb.CONFIRMED_FRAUD, "chargeback", txn_ts_ms=1,
                          receiver_account="mule_00007", device_fingerprint="farm_0003")
    accts, devs = fl.blacklist_targets(rec)
    assert accts == ["mule_00007"] and devs == ["farm_0003"]


def test_legit_label_blacklists_nothing():
    rec = lb.label_record("t2", lb.CONFIRMED_LEGIT, "simulation", txn_ts_ms=1,
                          receiver_account="bene_01", device_fingerprint="dev_01")
    assert fl.blacklist_targets(rec) == ([], [])


def test_untrusted_source_ignored():
    rec = lb.label_record("t3", lb.CONFIRMED_FRAUD, "anonymous_tip", txn_ts_ms=1,
                          receiver_account="mule_x", device_fingerprint="farm_x")
    assert fl.blacklist_targets(rec) == ([], [])


def test_missing_entities_yield_empty():
    rec = lb.label_record("t4", lb.CONFIRMED_FRAUD, "manual_review", txn_ts_ms=1)
    assert fl.blacklist_targets(rec) == ([], [])
