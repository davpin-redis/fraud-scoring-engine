"""Orchestrator blacklist-overlay unit tests (design doc §8.4.2 — engine R001/R002 semantics)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import orchestrator as orch  # noqa: E402


def test_blacklisted_receiver_forces_decline():
    assert orch.apply_blacklist("approve", "mule_1", "dev_1", {"mule_1"}, set()) == "decline"


def test_blacklisted_device_forces_decline():
    assert orch.apply_blacklist("approve", "bene_1", "farm_1", set(), {"farm_1"}) == "decline"


def test_clean_entities_unchanged():
    assert orch.apply_blacklist("approve", "bene_1", "dev_1", {"mule_9"}, {"farm_9"}) == "approve"
    assert orch.apply_blacklist("decline", "bene_1", "dev_1", set(), set()) == "decline"
