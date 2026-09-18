"""Phase 1 tests (design doc §13.6 / D2): the generated data has the properties that
make an improving learning curve possible, and generation is deterministic."""
import os
import sys

import numpy as np
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import generator as g  # noqa: E402


def test_small_dataset_passes_self_checks(tmp_path):
    out = str(tmp_path / "sim")
    m = g.generate("small", seed=42, out_dir=out)
    # fraud rate ~1% in the streaming rounds
    r1 = m["counts"]["rounds/round_01"]
    assert 0.003 <= r1["fraud"] / r1["rows"] <= 0.03
    # required data properties (joint learnable-not-trivial, blunt rule imprecise+incomplete, look-alikes exist)
    v = g.verify(out)
    assert v["ok"], v["assertions"]
    assert 0.80 <= v["joint_auc"] <= 0.985           # learnable but not trivial → room for a learning curve
    assert v["joint_auc"] - v["blunt_only_auc"] > 0.08
    assert v["blunt_rule_precision"] < 0.5           # false-positives on look-alikes
    assert v["blunt_rule_recall"] < 0.85             # misses stealth fraud the model must catch


def test_generation_is_deterministic(tmp_path):
    a, b = str(tmp_path / "a"), str(tmp_path / "b")
    g.generate("small", seed=7, out_dir=a)
    g.generate("small", seed=7, out_dir=b)
    ta = pq.read_table(os.path.join(a, "holdout.parquet"))
    tb = pq.read_table(os.path.join(b, "holdout.parquet"))
    assert np.array_equal(ta.column("label").to_numpy(), tb.column("label").to_numpy())
    assert np.array_equal(ta.column("device_surge").to_numpy(), tb.column("device_surge").to_numpy())
    assert ta.column("transaction_id").to_pylist() == tb.column("transaction_id").to_pylist()


def test_streaming_rounds_present(tmp_path):
    out = str(tmp_path / "sim")
    g.generate("small", seed=1, out_dir=out)
    for r in range(1, g.PRESETS["small"]["rounds"] + 1):
        assert os.path.exists(os.path.join(out, "rounds", f"round_{r:02d}.parquet"))
    assert os.path.exists(os.path.join(out, "warmup.parquet"))
    assert os.path.exists(os.path.join(out, "holdout.parquet"))
