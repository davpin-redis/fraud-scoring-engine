"""B4 (export side) — train a Track-A model on the sim and export it to ONNX for
in-engine scoring (design doc §8.4.3 / Phase 4).

Writes `<out>/<name>.onnx` plus a sidecar manifest, and (once) the shared
`feature-order.json` — the ordered feature contract the Java `OnnxModelScorer` uses to
build its input vector from the assembled feature map. Uses opset 17 (broad runtime
support) and zipmap=False so the model outputs a plain probabilities tensor.
"""
from __future__ import annotations

import argparse
import json
import os

import numpy as np
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType

import dataset
from generator import FEATURES
from model import train

OPSET = 17


def export_trained(model, out_dir: str, name: str, version: str, extra: dict | None = None) -> str:
    """Export an already-trained model to ONNX + manifest (the feature contract). Returns the
    .onnx path. Writes atomically (temp + rename) so an engine reload never sees a half file."""
    initial = [("features", FloatTensorType([None, len(FEATURES)]))]
    onx = to_onnx(model, initial_types=initial, target_opset=OPSET, options={id(model): {"zipmap": False}})
    os.makedirs(out_dir, exist_ok=True)
    onnx_path = os.path.join(out_dir, f"{name}.onnx")
    tmp = onnx_path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(onx.SerializeToString())
    manifest = {"version": version, "features": FEATURES, "n_features": len(FEATURES), "opset": OPSET}
    manifest.update(extra or {})
    with open(os.path.join(out_dir, f"{name}.manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)
    with open(os.path.join(out_dir, "feature-order.json"), "w") as fh:
        json.dump(FEATURES, fh, indent=2)
    os.replace(tmp, onnx_path)                     # atomic publish
    print(f"[export_onnx] {name}.onnx v={version} ({len(FEATURES)} features, opset {OPSET}) -> {out_dir}")
    return onnx_path


def export(sim_dir: str, out_dir: str, name: str, upto_round: int, version: str, seed: int = 0):
    X, y, _ = dataset.rounds_upto(sim_dir, upto_round, include_warmup=True)
    export_trained(train(X, y, seed=seed), out_dir, name, version, extra={"upto_round": upto_round})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sim", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="fraud-model")
    ap.add_argument("--round", type=int, default=10)
    ap.add_argument("--version", default="fb-v1")
    a = ap.parse_args()
    export(a.sim, a.out, a.name, a.round, a.version)
