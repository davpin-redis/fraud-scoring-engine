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


def export(sim_dir: str, out_dir: str, name: str, upto_round: int, version: str, seed: int = 0):
    X, y, _ = dataset.rounds_upto(sim_dir, upto_round, include_warmup=True)
    mdl = train(X, y, seed=seed)
    initial = [("features", FloatTensorType([None, len(FEATURES)]))]
    onx = to_onnx(mdl, initial_types=initial, target_opset=OPSET,
                  options={id(mdl): {"zipmap": False}})
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{name}.onnx"), "wb") as fh:
        fh.write(onx.SerializeToString())
    with open(os.path.join(out_dir, f"{name}.manifest.json"), "w") as fh:
        json.dump({"version": version, "features": FEATURES, "n_features": len(FEATURES),
                   "upto_round": upto_round, "opset": OPSET}, fh, indent=2)
    # the ordered feature contract shared with the engine
    with open(os.path.join(out_dir, "feature-order.json"), "w") as fh:
        json.dump(FEATURES, fh, indent=2)
    print(f"[export_onnx] {name}.onnx (round {upto_round}, {len(FEATURES)} features, opset {OPSET}) -> {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sim", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="fraud-model")
    ap.add_argument("--round", type=int, default=10)
    ap.add_argument("--version", default="fb-v1")
    a = ap.parse_args()
    export(a.sim, a.out, a.name, a.round, a.version)
