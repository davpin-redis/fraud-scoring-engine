"""B2 — Track-A model (design doc §8.4.2).

A CPU gradient-boosted-trees classifier over the engineered feature vector, retrained
each round on the accrued labels. Uses scikit-learn's HistGradientBoostingClassifier
(no OpenMP dylib dependency, so it runs on this laptop); LightGBM is the documented
production swap behind the same train()/predict() interface once libomp is available.

Deterministic given `seed` (fixed random_state, no early stopping) so the learning-curve
test is stable.
"""
from __future__ import annotations

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier


def train(X: np.ndarray, y: np.ndarray, seed: int = 0):
    model = HistGradientBoostingClassifier(
        random_state=seed,
        class_weight="balanced",     # handle the ~1% fraud imbalance
        early_stopping=False,        # determinism
        learning_rate=0.1,
        max_iter=200,
        max_leaf_nodes=31,
    )
    model.fit(X, y)
    return model


def predict(model, X: np.ndarray) -> np.ndarray:
    """Fraud probability in [0, 1]."""
    return model.predict_proba(X)[:, 1]
