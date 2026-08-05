"""Unsupervised anomaly detection with Isolation Forest.

Trained WITHOUT the is_fraud label -- the point of an unsupervised model in
this hybrid design is to catch novel fraud patterns the supervised model
was never trained on (Phase 0: "detect previously unseen patterns"). Trained
only on the training split's features, same leakage discipline as the
supervised model.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from src.ml.features import FEATURE_COLUMNS


def train_isolation_forest(
    train_df: pd.DataFrame,
    random_seed: int = 42,
    n_estimators: int = 200,
    contamination: float = "auto",
) -> IsolationForest:
    X_train = train_df[FEATURE_COLUMNS]
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_seed,
        n_jobs=-1,
    )
    model.fit(X_train)
    return model


def predict_anomaly_scores(model: IsolationForest, df: pd.DataFrame) -> np.ndarray:
    """Returns a score in [0, 1] where higher = more anomalous, normalized
    from sklearn's raw score_samples (which is lower = more anomalous, and
    unbounded) so it's directly comparable to the supervised model's
    predict_proba output in the hybrid engine."""
    raw = model.score_samples(df[FEATURE_COLUMNS])
    inverted = -raw
    min_v, max_v = inverted.min(), inverted.max()
    if max_v == min_v:
        return np.zeros_like(inverted)
    return (inverted - min_v) / (max_v - min_v)
