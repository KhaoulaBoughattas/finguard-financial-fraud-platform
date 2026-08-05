"""Supervised fraud classification with XGBoost.

Handles class imbalance via `scale_pos_weight` (ratio of negative to
positive class counts in the TRAINING set only -- computing it from the
full dataset would leak test-set class balance information into training).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import xgboost as xgb

from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN


def train_xgboost(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    random_seed: int = 42,
    n_estimators: int = 200,
    max_depth: int = 5,
    learning_rate: float = 0.1,
) -> xgb.XGBClassifier:
    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[LABEL_COLUMN].astype(int)
    X_val = val_df[FEATURE_COLUMNS]
    y_val = val_df[LABEL_COLUMN].astype(int)

    n_neg = int((y_train == 0).sum())
    n_pos = int((y_train == 1).sum())
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0

    model = xgb.XGBClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",  # PR-AUC, not accuracy -- consistent with evaluate.py's
        # position that accuracy is the wrong metric for this class imbalance.
        random_state=random_seed,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    return model


def predict_scores(model: xgb.XGBClassifier, df: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(df[FEATURE_COLUMNS])[:, 1]


def get_feature_importance(model: xgb.XGBClassifier) -> dict[str, float]:
    """Feature importances by gain -- used both for the model card/docs and
    for the hybrid engine's explainability (see hybrid.py)."""
    importances = model.get_booster().get_score(importance_type="gain")
    return {k: round(v, 2) for k, v in sorted(importances.items(), key=lambda kv: -kv[1])}
