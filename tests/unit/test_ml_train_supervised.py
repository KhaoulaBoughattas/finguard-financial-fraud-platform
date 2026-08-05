import numpy as np
import pandas as pd

from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN
from src.ml.train_supervised import (
    get_feature_importance,
    predict_scores,
    train_xgboost,
)


def _synthetic_df(n: int, fraud_rate: float, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    data = {col: rng.normal(size=n) for col in FEATURE_COLUMNS}
    data[LABEL_COLUMN] = rng.random(n) < fraud_rate
    df = pd.DataFrame(data)
    df.loc[df[LABEL_COLUMN], FEATURE_COLUMNS[0]] += 5.0
    return df


def test_train_xgboost_produces_a_fitted_model():
    train_df = _synthetic_df(200, fraud_rate=0.1, seed=1)
    val_df = _synthetic_df(50, fraud_rate=0.1, seed=2)
    model = train_xgboost(train_df, val_df, random_seed=42, n_estimators=20)
    assert model is not None
    scores = predict_scores(model, val_df)
    assert len(scores) == len(val_df)
    assert ((scores >= 0) & (scores <= 1)).all()


def test_train_xgboost_is_deterministic_with_same_seed():
    train_df = _synthetic_df(200, fraud_rate=0.1, seed=1)
    val_df = _synthetic_df(50, fraud_rate=0.1, seed=2)
    model_a = train_xgboost(train_df, val_df, random_seed=42, n_estimators=20)
    model_b = train_xgboost(train_df, val_df, random_seed=42, n_estimators=20)
    scores_a = predict_scores(model_a, val_df)
    scores_b = predict_scores(model_b, val_df)
    assert np.allclose(scores_a, scores_b)


def test_model_learns_the_boosted_signal_better_than_random():
    train_df = _synthetic_df(500, fraud_rate=0.15, seed=1)
    val_df = _synthetic_df(200, fraud_rate=0.15, seed=2)
    model = train_xgboost(train_df, val_df, random_seed=42, n_estimators=50)
    scores = predict_scores(model, val_df)
    fraud_scores = scores[val_df[LABEL_COLUMN].to_numpy()]
    normal_scores = scores[~val_df[LABEL_COLUMN].to_numpy()]
    assert fraud_scores.mean() > normal_scores.mean()


def test_feature_importance_covers_known_columns():
    train_df = _synthetic_df(200, fraud_rate=0.1, seed=1)
    val_df = _synthetic_df(50, fraud_rate=0.1, seed=2)
    model = train_xgboost(train_df, val_df, random_seed=42, n_estimators=20)
    importance = get_feature_importance(model)
    assert set(importance.keys()).issubset(set(FEATURE_COLUMNS))
    assert all(v >= 0 for v in importance.values())
