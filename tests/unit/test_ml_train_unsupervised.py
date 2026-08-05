import numpy as np
import pandas as pd

from src.ml.features import FEATURE_COLUMNS
from src.ml.train_unsupervised import predict_anomaly_scores, train_isolation_forest


def _synthetic_df(n: int, n_outliers: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    data = {col: rng.normal(size=n) for col in FEATURE_COLUMNS}
    df = pd.DataFrame(data)
    outlier_idx = rng.choice(n, size=n_outliers, replace=False)
    for col in FEATURE_COLUMNS:
        df.loc[outlier_idx, col] = rng.normal(loc=20, scale=1, size=n_outliers)
    df["is_outlier"] = False
    df.loc[outlier_idx, "is_outlier"] = True
    return df


def test_train_isolation_forest_produces_a_fitted_model():
    train_df = _synthetic_df(200, n_outliers=10, seed=1)
    model = train_isolation_forest(train_df, random_seed=42, n_estimators=50)
    assert model is not None


def test_anomaly_scores_are_normalized_to_unit_range():
    train_df = _synthetic_df(200, n_outliers=10, seed=1)
    model = train_isolation_forest(train_df, random_seed=42, n_estimators=50)
    scores = predict_anomaly_scores(model, train_df)
    assert ((scores >= 0) & (scores <= 1)).all()


def test_injected_outliers_score_higher_than_normal_points():
    train_df = _synthetic_df(300, n_outliers=20, seed=1)
    model = train_isolation_forest(train_df, random_seed=42, n_estimators=100)
    scores = predict_anomaly_scores(model, train_df)
    outlier_scores = scores[train_df["is_outlier"].to_numpy()]
    normal_scores = scores[~train_df["is_outlier"].to_numpy()]
    assert outlier_scores.mean() > normal_scores.mean()


def test_deterministic_with_same_seed():
    train_df = _synthetic_df(200, n_outliers=10, seed=1)
    model_a = train_isolation_forest(train_df, random_seed=42, n_estimators=50)
    model_b = train_isolation_forest(train_df, random_seed=42, n_estimators=50)
    scores_a = predict_anomaly_scores(model_a, train_df)
    scores_b = predict_anomaly_scores(model_b, train_df)
    assert np.allclose(scores_a, scores_b)
