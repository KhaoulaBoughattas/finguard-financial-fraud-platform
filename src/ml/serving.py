"""Real-time transaction scoring.

Reuses Phase 5's leakage-safe feature engineering (engineer_features)
rather than reimplementing feature math for the serving path: a NEW,
not-yet-persisted transaction is appended to that customer's PRIOR
transaction history (loaded via load_customer_history, strictly before
the new transaction's own timestamp) and run through the exact same
engineer_features() function training uses. This is the concrete answer
to the training/serving skew this project has flagged since Phase 5
(docs/ml.md §2): the FEATURE MATH is shared and identical; only the
mechanism computing it differs (batch pandas here too, not a live
feature store) -- documented, not hidden.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from src.common.config import get_settings
from src.ml.data import load_customer_history
from src.ml.features import FEATURE_COLUMNS
from src.ml.hybrid import RiskAssessment, assess_transaction
from src.ml.mlflow_tracking import configure_mlflow, get_production_model_uri


class UnknownCustomerError(Exception):
    """Raised when a scoring request references a customer_id, account_id,
    or merchant_id that doesn't exist in the reference data -- the API
    layer maps this to a 404, not a 500 or a silently-wrong prediction."""


@dataclass
class ScoringInput:
    transaction_id: str
    account_id: str
    customer_id: str
    merchant_id: str
    amount: float
    country: str
    device_id: str
    status: str = "APPROVED"
    timestamp: datetime | None = None


def _load_reference_row(settings, table: str, id_column: str, id_value: str) -> dict:
    import psycopg

    with (
        psycopg.connect(
            host=settings.host, port=settings.port, dbname=settings.db,
            user=settings.user, password=settings.password,
        ) as conn,
        conn.cursor() as cur,
    ):
        # table/id_column are internal constants passed by this module's
        # own callers below, never user input -- id_value (the only
        # user-supplied part) is correctly parameterized.
        cur.execute(f"SELECT * FROM {table} WHERE {id_column} = %s", (id_value,))
        row = cur.fetchone()
        if row is None:
            return {}
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))


def compute_features_for_scoring(scoring_input: ScoringInput) -> pd.Series:
    """Builds the point-in-time-correct feature row for a new transaction,
    by reusing engineer_features() over that customer's real prior
    history. Raises UnknownCustomerError if the customer/account/merchant
    aren't found in the reference data.
    """
    settings = get_settings().postgres
    timestamp = scoring_input.timestamp or datetime.now(timezone.utc)

    customer = _load_reference_row(settings, "customers", "customer_id", scoring_input.customer_id)
    account = _load_reference_row(settings, "accounts", "account_id", scoring_input.account_id)
    merchant = _load_reference_row(settings, "merchants", "merchant_id", scoring_input.merchant_id)
    if not customer or not account or not merchant:
        raise UnknownCustomerError(
            f"customer_id={scoring_input.customer_id!r}, "
            f"account_id={scoring_input.account_id!r}, or "
            f"merchant_id={scoring_input.merchant_id!r} not found in reference data"
        )

    history_df = load_customer_history(settings, scoring_input.customer_id, timestamp)

    new_row = pd.DataFrame([{
        "transaction_id": scoring_input.transaction_id,
        "account_id": scoring_input.account_id,
        "customer_id": scoring_input.customer_id,
        "merchant_id": scoring_input.merchant_id,
        "amount": scoring_input.amount,
        "tx_timestamp": pd.Timestamp(timestamp),
        "transaction_country": scoring_input.country,
        "device_id": scoring_input.device_id,
        "status": scoring_input.status,
        "is_fraud": False,
        "fraud_pattern": None,
        "customer_home_country": customer["country"],
        "customer_risk_segment": customer["risk_segment"],
        "merchant_category": merchant["category"],
        "merchant_risk_score_base": merchant["risk_score_base"],
    }])

    from src.ml.features import engineer_features

    combined = pd.concat([history_df, new_row], ignore_index=True)
    featured = engineer_features(combined)
    return featured.iloc[-1]


_model_cache: dict = {}


def _get_production_model():
    """Loads the production-aliased model once and caches it in-process --
    reloading from the MLflow registry on every request would add
    unnecessary latency and registry load for a model that only changes
    when model_training/model_validation (Phase 6) promotes a new version.
    """
    if "model" not in _model_cache:
        import mlflow

        settings = get_settings()
        configure_mlflow(settings.ml)
        model_uri = get_production_model_uri("finguard-fraud-xgboost")
        _model_cache["model"] = mlflow.pyfunc.load_model(model_uri)
    return _model_cache["model"]


def clear_model_cache() -> None:
    """Used by tests, and available for an ops endpoint later, to force a
    reload after a new model is promoted rather than restarting the
    service."""
    _model_cache.clear()


def _feature_row_to_model_input(feature_row: pd.Series) -> pd.DataFrame:
    """Builds a properly-typed one-row DataFrame for model.predict().

    Bug found and fixed here: `feature_row[FEATURE_COLUMNS].to_frame().T`
    looks correct but isn't -- a pandas Series can only hold ONE dtype, so
    extracting a single row (which mixes floats, ints, and bools) as a
    Series collapses every value to `object` dtype. XGBoost's dtype check
    then rejects the whole frame (`DataFrame.dtypes for data must be int,
    float, bool or category`), even though every individual value is
    perfectly valid. Fixed by building the one-row DataFrame from a dict
    (which preserves each column's own type inference) instead.
    """
    X = pd.DataFrame([feature_row[FEATURE_COLUMNS].to_dict()])
    bool_cols = [c for c in FEATURE_COLUMNS if X[c].dtype == bool or c in ("is_unusual_hour", "is_cross_border")]
    numeric_cols = [c for c in FEATURE_COLUMNS if c not in bool_cols]
    X[bool_cols] = X[bool_cols].astype(bool)
    X[numeric_cols] = X[numeric_cols].astype(float)
    return X


def score_transaction(scoring_input: ScoringInput) -> RiskAssessment:
    feature_row = compute_features_for_scoring(scoring_input)
    model = _get_production_model()
    model_input = _feature_row_to_model_input(feature_row)
    xgb_score = float(model.predict(model_input)[0])

    # Isolation Forest isn't registry-promoted in Phase 5/6 (only the
    # supervised model is) -- the hybrid engine degrades gracefully with
    # iforest_score=0.0 (neutral) rather than requiring a second registry
    # lookup for a model this phase doesn't manage the lifecycle of yet.
    iforest_score = 0.0

    return assess_transaction(feature_row, xgb_score, iforest_score)
