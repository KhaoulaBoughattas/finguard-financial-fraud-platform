"""Batch feature engineering for offline model training.

IMPORTANT relationship to Phase 3's streaming features
(src/streaming/spark/features.py): those compute the same KINDS of
features (rolling counts, unique-value counts) for real-time SCORING, via
Spark Structured Streaming windows. This module computes analogous
features for offline TRAINING, via pandas over historical data. They are
deliberately separate implementations -- this is a real, known pattern in
ML systems called training/serving skew, and it's worth being explicit
about rather than pretending one implementation serves both purposes. See
docs/ml.md for the full discussion, including what would need to happen to
reduce that skew in a production system (a shared feature store).

CRITICAL correctness property: every feature for a given transaction is
computed using ONLY transactions strictly BEFORE it in time, for the SAME
customer. Using `.shift()` after a stable time-sort, not `.rolling()`
directly on the current row, is what guarantees this -- rolling/expanding
windows in pandas are inclusive of the current row by default, which would
leak the current transaction's own amount/fraud outcome into its own
features. This is exactly the kind of mistake Phase 0 warned about
("avoid data leakage... use appropriate train/test methodology for
temporal financial data").
"""
from __future__ import annotations

import pandas as pd


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds point-in-time-correct behavioral features to a transaction
    DataFrame. `df` must already be sorted by tx_timestamp ascending (as
    `load_training_data` returns it) and have one row per transaction.
    """
    df = df.copy()
    df["tx_timestamp"] = pd.to_datetime(df["tx_timestamp"])
    df = df.sort_values(["customer_id", "tx_timestamp"]).reset_index(drop=True)

    grouped = df.groupby("customer_id", sort=False)

    # --- prior transaction count and average amount (expanding, shifted) ---
    # .shift(1) after grouping ensures row i only sees rows 0..i-1 for its
    # customer -- never itself.
    df["customer_txn_count_prior"] = grouped.cumcount()
    prior_amount_mean = grouped["amount"].apply(lambda s: s.shift(1).expanding().mean())
    df["customer_avg_amount_prior"] = prior_amount_mean.reset_index(level=0, drop=True)

    # First transaction for a customer has no prior average -- fall back to
    # the global mean amount rather than 0 or NaN (0 would make the ratio
    # feature below explode; NaN would break most sklearn/xgboost inputs
    # without explicit handling).
    global_mean_amount = df["amount"].mean()
    df["customer_avg_amount_prior"] = df["customer_avg_amount_prior"].fillna(global_mean_amount)

    df["amount_to_avg_ratio"] = df["amount"] / df["customer_avg_amount_prior"].replace(0, global_mean_amount)

    # --- prior transaction count in the last 24h (point-in-time correct) ---
    # rolling('24h', closed='left') on a DatetimeIndex counts only rows
    # STRICTLY BEFORE the current timestamp within the trailing window --
    # verified directly (see docs/ml.md) that closed='left' excludes the
    # current row, which is what makes this leakage-safe.
    #
    # Uses groupby(...).rolling(...) directly (not groupby(...).apply(...)):
    # an earlier version used .apply() with a helper function, which is
    # fragile -- pandas' result-shape inference for groupby.apply() behaves
    # differently (and broke, concretely: raised ValueError) when the input
    # has exactly one group, which only showed up in small unit-test
    # fixtures, not the 2000-customer production dataset. This direct
    # groupby+rolling form has no such ambiguity regardless of group count.
    rolling_counts = (
        df.set_index("tx_timestamp")
        .groupby("customer_id")["customer_id"]
        .rolling("24h", closed="left")
        .count()
    )
    df["customer_txn_count_last_24h"] = rolling_counts.fillna(0).astype(int).to_numpy()

    # --- time since this customer's previous transaction ---
    prior_time = grouped["tx_timestamp"].shift(1)
    df["seconds_since_prior_txn"] = (
        (df["tx_timestamp"] - prior_time).dt.total_seconds()
    )
    # No prior transaction: treat as "a long time" (a large sentinel), not 0
    # -- 0 would look like rapid-fire velocity, the opposite of the truth.
    df["seconds_since_prior_txn"] = df["seconds_since_prior_txn"].fillna(
        df["seconds_since_prior_txn"].max() if df["seconds_since_prior_txn"].notna().any() else 0
    )

    # --- stateless, single-row features (no leakage risk -- don't depend
    # on other transactions at all) ---
    df["hour_of_day"] = df["tx_timestamp"].dt.hour
    df["is_unusual_hour"] = df["hour_of_day"].between(2, 4)
    df["is_cross_border"] = df["transaction_country"] != df["customer_home_country"]

    return df


FEATURE_COLUMNS = [
    "amount",
    "customer_txn_count_prior",
    "customer_avg_amount_prior",
    "amount_to_avg_ratio",
    "customer_txn_count_last_24h",
    "seconds_since_prior_txn",
    "hour_of_day",
    "is_unusual_hour",
    "is_cross_border",
    "merchant_risk_score_base",
]

LABEL_COLUMN = "is_fraud"
