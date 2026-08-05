"""Temporal train/validation/test split.

Financial transaction data is a time series, not i.i.d. samples -- a random
split (sklearn's default train_test_split) would let the model train on
transactions that happen chronologically AFTER some of its test
transactions, which is a second, more subtle form of data leakage on top
of the point-in-time feature leakage features.py guards against: even with
leakage-free features, evaluating on the past after training on the future
gives an unrealistically optimistic picture of how the model would perform
in production, where it only ever sees the future.

Split is chronological: earliest N% -> train, next M% -> validation
(threshold tuning, early stopping), latest remainder -> test (final,
untouched-until-evaluation holdout).
"""
from __future__ import annotations

import pandas as pd


def temporal_split(
    df: pd.DataFrame,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
    time_col: str = "tx_timestamp",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split `df` chronologically into (train, val, test).

    `df` must already be sorted or sortable by `time_col`; this function
    sorts defensively rather than assuming the caller got it right.
    """
    if not (0 < train_frac < 1) or not (0 < val_frac < 1) or train_frac + val_frac >= 1:
        raise ValueError("train_frac and val_frac must be in (0,1) and sum to < 1")

    df_sorted = df.sort_values(time_col).reset_index(drop=True)
    n = len(df_sorted)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))

    train_df = df_sorted.iloc[:train_end]
    val_df = df_sorted.iloc[train_end:val_end]
    test_df = df_sorted.iloc[val_end:]
    return train_df, val_df, test_df
