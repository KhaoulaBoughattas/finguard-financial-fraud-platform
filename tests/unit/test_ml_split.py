import pandas as pd
import pytest

from src.ml.split import temporal_split


def _df_with_timestamps(n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "tx_timestamp": pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"),
        "value": range(n),
    })


def test_split_produces_expected_row_counts():
    df = _df_with_timestamps(100)
    train, val, test = temporal_split(df, train_frac=0.7, val_frac=0.15)
    assert len(train) == 70
    assert len(val) == 15
    assert len(test) == 15


def test_split_is_strictly_chronological():
    df = _df_with_timestamps(100)
    train, val, test = temporal_split(df, train_frac=0.7, val_frac=0.15)
    assert train["tx_timestamp"].max() <= val["tx_timestamp"].min()
    assert val["tx_timestamp"].max() <= test["tx_timestamp"].min()


def test_split_sorts_unsorted_input():
    df = _df_with_timestamps(20)
    shuffled = df.sample(frac=1.0, random_state=1).reset_index(drop=True)
    train, val, _test = temporal_split(shuffled, train_frac=0.7, val_frac=0.15)
    assert train["tx_timestamp"].max() <= val["tx_timestamp"].min()


def test_no_row_appears_in_more_than_one_split():
    df = _df_with_timestamps(50)
    train, val, test = temporal_split(df, train_frac=0.6, val_frac=0.2)
    train_ids = set(train["value"])
    val_ids = set(val["value"])
    test_ids = set(test["value"])
    assert not (train_ids & val_ids)
    assert not (val_ids & test_ids)
    assert not (train_ids & test_ids)
    assert len(train_ids) + len(val_ids) + len(test_ids) == 50


def test_invalid_fractions_raise():
    df = _df_with_timestamps(10)
    with pytest.raises(ValueError):
        temporal_split(df, train_frac=0.8, val_frac=0.3)  # sums > 1
