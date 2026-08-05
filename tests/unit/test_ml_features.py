import pandas as pd

from src.ml.features import FEATURE_COLUMNS, engineer_features


def _tx(transaction_id, customer_id, ts, amount, country="FR", home_country="FR",
        merchant_risk=0.05, is_fraud=False, fraud_pattern=None):
    return {
        "transaction_id": transaction_id, "account_id": f"A_{customer_id}",
        "customer_id": customer_id, "merchant_id": "M1", "amount": amount,
        "tx_timestamp": pd.Timestamp(ts, tz="UTC"), "transaction_country": country,
        "device_id": "D1", "status": "APPROVED", "is_fraud": is_fraud,
        "fraud_pattern": fraud_pattern, "customer_home_country": home_country,
        "customer_risk_segment": "LOW", "merchant_category": "GROCERY",
        "merchant_risk_score_base": merchant_risk,
    }


def test_first_transaction_has_zero_prior_count():
    df = pd.DataFrame([_tx("TX1", "C1", "2026-01-01T10:00:00", 50.0)])
    result = engineer_features(df)
    assert result.iloc[0]["customer_txn_count_prior"] == 0


def test_prior_count_increments_per_customer_transaction():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 50.0),
        _tx("TX2", "C1", "2026-01-02T10:00:00", 60.0),
        _tx("TX3", "C1", "2026-01-03T10:00:00", 70.0),
    ])
    result = engineer_features(df).sort_values("tx_timestamp")
    assert list(result["customer_txn_count_prior"]) == [0, 1, 2]


def test_prior_average_amount_excludes_current_row():
    """The core leakage check: a transaction's own amount must never
    contribute to its own customer_avg_amount_prior feature."""
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 100.0),
        _tx("TX2", "C1", "2026-01-02T10:00:00", 200.0),
        _tx("TX3", "C1", "2026-01-03T10:00:00", 9999.0),  # huge outlier
    ])
    result = engineer_features(df).sort_values("tx_timestamp").reset_index(drop=True)
    # TX3's prior average should be mean(100, 200) = 150, NOT influenced by
    # its own 9999 amount.
    assert result.iloc[2]["customer_avg_amount_prior"] == 150.0


def test_different_customers_do_not_leak_into_each_others_features():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 100.0),
        _tx("TX2", "C2", "2026-01-01T10:05:00", 5000.0),  # different customer, huge amount
        _tx("TX3", "C1", "2026-01-01T10:10:00", 120.0),
    ])
    result = engineer_features(df).sort_values("tx_timestamp").reset_index(drop=True)
    tx3 = result[result["transaction_id"] == "TX3"].iloc[0]
    # C1's second transaction should only average C1's own prior amount (100),
    # never C2's 5000.
    assert tx3["customer_avg_amount_prior"] == 100.0


def test_first_transaction_falls_back_to_global_mean_not_nan_or_zero():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 100.0),
        _tx("TX2", "C2", "2026-01-01T10:00:00", 300.0),
    ])
    result = engineer_features(df)
    global_mean = (100.0 + 300.0) / 2
    assert not result["customer_avg_amount_prior"].isna().any()
    assert (result["customer_avg_amount_prior"] == global_mean).all()


def test_count_last_24h_excludes_transactions_outside_window():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 50.0),
        _tx("TX2", "C1", "2026-01-03T10:00:00", 50.0),  # 48h later -- outside 24h window
        _tx("TX3", "C1", "2026-01-03T15:00:00", 50.0),  # 5h after TX2 -- inside window
    ])
    result = engineer_features(df).sort_values("tx_timestamp").reset_index(drop=True)
    assert result.iloc[0]["customer_txn_count_last_24h"] == 0  # first ever
    assert result.iloc[1]["customer_txn_count_last_24h"] == 0  # TX1 is 48h prior, excluded
    assert result.iloc[2]["customer_txn_count_last_24h"] == 1  # only TX2 is within 24h


def test_count_last_24h_excludes_current_transaction_itself():
    """Point-in-time correctness: a burst of 3 transactions in the same
    minute -- the FIRST of the burst must show 0 prior transactions in the
    window, not 2 (which would mean it's counting itself and its future)."""
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 10.0),
        _tx("TX2", "C1", "2026-01-01T10:00:10", 10.0),
        _tx("TX3", "C1", "2026-01-01T10:00:20", 10.0),
    ])
    result = engineer_features(df).sort_values("tx_timestamp").reset_index(drop=True)
    assert list(result["customer_txn_count_last_24h"]) == [0, 1, 2]


def test_is_cross_border_flag():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 50.0, country="FR", home_country="FR"),
        _tx("TX2", "C1", "2026-01-01T11:00:00", 50.0, country="DE", home_country="FR"),
    ])
    result = engineer_features(df).sort_values("tx_timestamp").reset_index(drop=True)
    assert result.iloc[0]["is_cross_border"] == False
    assert result.iloc[1]["is_cross_border"] == True


def test_unusual_hour_flag():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T03:00:00", 50.0),  # 3am -- unusual
        _tx("TX2", "C1", "2026-01-01T14:00:00", 50.0),  # 2pm -- normal
    ])
    result = engineer_features(df)
    row_3am = result[result["hour_of_day"] == 3].iloc[0]
    row_2pm = result[result["hour_of_day"] == 14].iloc[0]
    assert row_3am["is_unusual_hour"] == True
    assert row_2pm["is_unusual_hour"] == False


def test_all_feature_columns_present_and_no_nans():
    df = pd.DataFrame([
        _tx("TX1", "C1", "2026-01-01T10:00:00", 50.0),
        _tx("TX2", "C1", "2026-01-02T10:00:00", 60.0),
    ])
    result = engineer_features(df)
    for col in FEATURE_COLUMNS:
        assert col in result.columns
    assert not result[FEATURE_COLUMNS].isna().any().any()
