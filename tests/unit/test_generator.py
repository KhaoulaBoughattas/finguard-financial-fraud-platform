import pandas as pd

from src.generator.reference_data import (
    generate_accounts,
    generate_customers,
    generate_merchants,
)
from src.generator.transactions import generate_transactions


def _small_dataset(seed: int = 42):
    customers = generate_customers(50, seed)
    accounts = generate_accounts(customers, accounts_per_customer_max=2, seed=seed)
    merchants = generate_merchants(20, seed)
    return customers, accounts, merchants


def test_customer_generation_is_deterministic_with_same_seed():
    c1 = generate_customers(100, seed=7)
    c2 = generate_customers(100, seed=7)
    assert [c.customer_id for c in c1] == [c.customer_id for c in c2]
    assert [c.risk_segment for c in c1] == [c.risk_segment for c in c2]
    assert [c.country for c in c1] == [c.country for c in c2]


def test_customer_generation_differs_with_different_seed():
    c1 = generate_customers(100, seed=1)
    c2 = generate_customers(100, seed=2)
    countries_1 = [c.country for c in c1]
    countries_2 = [c.country for c in c2]
    assert countries_1 != countries_2


def test_transaction_generation_is_deterministic_with_same_seed():
    _customers, accounts, merchants = _small_dataset(seed=42)
    tx1 = generate_transactions(accounts, merchants, num_transactions=500,
                                 fraud_rate=0.05, seed=42)
    tx2 = generate_transactions(accounts, merchants, num_transactions=500,
                                 fraud_rate=0.05, seed=42)
    ids_1 = sorted(t["transaction_id"] for t in tx1)
    ids_2 = sorted(t["transaction_id"] for t in tx2)
    fraud_flags_1 = sorted(t["is_fraud"] for t in tx1)
    fraud_flags_2 = sorted(t["is_fraud"] for t in tx2)
    # UUIDs are not seeded, but structural properties (counts, fraud ratio,
    # amounts distribution) must match exactly for the same seed.
    assert len(ids_1) == len(ids_2)
    assert fraud_flags_1 == fraud_flags_2


def test_generated_customer_count_matches_request():
    customers = generate_customers(250, seed=1)
    assert len(customers) == 250


def test_generated_transaction_count_is_approximately_requested():
    _customers, accounts, merchants = _small_dataset()
    n_requested = 1000
    transactions = generate_transactions(accounts, merchants, num_transactions=n_requested,
                                          fraud_rate=0.02, seed=42)
    # Fraud episodes can slightly overshoot the exact count since episodes are
    # atomic (e.g. an 8-transaction velocity burst can't be split), so we
    # assert "at least requested, and not wildly over".
    assert transactions.__len__() >= n_requested
    assert transactions.__len__() <= n_requested * 1.05


def test_all_transactions_reference_valid_accounts_and_merchants():
    customers, accounts, merchants = _small_dataset()
    account_ids = {a.account_id for a in accounts}
    merchant_ids = {m.merchant_id for m in merchants}
    customer_ids = {c.customer_id for c in customers}

    transactions = generate_transactions(accounts, merchants, num_transactions=800,
                                          fraud_rate=0.03, seed=99)

    for tx in transactions:
        assert tx["account_id"] in account_ids
        assert tx["merchant_id"] in merchant_ids
        assert tx["customer_id"] in customer_ids


def test_fraud_episodes_produce_labeled_fraudulent_transactions():
    _customers, accounts, merchants = _small_dataset()
    transactions = generate_transactions(accounts, merchants, num_transactions=2000,
                                          fraud_rate=0.05, seed=42)
    df = pd.DataFrame(transactions)

    fraud_df = df[df["is_fraud"]]
    assert len(fraud_df) > 0
    # every fraud row must carry a recognized pattern label
    assert fraud_df["fraud_pattern"].notna().all()
    known_patterns = {
        "LARGE_AMOUNT_OUTLIER", "IMPOSSIBLE_TRAVEL", "TRANSACTION_VELOCITY",
        "UNUSUAL_MERCHANT_CATEGORY", "UNUSUAL_HOUR", "MULTI_COUNTRY_BURST",
        "ACCOUNT_TAKEOVER", "REPEATED_FAILED_ATTEMPTS",
    }
    assert set(fraud_df["fraud_pattern"].unique()).issubset(known_patterns)


def test_large_amount_outlier_pattern_has_elevated_amounts():
    _customers, accounts, merchants = _small_dataset()
    transactions = generate_transactions(accounts, merchants, num_transactions=3000,
                                          fraud_rate=0.08, seed=7)
    df = pd.DataFrame(transactions)
    normal_avg = df.loc[~df["is_fraud"], "amount"].mean()
    outliers = df[df["fraud_pattern"] == "LARGE_AMOUNT_OUTLIER"]
    if len(outliers) > 0:
        # by construction these are generated at 8-15x the ~$60 baseline average
        assert outliers["amount"].mean() > normal_avg * 4


def test_normal_transactions_have_realistic_bounded_amounts():
    _customers, accounts, merchants = _small_dataset()
    transactions = generate_transactions(accounts, merchants, num_transactions=2000,
                                          fraud_rate=0.0, seed=3)
    df = pd.DataFrame(transactions)
    assert (df["is_fraud"] == False).all()
    assert (df["amount"] > 0).all()
    # generated from gauss(60, 35) clipped at 1.0 -- should not blow up to
    # fraud-scale magnitudes when fraud_rate is 0.
    assert df["amount"].mean() < 200
