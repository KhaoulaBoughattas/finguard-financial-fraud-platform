"""Shared fixtures for tests/integration/.

`ensure_sufficient_data` generates a fresh, self-contained dataset before
a test module needs it, rather than trusting whatever happens to already
be in the shared Postgres `transactions` table. Required because
test_db_integration.py's fixture TRUNCATEs those same tables as part of
ITS OWN setup -- without this, test_api.py/test_metrics.py's real_ids
fixtures would non-deterministically fail or reference stale IDs
depending on pytest's file execution order. Same root cause and same fix
pattern as the one found in Phase 5 for test_ml_training_integration.py
(see docs/adr/007-machine-learning.md) -- generalized here into a shared
fixture so every integration test module that needs real reference data
uses the same, order-independent mechanism instead of each re-solving it
(or not) independently.
"""
import pytest


@pytest.fixture(scope="module")
def ensure_sufficient_data():
    from src.generator.generate import run as generate_run

    generate_run(
        num_customers=500, num_accounts_max_per_customer=1, num_merchants=50,
        num_transactions=5000, fraud_rate=0.02, seed=123,
        output_dir="/tmp/finguard_integration_test_data", persist_db=True,
    )
