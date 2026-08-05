"""Integration tests against a real PostgreSQL instance.

These are skipped automatically if no database is reachable (e.g. CI stage
that hasn't started the postgres service), rather than failing the whole
suite -- consistent with keeping unit tests independent of infrastructure.
"""
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from src.common import db as db_module
from src.common.config import get_settings

SCHEMA_PATH = str(Path(__file__).resolve().parents[2] / "infra" / "docker" / "init.sql")


def _db_available() -> bool:
    try:
        settings = get_settings()
        conn = db_module.get_connection(settings.postgres)
        conn.close()
        return True
    except Exception:  # noqa: BLE001 -- availability probe: any failure (auth, DNS,
        # connection refused, ...) means "treat as unavailable and skip", not a
        # specific error type we need to distinguish here.
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason="PostgreSQL not reachable")


@pytest.fixture
def conn():
    settings = get_settings()
    connection = db_module.get_connection(settings.postgres)
    db_module.apply_schema(connection, SCHEMA_PATH)
    # clean slate for the test
    with connection.cursor() as cur:
        cur.execute("TRUNCATE transactions, accounts, merchants, customers CASCADE;")
    connection.commit()
    yield connection
    connection.close()


def test_schema_applies_and_tables_exist(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
        )
        tables = {row[0] for row in cur.fetchall()}
    assert {"customers", "accounts", "merchants", "transactions"}.issubset(tables)


def test_load_customers_accounts_merchants_transactions_roundtrip(conn):
    customers_df = pd.DataFrame([{
        "customer_id": "CUST0000001", "full_name_hash": "abc123",
        "country": "FR", "signup_date": pd.Timestamp("2023-01-01", tz="UTC"),
        "risk_segment": "LOW",
    }])
    accounts_df = pd.DataFrame([{
        "account_id": "ACCT0000001", "customer_id": "CUST0000001",
        "account_type": "CHECKING", "opened_at": pd.Timestamp("2023-01-02", tz="UTC"),
        "home_country": "FR",
    }])
    merchants_df = pd.DataFrame([{
        "merchant_id": "MERCH000001", "merchant_name": "Acme", "category": "GROCERY",
        "country": "FR", "risk_score_base": 0.05,
    }])
    transactions_df = pd.DataFrame([{
        "transaction_id": "TX0000000000000001", "account_id": "ACCT0000001",
        "customer_id": "CUST0000001", "merchant_id": "MERCH000001", "amount": 42.5,
        "timestamp": pd.Timestamp("2023-02-01T10:00:00", tz="UTC"), "country": "FR",
        "device_id": "DEV1000", "status": "APPROVED", "is_fraud": False,
        "fraud_pattern": None,
    }])

    n_cust = db_module.load_customers(conn, customers_df)
    n_acct = db_module.load_accounts(conn, accounts_df)
    n_merch = db_module.load_merchants(conn, merchants_df)
    n_tx = db_module.load_transactions(conn, transactions_df)

    assert (n_cust, n_acct, n_merch, n_tx) == (1, 1, 1, 1)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM transactions;")
        assert cur.fetchone()[0] == 1


def test_foreign_key_constraint_is_enforced(conn):
    orphan_transaction = pd.DataFrame([{
        "transaction_id": "TX_ORPHAN", "account_id": "ACCT_DOES_NOT_EXIST",
        "customer_id": "CUST_DOES_NOT_EXIST", "merchant_id": "MERCH_DOES_NOT_EXIST",
        "amount": 10.0, "timestamp": pd.Timestamp.now(tz="UTC"), "country": "US",
        "device_id": "DEV0001", "status": "APPROVED", "is_fraud": False,
        "fraud_pattern": None,
    }])
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db_module.load_transactions(conn, orphan_transaction)
    conn.rollback()
