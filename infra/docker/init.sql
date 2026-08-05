-- FinGuard Financial -- Phase 1 reference schema
--
-- Scope decision (see docs/adr/003-postgres-scope-phase1.md):
-- Phase 1 persists REFERENCE data (customers, accounts, merchants) and the
-- synthetic TRANSACTIONS batch to Postgres so the generator output is
-- queryable and testable end-to-end. This is NOT yet the streaming serving
-- layer described in the Phase 0 architecture (current_customer_risk,
-- recent_alerts, live features) -- that lands in Phase 3/7 once Spark and
-- the fraud engine exist and can populate it via foreachBatch upserts.
-- Large-scale historical transaction analytics still belongs in Delta Lake
-- (Phase 4), not Postgres; what we persist here at 1M-5M rows is meant for
-- local development/testing, not as the production analytical store.

CREATE TABLE IF NOT EXISTS customers (
    customer_id      TEXT PRIMARY KEY,
    full_name_hash   TEXT NOT NULL,
    country           TEXT NOT NULL,
    signup_date       TIMESTAMPTZ NOT NULL,
    risk_segment      TEXT NOT NULL CHECK (risk_segment IN ('LOW', 'MEDIUM', 'HIGH'))
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id      TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    account_type    TEXT NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    home_country    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id       TEXT PRIMARY KEY,
    merchant_name     TEXT NOT NULL,
    category          TEXT NOT NULL,
    country           TEXT NOT NULL,
    risk_score_base   DOUBLE PRECISION NOT NULL CHECK (risk_score_base BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   TEXT PRIMARY KEY,
    account_id       TEXT NOT NULL REFERENCES accounts(account_id),
    customer_id      TEXT NOT NULL REFERENCES customers(customer_id),
    merchant_id      TEXT NOT NULL REFERENCES merchants(merchant_id),
    amount           DOUBLE PRECISION NOT NULL CHECK (amount > 0),
    tx_timestamp     TIMESTAMPTZ NOT NULL,
    country          TEXT NOT NULL,
    device_id        TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('APPROVED', 'DECLINED', 'FAILED')),
    is_fraud         BOOLEAN NOT NULL,
    fraud_pattern    TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_tx_timestamp ON transactions(tx_timestamp);
