"""Loads the raw data ML training needs directly from the Postgres
reference tables (Phase 1) -- not the dbt Gold layer.

Deliberate choice, explained in docs/ml.md: feature engineering here needs
row-level, point-in-time-correct computation (each transaction's features
must only use data available strictly BEFORE it, to avoid leakage) which is
naturally expressed as a pandas operation over the raw transaction stream,
not a pre-aggregated Gold fact table. Gold's fact_account_activity, for
example, aggregates over ALL of an account's history including transactions
that happen AFTER the one being scored -- using it as a feature source would
leak future information into the model.
"""
from __future__ import annotations

import pandas as pd
import psycopg

from src.common.config import PostgresSettings

_SELECT_JOIN = """
        select
            t.transaction_id,
            t.account_id,
            a.customer_id,
            t.merchant_id,
            t.amount,
            t.tx_timestamp,
            t.country as transaction_country,
            t.device_id,
            t.status,
            t.is_fraud,
            t.fraud_pattern,
            c.country as customer_home_country,
            c.risk_segment as customer_risk_segment,
            m.category as merchant_category,
            m.risk_score_base as merchant_risk_score_base
        from transactions t
        join accounts a on t.account_id = a.account_id
        join customers c on a.customer_id = c.customer_id
        join merchants m on t.merchant_id = m.merchant_id
"""


def load_training_data(settings: PostgresSettings) -> pd.DataFrame:
    """Returns one row per transaction, joined with customer/account/
    merchant attributes, sorted by tx_timestamp -- the raw material for
    point-in-time feature engineering (see features.py)."""
    query = _SELECT_JOIN + " order by t.tx_timestamp asc"
    with psycopg.connect(
        host=settings.host, port=settings.port, dbname=settings.db,
        user=settings.user, password=settings.password,
    ) as conn:
        df = pd.read_sql(query, conn)
    return df


def load_customer_history(
    settings: PostgresSettings, customer_id: str, before_timestamp,
) -> pd.DataFrame:
    """Returns one row per PRIOR transaction for a single customer, joined
    the same way as load_training_data -- used at serving time (Phase 7's
    API) to compute point-in-time features for a NEW, not-yet-persisted
    transaction. `before_timestamp` filters strictly (< not <=) so the new
    transaction's own point in time never includes itself, matching the
    same leakage discipline features.py already enforces for training.

    Shares the SELECT/JOIN with load_training_data via _SELECT_JOIN --
    real Phase 7 bug found and fixed here: an earlier version duplicated
    the query text by hand, and a subsequent edit meant to ADD this
    function accidentally deleted load_training_data's own `def` line,
    silently orphaning its body and breaking every import of this module.
    Deduplicating the SELECT/JOIN into one constant removes the
    copy-paste surface that caused it.
    """
    query = _SELECT_JOIN + (
        " where a.customer_id = %(customer_id)s and t.tx_timestamp < %(before_timestamp)s"
        " order by t.tx_timestamp asc"
    )
    with psycopg.connect(
        host=settings.host, port=settings.port, dbname=settings.db,
        user=settings.user, password=settings.password,
    ) as conn:
        df = pd.read_sql(
            query, conn,
            params={"customer_id": customer_id, "before_timestamp": before_timestamp},
        )
    return df
