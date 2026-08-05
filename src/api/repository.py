"""Read-only database access for the API layer.

Queries the dbt Gold layer (`public_marts` schema, Phase 4) for
customer-risk and fraud-alert lookups, and the raw Postgres reference
tables (Phase 1) for transaction lookups -- matching the Phase 0 serving-
layer principle that the API reads from serving/analytical tables, never
recomputing business logic inline in a route handler.
"""
from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from src.common.config import PostgresSettings


def _connect(settings: PostgresSettings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.host, port=settings.port, dbname=settings.db,
        user=settings.user, password=settings.password, row_factory=dict_row,
    )


def check_database_health(settings: PostgresSettings) -> bool:
    try:
        with _connect(settings) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:  # noqa: BLE001 -- health check: any failure means "unhealthy",
        # the caller doesn't need to distinguish which failure.
        return False


def get_customer_risk(settings: PostgresSettings, customer_id: str) -> dict | None:
    query = """
        SELECT customer_id, risk_score, risk_level, risk_reasons,
               effective_from, is_current
        FROM public_marts.fact_customer_risk
        WHERE customer_id = %s AND is_current = true
        LIMIT 1
    """
    with _connect(settings) as conn, conn.cursor() as cur:
        cur.execute(query, (customer_id,))
        return cur.fetchone()


def get_transaction(settings: PostgresSettings, transaction_id: str) -> dict | None:
    query = """
        SELECT t.transaction_id, t.account_id, a.customer_id, t.merchant_id,
               t.amount, t.tx_timestamp, t.country, t.device_id, t.status,
               t.is_fraud, t.fraud_pattern
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        WHERE t.transaction_id = %s
    """
    with _connect(settings) as conn, conn.cursor() as cur:
        cur.execute(query, (transaction_id,))
        return cur.fetchone()


def list_alerts(
    settings: PostgresSettings, customer_id: str | None = None, limit: int = 50
) -> list[dict]:
    limit = max(1, min(limit, 200))  # bounded, never an unbounded scan
    if customer_id:
        query = """
            SELECT alert_id, transaction_id, customer_id, account_id, merchant_id,
                   amount, alert_timestamp, alert_reason, alert_source
            FROM public_marts.fact_fraud_alerts
            WHERE customer_id = %s
            ORDER BY alert_timestamp DESC
            LIMIT %s
        """
        params = (customer_id, limit)
    else:
        query = """
            SELECT alert_id, transaction_id, customer_id, account_id, merchant_id,
                   amount, alert_timestamp, alert_reason, alert_source
            FROM public_marts.fact_fraud_alerts
            ORDER BY alert_timestamp DESC
            LIMIT %s
        """
        params = (limit,)
    with _connect(settings) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()
