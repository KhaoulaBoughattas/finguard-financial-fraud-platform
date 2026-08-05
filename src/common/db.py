"""Persistence layer for writing generator output into PostgreSQL.

Kept deliberately simple for Phase 1: bulk upsert via psycopg's execute_values
equivalent (executemany with ON CONFLICT DO NOTHING), no ORM. This is
reference/dev data loading, not the high-throughput streaming write path
that Phase 3/7 will implement with foreachBatch.
"""
from __future__ import annotations

import pandas as pd
import psycopg
from psycopg import sql

from src.common.config import PostgresSettings
from src.common.logging import get_logger

logger = get_logger(__name__)


def get_connection(settings: PostgresSettings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.db,
        user=settings.user,
        password=settings.password,
    )


def apply_schema(conn: psycopg.Connection, schema_path: str) -> None:
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Schema applied", extra={"context": {"schema_path": schema_path}})


def _bulk_upsert(conn: psycopg.Connection, table: str, columns: list[str],
                  rows: list[tuple], conflict_col: str) -> int:
    if not rows:
        return 0
    query = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
        "ON CONFLICT ({conflict}) DO NOTHING"
    ).format(
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        conflict=sql.Identifier(conflict_col),
    )
    with conn.cursor() as cur:
        cur.executemany(query, rows)
    conn.commit()
    return len(rows)


def load_customers(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    cols = ["customer_id", "full_name_hash", "country", "signup_date", "risk_segment"]
    rows = list(df[cols].itertuples(index=False, name=None))
    return _bulk_upsert(conn, "customers", cols, rows, "customer_id")


def load_accounts(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    cols = ["account_id", "customer_id", "account_type", "opened_at", "home_country"]
    rows = list(df[cols].itertuples(index=False, name=None))
    return _bulk_upsert(conn, "accounts", cols, rows, "account_id")


def load_merchants(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    cols = ["merchant_id", "merchant_name", "category", "country", "risk_score_base"]
    rows = list(df[cols].itertuples(index=False, name=None))
    return _bulk_upsert(conn, "merchants", cols, rows, "merchant_id")


def load_transactions(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    db_cols = ["transaction_id", "account_id", "customer_id", "merchant_id", "amount",
               "tx_timestamp", "country", "device_id", "status", "is_fraud", "fraud_pattern"]
    df = df.rename(columns={"timestamp": "tx_timestamp"})
    rows = list(df[db_cols].itertuples(index=False, name=None))
    return _bulk_upsert(conn, "transactions", db_cols, rows, "transaction_id")
