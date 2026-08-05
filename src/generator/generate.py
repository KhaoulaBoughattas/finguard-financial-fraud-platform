"""CLI entrypoint: `python -m src.generator.generate`

Generates customers, accounts, merchants and a labeled transaction stream,
and writes them to Parquet under the configured output directory.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.common.config import get_settings
from src.common.logging import configure_logging, get_logger
from src.generator.reference_data import (
    generate_accounts,
    generate_customers,
    generate_merchants,
)
from src.generator.transactions import generate_transactions

logger = get_logger(__name__)

SCHEMA_PATH = str(Path(__file__).resolve().parents[2] / "infra" / "docker" / "init.sql")


def persist_to_postgres(customers_df, accounts_df, merchants_df, transactions_df) -> dict:
    """Load generated dataframes into PostgreSQL. Isolated so tests/CLI can
    opt in without requiring a running database for pure-generation use."""
    from src.common import (
        db as db_module,  # local import: psycopg is optional at generation time
    )

    settings = get_settings()
    conn = db_module.get_connection(settings.postgres)
    try:
        db_module.apply_schema(conn, SCHEMA_PATH)
        counts = {
            "customers_loaded": db_module.load_customers(conn, customers_df),
            "accounts_loaded": db_module.load_accounts(conn, accounts_df),
            "merchants_loaded": db_module.load_merchants(conn, merchants_df),
            "transactions_loaded": db_module.load_transactions(conn, transactions_df),
        }
        return counts
    finally:
        conn.close()


def publish_to_kafka(customers_df, accounts_df, merchants_df, transactions_df,
                      bootstrap_servers: str) -> dict:
    """Publish generated records to their respective Kafka topics. Isolated
    (like persist_to_postgres) so confluent_kafka is only required when this
    path is actually used -- generation to Parquet never needs a broker."""
    from src.streaming.common.events import (
        EventType,  # local import: keeps Kafka optional
    )
    from src.streaming.producer.producer import FinGuardProducer

    producer = FinGuardProducer(bootstrap_servers=bootstrap_servers, client_id="finguard-generator")
    try:
        for _, row in customers_df.iterrows():
            producer.publish("customers", EventType.CUSTOMER, row.to_dict())
        for _, row in accounts_df.iterrows():
            producer.publish("accounts", EventType.ACCOUNT, row.to_dict())
        for _, row in merchants_df.iterrows():
            producer.publish("merchant-events", EventType.MERCHANT_EVENT, row.to_dict())
        for _, row in transactions_df.iterrows():
            producer.publish("transactions", EventType.TRANSACTION, row.to_dict())
        producer.flush(timeout=30.0)
        return producer.metrics.as_dict()
    finally:
        producer.close()


def run(num_customers: int, num_accounts_max_per_customer: int, num_merchants: int,
        num_transactions: int, fraud_rate: float, seed: int, output_dir: str,
        persist_db: bool = False, publish_kafka: bool = False,
        kafka_bootstrap_servers: str = "localhost:9092") -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    logger.info("Generating customers", extra={"context": {"count": num_customers}})
    customers = generate_customers(num_customers, seed)

    logger.info("Generating accounts")
    accounts = generate_accounts(customers, num_accounts_max_per_customer, seed)

    logger.info("Generating merchants", extra={"context": {"count": num_merchants}})
    merchants = generate_merchants(num_merchants, seed)

    logger.info(
        "Generating transactions",
        extra={"context": {"count": num_transactions, "fraud_rate": fraud_rate}},
    )
    transactions = generate_transactions(
        accounts=accounts,
        merchants=merchants,
        num_transactions=num_transactions,
        fraud_rate=fraud_rate,
        seed=seed,
    )

    customers_df = pd.DataFrame([c.model_dump() for c in customers])
    accounts_df = pd.DataFrame([a.model_dump() for a in accounts])
    merchants_df = pd.DataFrame([m.model_dump() for m in merchants])
    transactions_df = pd.DataFrame(transactions)

    customers_df.to_parquet(out / "customers.parquet", index=False)
    accounts_df.to_parquet(out / "accounts.parquet", index=False)
    merchants_df.to_parquet(out / "merchants.parquet", index=False)
    transactions_df.to_parquet(out / "transactions.parquet", index=False)

    actual_fraud_rate = transactions_df["is_fraud"].mean()
    summary = {
        "customers": len(customers_df),
        "accounts": len(accounts_df),
        "merchants": len(merchants_df),
        "transactions": len(transactions_df),
        "actual_fraud_rate": round(float(actual_fraud_rate), 5),
        "fraud_pattern_counts": transactions_df.loc[
            transactions_df["is_fraud"], "fraud_pattern"
        ].value_counts().to_dict(),
    }

    if persist_db:
        logger.info("Persisting generated data to PostgreSQL")
        db_summary = persist_to_postgres(customers_df, accounts_df, merchants_df, transactions_df)
        summary["postgres"] = db_summary

    if publish_kafka:
        logger.info("Publishing generated data to Kafka", extra={
            "context": {"bootstrap_servers": kafka_bootstrap_servers}
        })
        kafka_summary = publish_to_kafka(
            customers_df, accounts_df, merchants_df, transactions_df, kafka_bootstrap_servers
        )
        summary["kafka"] = kafka_summary

    logger.info("Generation complete", extra={"context": summary})
    return summary


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    parser = argparse.ArgumentParser(description="FinGuard synthetic data generator")
    parser.add_argument("--customers", type=int, default=settings.generator.num_customers)
    parser.add_argument("--merchants", type=int, default=settings.generator.num_merchants)
    parser.add_argument("--transactions", type=int, default=settings.generator.num_transactions)
    parser.add_argument("--fraud-rate", type=float, default=settings.generator.fraud_rate)
    parser.add_argument("--seed", type=int, default=settings.generator.seed)
    parser.add_argument("--output-dir", type=str, default=settings.generator.output_dir)
    parser.add_argument("--max-accounts-per-customer", type=int, default=2)
    parser.add_argument(
        "--persist-db", action="store_true",
        help="Also load generated data into PostgreSQL (requires a reachable database).",
    )
    parser.add_argument(
        "--publish-kafka", action="store_true",
        help="Also publish generated data to Kafka topics (requires a reachable broker).",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers", type=str, default=settings.kafka.bootstrap_servers,
        help="Kafka bootstrap servers, used only when --publish-kafka is set.",
    )
    args = parser.parse_args()

    summary = run(
        num_customers=args.customers,
        num_accounts_max_per_customer=args.max_accounts_per_customer,
        num_merchants=args.merchants,
        num_transactions=args.transactions,
        fraud_rate=args.fraud_rate,
        seed=args.seed,
        output_dir=args.output_dir,
        persist_db=args.persist_db,
        publish_kafka=args.publish_kafka,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
    )
    print(summary)


if __name__ == "__main__":
    main()
