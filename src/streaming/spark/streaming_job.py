"""Phase 3 streaming job: Kafka -> Spark Structured Streaming -> Bronze/Silver.

Usage:
    python -m src.streaming.spark.streaming_job --topic transactions

Requires: a reachable Kafka broker (Phase 2) AND network access to Maven
Central to resolve the Delta Lake Spark connector on first run (see
docs/spark.md -- this was NOT executable in the sandbox this was built in;
see the Phase 3 close-out report).
"""
from __future__ import annotations

import argparse

from src.common.config import get_settings
from src.common.logging import configure_logging, get_logger
from src.streaming.spark.bronze import write_bronze_stream
from src.streaming.spark.session import build_spark_session
from src.streaming.spark.silver import write_silver_stream
from src.streaming.spark.transformations import (
    add_ingestion_metadata,
    parse_kafka_envelope,
)

logger = get_logger(__name__)


def run(topic: str, bootstrap_servers: str, bronze_path: str, silver_path: str,
        bronze_checkpoint: str, silver_checkpoint: str) -> None:
    spark = build_spark_session()

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        # earliest here mirrors Phase 2's consumer default for a brand-new
        # checkpoint (no prior committed offsets); an EXISTING checkpoint
        # always resumes from its own recorded offsets regardless of this
        # setting, exactly like Phase 2's auto.offset.reset semantics.
        .option("startingOffsets", "earliest")
        .load()
    )

    envelope_df = parse_kafka_envelope(raw_stream)
    bronze_input_df = add_ingestion_metadata(envelope_df)

    bronze_query = write_bronze_stream(bronze_input_df, bronze_path, bronze_checkpoint)
    logger.info("Bronze stream started", extra={"context": {"path": bronze_path}})

    bronze_stream_df = spark.readStream.format("delta").load(bronze_path)
    silver_query = write_silver_stream(bronze_stream_df, silver_path, silver_checkpoint)
    logger.info("Silver stream started", extra={"context": {"path": silver_path}})

    bronze_query.awaitTermination()
    silver_query.awaitTermination()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    parser = argparse.ArgumentParser(description="FinGuard Spark streaming job")
    parser.add_argument("--topic", type=str, default="transactions")
    parser.add_argument("--bootstrap-servers", type=str, default=settings.kafka.bootstrap_servers)
    parser.add_argument("--bronze-path", type=str, default=settings.delta.bronze_transactions_path)
    parser.add_argument("--silver-path", type=str, default=settings.delta.silver_transactions_path)
    parser.add_argument("--bronze-checkpoint", type=str, default=settings.delta.bronze_checkpoint_path)
    parser.add_argument("--silver-checkpoint", type=str, default=settings.delta.silver_checkpoint_path)
    args = parser.parse_args()

    run(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        bronze_checkpoint=args.bronze_checkpoint,
        silver_checkpoint=args.silver_checkpoint,
    )


if __name__ == "__main__":
    main()
