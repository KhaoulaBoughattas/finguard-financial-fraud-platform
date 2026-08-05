"""Transformation functions for the Bronze/Silver transaction pipeline.

Every function here takes and returns a DataFrame and has no Kafka/Delta
dependency -- this is what makes them testable with a plain local Spark
session (see tests/unit/test_spark_transformations.py), independent of
whether a real Kafka broker or Delta's Maven artifact is reachable.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.spark.schemas import ENVELOPE_SCHEMA, TRANSACTION_PAYLOAD_SCHEMA


def parse_kafka_envelope(raw_df: DataFrame) -> DataFrame:
    """Parse the Kafka `value` column (bytes) as an EventEnvelope.

    Input: a DataFrame with at least a `value` column (as produced by
    Spark's built-in `kafka` source, or a test fixture shaped the same way).
    Output: one row per input message, with envelope fields as columns,
    the original raw value string (kept for diagnostics if downstream
    parsing fails), and passthrough Kafka metadata (`kafka_key`,
    `kafka_topic`, `kafka_partition`, `kafka_offset`) when the source
    DataFrame provides them -- the real `kafka` source always does; test
    fixtures built from a plain `(key, value)` tuple won't, and those
    columns come back NULL rather than raising, so existing tests that
    don't set up topic/partition/offset are unaffected.
    """
    value_str = F.col("value").cast("string")
    parsed = F.from_json(value_str, ENVELOPE_SCHEMA)
    df = raw_df.withColumn("_raw_value", value_str).withColumn("_envelope", parsed)

    def _passthrough(col_name: str, alias: str):
        if col_name in raw_df.columns:
            return F.col(col_name).cast("string").alias(alias)
        return F.lit(None).cast("string").alias(alias)

    return df.select(
        _passthrough("key", "kafka_key"),
        _passthrough("topic", "kafka_topic"),
        _passthrough("partition", "kafka_partition"),
        _passthrough("offset", "kafka_offset"),
        "_raw_value",
        F.col("_envelope.schema_version").alias("schema_version"),
        F.col("_envelope.event_type").alias("event_type"),
        F.col("_envelope.event_id").alias("event_id"),
        F.col("_envelope.event_timestamp").alias("event_timestamp"),
        F.col("_envelope.payload").alias("payload_json"),
    )


def add_ingestion_metadata(envelope_df: DataFrame) -> DataFrame:
    """Adds `ingestion_timestamp` (Spark processing-time, i.e. when THIS
    job saw the record -- distinct from `event_timestamp`, which is when
    the producer created it) and `ingestion_date` (used for Bronze
    partitioning, see docs/spark-streaming.md §partitioning).

    This is the last step before the Bronze write -- Bronze otherwise
    preserves the envelope exactly as parsed, with no business-rule
    filtering or transformation (that's Silver's job).
    """
    return envelope_df.withColumn("ingestion_timestamp", F.current_timestamp()).withColumn(
        "ingestion_date", F.to_date(F.col("ingestion_timestamp"))
    )


def filter_and_parse_transaction_payloads(envelope_df: DataFrame) -> DataFrame:
    """Filter to event_type='transaction' and parse the nested payload JSON.

    Rows where the envelope itself failed to parse (schema_version is null
    -- from_json returns an all-null struct on unparseable JSON) or where
    event_type isn't 'transaction' are dropped here; a future iteration
    could route those to a Spark-side dead-letter sink mirroring Phase 2's
    Kafka-consumer DLQ, but Phase 2's consumer already guarantees only
    schema-valid transaction envelopes reach this topic in the first place
    -- this filter is a defensive second check, not the primary validation
    boundary.
    """
    tx_envelopes = envelope_df.filter(
        (F.col("schema_version").isNotNull()) & (F.col("event_type") == "transaction")
    )
    payload = F.from_json(F.col("payload_json"), TRANSACTION_PAYLOAD_SCHEMA)
    return tx_envelopes.withColumn("payload", payload).select(
        "kafka_key", "schema_version", "event_id", "event_timestamp",
        F.col("payload.transaction_id").alias("transaction_id"),
        F.col("payload.account_id").alias("account_id"),
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.merchant_id").alias("merchant_id"),
        F.col("payload.amount").alias("amount"),
        F.col("payload.timestamp").alias("tx_timestamp"),
        F.col("payload.country").alias("country"),
        F.col("payload.device_id").alias("device_id"),
        F.col("payload.status").alias("status"),
        F.col("payload.is_fraud").alias("is_fraud"),
        F.col("payload.fraud_pattern").alias("fraud_pattern"),
    )


VALID_STATUSES = ("APPROVED", "DECLINED", "FAILED")


def split_valid_invalid(parsed_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split parsed transaction rows into (valid, invalid) based on the
    same business rules documented in Phase 0's data-quality framework:
    required fields present, amount positive, status recognized, timestamp
    present and not in the future.

    Returns (valid_df, invalid_df) -- invalid_df retains all original
    columns plus a `rejection_reason` column, so it can be routed to a
    quarantine/DLQ-equivalent sink instead of silently dropped.
    """
    now = F.current_timestamp()
    reasons = F.array_compact(
        F.array(
            F.when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id")),
            F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
            F.when(F.col("account_id").isNull(), F.lit("missing_account_id")),
            F.when(F.col("merchant_id").isNull(), F.lit("missing_merchant_id")),
            F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("invalid_amount")),
            F.when(F.col("tx_timestamp").isNull(), F.lit("missing_timestamp")),
            F.when(F.col("tx_timestamp") > now, F.lit("future_timestamp")),
            F.when(~F.col("status").isin(*VALID_STATUSES), F.lit("invalid_status")),
        )
    )
    with_reasons = parsed_df.withColumn("rejection_reasons", reasons)
    valid_df = with_reasons.filter(F.size("rejection_reasons") == 0).drop("rejection_reasons")
    invalid_df = with_reasons.filter(F.size("rejection_reasons") > 0)
    return valid_df, invalid_df


def deduplicate_transactions(valid_df: DataFrame) -> DataFrame:
    """Drop duplicate transaction_ids within the current micro-batch/window.

    Deduplication key: `transaction_id` -- the natural, producer-assigned
    unique identifier for a transaction event (see Phase 2's
    `EventEnvelope`/message-key design). This is deliberately NOT `event_id`
    (a fresh UUID minted per publish call, per src/streaming/common/events.py
    -- two publishes of the "same" logical transaction get different
    event_ids by design, so deduping on it would miss exactly the
    duplicates we need to catch) and NOT any arbitrary column such as
    `(customer_id, amount, timestamp)` (two genuinely different
    transactions can share all three).

    This is the Bronze-adjacent, batch-local half of the idempotency story;
    it does NOT replace the Delta MERGE-based dedup across micro-batches
    (see silver.py) -- Spark's own `dropDuplicates` only sees rows within
    the same call, so cross-batch duplicates (the actual at-least-once
    replay case) must be handled at the Delta write layer, not here.
    """
    return valid_df.dropDuplicates(["transaction_id"])


def add_event_date(valid_df: DataFrame, event_time_col: str = "tx_timestamp") -> DataFrame:
    """Adds `event_date`, the Silver-layer partition column (see
    docs/spark-streaming.md §partitioning for why event date -- not
    customer_id -- was chosen)."""
    return valid_df.withColumn("event_date", F.to_date(F.col(event_time_col)))
