"""Spark schemas for parsing Kafka message values.

These deliberately mirror `src/streaming/common/events.py` (EventEnvelope)
and `src/common/schemas.py` (Transaction) -- Spark needs an explicit
StructType to parse JSON via from_json, it can't import the Pydantic models
directly (those run in the driver/client Python process, not as a JVM-side
parsing schema). Keeping the field lists visibly aligned with those modules
(rather than auto-generating one from the other, which would be more
"magic" than this project needs) is the deliberate trade-off here.
"""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Mirrors EventEnvelope in src/streaming/common/events.py.
# `payload` is intentionally left as a raw JSON string here rather than a
# nested StructType: different event_types (transaction/customer/account/
# merchant_event) have different payload shapes, and Spark's from_json can't
# apply a schema conditionally per-row. The payload string is parsed with
# its OWN schema (below) in a second from_json pass, after filtering to
# transaction events. See transformations.py.
ENVELOPE_SCHEMA = StructType([
    StructField("schema_version", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_id", StringType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("payload", StringType(), nullable=False),  # raw JSON, parsed separately
])

# Mirrors Transaction in src/common/schemas.py (Schema V1 baseline).
# All fields nullable=True deliberately: Spark's from_json returns NULL for
# a field that fails to parse/cast rather than raising, which is exactly
# what lets Silver-layer validation (transformations.py) distinguish and
# reject malformed records instead of crashing the streaming job.
TRANSACTION_PAYLOAD_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("account_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("merchant_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("device_id", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("is_fraud", BooleanType(), nullable=True),
    StructField("fraud_pattern", StringType(), nullable=True),
])
