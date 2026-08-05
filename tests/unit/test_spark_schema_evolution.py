"""Demonstrates the documented V1 -> V2 transaction schema evolution
(docs/kafka.md §7, docs/adr/004) at the Spark parsing layer, with a real
Spark session -- not just an assertion about intent.

V1: transaction_id, customer_id, amount
V2: adds currency, merchant_category (optional, additive)
"""
import json

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from src.streaming.spark.schemas import TRANSACTION_PAYLOAD_SCHEMA

# A V2 schema: the same fields as TRANSACTION_PAYLOAD_SCHEMA plus two new,
# nullable, additive columns -- exactly the kind of change docs/kafka.md §7
# calls backward-compatible.
V2_TRANSACTION_PAYLOAD_SCHEMA = StructType(
    TRANSACTION_PAYLOAD_SCHEMA.fields
    + [
        StructField("currency", StringType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
    ]
)


def _v1_payload() -> dict:
    return {
        "transaction_id": "TX1", "account_id": "A1", "customer_id": "C001",
        "merchant_id": "M1", "amount": 42.5, "timestamp": "2026-01-01T00:00:00Z",
        "country": "FR", "device_id": "D1", "status": "APPROVED",
        "is_fraud": False, "fraud_pattern": None,
    }


def _v2_payload() -> dict:
    return {**_v1_payload(), "currency": "EUR", "merchant_category": "GROCERY"}


def test_v1_message_parses_cleanly_against_the_v1_schema(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(json.dumps(_v1_payload()),)], ["payload_json"])
    parsed = df.withColumn("payload", F.from_json("payload_json", TRANSACTION_PAYLOAD_SCHEMA))
    row = parsed.select("payload.*").collect()[0]
    assert row["transaction_id"] == "TX1"
    assert row["amount"] == 42.5


def test_v1_message_parses_against_v2_schema_with_null_new_fields(spark):
    """A V2-aware consumer reading an OLD V1 message: the new columns come
    back as NULL rather than causing a parse failure -- this is what makes
    the change backward-compatible in practice, not just in documentation."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(json.dumps(_v1_payload()),)], ["payload_json"])
    parsed = df.withColumn("payload", F.from_json("payload_json", V2_TRANSACTION_PAYLOAD_SCHEMA))
    row = parsed.select("payload.*").collect()[0]
    assert row["transaction_id"] == "TX1"
    assert row["currency"] is None
    assert row["merchant_category"] is None


def test_v2_message_parses_against_v1_schema_ignoring_new_fields(spark):
    """An OLD V1-only consumer reading a NEW V2 message: from_json simply
    doesn't project the columns it doesn't know about -- no error, no
    crash, matching the "both versions coexist during rollout" claim."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(json.dumps(_v2_payload()),)], ["payload_json"])
    parsed = df.withColumn("payload", F.from_json("payload_json", TRANSACTION_PAYLOAD_SCHEMA))
    row = parsed.select("payload.*").collect()[0]
    assert row["transaction_id"] == "TX1"
    assert row["amount"] == 42.5
    assert "currency" not in row.asDict()


def test_v2_message_parses_against_v2_schema_with_new_fields_populated(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(json.dumps(_v2_payload()),)], ["payload_json"])
    parsed = df.withColumn("payload", F.from_json("payload_json", V2_TRANSACTION_PAYLOAD_SCHEMA))
    row = parsed.select("payload.*").collect()[0]
    assert row["currency"] == "EUR"
    assert row["merchant_category"] == "GROCERY"
