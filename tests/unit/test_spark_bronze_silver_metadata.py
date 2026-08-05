import json

from src.streaming.spark.transformations import (
    add_event_date,
    add_ingestion_metadata,
    filter_and_parse_transaction_payloads,
    parse_kafka_envelope,
)


def _envelope(payload: dict) -> bytes:
    return json.dumps({
        "schema_version": "1.0", "event_type": "transaction", "event_id": "e1",
        "event_timestamp": "2026-01-01T00:00:00Z", "payload": json.dumps(payload),
    }).encode()


def _payload(**overrides) -> dict:
    base = {
        "transaction_id": "TX1", "account_id": "A1", "customer_id": "C001",
        "merchant_id": "M1", "amount": 10.0, "timestamp": "2026-02-15T10:00:00Z",
        "country": "FR", "device_id": "D1", "status": "APPROVED",
        "is_fraud": False, "fraud_pattern": None,
    }
    base.update(overrides)
    return base


def test_parse_kafka_envelope_passes_through_topic_partition_offset_when_present(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_payload()), "transactions", 3, 42)],
        ["key", "value", "topic", "partition", "offset"],
    )
    row = parse_kafka_envelope(raw_df).collect()[0]
    assert row["kafka_topic"] == "transactions"
    assert row["kafka_partition"] == "3"
    assert row["kafka_offset"] == "42"


def test_parse_kafka_envelope_tolerates_missing_topic_partition_offset(spark):
    """Test fixtures shaped as plain (key, value) -- like most of this
    suite -- must not break just because they don't simulate the full
    Kafka source schema."""
    raw_df = spark.createDataFrame([(b"C001", _envelope(_payload()))], ["key", "value"])
    row = parse_kafka_envelope(raw_df).collect()[0]
    assert row["kafka_topic"] is None
    assert row["kafka_partition"] is None
    assert row["kafka_offset"] is None


def test_add_ingestion_metadata_adds_timestamp_and_date(spark):
    raw_df = spark.createDataFrame([(b"C001", _envelope(_payload()))], ["key", "value"])
    envelope_df = parse_kafka_envelope(raw_df)
    result = add_ingestion_metadata(envelope_df).collect()[0]
    assert result["ingestion_timestamp"] is not None
    assert result["ingestion_date"] is not None
    # ingestion_date should be today's calendar date, i.e. derived from the
    # timestamp just added, not from the (unrelated) event_timestamp.
    assert str(result["ingestion_date"]) == str(result["ingestion_timestamp"].date())


def test_add_event_date_derives_from_transaction_timestamp_not_ingestion_time(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_payload(timestamp="2026-02-15T10:00:00Z")))], ["key", "value"]
    )
    envelope_df = parse_kafka_envelope(raw_df)
    parsed = filter_and_parse_transaction_payloads(envelope_df)
    result = add_event_date(parsed).collect()[0]
    assert str(result["event_date"]) == "2026-02-15"
