import json

from src.streaming.spark.transformations import (
    deduplicate_transactions,
    filter_and_parse_transaction_payloads,
    parse_kafka_envelope,
    split_valid_invalid,
)


def _envelope(payload: dict, event_type: str = "transaction") -> bytes:
    return json.dumps({
        "schema_version": "1.0",
        "event_type": event_type,
        "event_id": "evt-1",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "payload": json.dumps(payload),
    }).encode()


def _valid_payload(**overrides) -> dict:
    base = {
        "transaction_id": "TX1", "account_id": "A1", "customer_id": "C001",
        "merchant_id": "M1", "amount": 42.5, "timestamp": "2026-01-01T00:00:00Z",
        "country": "FR", "device_id": "D1", "status": "APPROVED",
        "is_fraud": False, "fraud_pattern": None,
    }
    base.update(overrides)
    return base


def test_parse_kafka_envelope_extracts_fields(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_valid_payload()))], ["key", "value"]
    )
    result = parse_kafka_envelope(raw_df).collect()
    assert len(result) == 1
    row = result[0]
    assert row["schema_version"] == "1.0"
    assert row["event_type"] == "transaction"
    assert row["kafka_key"] == "C001"


def test_filter_and_parse_keeps_only_transaction_events(spark):
    raw_df = spark.createDataFrame(
        [
            (b"C001", _envelope(_valid_payload())),
            (b"C002", _envelope({"anything": 1}, event_type="customer")),
        ],
        ["key", "value"],
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    assert parsed.count() == 1
    assert parsed.collect()[0]["transaction_id"] == "TX1"


def test_filter_and_parse_extracts_correct_payload_values(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_valid_payload(amount=99.99, country="DE")))],
        ["key", "value"],
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    row = parsed.collect()[0]
    assert row["amount"] == 99.99
    assert row["country"] == "DE"
    assert row["customer_id"] == "C001"


def test_split_valid_invalid_accepts_well_formed_transaction(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_valid_payload()))], ["key", "value"]
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    valid_df, invalid_df = split_valid_invalid(parsed)
    assert valid_df.count() == 1
    assert invalid_df.count() == 0


def test_split_valid_invalid_rejects_negative_amount(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_valid_payload(amount=-50.0)))], ["key", "value"]
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    valid_df, invalid_df = split_valid_invalid(parsed)
    assert valid_df.count() == 0
    reasons = invalid_df.collect()[0]["rejection_reasons"]
    assert "invalid_amount" in reasons


def test_split_valid_invalid_rejects_invalid_status(spark):
    raw_df = spark.createDataFrame(
        [(b"C001", _envelope(_valid_payload(status="NOT_A_STATUS")))], ["key", "value"]
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    _, invalid_df = split_valid_invalid(parsed)
    assert invalid_df.count() == 1
    assert "invalid_status" in invalid_df.collect()[0]["rejection_reasons"]


def test_split_valid_invalid_rejects_missing_customer_id(spark):
    payload = _valid_payload()
    del payload["customer_id"]
    raw_df = spark.createDataFrame([(b"C001", _envelope(payload))], ["key", "value"])
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    _, invalid_df = split_valid_invalid(parsed)
    assert invalid_df.count() == 1
    assert "missing_customer_id" in invalid_df.collect()[0]["rejection_reasons"]


def test_split_valid_invalid_collects_multiple_reasons_at_once(spark):
    payload = _valid_payload(amount=-1.0, status="BOGUS")
    raw_df = spark.createDataFrame([(b"C001", _envelope(payload))], ["key", "value"])
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    _, invalid_df = split_valid_invalid(parsed)
    reasons = invalid_df.collect()[0]["rejection_reasons"]
    assert "invalid_amount" in reasons
    assert "invalid_status" in reasons


def test_deduplicate_transactions_drops_duplicate_transaction_ids(spark):
    raw_df = spark.createDataFrame(
        [
            (b"C001", _envelope(_valid_payload(transaction_id="TX1"))),
            (b"C001", _envelope(_valid_payload(transaction_id="TX1"))),  # duplicate
            (b"C001", _envelope(_valid_payload(transaction_id="TX2"))),
        ],
        ["key", "value"],
    )
    parsed = filter_and_parse_transaction_payloads(parse_kafka_envelope(raw_df))
    valid_df, _ = split_valid_invalid(parsed)
    deduped = deduplicate_transactions(valid_df)
    assert deduped.count() == 2
    ids = {row["transaction_id"] for row in deduped.collect()}
    assert ids == {"TX1", "TX2"}
