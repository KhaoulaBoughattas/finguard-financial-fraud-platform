from src.streaming.common.events import (
    CURRENT_SCHEMA_VERSION,
    DeadLetterEvent,
    EventEnvelope,
    EventType,
    build_dlq_event,
)


def test_envelope_defaults_schema_version_and_generates_event_id():
    env = EventEnvelope(event_type=EventType.TRANSACTION, payload={"a": 1})
    assert env.schema_version == CURRENT_SCHEMA_VERSION
    assert env.event_id  # non-empty
    assert env.event_timestamp is not None


def test_envelope_two_instances_get_different_event_ids():
    env1 = EventEnvelope(event_type=EventType.TRANSACTION, payload={})
    env2 = EventEnvelope(event_type=EventType.TRANSACTION, payload={})
    assert env1.event_id != env2.event_id


def test_envelope_preserves_arbitrary_payload_dict():
    payload = {"transaction_id": "TX1", "amount": 42.5, "nested": {"a": 1}}
    env = EventEnvelope(event_type=EventType.TRANSACTION, payload=payload)
    assert env.payload == payload


def test_build_dlq_event_captures_error_context():
    err = ValueError("amount must be positive")
    dlq = build_dlq_event(
        original_topic="transactions",
        error=err,
        original_payload={"transaction_id": "TX1", "amount": -5},
        partition=2,
        offset=42,
        event_id="evt-123",
    )
    assert isinstance(dlq, DeadLetterEvent)
    assert dlq.original_topic == "transactions"
    assert dlq.error_type == "ValueError"
    assert "positive" in dlq.error_message
    assert dlq.partition == 2
    assert dlq.offset == 42
    assert dlq.event_id == "evt-123"
    assert dlq.original_payload == {"transaction_id": "TX1", "amount": -5}


def test_dlq_event_serializes_to_json_without_error():
    dlq = build_dlq_event(
        original_topic="transactions",
        error=RuntimeError("boom"),
        original_payload={"x": 1},
    )
    raw = dlq.model_dump_json()
    assert "original_topic" in raw
    assert "boom" in raw
