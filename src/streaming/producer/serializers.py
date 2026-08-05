"""JSON (de)serialization for Kafka messages.

Kept as pure functions with no Kafka dependency so they're trivially unit
testable without a broker.
"""
from __future__ import annotations

import json

from pydantic import BaseModel

from src.streaming.common.events import (
    EnvelopeValidationError,
    EventEnvelope,
    EventType,
)


def serialize_envelope(event_type: EventType, payload: BaseModel | dict) -> bytes:
    """Wrap a payload in an EventEnvelope and serialize to JSON bytes."""
    payload_dict = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    envelope = EventEnvelope(event_type=event_type, payload=payload_dict)
    return envelope.model_dump_json().encode("utf-8")


def deserialize_envelope(raw: bytes) -> EventEnvelope:
    """Parse raw Kafka message bytes into an EventEnvelope.

    Raises EnvelopeValidationError (bad JSON, or an envelope shape that
    fails Pydantic validation) -- callers (the consumer) are expected to
    catch this and route the message to the DLQ.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
        return EventEnvelope.model_validate(data)
    except Exception as exc:
        raise EnvelopeValidationError(f"Failed to parse event envelope: {exc}") from exc


def serialize_key(key: str) -> bytes:
    return key.encode("utf-8")
