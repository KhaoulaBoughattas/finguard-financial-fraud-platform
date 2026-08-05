"""The event envelope every Kafka message is wrapped in, plus DLQ message
construction. Kept separate from src/common/schemas.py deliberately: those
are the *payload* schemas (Transaction, Customer, ...); this is the
*transport* envelope wrapped around any payload, and does not duplicate
payload field definitions.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

CURRENT_SCHEMA_VERSION = "1.0"


class EnvelopeValidationError(Exception):
    """Raised when the outer envelope itself is malformed (bad JSON,
    missing schema_version/event_type, etc.)."""


class PayloadValidationError(Exception):
    """Raised when the envelope parses but its payload fails validation
    against the schema for its declared event_type."""


class EventType(str, Enum):
    TRANSACTION = "transaction"
    CUSTOMER = "customer"
    ACCOUNT = "account"
    MERCHANT_EVENT = "merchant_event"
    FRAUD_ALERT = "fraud_alert"
    DEAD_LETTER = "dead_letter"


class EventEnvelope(BaseModel):
    """Wraps every payload published to Kafka.

    schema_version supports the evolution strategy documented in
    docs/kafka.md and docs/adr/004-kafka-streaming.md: consumers can branch
    on schema_version to decide how to parse `payload`, so V1 and V2
    producers/consumers can coexist during a rollout.
    """

    schema_version: str = Field(default=CURRENT_SCHEMA_VERSION)
    event_type: EventType
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any]


class DeadLetterEvent(BaseModel):
    """DLQ record -- carries enough context to diagnose and potentially
    replay the failure without needing to correlate against broker logs."""

    original_topic: str
    partition: int | None
    offset: int | None
    event_id: str | None
    error_type: str
    error_message: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    original_payload: Any  # kept as raw/opaque -- may not even be valid JSON structure


def build_dlq_event(
    original_topic: str,
    error: Exception,
    original_payload: Any,
    partition: int | None = None,
    offset: int | None = None,
    event_id: str | None = None,
) -> DeadLetterEvent:
    return DeadLetterEvent(
        original_topic=original_topic,
        partition=partition,
        offset=offset,
        event_id=event_id,
        error_type=type(error).__name__,
        error_message=str(error),
        original_payload=original_payload,
    )
