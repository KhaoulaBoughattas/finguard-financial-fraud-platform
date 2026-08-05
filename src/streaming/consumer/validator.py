"""Validates a consumed message's envelope and payload.

Two-stage validation, matching the Phase 0 streaming pipeline design
(schema validation -> cleaning): first the transport envelope, then the
payload against the schema for its declared event_type. Both stages can
fail independently and both route to the DLQ with a distinct error_type.
"""
from __future__ import annotations

from pydantic import BaseModel, ValidationError

from src.common.schemas import Account, Customer, Merchant, Transaction
from src.streaming.common.events import EventEnvelope, EventType, PayloadValidationError

PAYLOAD_SCHEMAS: dict[EventType, type[BaseModel]] = {
    EventType.TRANSACTION: Transaction,
    EventType.CUSTOMER: Customer,
    EventType.ACCOUNT: Account,
    EventType.MERCHANT_EVENT: Merchant,
}


def validate_payload(envelope: EventEnvelope) -> BaseModel:
    """Validate envelope.payload against the schema for envelope.event_type.

    Returns the validated, typed payload model on success.
    Raises PayloadValidationError on failure, wrapping the underlying
    pydantic ValidationError with context about which event_type it was
    validated against.
    """
    schema = PAYLOAD_SCHEMAS.get(envelope.event_type)
    if schema is None:
        raise PayloadValidationError(
            f"No payload schema registered for event_type '{envelope.event_type}'"
        )
    try:
        return schema.model_validate(envelope.payload)
    except ValidationError as exc:
        raise PayloadValidationError(
            f"Payload failed validation for event_type '{envelope.event_type}': {exc}"
        ) from exc
