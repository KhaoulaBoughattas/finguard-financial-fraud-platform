import pytest

from src.common.schemas import Transaction
from src.streaming.common.events import EventEnvelope, EventType
from src.streaming.consumer.validator import PayloadValidationError, validate_payload


def _valid_transaction_payload() -> dict:
    return {
        "transaction_id": "TX1", "account_id": "A1", "customer_id": "C001",
        "merchant_id": "M1", "amount": 12.5, "timestamp": "2026-01-01T00:00:00Z",
        "country": "FR", "device_id": "DEV1", "status": "APPROVED",
        "is_fraud": False, "fraud_pattern": None,
    }


def test_validate_payload_accepts_valid_transaction():
    env = EventEnvelope(event_type=EventType.TRANSACTION, payload=_valid_transaction_payload())
    result = validate_payload(env)
    assert isinstance(result, Transaction)
    assert result.transaction_id == "TX1"


def test_validate_payload_rejects_missing_required_field():
    payload = _valid_transaction_payload()
    del payload["amount"]
    env = EventEnvelope(event_type=EventType.TRANSACTION, payload=payload)
    with pytest.raises(PayloadValidationError):
        validate_payload(env)


def test_validate_payload_rejects_invalid_status_enum():
    payload = _valid_transaction_payload()
    payload["status"] = "NOT_A_STATUS"
    env = EventEnvelope(event_type=EventType.TRANSACTION, payload=payload)
    with pytest.raises(PayloadValidationError):
        validate_payload(env)


def test_validate_payload_raises_for_unregistered_event_type():
    env = EventEnvelope(event_type=EventType.FRAUD_ALERT, payload={"anything": 1})
    with pytest.raises(PayloadValidationError):
        validate_payload(env)
