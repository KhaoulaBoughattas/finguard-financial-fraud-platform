import pytest

from src.streaming.common.events import EnvelopeValidationError, EventType
from src.streaming.producer.serializers import (
    deserialize_envelope,
    serialize_envelope,
    serialize_key,
)


def test_serialize_then_deserialize_roundtrips_payload():
    payload = {"transaction_id": "TX1", "amount": 10.0}
    raw = serialize_envelope(EventType.TRANSACTION, payload)
    env = deserialize_envelope(raw)
    assert env.event_type == EventType.TRANSACTION
    assert env.payload == payload


def test_serialize_accepts_pydantic_model():
    from src.common.schemas import Merchant, TransactionCategory

    merchant = Merchant(
        merchant_id="M1", merchant_name="Acme", category=TransactionCategory.GROCERY,
        country="FR", risk_score_base=0.1,
    )
    raw = serialize_envelope(EventType.MERCHANT_EVENT, merchant)
    env = deserialize_envelope(raw)
    assert env.payload["merchant_id"] == "M1"


def test_deserialize_rejects_malformed_json():
    with pytest.raises(EnvelopeValidationError):
        deserialize_envelope(b"{not valid json")


def test_deserialize_rejects_envelope_missing_required_fields():
    with pytest.raises(EnvelopeValidationError):
        deserialize_envelope(b'{"schema_version": "1.0"}')  # missing event_type/payload


def test_serialize_key_returns_utf8_bytes():
    assert serialize_key("C001") == b"C001"
