from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.common.schemas import (
    Merchant,
    Transaction,
    TransactionCategory,
    TransactionStatus,
)


def _valid_transaction_kwargs() -> dict:
    return {
        "transaction_id": "TX0000000000000001",
        "account_id": "ACCT0000001",
        "customer_id": "CUST0000001",
        "merchant_id": "MERCH000001",
        "amount": 42.50,
        "timestamp": datetime.now(timezone.utc),
        "country": "FR",
        "device_id": "DEV1000",
        "status": TransactionStatus.APPROVED,
        "is_fraud": False,
        "fraud_pattern": None,
    }


def test_valid_transaction_passes_validation():
    tx = Transaction(**_valid_transaction_kwargs())
    assert tx.amount == 42.50
    assert tx.status == TransactionStatus.APPROVED


def test_transaction_rejects_invalid_amount_type():
    kwargs = _valid_transaction_kwargs()
    kwargs["amount"] = "not-a-number"
    with pytest.raises(ValidationError):
        Transaction(**kwargs)


def test_transaction_rejects_invalid_status_enum():
    kwargs = _valid_transaction_kwargs()
    kwargs["status"] = "NOT_A_REAL_STATUS"
    with pytest.raises(ValidationError):
        Transaction(**kwargs)


def test_transaction_rejects_invalid_timestamp():
    kwargs = _valid_transaction_kwargs()
    kwargs["timestamp"] = "not-a-timestamp"
    with pytest.raises(ValidationError):
        Transaction(**kwargs)


def test_transaction_rejects_missing_required_field():
    kwargs = _valid_transaction_kwargs()
    del kwargs["transaction_id"]
    with pytest.raises(ValidationError):
        Transaction(**kwargs)


def test_merchant_rejects_risk_score_out_of_range():
    with pytest.raises(ValidationError):
        Merchant(
            merchant_id="MERCH000001",
            merchant_name="Acme Co",
            category=TransactionCategory.ELECTRONICS,
            country="US",
            risk_score_base=1.5,  # out of [0, 1] range
        )


def test_merchant_accepts_valid_risk_score():
    m = Merchant(
        merchant_id="MERCH000001",
        merchant_name="Acme Co",
        category=TransactionCategory.ELECTRONICS,
        country="US",
        risk_score_base=0.25,
    )
    assert m.risk_score_base == 0.25
