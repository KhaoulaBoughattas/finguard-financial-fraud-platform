"""API request/response schemas.

Deliberately separate from src/common/schemas.py (the Kafka/generator
payload schemas) and src/streaming/common/events.py (the envelope) --
this is the HTTP API's own contract, which is allowed to differ from the
internal event schema (e.g. it doesn't need is_fraud/fraud_pattern, which
are generator-only ground-truth fields that make no sense in a real
scoring request).
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ScoreTransactionRequest(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    account_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    merchant_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    country: str = Field(..., min_length=2, max_length=2)
    device_id: str = Field(..., min_length=1)
    status: str = Field(default="APPROVED")
    timestamp: datetime | None = Field(
        default=None, description="Defaults to now if omitted."
    )


class ScoreTransactionResponse(BaseModel):
    transaction_id: str
    risk_score: float
    risk_level: str
    decision: str
    reasons: list[str]


class CustomerRiskResponse(BaseModel):
    customer_id: str
    risk_score: float
    risk_level: str
    risk_reasons: list[str]
    effective_from: datetime | None
    is_current: bool


class TransactionResponse(BaseModel):
    transaction_id: str
    account_id: str
    customer_id: str
    merchant_id: str
    amount: float
    tx_timestamp: datetime
    country: str
    device_id: str
    status: str
    is_fraud: bool
    fraud_pattern: str | None


class AlertResponse(BaseModel):
    alert_id: str
    transaction_id: str
    customer_id: str
    account_id: str
    merchant_id: str
    amount: float
    alert_timestamp: datetime
    alert_reason: str | None
    alert_source: str


class HealthResponse(BaseModel):
    status: str
    database: str
    model: str


class ErrorResponse(BaseModel):
    detail: str
