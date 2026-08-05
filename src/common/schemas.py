"""Core entity schemas (schema V1 baseline -- see docs/adr for evolution).

These are the canonical Python representations of the records that will
later be published to Kafka. Keeping them as Pydantic models gives us
validation for free in the generator, the producer, and the API.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TransactionCategory(str, Enum):
    GROCERY = "GROCERY"
    ELECTRONICS = "ELECTRONICS"
    TRAVEL = "TRAVEL"
    RESTAURANT = "RESTAURANT"
    UTILITIES = "UTILITIES"
    ENTERTAINMENT = "ENTERTAINMENT"
    HEALTHCARE = "HEALTHCARE"
    ATM_WITHDRAWAL = "ATM_WITHDRAWAL"
    ONLINE_RETAIL = "ONLINE_RETAIL"
    JEWELRY = "JEWELRY"


class TransactionStatus(str, Enum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    FAILED = "FAILED"


class Customer(BaseModel):
    customer_id: str
    full_name_hash: str  # never store raw PII in the synthetic dataset either
    country: str
    signup_date: datetime
    risk_segment: str  # LOW / MEDIUM / HIGH baseline segment used to bias fraud


class Account(BaseModel):
    account_id: str
    customer_id: str
    account_type: str
    opened_at: datetime
    home_country: str


class Merchant(BaseModel):
    merchant_id: str
    merchant_name: str
    category: TransactionCategory
    country: str
    risk_score_base: float = Field(ge=0.0, le=1.0)


class Transaction(BaseModel):
    """Schema V1. V2 will add `currency` and `merchant_category` as optional
    additive fields -- see docs/adr/00X-schema-evolution.md."""

    transaction_id: str
    account_id: str
    customer_id: str
    merchant_id: str
    amount: float
    timestamp: datetime
    country: str
    device_id: str
    status: TransactionStatus
    is_fraud: bool  # ground-truth label, generator-only, never sent to scoring
    fraud_pattern: str | None = None  # which synthetic pattern produced this, if any
