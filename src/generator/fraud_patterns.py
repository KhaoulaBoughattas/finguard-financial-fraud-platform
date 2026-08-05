"""Deliberate, documented fraud pattern generators.

Each function returns a list of Transaction-shaped dicts representing one
fraud "episode" for a given customer/account, all tagged with `is_fraud=True`
and a `fraud_pattern` label. These are injected into the otherwise-normal
transaction stream by generate_transactions(), at a rate controlled by
GeneratorSettings.fraud_rate. Patterns are intentionally structured (not
random noise) so downstream feature engineering has something real to learn.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

from src.common.schemas import Account, Merchant, TransactionCategory, TransactionStatus

FRAUD_PATTERNS = [
    "LARGE_AMOUNT_OUTLIER",
    "IMPOSSIBLE_TRAVEL",
    "TRANSACTION_VELOCITY",
    "UNUSUAL_MERCHANT_CATEGORY",
    "UNUSUAL_HOUR",
    "MULTI_COUNTRY_BURST",
    "ACCOUNT_TAKEOVER",
    "REPEATED_FAILED_ATTEMPTS",
]


def _tx(account: Account, merchant: Merchant, amount: float, ts: datetime,
        country: str, status: TransactionStatus, pattern: str,
        device_id: str) -> dict:
    return {
        "transaction_id": f"TX{uuid.uuid4().hex[:16]}",
        "account_id": account.account_id,
        "customer_id": account.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": round(amount, 2),
        "timestamp": ts,
        "country": country,
        "device_id": device_id,
        "status": status,
        "is_fraud": True,
        "fraud_pattern": pattern,
    }


def large_amount_outlier(account: Account, merchant: Merchant, avg_amount: float,
                          base_time: datetime) -> list[dict]:
    amount = avg_amount * random.uniform(8, 15)  # ~8-15x customer average
    return [_tx(account, merchant, amount, base_time, account.home_country,
                TransactionStatus.APPROVED, "LARGE_AMOUNT_OUTLIER", f"DEV{random.randint(1000,9999)}")]


def impossible_travel(account: Account, merchants: list[Merchant],
                       base_time: datetime, countries: list[str]) -> list[dict]:
    """Two transactions in different countries within an impossibly short window."""
    c1, c2 = random.sample(countries, 2)
    m1, m2 = random.sample(merchants, 2)
    t1 = base_time
    t2 = base_time + timedelta(minutes=random.randint(5, 25))
    return [
        _tx(account, m1, random.uniform(20, 200), t1, c1, TransactionStatus.APPROVED,
            "IMPOSSIBLE_TRAVEL", "DEV0001"),
        _tx(account, m2, random.uniform(20, 200), t2, c2, TransactionStatus.APPROVED,
            "IMPOSSIBLE_TRAVEL", "DEV0002"),
    ]


def transaction_velocity(account: Account, merchants: list[Merchant],
                          base_time: datetime) -> list[dict]:
    """Many small transactions in a very short window (card testing)."""
    out = []
    n = random.randint(6, 12)
    for i in range(n):
        m = random.choice(merchants)
        ts = base_time + timedelta(seconds=random.randint(1, 90) * i)
        out.append(_tx(account, m, random.uniform(1, 15), ts, account.home_country,
                        TransactionStatus.APPROVED, "TRANSACTION_VELOCITY", "DEV0003"))
    return out


def unusual_merchant_category(account: Account, merchants: list[Merchant],
                               base_time: datetime) -> list[dict]:
    high_value = [m for m in merchants if m.category == TransactionCategory.JEWELRY]
    m = random.choice(high_value) if high_value else random.choice(merchants)
    return [_tx(account, m, random.uniform(500, 3000), base_time, account.home_country,
                TransactionStatus.APPROVED, "UNUSUAL_MERCHANT_CATEGORY", "DEV0004")]


def unusual_hour(account: Account, merchant: Merchant, base_date: datetime) -> list[dict]:
    ts = base_date.replace(hour=random.choice([2, 3, 4]), minute=random.randint(0, 59))
    return [_tx(account, merchant, random.uniform(50, 400), ts, account.home_country,
                TransactionStatus.APPROVED, "UNUSUAL_HOUR", "DEV0005")]


def multi_country_burst(account: Account, merchants: list[Merchant],
                         base_time: datetime, countries: list[str]) -> list[dict]:
    picked = random.sample(countries, min(4, len(countries)))
    out = []
    for i, country in enumerate(picked):
        m = random.choice(merchants)
        ts = base_time + timedelta(minutes=10 * i)
        out.append(_tx(account, m, random.uniform(30, 250), ts, country,
                        TransactionStatus.APPROVED, "MULTI_COUNTRY_BURST", f"DEV{6000+i}"))
    return out


def account_takeover(account: Account, merchants: list[Merchant],
                      base_time: datetime, foreign_country: str) -> list[dict]:
    """New device, new country, immediately followed by a high-value purchase."""
    m = random.choice(merchants)
    probe = _tx(account, m, random.uniform(1, 5), base_time, foreign_country,
                TransactionStatus.APPROVED, "ACCOUNT_TAKEOVER", "DEV_NEW_0001")
    payout = _tx(account, m, random.uniform(800, 2500),
                 base_time + timedelta(minutes=random.randint(2, 10)), foreign_country,
                 TransactionStatus.APPROVED, "ACCOUNT_TAKEOVER", "DEV_NEW_0001")
    return [probe, payout]


def repeated_failed_attempts(account: Account, merchant: Merchant,
                              base_time: datetime) -> list[dict]:
    out = []
    n = random.randint(4, 8)
    for i in range(n):
        ts = base_time + timedelta(seconds=30 * i)
        status = TransactionStatus.DECLINED if i < n - 1 else TransactionStatus.APPROVED
        out.append(_tx(account, merchant, random.uniform(50, 500), ts, account.home_country,
                        status, "REPEATED_FAILED_ATTEMPTS", "DEV0007"))
    return out
