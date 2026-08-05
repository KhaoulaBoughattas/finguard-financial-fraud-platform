"""Combines normal transaction generation with injected fraud episodes to
produce the final synthetic transaction stream.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

from src.common.schemas import Account, Merchant, TransactionStatus
from src.generator import fraud_patterns as fp

COUNTRIES_POOL = [
    "US", "GB", "FR", "DE", "TN", "ES", "IT", "NL", "BE", "CA",
    "AE", "SG", "BR", "NG", "IN",
]


def _normal_transaction(account: Account, merchant: Merchant, ts: datetime) -> dict:
    return {
        "transaction_id": f"TX{uuid.uuid4().hex[:16]}",
        "account_id": account.account_id,
        "customer_id": account.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": round(max(1.0, random.gauss(60, 35)), 2),
        "timestamp": ts,
        "country": account.home_country,
        "device_id": f"DEV{random.randint(1000, 1050)}",  # small pool -> "known" devices
        "status": TransactionStatus.APPROVED,
        "is_fraud": False,
        "fraud_pattern": None,
    }


def generate_transactions(
    accounts: list[Account],
    merchants: list[Merchant],
    num_transactions: int,
    fraud_rate: float,
    seed: int,
    days_span: int = 90,
) -> list[dict]:
    """Generate `num_transactions` total records, with approximately
    `fraud_rate` of them belonging to a labeled fraud episode.

    Fraud is injected as *episodes* (a burst of 1-12 related transactions
    sharing a pattern) rather than independently-random rows, because real
    fraud is behavioral, not row-level noise.
    """
    random.seed(seed + 3)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_span)

    target_fraud_records = int(num_transactions * fraud_rate)
    records: list[dict] = []

    pattern_fns = [
        "large_amount_outlier", "impossible_travel", "transaction_velocity",
        "unusual_merchant_category", "unusual_hour", "multi_country_burst",
        "account_takeover", "repeated_failed_attempts",
    ]

    # 1. Inject fraud episodes until we reach (approximately) the target count.
    fraud_records_so_far = 0
    while fraud_records_so_far < target_fraud_records:
        account = random.choice(accounts)
        merchant = random.choice(merchants)
        base_time = start + timedelta(
            seconds=random.randint(0, days_span * 24 * 3600)
        )
        pattern = random.choice(pattern_fns)

        if pattern == "large_amount_outlier":
            episode = fp.large_amount_outlier(account, merchant, 60.0, base_time)
        elif pattern == "impossible_travel":
            episode = fp.impossible_travel(account, merchants, base_time, COUNTRIES_POOL)
        elif pattern == "transaction_velocity":
            episode = fp.transaction_velocity(account, merchants, base_time)
        elif pattern == "unusual_merchant_category":
            episode = fp.unusual_merchant_category(account, merchants, base_time)
        elif pattern == "unusual_hour":
            episode = fp.unusual_hour(account, merchant, base_time)
        elif pattern == "multi_country_burst":
            episode = fp.multi_country_burst(account, merchants, base_time, COUNTRIES_POOL)
        elif pattern == "account_takeover":
            foreign = random.choice([c for c in COUNTRIES_POOL if c != account.home_country])
            episode = fp.account_takeover(account, merchants, base_time, foreign)
        else:  # repeated_failed_attempts
            episode = fp.repeated_failed_attempts(account, merchant, base_time)

        records.extend(episode)
        fraud_records_so_far += len(episode)

    # 2. Fill the remainder with normal transactions.
    remaining = max(0, num_transactions - len(records))
    for _ in range(remaining):
        account = random.choice(accounts)
        merchant = random.choice(merchants)
        ts = start + timedelta(seconds=random.randint(0, days_span * 24 * 3600))
        records.append(_normal_transaction(account, merchant, ts))

    random.shuffle(records)
    return records
