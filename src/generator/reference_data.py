"""Generates the reference (slowly-changing) entities: customers, accounts,
merchants. Transaction generation and fraud injection live in separate
modules so each has a single responsibility and is independently testable.
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone

from faker import Faker

from src.common.schemas import Account, Customer, Merchant, TransactionCategory

COUNTRIES = [
    "US", "GB", "FR", "DE", "TN", "ES", "IT", "NL", "BE", "CA",
    "AE", "SG", "BR", "NG", "IN",
]

RISK_SEGMENTS = ["LOW", "MEDIUM", "HIGH"]
# Baseline population weights -- most customers are low risk.
RISK_SEGMENT_WEIGHTS = [0.75, 0.20, 0.05]


def _seeded_faker(seed: int) -> Faker:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    return fake


def generate_customers(n: int, seed: int) -> list[Customer]:
    fake = _seeded_faker(seed)
    customers: list[Customer] = []
    start = datetime.now(timezone.utc) - timedelta(days=5 * 365)
    for i in range(n):
        name = fake.name()
        name_hash = hashlib.sha256(name.encode()).hexdigest()[:16]
        signup_offset = random.randint(0, 5 * 365)
        customers.append(
            Customer(
                customer_id=f"CUST{i:07d}",
                full_name_hash=name_hash,
                country=random.choice(COUNTRIES),
                signup_date=start + timedelta(days=signup_offset),
                risk_segment=random.choices(
                    RISK_SEGMENTS, weights=RISK_SEGMENT_WEIGHTS, k=1
                )[0],
            )
        )
    return customers


def generate_accounts(customers: list[Customer], accounts_per_customer_max: int, seed: int) -> list[Account]:
    random.seed(seed + 1)
    accounts: list[Account] = []
    account_types = ["CHECKING", "SAVINGS", "CREDIT_CARD"]
    idx = 0
    for customer in customers:
        num_accounts = random.randint(1, accounts_per_customer_max)
        for _ in range(num_accounts):
            accounts.append(
                Account(
                    account_id=f"ACCT{idx:07d}",
                    customer_id=customer.customer_id,
                    account_type=random.choice(account_types),
                    opened_at=customer.signup_date
                    + timedelta(days=random.randint(0, 30)),
                    home_country=customer.country,
                )
            )
            idx += 1
    return accounts


def generate_merchants(n: int, seed: int) -> list[Merchant]:
    fake = _seeded_faker(seed + 2)
    merchants: list[Merchant] = []
    categories = list(TransactionCategory)
    for i in range(n):
        category = random.choice(categories)
        # Some categories are structurally higher risk (e.g. jewelry, electronics
        # are common fraud targets due to resale value).
        base_risk = {
            TransactionCategory.JEWELRY: 0.35,
            TransactionCategory.ELECTRONICS: 0.25,
            TransactionCategory.ONLINE_RETAIL: 0.15,
            TransactionCategory.TRAVEL: 0.12,
        }.get(category, 0.05)
        merchants.append(
            Merchant(
                merchant_id=f"MERCH{i:06d}",
                merchant_name=fake.company(),
                category=category,
                country=random.choice(COUNTRIES),
                risk_score_base=min(base_risk + random.uniform(-0.03, 0.05), 1.0),
            )
        )
    return merchants
