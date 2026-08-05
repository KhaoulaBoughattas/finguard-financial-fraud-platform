"""Kafka topic configuration and message-key strategy for FinGuard.

Centralizing this (rather than scattering topic names / partition counts
across producer and consumer modules) means Phase 3+ code and the DLQ
consumer both agree on the same source of truth.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    # Local development values -- see docs/kafka.md for the full
    # local-vs-production comparison and rationale.
    local_partitions: int
    local_replication_factor: int
    retention_ms: int
    cleanup_policy: str
    key_field: str  # the payload field used to derive the Kafka message key
    key_rationale: str


TOPICS: dict[str, TopicSpec] = {
    "transactions": TopicSpec(
        name="transactions",
        local_partitions=6,
        local_replication_factor=1,
        retention_ms=7 * 24 * 60 * 60 * 1000,  # 7 days
        cleanup_policy="delete",
        key_field="customer_id",
        key_rationale=(
            "Keying by customer_id guarantees all of one customer's transactions "
            "land on the same partition, so Kafka's per-partition ordering "
            "guarantee gives us per-customer event ordering -- required for "
            "correct velocity/behavioral feature computation in Phase 3. "
            "Trade-off: a very high-volume single customer could create a hot "
            "partition; acceptable at this scale, revisit if it becomes real."
        ),
    ),
    "customers": TopicSpec(
        name="customers",
        local_partitions=3,
        local_replication_factor=1,
        retention_ms=-1,  # compacted, not time-based
        cleanup_policy="compact",
        key_field="customer_id",
        key_rationale=(
            "Reference/dimension data: compacted so the latest state per "
            "customer_id is retained indefinitely, acting as a changelog."
        ),
    ),
    "accounts": TopicSpec(
        name="accounts",
        local_partitions=3,
        local_replication_factor=1,
        retention_ms=-1,
        cleanup_policy="compact",
        key_field="account_id",
        key_rationale="Compacted reference data, keyed by its own natural id.",
    ),
    "merchant-events": TopicSpec(
        name="merchant-events",
        local_partitions=3,
        local_replication_factor=1,
        retention_ms=-1,
        cleanup_policy="compact",
        key_field="merchant_id",
        key_rationale="Compacted reference data, keyed by its own natural id.",
    ),
    "fraud-alerts": TopicSpec(
        name="fraud-alerts",
        local_partitions=6,
        local_replication_factor=1,
        retention_ms=30 * 24 * 60 * 60 * 1000,  # 30 days -- longer for investigation
        cleanup_policy="delete",
        key_field="customer_id",
        key_rationale=(
            "Keyed by customer_id (not transaction_id) so all alerts for a "
            "customer are ordered relative to each other -- produced by the "
            "fraud engine in Phase 3+, not Phase 2."
        ),
    ),
    "dead-letter-events": TopicSpec(
        name="dead-letter-events",
        local_partitions=3,
        local_replication_factor=1,
        retention_ms=14 * 24 * 60 * 60 * 1000,  # 14 days -- time to investigate
        cleanup_policy="delete",
        key_field="original_topic",
        key_rationale=(
            "Keyed by original_topic so DLQ consumers can selectively replay "
            "or inspect failures from a specific source topic in order."
        ),
    ),
}

# Production target values (documented, not applied locally -- see docs/kafka.md)
PRODUCTION_REPLICATION_FACTOR = 3
PRODUCTION_MIN_ISR = 2


def get_topic_spec(topic: str) -> TopicSpec:
    try:
        return TOPICS[topic]
    except KeyError as exc:
        raise ValueError(f"Unknown topic '{topic}'. Known topics: {list(TOPICS)}") from exc


def extract_key(topic: str, payload: dict) -> str:
    """Extract the Kafka message key for a given topic's payload, per the
    documented key strategy above."""
    spec = get_topic_spec(topic)
    key = payload.get(spec.key_field)
    if key is None:
        raise ValueError(
            f"Payload for topic '{topic}' is missing required key field "
            f"'{spec.key_field}': {payload}"
        )
    return str(key)


def compute_partition(key: str, num_partitions: int) -> int:
    """Reproduce librdkafka/Kafka's default partitioner (murmur2 hash, with
    the top bit cleared) so we can deterministically demonstrate and test
    "same key -> same partition" WITHOUT requiring a running broker.

    This is the same algorithm the Java/librdkafka default partitioner uses:
    `murmur2(key) & 0x7fffffff % num_partitions`.
    """
    return (_murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % num_partitions


def _murmur2(data: bytes) -> int:
    """Murmur2 hash implementation matching Kafka's org.apache.kafka.common
    .utils.Utils.murmur2, used for deterministic key->partition mapping."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = seed ^ length

    length4 = length // 4
    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4] & 0xFF)
            | ((data[i4 + 1] & 0xFF) << 8)
            | ((data[i4 + 2] & 0xFF) << 16)
            | ((data[i4 + 3] & 0xFF) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r)
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    extra = length % 4
    if extra >= 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if extra >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if extra >= 1:
        h ^= (data[length & ~3] & 0xFF)
        h = (h * m) & 0xFFFFFFFF

    h ^= (h >> 13)
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15)

    # convert to signed 32-bit like Java's int
    if h >= 0x80000000:
        h -= 0x100000000
    return h
