import pytest

from src.streaming.producer.topic_config import (
    TOPICS,
    compute_partition,
    extract_key,
    get_topic_spec,
)


def test_all_required_topics_are_defined():
    expected = {
        "transactions", "customers", "accounts",
        "merchant-events", "fraud-alerts", "dead-letter-events",
    }
    assert expected.issubset(set(TOPICS.keys()))


def test_get_topic_spec_raises_for_unknown_topic():
    with pytest.raises(ValueError):
        get_topic_spec("not-a-real-topic")


def test_extract_key_uses_configured_key_field():
    payload = {"customer_id": "C001", "amount": 10.0}
    assert extract_key("transactions", payload) == "C001"


def test_extract_key_raises_when_key_field_missing():
    with pytest.raises(ValueError):
        extract_key("transactions", {"amount": 10.0})


def test_compute_partition_is_deterministic_for_same_key():
    p1 = compute_partition("C001", num_partitions=6)
    p2 = compute_partition("C001", num_partitions=6)
    assert p1 == p2


def test_compute_partition_stays_within_bounds():
    for key in ["C001", "C002", "C003", "some-longer-customer-id-42"]:
        p = compute_partition(key, num_partitions=6)
        assert 0 <= p < 6


def test_same_customer_id_all_transactions_map_to_one_partition():
    """Demonstrates the interview-relevant guarantee: Kafka orders within a
    partition, not globally -- keying by customer_id is what makes
    per-customer ordering possible."""
    customer_key = "C001"
    num_partitions = TOPICS["transactions"].local_partitions
    partitions_seen = {
        compute_partition(customer_key, num_partitions) for _ in range(5)
    }
    assert len(partitions_seen) == 1


def test_different_customers_can_map_to_different_partitions():
    num_partitions = 6
    partitions = {compute_partition(f"C{i:03d}", num_partitions) for i in range(20)}
    # not a strict requirement of the hash, but with 20 keys over 6
    # partitions we expect meaningful spread, not everything colliding on one
    assert len(partitions) > 1
