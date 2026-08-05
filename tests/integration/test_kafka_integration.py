"""Integration tests against a real Kafka broker.

Skipped automatically if no broker is reachable at KAFKA_BOOTSTRAP_SERVERS
(default localhost:9092) -- consistent with the Postgres integration test
pattern from Phase 1. As documented in the Phase 2 close-out report, this
sandbox has no Kafka broker available, so these tests are expected to be
SKIPPED here. To actually run them:

    docker compose up -d kafka
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest -v tests/integration/test_kafka_integration.py
"""
import os
import time
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from src.streaming.common.events import EventType
from src.streaming.consumer.consumer import FinGuardConsumer
from src.streaming.producer.producer import FinGuardProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _kafka_available() -> bool:
    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS, "socket.timeout.ms": 3000})
        admin.list_topics(timeout=3)
        return True
    except Exception:  # noqa: BLE001 -- availability probe, same rationale as the
        # Postgres one in test_db_integration.py: any failure means "skip".
        return False


pytestmark = pytest.mark.skipif(
    not _kafka_available(),
    reason=(
        f"Kafka not reachable at {BOOTSTRAP_SERVERS} -- start it with "
        "'docker compose up -d kafka' to run these tests"
    ),
)


@pytest.fixture(scope="module")
def ensure_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topics = ["transactions", "dead-letter-events"]
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for future in futures.values():
        try:
            future.result()
        except Exception:  # noqa: BLE001, S110 -- best-effort topic creation; "already
            # exists" and any other create-topic failure are both fine to ignore here,
            # since ensure_topics only needs the topics to exist by the time tests run.
            pass
    return topics


def test_valid_transaction_is_produced_and_consumed(ensure_topics):
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"
    producer = FinGuardProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = FinGuardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic="transactions", group_id=group_id,
    )
    try:
        payload = {
            "transaction_id": f"TX{uuid.uuid4().hex[:8]}", "account_id": "A1",
            "customer_id": "C001", "merchant_id": "M1", "amount": 42.0,
            "timestamp": "2026-01-01T00:00:00Z", "country": "FR", "device_id": "DEV1",
            "status": "APPROVED", "is_fraud": False, "fraud_pattern": None,
        }
        producer.publish("transactions", EventType.TRANSACTION, payload)
        producer.flush(timeout=10)

        processed = False
        deadline = time.time() + 15
        while time.time() < deadline and not processed:
            processed = consumer.poll_once(timeout=1.0)

        assert processed
        assert consumer.metrics.messages_consumed == 1
        assert consumer.metrics.messages_failed == 0
    finally:
        producer.close()
        consumer.close()


def test_invalid_message_is_routed_to_dlq(ensure_topics):
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"
    producer = FinGuardProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = FinGuardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic="transactions", group_id=group_id,
    )
    dlq_consumer = FinGuardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic="dead-letter-events",
        group_id=f"dlq-check-{uuid.uuid4().hex[:8]}",
    )
    try:
        # invalid: missing required fields, will fail payload validation
        invalid_payload = {"transaction_id": "TX_BAD", "customer_id": "C001"}
        producer.publish("transactions", EventType.TRANSACTION, invalid_payload)
        producer.flush(timeout=10)

        processed = False
        deadline = time.time() + 15
        while time.time() < deadline and not processed:
            processed = consumer.poll_once(timeout=1.0)

        assert processed
        assert consumer.metrics.messages_failed == 1
        assert consumer.metrics.messages_sent_to_dlq == 1

        # confirm it actually landed on the DLQ topic
        dlq_seen = False
        deadline = time.time() + 15
        while time.time() < deadline and not dlq_seen:
            dlq_seen = dlq_consumer.poll_once(timeout=1.0)
        assert dlq_seen
    finally:
        producer.close()
        consumer.close()
        dlq_consumer.close()


def test_consumer_group_resumes_from_committed_offset(ensure_topics):
    """Simulates a consumer restart: a second consumer instance in the same
    group must not reprocess what the first already committed."""
    group_id = f"resume-group-{uuid.uuid4().hex[:8]}"
    producer = FinGuardProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    payload = {
        "transaction_id": f"TX{uuid.uuid4().hex[:8]}", "account_id": "A1",
        "customer_id": "C002", "merchant_id": "M1", "amount": 15.0,
        "timestamp": "2026-01-01T00:00:00Z", "country": "FR", "device_id": "DEV1",
        "status": "APPROVED", "is_fraud": False, "fraud_pattern": None,
    }
    producer.publish("transactions", EventType.TRANSACTION, payload)
    producer.flush(timeout=10)
    producer.close()

    consumer1 = FinGuardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic="transactions", group_id=group_id,
    )
    try:
        deadline = time.time() + 15
        got_it = False
        while time.time() < deadline and not got_it:
            got_it = consumer1.poll_once(timeout=1.0)
        assert got_it
    finally:
        consumer1.close()

    # a fresh consumer in the SAME group should see nothing new to process
    # within a short poll window, since the offset was already committed.
    consumer2 = FinGuardConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic="transactions", group_id=group_id,
    )
    try:
        saw_message = consumer2.poll_once(timeout=3.0)
        assert saw_message is False
    finally:
        consumer2.close()
