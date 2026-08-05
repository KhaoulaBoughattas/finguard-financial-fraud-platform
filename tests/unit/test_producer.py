import json

from src.streaming.common.events import EventType
from src.streaming.producer.producer import FinGuardProducer


class _FakeConfluentProducer:
    """Stand-in for confluent_kafka.Producer so producer logic can be unit
    tested without a running broker."""

    def __init__(self, config):
        self.config = config
        self.produced = []
        self.flushed = False

    def produce(self, topic, key, value, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, _FakeMessage(topic))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


class _FakeMessage:
    def __init__(self, topic):
        self._topic = topic

    def topic(self):
        return self._topic

    def partition(self):
        return 0

    def offset(self):
        return 1


def _make_producer_with_fake_backend(monkeypatch) -> FinGuardProducer:
    monkeypatch.setattr(
        "src.streaming.producer.producer.Producer", _FakeConfluentProducer
    )
    return FinGuardProducer(bootstrap_servers="fake:9092")


def test_producer_configures_idempotence_and_acks_all(monkeypatch):
    producer = _make_producer_with_fake_backend(monkeypatch)
    config = producer._producer.config
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["retries"] >= 1


def test_publish_serializes_and_keys_message(monkeypatch):
    producer = _make_producer_with_fake_backend(monkeypatch)
    payload = {"customer_id": "C001", "transaction_id": "TX1", "amount": 10.0}
    producer.publish("transactions", EventType.TRANSACTION, payload)

    assert len(producer._producer.produced) == 1
    sent = producer._producer.produced[0]
    assert sent["topic"] == "transactions"
    assert sent["key"] == b"C001"
    decoded = json.loads(sent["value"])
    assert decoded["payload"]["transaction_id"] == "TX1"
    assert decoded["event_type"] == "transaction"


def test_publish_increments_metrics_on_success(monkeypatch):
    producer = _make_producer_with_fake_backend(monkeypatch)
    payload = {"customer_id": "C001", "transaction_id": "TX1", "amount": 10.0}
    producer.publish("transactions", EventType.TRANSACTION, payload)

    assert producer.metrics.messages_produced == 1
    assert producer.metrics.delivery_confirmed == 1
    assert producer.metrics.producer_errors == 0


def test_publish_raises_and_counts_error_on_missing_key_field(monkeypatch):
    producer = _make_producer_with_fake_backend(monkeypatch)
    payload = {"transaction_id": "TX1"}  # missing customer_id key field
    try:
        producer.publish("transactions", EventType.TRANSACTION, payload)
        assert False, "expected ValueError"
    except ValueError:
        pass
    assert producer.metrics.producer_errors == 1


def test_close_flushes_producer(monkeypatch):
    producer = _make_producer_with_fake_backend(monkeypatch)
    producer.close()
    assert producer._producer.flushed is True
