from src.generator.generate import run


class _FakeConfluentProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []

    def produce(self, topic, key, value, on_delivery=None):
        self.produced.append(topic)
        if on_delivery:
            on_delivery(None, _FakeMessage(topic))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
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


def test_run_with_publish_kafka_sends_all_record_types(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "src.streaming.producer.producer.Producer", _FakeConfluentProducer
    )
    output_dir = tmp_path / "raw"
    summary = run(
        num_customers=5,
        num_accounts_max_per_customer=1,
        num_merchants=3,
        num_transactions=20,
        fraud_rate=0.1,
        seed=1,
        output_dir=str(output_dir),
        persist_db=False,
        publish_kafka=True,
        kafka_bootstrap_servers="fake:9092",
    )
    assert "kafka" in summary
    # 5 customers + 5 accounts (1 per customer) + 3 merchants + >=20 transactions
    assert summary["kafka"]["messages_produced"] >= 5 + 5 + 3 + 20
    assert summary["kafka"]["producer_errors"] == 0
