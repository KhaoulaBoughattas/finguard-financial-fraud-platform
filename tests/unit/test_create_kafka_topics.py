from scripts.create_kafka_topics import create_topics


class _FakeTopicMetadata:
    def __init__(self, topics: dict):
        self.topics = topics


class _FakeFuture:
    def __init__(self, exc=None):
        self._exc = exc

    def result(self):
        if self._exc:
            raise self._exc


class _FakeAdminClient:
    def __init__(self, config):
        self.config = config
        self.existing_topics = {"transactions": object()}
        self.create_calls = []

    def list_topics(self, timeout=None):
        return _FakeTopicMetadata(self.existing_topics)

    def create_topics(self, new_topics, request_timeout=None):
        self.create_calls.append(new_topics)
        return {t.topic: _FakeFuture() for t in new_topics}


def test_create_topics_skips_existing_and_creates_the_rest(monkeypatch):
    fake_admin = _FakeAdminClient({})
    monkeypatch.setattr(
        "scripts.create_kafka_topics.AdminClient", lambda config: fake_admin
    )
    results = create_topics("fake:9092")

    assert results["transactions"] == "already_exists"
    created = {k for k, v in results.items() if v == "created"}
    assert created == {"customers", "accounts", "merchant-events", "fraud-alerts", "dead-letter-events"}


def test_create_topics_reports_failures(monkeypatch):
    class _FailingAdmin(_FakeAdminClient):
        def create_topics(self, new_topics, request_timeout=None):
            return {t.topic: _FakeFuture(exc=RuntimeError("broker unavailable")) for t in new_topics}

    fake_admin = _FailingAdmin({})
    monkeypatch.setattr(
        "scripts.create_kafka_topics.AdminClient", lambda config: fake_admin
    )
    results = create_topics("fake:9092")
    assert results["customers"].startswith("failed")
