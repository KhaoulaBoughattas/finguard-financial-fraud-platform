"""Kafka producer for FinGuard event publishing.

Delivery semantics (see docs/kafka.md for the full explanation): configured
for reliable AT-LEAST-ONCE delivery with producer-side idempotence enabled,
which prevents *broker-side* duplicates caused by the producer's own retries
but does NOT provide end-to-end exactly-once -- if the producer process
crashes after a broker ack but before the caller records that success, the
caller may retry and produce a genuine duplicate event. Downstream
idempotent consumption (dedup by transaction_id, as implemented in the
Postgres/Delta write paths of later phases) is what closes that gap.
"""
from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka import KafkaError, Message, Producer

from src.common.logging import get_logger
from src.streaming.common.events import EventType
from src.streaming.producer.serializers import serialize_envelope, serialize_key
from src.streaming.producer.topic_config import extract_key

logger = get_logger(__name__)


@dataclass
class ProducerMetrics:
    """In-process counters. Deliberately simple (no Prometheus client) --
    Phase 8 will wire real metrics export; this keeps the counters in a
    shape that's trivial to expose later."""

    messages_produced: int = 0
    producer_errors: int = 0
    delivery_confirmed: int = 0
    delivery_failed: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "messages_produced": self.messages_produced,
            "producer_errors": self.producer_errors,
            "delivery_confirmed": self.delivery_confirmed,
            "delivery_failed": self.delivery_failed,
        }


class FinGuardProducer:
    """Thin, testable wrapper around confluent_kafka.Producer.

    Configuration choices (each documented, none copied blindly):

    - enable.idempotence=True: turns on the idempotent producer, which
      attaches a producer ID + sequence number to each message so the
      broker can detect and drop duplicate retries of the SAME in-flight
      batch. This requires acks=all and max.in.flight.requests<=5
      (librdkafka enforces this automatically once idempotence is on).
    - acks=all: the leader waits for all in-sync replicas to acknowledge
      before confirming -- required for idempotence, and means a confirmed
      write survives a single broker failure (given replication factor>1
      in production; locally RF=1 so this mainly protects against leader
      restart, not disk loss).
    - retries=5 / retry.backoff.ms: bounded retries so a transient broker
      blip doesn't fail the whole batch, but we don't retry forever.
    - delivery.timeout.ms: upper bound on total time (including retries)
      before delivery is reported as failed, so the caller doesn't hang.
    """

    def __init__(self, bootstrap_servers: str, client_id: str = "finguard-producer"):
        self._metrics = ProducerMetrics()
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 30_000,
            "linger.ms": 20,  # small batching window -- throughput/latency trade-off
        })

    @property
    def metrics(self) -> ProducerMetrics:
        return self._metrics

    def _delivery_callback(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self._metrics.delivery_failed += 1
            logger.error(
                "Kafka delivery failed",
                extra={"context": {"topic": msg.topic(), "error": str(err)}},
            )
        else:
            self._metrics.delivery_confirmed += 1
            logger.debug(
                "Kafka delivery confirmed",
                extra={"context": {
                    "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                }},
            )

    def publish(self, topic: str, event_type: EventType, payload: dict) -> None:
        """Serialize, key, and enqueue a message for async delivery.

        Non-blocking: call flush() (or let close() do it) to guarantee
        delivery before the process exits.
        """
        try:
            key = extract_key(topic, payload)
            value = serialize_envelope(event_type, payload)
            self._producer.produce(
                topic=topic,
                key=serialize_key(key),
                value=value,
                on_delivery=self._delivery_callback,
            )
            self._metrics.messages_produced += 1
            # Serve delivery-report callbacks without blocking on ack.
            self._producer.poll(0)
        except Exception:
            self._metrics.producer_errors += 1
            logger.exception("Failed to publish message", extra={"context": {"topic": topic}})
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all outstanding messages are delivered or timeout.
        Returns the number of messages still undelivered (0 = fully flushed)."""
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Graceful shutdown: flush outstanding messages before releasing
        the producer, so a normal exit never silently drops in-flight
        messages."""
        remaining = self.flush(timeout=10.0)
        if remaining > 0:
            logger.warning(
                "Producer closed with undelivered messages",
                extra={"context": {"remaining": remaining}},
            )
