"""Kafka consumer for FinGuard event ingestion.

Offset strategy (see docs/kafka.md for full detail): manual commits,
committed only AFTER a message has been fully validated (and, on failure,
after the DLQ write has been produced and flushed) -- never before. This
means a consumer crash between "message read" and "offset committed" causes
the SAME message to be re-delivered on restart (at-least-once consumption),
never silently skipped. Combined with idempotent downstream writes (Phase 1's
Postgres upserts, Phase 4's Delta MERGE), reprocessing that message is safe.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException

from src.common.logging import get_logger
from src.streaming.common.events import EventType, build_dlq_event
from src.streaming.consumer.validator import validate_payload
from src.streaming.producer.producer import FinGuardProducer
from src.streaming.producer.serializers import deserialize_envelope

logger = get_logger(__name__)

DLQ_TOPIC = "dead-letter-events"


@dataclass
class ConsumerMetrics:
    messages_consumed: int = 0
    messages_failed: int = 0
    messages_sent_to_dlq: int = 0
    consumer_errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "messages_consumed": self.messages_consumed,
            "messages_failed": self.messages_failed,
            "messages_sent_to_dlq": self.messages_sent_to_dlq,
            "consumer_errors": self.consumer_errors,
        }


class FinGuardConsumer:
    """Wraps confluent_kafka.Consumer with validation + DLQ routing.

    - enable.auto.commit=False: we commit manually, only after successful
      processing (or successful DLQ routing of a failure) -- see module
      docstring for why.
    - auto.offset.reset='earliest': a brand-new consumer group starts from
      the beginning of the topic rather than silently skipping to 'latest'
      and missing whatever was already there. Existing groups always resume
      from their last committed offset regardless of this setting.
    """

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str,
                 dlq_producer: FinGuardProducer | None = None):
        self.topic = topic
        self.metrics = ConsumerMetrics()
        self._dlq_producer = dlq_producer or FinGuardProducer(
            bootstrap_servers=bootstrap_servers, client_id=f"{group_id}-dlq-producer"
        )
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        self._consumer.subscribe([topic])

    def _send_to_dlq(self, error: Exception, raw_value: bytes, partition: int,
                      offset: int, event_id: str | None) -> None:
        try:
            original_payload = json.loads(raw_value.decode("utf-8"))
        except Exception:  # noqa: BLE001 -- deliberately broad: this is the DLQ's own
            # last-resort decode of an already-failed message; if it isn't
            # valid JSON either, we still must not raise here (that would
            # lose the message a second time) -- fall back to a raw string.
            original_payload = raw_value.decode("utf-8", errors="replace")

        dlq_event = build_dlq_event(
            original_topic=self.topic,
            error=error,
            original_payload=original_payload,
            partition=partition,
            offset=offset,
            event_id=event_id,
        )
        self._dlq_producer.publish(
            topic=DLQ_TOPIC,
            event_type=EventType.DEAD_LETTER,
            payload=dlq_event.model_dump(mode="json"),
        )
        self._dlq_producer.flush(timeout=5.0)
        self.metrics.messages_sent_to_dlq += 1

    def poll_once(self, timeout: float = 1.0) -> bool:
        """Poll for a single message, process it, and commit. Returns True
        if a message was processed (valid or DLQ'd), False on empty poll."""
        msg = self._consumer.poll(timeout)
        if msg is None:
            return False
        if msg.error():
            self.metrics.consumer_errors += 1
            logger.error("Kafka consumer error", extra={"context": {"error": str(msg.error())}})
            raise KafkaException(msg.error())

        partition, offset = msg.partition(), msg.offset()
        event_id = None
        try:
            envelope = deserialize_envelope(msg.value())
            event_id = envelope.event_id
            validate_payload(envelope)
            self.metrics.messages_consumed += 1
            logger.info(
                "Event validated successfully",
                extra={"context": {
                    "topic": self.topic, "partition": partition, "offset": offset,
                    "event_id": event_id, "event_type": envelope.event_type,
                }},
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: the DLQ contract is
            # "never lose a malformed event," so ANY failure while parsing/
            # validating this message (not just the two named validation
            # error types) must route to the DLQ rather than crash the
            # consumer loop or silently drop the message.
            self.metrics.messages_failed += 1
            logger.warning(
                "Event failed validation, routing to DLQ",
                extra={"context": {
                    "topic": self.topic, "partition": partition, "offset": offset,
                    "error": str(exc),
                }},
            )
            self._send_to_dlq(exc, msg.value(), partition, offset, event_id)

        # Commit only after the message has been fully handled (processed
        # OR successfully DLQ'd) -- see module docstring.
        self._consumer.commit(message=msg, asynchronous=False)
        return True

    def close(self) -> None:
        self._consumer.close()
        self._dlq_producer.close()
