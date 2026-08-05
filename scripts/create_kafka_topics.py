"""Creates all FinGuard Kafka topics with their documented configuration.

Reproducible and idempotent: safe to run multiple times (existing topics are
left alone, not recreated) or against a freshly started broker as part of
local setup.

Usage:
    python -m scripts.create_kafka_topics --bootstrap-servers localhost:9092
"""
from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings
from src.common.logging import configure_logging, get_logger
from src.streaming.producer.topic_config import TOPICS

logger = get_logger(__name__)


def create_topics(bootstrap_servers: str, timeout: float = 15.0) -> dict[str, str]:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout).topics.keys())

    to_create = []
    results: dict[str, str] = {}
    for name, spec in TOPICS.items():
        if name in existing:
            results[name] = "already_exists"
            continue
        config = {"cleanup.policy": spec.cleanup_policy}
        if spec.retention_ms >= 0:
            config["retention.ms"] = str(spec.retention_ms)
        to_create.append(
            NewTopic(
                name,
                num_partitions=spec.local_partitions,
                replication_factor=spec.local_replication_factor,
                config=config,
            )
        )

    if not to_create:
        logger.info("All topics already exist, nothing to create")
        return results

    futures = admin.create_topics(to_create, request_timeout=timeout)
    for topic, future in futures.items():
        try:
            future.result()
            results[topic] = "created"
            logger.info("Topic created", extra={"context": {"topic": topic}})
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: this per-topic
            # result reporting must capture ANY create-topic failure (auth, broker,
            # already-exists races, ...) into `results` rather than aborting the
            # whole batch on the first one.
            results[topic] = f"failed: {exc}"
            logger.error(
                "Topic creation failed", extra={"context": {"topic": topic, "error": str(exc)}}
            )
    return results


def main() -> None:
    configure_logging("INFO")
    parser = argparse.ArgumentParser(description="Create FinGuard Kafka topics")
    parser.add_argument(
        "--bootstrap-servers", type=str, default=get_settings().kafka.bootstrap_servers
    )
    args = parser.parse_args()

    results = create_topics(args.bootstrap_servers)
    print(results)
    if any(v.startswith("failed") for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
