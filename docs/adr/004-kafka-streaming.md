# ADR 004: Kafka Streaming Ingestion

## Context
Phase 0 requires a real-time ingestion layer capable of carrying millions of
transaction events reliably, in per-customer order, with a clear story for
malformed data and schema change. Phase 2 had to decide the messaging
technology and its concrete local configuration, independent of any
processing engine (Spark, which reads this same topic layout, is Phase 3).

## Decision: Kafka in KRaft mode, not Zookeeper
Kafka is used as specified in Phase 0. Within Kafka, **KRaft mode** (broker
and controller roles combined on a single node for local dev) is used
instead of a Zookeeper-backed cluster: Zookeeper is deprecated for new Kafka
deployments (removed entirely in Kafka 4.0), so building on it in a 2026
portfolio project would demonstrate outdated operational knowledge, and
KRaft removes an entire extra stateful service (Zookeeper itself) from local
development for no loss of functionality at this scale.

**Why Kafka at all, and not a simpler queue** (e.g. RabbitMQ, SQS, Redis
Streams): the requirement is ordered, replayable, multi-consumer-group event
streams at high throughput with long retention -- Kafka's partition log
model is built specifically for that combination. A simpler queue (RabbitMQ)
optimizes for per-message work distribution, not for multiple independent
consumers replaying the same ordered history, which Phase 3 (Spark) and
Phase 6+ (batch reprocessing, model retraining pipelines) both need.

## Decision: Topic architecture
Six topics, each with a documented partition count, replication factor,
cleanup policy, retention, and key strategy -- see `docs/kafka.md §2` for
the full table and `src/streaming/producer/topic_config.py` (`TOPICS`) for
the single source of truth both the producer/consumer code and the topic
creation script (`scripts/create_kafka_topics.py`) read from.

**Alternative considered**: one generic `events` topic with an `event_type`
field, filtered client-side. Rejected -- it would force every consumer to
subscribe to and filter all event types, defeats topic-level retention/
compaction policy differences (transactional data needs time-based deletion;
reference data needs compaction), and loses the ability to scale partition
counts independently per data shape.

## Decision: customer_id as the primary partition key
`transactions` and `fraud-alerts` are keyed by `customer_id`, not
`transaction_id`. Trade-off made explicitly: this sacrifices maximal
partition spread (a very high-volume customer could create a hot partition)
in exchange for guaranteeing Kafka's partition-level ordering translates
into *per-customer* ordering -- required correctness property for the
windowed behavioral features (`transactions_last_5min`, velocity, etc.)
Phase 3 will compute. Reference topics (`customers`, `accounts`,
`merchant-events`) are keyed by their own natural id and compacted, since
they're changelogs of entity state, not event streams.

## Decision: reliable at-least-once delivery, not exactly-once
`enable.idempotence=True` + `acks=all` on the producer eliminates
broker-side duplicates from the producer's own network retries, but this is
**not** end-to-end exactly-once -- an application-level retry (a whole
process re-publishing after an ambiguous failure) can still create a
genuine duplicate event. The consumer commits offsets manually, only after
a message is fully handled, so a consumer crash mid-processing causes
redelivery, never silent loss. **Alternative considered**: Kafka
transactions (`transactional.id`, read-committed) for true exactly-once.
Rejected for Phase 2 -- it adds real operational complexity (transaction
coordinator, `read_committed` isolation on every consumer) for a guarantee
this pipeline doesn't need yet, since duplicates are already handled
correctly downstream via idempotent writes (Postgres `ON CONFLICT ...
DO NOTHING` today, Delta `MERGE` in Phase 4). Revisit only if a real
correctness gap shows up that idempotent writes can't cover.

## Decision: two Kafka listeners (HOST + PLAINTEXT), not one
Discovered as a genuine bug during Phase 2 build-out, not designed in from
the start: an early draft published port 9092 mapped to the *internal*
listener, which silently broke every host-side client (including this
repository's own generator CLI and tests) because Kafka's
`advertised.listeners` handshake would hand back an unresolvable Docker
hostname. Fixed by giving the host-facing `HOST` listener (advertised as
`localhost:9092`, published to the host) and the container-facing
`PLAINTEXT` listener (advertised as `kafka:19092`, internal-only, for a
future Phase 3 Spark container) separate ports. See `docs/kafka.md §1` for
the full listener diagram and explanation, and the Phase 2 close-out report
for how this was found (careful manual review of the compose file's
semantics, not just YAML-syntax validation).

## Decision: envelope-based schema versioning from day one
Every message is wrapped in an `EventEnvelope` (`schema_version`,
`event_type`, `event_id`, `event_timestamp`, `payload`) rather than
publishing bare payloads. This was a deliberate up-front cost (one extra
layer of (de)serialization) taken specifically so that schema evolution
(Phase 0's documented V1→V2 transaction example) has a mechanism to hook
into later without a breaking migration of every existing message. See
`docs/kafka.md §7` for the concrete V1→V2 walkthrough and the
backward-compatible-vs-breaking-change distinction.

## Decision: dead-letter-events for both envelope and payload failures
Two independent validation stages (envelope shape, then payload schema),
both routing to one shared `dead-letter-events` topic with enough context
(`original_topic`, `partition`, `offset`, `event_id`, `error_type`,
`error_message`, `failed_at`, `original_payload`) to diagnose without
correlating against broker logs. **Alternative considered**: a separate DLQ
topic per source topic (`transactions-dlq`, `customers-dlq`, ...). Rejected
for Phase 2 -- at this volume a single DLQ topic keyed by `original_topic`
(see `docs/kafka.md §3`) gives the same selective-replay capability with
one fewer topic to manage; revisit if DLQ volume from one source topic ever
needs independent scaling/retention from the others.

## Local vs. Production Architecture
| | Local (this repo) | Production target |
|---|---|---|
| Nodes | 1 (combined broker+controller) | 3+ brokers, separate controller quorum |
| Replication factor | 1 | 3 |
| min.insync.replicas | 1 | 2 |
| Security | None (unauthenticated PLAINTEXT) | TLS + SASL + ACLs |
| Secrets | N/A locally | Azure Key Vault |
| Likely Azure target | N/A | Event Hubs (Kafka-compatible endpoint) |

## Known Limitations
- Local replication factor of 1 means the local Kafka node is a single
  point of failure by design -- acceptable for a disposable dev
  environment, explicitly not representative of the production target.
- No Kafka broker was available in the sandbox this phase was built in;
  the full producer→broker→consumer path is implemented and unit-tested,
  but has not been executed against a live broker in that environment --
  see the Phase 2 close-out report for exact validation commands to run
  where Docker is available.
- Kafka transactions / exactly-once semantics are explicitly out of scope
  for this phase (see decision above) -- at-least-once + idempotent
  downstream writes is the documented guarantee.

## Consequences
- `src/streaming/producer/topic_config.py` is the single source of truth
  for topic names, partitions, replication, retention, and key fields --
  Phase 3 (Spark) and any future service must read from it rather than
  redefining topic configuration.
- The `EventEnvelope`/`schema_version` mechanism must be used by every
  future producer (Phase 3's fraud engine publishing to `fraud-alerts`,
  etc.), not bypassed for convenience.
- `KafkaSettings` (in `src/common/config.py`) is the only place a
  bootstrap-servers default should live; no future code should hardcode
  `localhost:9092` or a Docker hostname directly.
