# Kafka Streaming Ingestion (Phase 2)

## 1. Architecture

```
Synthetic Generator ──▶ FinGuardProducer ──▶ Kafka Topics ──▶ FinGuardConsumer ──▶ validation
                                                                                       │
                                                                          ┌────────────┴────────────┐
                                                                          ▼                          ▼
                                                                      valid                      invalid
                                                                  (logged, offset               dead-letter-events
                                                                   committed)                    (offset committed
                                                                                                  after DLQ write)
```

Spark Structured Streaming (Phase 3) is the eventual consumer of `transactions`,
`customers`, `accounts`, `merchant-events`; the Phase 2 consumer exists to
prove and test the ingestion contract (envelope, validation, DLQ, offsets)
independently of Spark.

### Listener architecture (local development)

This is the single most failure-prone part of a local Kafka setup, and was
the source of a real bug found and fixed during Phase 2 (see §9). Two
listeners exist for two different network paths:

```
                    ┌──────────────────────┐
Host machine ──────▶│ localhost:9092       │
(generator CLI,     │ HOST listener        │
 pytest, IDE)       └──────────┬───────────┘
                                │
                          Kafka broker
                        (single KRaft node,
                         broker + controller)
                                │
                    ┌───────────┴───────────┐
Docker services ───▶│ kafka:19092           │
(future Phase 3     │ PLAINTEXT listener    │
 Spark container)    └──────────────────────┘
```

Why two listeners are required: Kafka's `advertised.listeners` value is what
a client is told to connect to *after* its initial bootstrap connection, not
just the address it dialed. A host-machine client bootstrapping to
`localhost:9092` but then being handed back `kafka:9092` (a hostname that
only resolves inside the Docker network) would fail on every subsequent
request. Conversely, a container-to-container client should not be told to
reconnect to `localhost`, since `localhost` inside a container refers to
that container itself. Splitting `HOST` (advertised as `localhost:9092`,
port published to the host) from `PLAINTEXT` (advertised as `kafka:19092`,
internal-only, not published) resolves both paths correctly. `CONTROLLER`
(port 9093) is a third, separate listener used only for KRaft
controller-quorum traffic between broker/controller roles -- clients never
connect to it directly.

**Bug found and fixed during Phase 2** (see §9 for detail): the first draft
of `docker-compose.yml` published port 9092 but mapped it to the internal
`PLAINTEXT` listener rather than `HOST`. A host client connecting to
`localhost:9092` would complete the initial TCP connection, then be handed
back `finguard_kafka:9092` (unresolvable outside Docker) for every real
request -- silently breaking every producer/consumer running outside a
container, including this repository's own tests and CLI. Fixed by giving
`HOST` its own listener/port and publishing that one instead.

## 2. Topics

| Topic | Partitions (local) | Replication (local) | Cleanup policy | Retention | Key field |
|---|---|---|---|---|---|
| `transactions` | 6 | 1 | delete | 7 days | `customer_id` |
| `customers` | 3 | 1 | compact | infinite (compacted) | `customer_id` |
| `accounts` | 3 | 1 | compact | infinite (compacted) | `account_id` |
| `merchant-events` | 3 | 1 | compact | infinite (compacted) | `merchant_id` |
| `fraud-alerts` | 6 | 1 | delete | 30 days | `customer_id` |
| `dead-letter-events` | 3 | 1 | delete | 14 days | `original_topic` |

Defined once in `src/streaming/producer/topic_config.py` (`TOPICS`) and
created reproducibly by `scripts/create_kafka_topics.py` -- both the
producer/consumer code and the init script read from this single source of
truth, so there is no duplicated topic configuration.

**Local development vs. production target:**

| | Local | Production |
|---|---|---|
| Brokers | 1 | 3+ |
| Replication factor | 1 | 3 |
| min.insync.replicas | 1 | 2 |
| Rationale | A single broker can't survive any failure; acceptable for local dev where the whole stack is disposable | Tolerates one broker failure without data loss (RF=3, min ISR=2 means writes still succeed with one broker down, and no acknowledged write is lost) |

## 3. Message Key Strategy

See the `key_rationale` field on each `TopicSpec` in `topic_config.py` for
the authoritative explanation per topic; summarized:

- **`transactions` / `fraud-alerts` → `customer_id`**: guarantees all of one
  customer's events land on the same partition, so Kafka's per-partition
  ordering guarantee becomes per-*customer* ordering -- required for
  correct behavioral/velocity feature computation in Phase 3 (you cannot
  correctly compute "transactions in the last 5 minutes" for a customer if
  their events can arrive out of order across partitions).
- **`customers` / `accounts` / `merchant-events` → their own natural id**:
  these are compacted changelog topics; keying by the entity's own id is
  what makes compaction (retain only the latest record per key) correct.
- **`dead-letter-events` → `original_topic`**: groups failures by source
  topic so a DLQ consumer can selectively inspect or replay failures from
  one pipeline stage without wading through unrelated ones.

**Interview-relevant point, demonstrated in
`tests/unit/test_topic_config.py::test_same_customer_id_all_transactions_map_to_one_partition`**:
Kafka guarantees ordering *within* a partition, never globally across
partitions. Keying correctly is what turns a global (impossible) ordering
requirement into a per-partition (achievable) one.

Partition assignment is deterministic and was verified directly, without
requiring a running broker, by reimplementing Kafka's own default
partitioner (murmur2 hash) in `compute_partition()`:
```
C001 -> partition 5 (consistently, every time)
C002 -> partition 0 (consistently, every time)
```

## 4. Delivery Semantics

**What this phase guarantees: reliable at-least-once delivery, with
producer-side idempotence to suppress broker-side duplicate writes caused
by the producer's own retries.** It does **not** guarantee end-to-end
exactly-once. Concretely:

- `enable.idempotence=True` + `acks=all`: the broker deduplicates retried
  *sends of the same in-flight message* (identified by producer ID +
  sequence number) -- so a network blip that causes librdkafka to retry a
  produce call does not create a duplicate on the broker.
- This does **not** prevent duplicates caused by the *application* retrying
  a logically-identical call from scratch (e.g. the generator CLI process
  crashes after `producer.publish()` returned successfully but before the
  caller recorded that fact, and a re-run republishes the same logical
  transaction as a brand-new message with a new `event_id`). That is a
  genuine, expected duplicate at this layer.
- **Consumer side**: offsets are committed manually, only after a message
  is fully handled (validated + processed, or validated-as-invalid +
  DLQ'd). A consumer crash between reading a message and committing its
  offset causes that message to be re-delivered on restart -- at-least-once
  consumption, never silent loss.
- **Where duplicates actually get resolved**: downstream idempotent writes.
  Phase 1's Postgres loader already does this (`INSERT ... ON CONFLICT
  transaction_id DO NOTHING`); Phase 4's Delta `MERGE` will do the same for
  the lakehouse. Phase 2's job is to never *lose* a message and to bound how
  duplicates can occur -- not to eliminate them at the transport layer.

## 5. Offset Strategy

- **Consumer groups**: one group per logical consumer (e.g.
  `finguard-transactions-processor`); tests use a fresh UUID-suffixed group
  per test to avoid cross-test offset interference.
- **`auto.offset.reset=earliest`**: a brand-new consumer group (one that has
  never committed an offset) starts from the beginning of the topic, not
  `latest` -- so nothing published before the consumer first connects is
  silently skipped. An *existing* group always resumes from its last
  committed offset regardless of this setting.
- **Manual commit, after processing**: `enable.auto.commit=False`;
  `consumer.commit()` is called only once a message has been fully handled.
- **Consumer crash before commit**: on restart, the same consumer group
  resumes from the last *committed* offset -- the crashed message (and
  anything after it that was read but not committed) is re-delivered.
  Verified in `tests/integration/test_kafka_integration.py::
  test_consumer_group_resumes_from_committed_offset` (skipped in this
  sandbox -- see §14).
- **Replay**: because offsets are explicit and topics retain data for their
  configured retention window, a consumer group's offsets can be reset
  (e.g. `kafka-consumer-groups.sh --reset-offsets --to-earliest`) to
  reprocess a historical range -- useful after fixing a bug in downstream
  processing logic.

## 6. Dead Letter Queue

Two independent validation stages, each producing a distinct DLQ
`error_type`:

1. **Envelope validation** (`EnvelopeValidationError`): the raw bytes
   aren't valid JSON, or don't match the `EventEnvelope` shape
   (missing `event_type`, wrong types, etc.).
2. **Payload validation** (`PayloadValidationError`): the envelope parses,
   but `envelope.payload` fails Pydantic validation against the schema
   registered for its `event_type` (e.g. a `Transaction` missing `amount`,
   or an invalid `status` enum value).

Either failure produces a `DeadLetterEvent` (`src/streaming/common/events.py`)
containing: `original_topic`, `partition`, `offset`, `event_id` (if the
envelope parsed far enough to have one), `error_type`, `error_message`,
`failed_at`, and `original_payload` -- enough to diagnose and potentially
replay the failure without needing to correlate against broker logs. The
DLQ event is itself published (as its own envelope) to `dead-letter-events`
before the original message's offset is committed, so a crash between
"validation failed" and "DLQ write confirmed" causes redelivery of the
*original* message, not silent loss of the failure record.

## 7. Schema Evolution

`EventEnvelope.schema_version` (currently `"1.0"`) is carried on every
message specifically so this doesn't need to be solved later from scratch.

**V1 → V2 example** (the same one used in the Phase 0/1 architecture docs):
```
V1: transaction_id, customer_id, amount
V2: transaction_id, customer_id, amount, currency, merchant_category
```
This is a **backward-compatible, additive** change: new fields are optional
with sensible defaults. A V1 consumer reading a V2 message ignores the
fields it doesn't know about (Pydantic's default `extra` handling on the
payload schema); a V2 consumer reading a V1 message fills in defaults for
the missing optional fields. Both producer and consumer versions can coexist
during a rollout -- there is no hard cutover moment required.

A **breaking** change (renaming/removing/retyping an existing required
field) is different: it cannot be silently tolerated. The migration
strategy is: bump `schema_version` to `"2.0"`, have V2 consumers branch on
`envelope.schema_version` to select the right payload schema/handling, and
treat any message whose `schema_version` an consumer doesn't recognize as
an envelope-validation failure -- i.e. it goes to `dead-letter-events`
rather than being guessed at, so a genuinely incompatible message is never
silently mis-parsed.

Phase 2 deliberately does **not** implement V2 -- only the mechanism
(the `schema_version` field, and the DLQ-on-unknown-version behavior) that
would carry it. Building V2 without a real reason to change the schema
would be exactly the kind of unnecessary complexity this project is meant
to avoid.

## 8. Failure Scenarios (interview preparation)

| # | Scenario | What happens | Duplicated? | Lost? | Recovery |
|---|---|---|---|---|---|
| 1 | Broker temporarily unavailable | Producer buffers/retries per `retries`/`retry.backoff.ms` up to `delivery.timeout.ms`; consumer's `poll()` returns no messages and retries the connection | No | No, as long as delivery.timeout.ms isn't exceeded | Automatic once the broker returns; if the timeout IS exceeded, the caller sees a delivery failure and must decide whether to retry the publish |
| 2 | Producer sends the same transaction twice (application-level retry) | Broker idempotence dedupes retries of the *same* in-flight send; a fresh, logically-duplicate `publish()` call is NOT deduped at this layer -- it's a new message with a new `event_id` | Possibly, at the application level | No | Downstream idempotent write (Postgres `ON CONFLICT transaction_id DO NOTHING`, later Delta `MERGE`) collapses it |
| 3 | Consumer crashes before committing offset | Message was never marked processed | No | No | On restart, the same offset is re-read and reprocessed |
| 4 | Consumer crashes after processing but before committing offset | Business-logic side effect already happened once; offset wasn't advanced | Yes -- the same message is reprocessed on restart | No | Reprocessing must be idempotent downstream (same mechanism as #2) |
| 5 | Malformed JSON | `deserialize_envelope` raises `EnvelopeValidationError` | No | No -- routed to DLQ | Message is inspectable in `dead-letter-events`; original offset is committed only after the DLQ write succeeds |
| 6 | Valid JSON, invalid transaction schema | `validate_payload` raises `PayloadValidationError` | No | No -- routed to DLQ | Same as #5 |
| 7 | Kafka topic partition becomes unavailable (its leader broker is down) | Producers/consumers for that partition block/retry until a new leader is elected; other partitions are unaffected | No | No, with RF>=2 and a leader election (not possible at local RF=1) | Requires production-target replication (RF=3) to actually tolerate this; at local RF=1 a lost broker means that partition is unavailable until it returns |
| 8 | Schema V2 arrives while some consumers still understand only V1 | With additive-only V2 changes, a V1 consumer using a V1 payload schema simply ignores the new fields it doesn't validate against; if V2 introduced a breaking change instead, the consumer's schema_version check would route the message to DLQ rather than mis-parse it | No | No (DLQ, not silent corruption) | Roll out V2-aware consumers before/alongside producers emitting `schema_version: "2.0"`; see §7 |

## 9. Local Setup

```bash
cp .env.example .env
# set POSTGRES_PASSWORD

make up                       # starts postgres + kafka, waits for both healthchecks
python -m scripts.create_kafka_topics --bootstrap-servers localhost:9092
python -m src.generator.generate --customers 100 --transactions 1000 --seed 42 --publish-kafka
pytest -v tests/integration/test_kafka_integration.py
```

> As documented in the Phase 2 close-out report, the sandbox used to build
> this code has no Docker daemon, so the steps above were written and
> reviewed carefully (including finding and fixing the listener bug) but
> **not executed against a real broker** here. `pytest -v` in this
> environment reports the Kafka integration tests as **skipped**, not
> passed -- see the close-out report for the exact commands to run them for
> real on a machine with Docker.

## 10. Production Kafka Architecture

- 3+ brokers, replication factor 3, `min.insync.replicas=2`.
- Separate KRaft controller quorum (not combined broker+controller roles) at
  that scale, for isolation between metadata and data-plane load.
- TLS between clients and brokers, SASL (e.g. SASL/SCRAM or OAUTHBEARER) for
  authentication, and ACLs scoping which principals may produce/consume
  which topics -- none of this is needed locally (single-developer,
  unauthenticated), but would be non-negotiable before handling real
  financial data.
- Secrets (SASL credentials, TLS keys) via Azure Key Vault in the target
  architecture, injected as environment variables at deploy time -- never
  committed, matching how `POSTGRES_PASSWORD`/`KAFKA_BOOTSTRAP_SERVERS`
  are already handled via `.env` in this repository.
- On Azure specifically: Event Hubs' Kafka-compatible endpoint is the likely
  managed target (avoids operating brokers directly); the topic/key/DLQ
  design here carries over unchanged since it's protocol-level, not
  broker-implementation-specific.
