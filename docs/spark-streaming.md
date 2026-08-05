# Spark Structured Streaming + Delta Lake (Phase 3)

## 1. Architecture

```
Kafka (transactions topic, Phase 2)
        │
        ▼
Spark Structured Streaming
  parse_kafka_envelope        -- EventEnvelope, ALL event types, + Kafka
                                  metadata (topic/partition/offset)
  add_ingestion_metadata      -- ingestion_timestamp, ingestion_date
        │
        ▼
Bronze Delta table (append-only, raw, partitioned by event_type/ingestion_date)
        │
        ▼ (read as a stream)
Spark Structured Streaming (foreachBatch)
  filter_and_parse_transaction_payloads  -- filter event_type='transaction', parse payload
  split_valid_invalid                     -- business-rule validation
  deduplicate_transactions                -- in-batch dedup by transaction_id
  add_event_date                          -- Silver partition column
  Delta MERGE keyed on transaction_id     -- cross-batch idempotent upsert
        │
        ▼
Silver Delta table (validated, deduplicated, partitioned by event_date)
```

`src/streaming/spark/streaming_job.py` wires this end to end and is the
actual entrypoint (`python -m src.streaming.spark.streaming_job`).

## 2. Bronze vs. Silver responsibilities

**Bronze** (`bronze.py`): preserves what was received as faithfully as
possible. It parses the envelope (so the data is at least structured, not
opaque bytes) and stamps ingestion metadata, but does **no** business
validation, **no** filtering by event_type, and **no** deduplication.
Every event type Kafka carries (`transaction`, `customer`, `account`,
`merchant_event`) lands in Bronze. This is deliberate: if a bug is later
found in Silver's validation or feature logic, Bronze can be replayed to
fix it without re-reading Kafka, whose retention (7 days for
`transactions`, see `docs/kafka.md`) is far shorter than Delta's.

Bronze columns: `kafka_key`, `kafka_topic`, `kafka_partition`,
`kafka_offset`, `_raw_value` (original JSON string), `schema_version`,
`event_type`, `event_id`, `event_timestamp`, `payload_json` (still a raw
string -- payload shape differs per event_type, so it isn't parsed until
Silver knows which schema to apply), `ingestion_timestamp`,
`ingestion_date`.

**Silver** (`silver.py`): does the actual work. Filters to
`event_type='transaction'`, parses `payload_json` against
`TRANSACTION_PAYLOAD_SCHEMA`, validates (required fields, positive amount,
recognized status, non-future timestamp), deduplicates by
`transaction_id`, assigns `event_date`, and MERGEs into the Silver Delta
table. Rows that fail validation are **not** silently dropped -- they're
written to a `transactions_rejected` Delta table alongside Silver,
mirroring the Phase 2 Kafka-consumer DLQ contract at the Spark layer.

## 3. Event Time vs. Processing Time

- **Event time** (`tx_timestamp`, from the payload -- when the producer
  says the transaction happened) is what windowing and watermarking key
  on, because behavioral features ("transactions in the last 5 minutes")
  are meaningful relative to when things actually happened, not when Spark
  happened to see them.
- **Processing time** (`ingestion_timestamp`, from `current_timestamp()` at
  the moment Bronze writes the row) is when *this job* saw the record.
  These can diverge significantly under replay, backfill, or a consumer
  that's behind -- a record ingested today can carry an event_timestamp
  from last week.
- Partitioning uses each concept where it's actually useful: Bronze
  partitions by `ingestion_date` (bounds how many new files a given day's
  ingestion run touches, regardless of what event dates are inside them);
  Silver partitions by `event_date` (what analytical queries and Power BI
  reports actually filter on).

## 4. Windowing and Watermarking

Demonstrated with a REAL running Structured Streaming query in
`tests/unit/test_spark_watermark.py` (not asserted from documentation
alone) -- see `src/streaming/spark/features.py` for the two aggregation
patterns (`compute_windowed_count`, `compute_windowed_distinct_count`) that
every documented behavioral feature (`transactions_last_5min`,
`unique_countries_last_1h`, etc.) reduces to.

- **Watermark delay** bounds how long Spark keeps a window's state around
  waiting for late-arriving events before finalizing and evicting it. A
  2-minute watermark on a 5-minute window means: once the maximum
  event-time seen so far is more than 2 minutes past a window's end, that
  window's state is dropped, and NO further event for that window --
  however legitimately it happened during that window -- will be counted.
- **What was actually verified**: publish an on-time event at 10:00 into a
  [10:00, 10:05) window (count=1, emitted). Publish an event at 10:08,
  advancing the watermark to 10:06 -- past the window's 10:05 end, so it's
  finalized. Publish a LATE event at 10:01 (inside the window's time range,
  but arriving after finalization) -- and confirm no output row ever shows
  count=2 for that window. The late event is silently dropped by Spark's
  watermarking, not merged in and not erred on.
- **Watermarking does NOT guarantee zero late data reaches your
  aggregation** -- it guarantees a *bound* on how long state is held, which
  is a deliberate trade-off (unbounded state growth vs. some late data
  loss). An event arriving within the watermark delay IS still correctly
  incorporated; only events later than the configured delay are dropped.
  Choosing the delay is a business call (Phase 0 documented 10 minutes for
  the fraud-scoring windows) -- too short drops real data, too long grows
  state and delays finalization.

## 5. Deduplication

Two layers, deliberately different scopes:

1. **In-batch** (`deduplicate_transactions`, `dropDuplicates(["transaction_id"])`
   in `transformations.py`): catches duplicates that land in the SAME
   micro-batch (e.g. a producer retry that both messages happen to arrive
   together).
2. **Cross-batch** (Delta `MERGE ... WHEN NOT MATCHED THEN INSERT` in
   `silver.py`): catches duplicates that arrive in DIFFERENT micro-batches
   -- the actual at-least-once-replay case from Kafka (Phase 2) or from
   this job's own checkpoint-recovery reprocessing a batch after a crash.
   In-batch dedup alone cannot catch this, since `dropDuplicates` only ever
   sees one micro-batch's rows at a time.

**Deduplication key: `transaction_id`**, not `event_id` and not an
arbitrary combination of columns. `event_id` is a fresh UUID minted on
every `publish()` call (see `src/streaming/common/events.py`) -- two
publishes of the logically same transaction get *different* event_ids by
design, so deduping on it would miss exactly the duplicates that matter.
`transaction_id` is the producer-assigned natural identifier for the
underlying business event and is what Phase 2's Kafka message key and this
Silver MERGE both key on -- consistent end to end.

## 6. Checkpointing

- **Why required**: a streaming job's checkpoint directory is where Spark
  records (a) which Kafka offsets have been fully processed and committed,
  and (b) any stateful aggregation state (e.g. open windows). Without it,
  a restart has no way to know where it left off and would have to
  either reprocess everything or risk silently skipping data.
- **Where locally**: `data/checkpoints/bronze/transactions` and
  `data/checkpoints/silver/transactions` (configurable via `DeltaSettings`
  in `src/common/config.py`, env-prefixed `DELTA_`) -- one checkpoint
  directory per streaming query; Bronze and Silver each have their own
  since they're separate queries with separate progress.
- **Recovery**: on restart, Spark reads the checkpoint, resumes from the
  last successfully committed micro-batch's offset, and reprocesses
  anything that was read but not confirmed committed.
- **What happens after a crash, precisely**: if Spark crashes after
  writing a micro-batch's Delta commit but before the checkpoint's offset
  record is updated, the SAME micro-batch is re-read and reprocessed on
  restart. For Bronze (blind append), this would normally risk a
  duplicate append -- but Delta's transactional write + Spark's
  checkpoint-and-source-offset coordination for the built-in Kafka source
  and Delta sink is designed so the commit and the checkpoint advance
  together; a genuinely partial commit is rolled back, not left half-applied.
  For Silver, the MERGE makes any reprocessing a no-op for
  already-committed transaction_ids regardless.
- **Relationship to exactly-once**: this setup provides **at-least-once
  reprocessing at the Spark layer, made idempotent by the Silver MERGE** --
  the same "effectively-once outcome, not exactly-once mechanism" story as
  Phase 2's Kafka consumer. This is NOT global exactly-once semantics: it
  relies on the MERGE key being a true unique business identifier, and
  Bronze itself (blind append, no MERGE) can in principle carry duplicate
  rows across a crash-and-replay -- acceptable because Bronze is a raw,
  replayable staging layer, not the layer anything is queried from
  directly.

## 7. Delta Lake

**Implementation: DONE. Integration execution: NOT RUN.**

`bronze.py` and `silver.py` implement the documented Bronze/Silver
architecture using Delta's real APIs (`format("delta")`, `mergeSchema`,
`DeltaTable.merge()`). `session.py`'s `build_spark_session()` registers the
Delta catalog/extensions and resolves `io.delta:delta-spark_2.12:3.2.1`
via Maven coordinates -- the standard, correct way to enable Delta on a
Spark session.

**This could not be executed in the sandbox this was built in**: Delta's
Spark connector JAR is distributed only via Maven Central, which returns
`403 host_not_allowed` under this sandbox's network policy (verified
directly with `curl`). Delta's GitHub releases were checked as a possible
alternative and only publish Hive-assembly jars, not the core Spark
connector. This is a sandbox network-policy limitation, not a code defect
-- `tests/integration/test_spark_delta_integration.py` exists, is written
correctly (verified by inspection and by successfully triggering its
skip-path), and will run for real the moment it's executed somewhere with
Maven Central access (a real CI runner, a developer's own machine, or with
the jar pre-downloaded onto the classpath).

```text
PySpark transformation tests: VERIFIED (real local Spark session, 30 tests)
Watermark tests: VERIFIED (real streaming query, checkpointed across restarts)
Delta Lake write integration: NOT RUN
Reason: Delta Spark connector unavailable because Maven Central is blocked in the sandbox.
```

To actually validate the Delta path: run `pytest -v
tests/integration/test_spark_delta_integration.py` on a machine/CI with
Maven Central reachable (no other setup needed -- `requirements.txt`
already pins `delta-spark==3.2.1`).

## 8. Partitioning Strategy

| Table | Partition columns | Why |
|---|---|---|
| Bronze | `event_type`, `ingestion_date` | `event_type` has only 4-5 distinct values (no fragmentation); `ingestion_date` bounds how many files one day's ingestion touches and lets a backfill/replay job prune by day. |
| Silver | `event_date` | The date analysts and Power BI reports actually filter on ("fraud alerts last week") -- not ingestion date, since replays/backfills would otherwise scatter one logical day's transactions across many ingestion-date partitions. |

**Explicitly rejected: `customer_id` as a partition column for either
table.** With thousands of customers, partitioning by customer_id would
create many small per-customer files rather than a few well-sized daily
ones -- the classic Spark "small file problem," which hurts both write
throughput (many tiny files per micro-batch) and read performance (file
listing/open overhead dominates actual data scanning). Customer-level
access patterns (e.g. "this customer's history") are served by filtering
within a date-partitioned table, or by the Postgres serving layer
(Phase 0/1) for point lookups -- not by file-system-level partitioning.

## 9. Failure / Recovery Scenarios

| # | Scenario | What happens | Duplicated? | Lost? | How checkpointing helps |
|---|---|---|---|---|---|
| 1 | Spark driver/job crashes | Job stops; checkpoint retains last committed offset/state | No | No | Restart resumes from checkpoint, reprocessing at most the in-flight micro-batch |
| 2 | Kafka consumer (this job's source) restarts | Spark's built-in Kafka source re-subscribes using the checkpointed offsets, not `startingOffsets` (which only applies to a checkpoint-less first run) | No | No | Checkpoint IS the offset store here -- same mechanism as Phase 2's consumer group, but Spark-managed rather than broker-managed |
| 3 | Duplicate event arrives (Kafka at-least-once replay) | In-batch: `dropDuplicates`. Cross-batch: Delta `MERGE` on `transaction_id` | No (deduplicated) | No | N/A directly, but the checkpoint is *why* replay happens at all after a crash -- the MERGE is what makes that safe |
| 4 | Late event arrives (beyond watermark) | Dropped from the relevant windowed aggregation; if it's a Bronze/Silver write (not windowed), it's still processed normally -- watermarking only affects windowed/stateful aggregations, not the base Bronze/Silver writes | No | Yes, from that specific window's aggregate only | Not applicable -- this is a watermark trade-off, not a checkpoint concern |
| 5 | Malformed event (bad JSON / schema mismatch) | `from_json` returns NULL fields rather than raising; `split_valid_invalid` catches and routes to `transactions_rejected` | No | No -- quarantined, not dropped | N/A -- handled by validation logic, independent of checkpointing |

## 10. Performance Considerations

Not implemented as premature optimization -- documented as what would
actually change at each documented scale target:

- **100K transactions**: current local config (`local[*]`,
  `spark.sql.shuffle.partitions=4`, 6 Kafka partitions from Phase 2) is
  more than sufficient; the bottleneck, if any, would be Kafka producer
  throughput from the generator, not Spark.
- **1M transactions**: shuffle partition count should roughly track
  available cores, not stay at the local dev default of 4 -- on a real
  cluster this becomes a per-environment tuning value, not a hardcoded
  constant (already isolated to one `.config()` call in `session.py` for
  exactly this reason). Watermark-driven state store size becomes worth
  monitoring (Spark UI's "state store" metrics) since 1M transactions
  spread across a day means meaningfully more concurrent open windows.
- **5M+ transactions**: Kafka partition count (currently 6 for
  `transactions`, see `docs/kafka.md`) becomes the ceiling on Spark's
  read parallelism -- Spark can't consume a topic with more parallelism
  than it has partitions, so this is the first thing to revisit, not Spark
  configuration. Bronze's `ingestion_date` partitioning keeps individual
  partition file counts bounded regardless of total volume, but very high
  throughput within a single day could still produce many small files per
  micro-batch trigger interval -- worth adding a periodic `OPTIMIZE`
  (Delta's file-compaction command) as a batch job at that scale, not
  implemented here since it's not yet a real problem at this project's
  actual measured volume.
- **Kafka partitions vs. Spark parallelism**: this is the single most
  interview-relevant number in this section -- Spark's Kafka source
  creates one task per Kafka partition per micro-batch, so read
  parallelism is hard-capped at the topic's partition count no matter how
  many Spark executors/cores are available.

## 11. Local Execution

```bash
pip install -r requirements.txt   # includes pyspark==3.5.3, delta-spark==3.2.1
make up                            # Postgres + Kafka
python -m scripts.create_kafka_topics
python -m src.generator.generate --customers 100 --transactions 1000 --seed 42 --publish-kafka
python -m src.streaming.spark.streaming_job --topic transactions
```

> As with Phase 2's Kafka broker, this was written and reviewed carefully
> but not run end-to-end in this sandbox -- `streaming_job.py` requires
> BOTH a reachable Kafka broker (Phase 2's limitation) AND Maven Central
> access for Delta (this phase's limitation). What WAS run for real:
> every transformation, windowing, watermarking, and schema-evolution
> function, against a genuine local Spark session with genuine streaming
> queries (not batch stand-ins) -- see the Phase 3 close-out report for
> the full test list.
