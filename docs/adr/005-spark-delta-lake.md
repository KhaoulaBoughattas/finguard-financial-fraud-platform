# ADR 005: Spark Structured Streaming + Delta Lake

## Context
Phase 0 specifies Spark Structured Streaming reading from Kafka and writing
a Bronze/Silver/Gold Delta lakehouse. Phase 3 scopes this to Bronze/Silver
only (Gold + dbt is Phase 4) and had to decide: Spark version, Delta
version, Bronze/Silver responsibility split, deduplication key and
mechanism, watermarking parameters, checkpoint strategy, and partitioning
-- all against the constraint that this sandbox has no Kafka broker
(Phase 2) and, newly discovered this phase, no Maven Central access either.

## Decision: Spark 3.5.x + Delta 3.2.x, not Spark 4.x + Delta 4.x
Both lines were available. Spark 3.5.x is the version most widely deployed
in industry as of this writing (Spark 4.0 is recent), and Delta 3.2.x is
the verified-compatible pairing per Delta's own compatibility matrix. Using
the more conservative, widely-deployed pairing is more defensible in an EY
interview than using the newest available version for its own sake --
consistent with this project's stated principle of not adding
sophistication without a reason.

## Decision: Bronze holds ALL event types; Silver filters to transactions
Initially built with Bronze pre-filtered to transaction events only
(matching the Phase 0 diagram's apparent flow). Revised during this phase:
Bronze now parses the envelope generically (any `event_type`) and adds only
ingestion metadata, with zero business filtering; Silver is where
`event_type='transaction'` filtering, payload parsing, and validation
happen. **Why changed**: Bronze's documented purpose (Phase 0) is to
preserve raw data for replay -- pre-filtering by event_type there would
mean a future Phase (e.g. a customer/account processing pipeline) couldn't
replay from Bronze for its own needs, defeating the point of having a raw
layer at all. This is a real design correction made during Phase 3, not
the original plan.

## Decision: transaction_id as the deduplication key, with two layers
`dropDuplicates(["transaction_id"])` for in-batch duplicates, Delta
`MERGE ... WHEN NOT MATCHED THEN INSERT` keyed on `transaction_id` for
cross-batch duplicates (the actual at-least-once-replay case). `event_id`
(a fresh UUID per publish, from Phase 2's `EventEnvelope`) was explicitly
rejected as the key -- it would fail to catch exactly the duplicates that
matter, since two publishes of the same logical transaction get different
event_ids by design. See `docs/spark-streaming.md §5` for the full
reasoning and `tests/integration/test_spark_delta_integration.py::
test_silver_merge_deduplicates_across_micro_batches` for the test that
would prove this (currently skipped -- see Delta decision below).

## Decision: partition by event_type/ingestion_date (Bronze) and
event_date (Silver), not customer_id
customer_id was considered and explicitly rejected: with thousands of
customers, it would fragment the table into many small per-customer files
(the Spark "small file problem"), hurting both write and read performance,
for a query pattern (customer-level lookups) already served by the
Postgres serving layer from Phase 0/1. Date-based partitioning matches the
actual dominant query pattern (date-range analytical queries, Power BI
reports) with bounded partition counts. Bronze and Silver use *different*
date columns deliberately (ingestion_date vs. event_date) because they
answer different questions -- see `docs/spark-streaming.md §3` for why
that distinction matters under replay/backfill.

## Decision: at-least-once + idempotent MERGE, not Delta/Spark
transactional exactly-once
Same posture as Phase 2's Kafka decision, for the same reason: Spark's
checkpoint + Kafka source coordination gives at-least-once reprocessing
after a crash; the Silver MERGE makes that reprocessing idempotent in
practice. This is explicitly NOT claimed as global exactly-once -- Bronze
(blind append, no MERGE) does not have this protection, which is
acceptable because Bronze is a raw staging layer nothing queries directly,
not a correctness-critical table.

## Decision: 5-minute/2-minute example window/watermark, pattern
generalizes
`compute_windowed_count` and `compute_windowed_distinct_count` take
duration/watermark as parameters rather than hardcoding one feature's
values, specifically so the same tested mechanism covers every documented
Phase 0 feature (`transactions_last_5min` through `unique_merchants_last_24h`)
by changing arguments, not by writing near-duplicate functions per feature.
Building all ~10 features individually was considered and rejected as
volume without additional engineering signal over demonstrating the
pattern once, correctly, with a real test proving the watermark's late-data
behavior.

## Decision (forced): Delta integration implemented, not executed, in this
sandbox
Delta Lake's Spark connector is distributed exclusively via Maven Central.
This sandbox's network allowlist blocks Maven Central (`403
host_not_allowed`, verified directly); Delta's GitHub releases were checked
as a fallback and only publish Hive-assembly jars, not the core Spark
connector needed here. This is not a design decision so much as a
documented environment constraint: `bronze.py`/`silver.py` implement the
correct, real Delta APIs (`format("delta")`, `mergeSchema`,
`DeltaTable.merge()`), and `tests/integration/test_spark_delta_integration.py`
is written to validate them for real -- both are simply unexecuted here.
See `docs/spark-streaming.md §7` for the full detail and exact commands to
run elsewhere.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Spark deployment | `local[*]`, single JVM | Databricks (per Phase 0 Azure target) or Spark-on-AKS |
| Delta storage | Local filesystem (`data/delta/...`) | ADLS Gen2 (`abfss://...`) -- `DeltaSettings.lake_root` is the only place this would need to change |
| Shuffle partitions | 4 (hardcoded default, dev-scale) | Tuned to cluster core count |
| File compaction | Not implemented (not yet needed at dev volume) | Periodic `OPTIMIZE` batch job (see `docs/spark-streaming.md §10`) |

## Known Limitations
- Delta Lake write path is implemented but **not executed** in this
  sandbox (Maven Central blocked) -- see decision above.
- The end-to-end Kafka→Spark→Bronze→Silver flow additionally requires a
  live Kafka broker (Phase 2's limitation), so `streaming_job.py` itself
  has not been run even independent of the Delta question.
- No Gold layer yet (Phase 4, by design -- out of scope here).
- `transactions_rejected` (the Spark-layer DLQ-equivalent) is implemented
  in `silver.py` but not covered by a dedicated automated test beyond the
  `split_valid_invalid` unit tests that produce its input.

## Consequences
- Any future Phase (Gold/dbt in Phase 4, ML feature engineering in Phase 5)
  reading from Silver can rely on: validated transactions only, one row per
  transaction_id, `event_date`-partitioned.
- Any future consumer needing non-transaction Bronze data (customers,
  accounts, merchant_events) can read Bronze directly and apply its own
  Silver-equivalent logic, without needing changes to this phase's code.
- `src/common/config.py`'s `DeltaSettings` is the only place Delta
  table/checkpoint paths should be defined; no future code should hardcode
  a `data/delta/...` path directly.
