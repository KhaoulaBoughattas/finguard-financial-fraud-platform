# Interview Preparation — FinGuard Financial

Every answer here references a real, specific artifact in this repo
(a file, a metric, a test count) — not a general description of what the
technology "usually does." If asked to elaborate, the underlying doc is
named so the conversation can go as deep as needed.

## 20 Technical Questions

### 1. Why Kafka instead of a simpler queue (RabbitMQ, SQS)?
Kafka's partition log model gives ordered, replayable, multi-consumer-
group streams — Phase 3 (Spark) and any future batch reprocessing both
need to read the same history independently. A work-queue system
optimizes for distributing individual jobs, not multiple independent
consumers replaying the same ordered log. See `docs/adr/004-kafka-streaming.md`.
*Follow-up: "What would change at 100M events/day?"* → Kafka partition
count becomes the hard ceiling on Spark's read parallelism (one task per
partition per micro-batch) — documented in `docs/spark-streaming.md §10`.

### 2. Why Spark instead of pandas for the streaming pipeline?
Distributed processing, native Delta Lake integration, and a real
watermarking/windowing model for late data — none of which pandas has.
Verified directly: `tests/unit/test_spark_watermark.py` runs an actual
Structured Streaming query across three checkpointed restarts and proves
a late event gets dropped after its window's watermark passes, not just
asserts it in a docstring.

### 3. Why Delta Lake, and what happened when it couldn't run here?
ACID transactions and MERGE semantics on object storage, plus time
travel and schema evolution — the concrete mechanism behind
"effectively-once" Silver writes (`silver.py`'s `DeltaTable.merge()`).
It couldn't be executed in this sandbox: Maven Central (the only
distribution channel for Delta's Spark connector JAR) returns
`403 host_not_allowed`, confirmed directly with curl. Everything
upstream of the actual Delta write — parsing, validation, dedup,
partition assignment — was independently verified against a real local
Spark session instead. See `docs/spark-streaming.md §7`.

### 4. Why Bronze/Silver/Gold instead of a single table?
Bronze preserves raw data (all event types, zero business filtering) so
a bug in Silver logic can be fixed and replayed without re-reading Kafka
(whose retention is far shorter than Delta's). Silver is validated,
deduplicated, one row per transaction. Gold is the star schema BI/ML
consumes. A design correction happened mid-Phase-3: Bronze originally
pre-filtered to transactions only, which defeated its own replay
purpose — fixed to hold all event types once I recognized the
contradiction (`docs/adr/005-spark-delta-lake.md`).

### 5. Why dbt, and where does it actually read from in this project?
SQL-native, testable, documented lineage. It reads from PostgreSQL, not
Silver Delta — because Delta never ran here (see Q3). Documented
explicitly, not silently substituted (`docs/dbt.md §2`) — the models
themselves are source-agnostic; only `sources.yml` would change to point
at the real target.

### 6. Why Airflow 2.x instead of 3.x?
Airflow 3 restructures the architecture (separate API server, DAG
processor) and is newer/less universally deployed as of this writing.
Same reasoning as Spark 3.5 over 4.x and Delta 3.x over 4.x — the
widely-deployed, defensible choice over the newest available one
(`docs/adr/008-airflow-orchestration.md`).

### 7. How do you handle late-arriving events?
Watermarking bounds how long Spark keeps window state before finalizing
it — proven with a real running query (Q2). Explicitly NOT a claim of
zero late data loss: an event arriving after the watermark delay is
dropped, a documented trade-off between state size and completeness
(`docs/spark-streaming.md §4`).

### 8. How do you guarantee exactly-once (or something close to it)?
I don't claim exactly-once — I claim reliable at-least-once with
idempotent downstream writes, and I'm precise about why that's different.
Kafka producer idempotence (`enable.idempotence=True`) removes
broker-side duplicates from the producer's own retries but not
application-level re-publishes. Consumer offsets commit only after full
processing. Delta's Silver MERGE (keyed on `transaction_id`, not
`event_id` — a fresh UUID per publish that would miss real duplicates)
makes reprocessing after a crash a no-op. The precise "what happens if
Spark crashes mid-batch" answer is in `docs/spark-streaming.md §6`.

### 9. How do you handle schema evolution?
`EventEnvelope.schema_version` on every message. V1→V2 (adding
`currency`, `merchant_category`) is additive/backward-compatible —
verified with real Spark parsing tests showing a V1 message read against
a V2 schema gets NULL for the new fields, and a V2 message read against
a V1 schema simply doesn't project the extra columns
(`tests/unit/test_spark_schema_evolution.py`). A breaking change would
bump to `2.0` and route unrecognized versions to the DLQ rather than
silently mis-parse.

### 10. How do you handle Kafka consumer lag?
Not directly load-tested at scale in this sandbox (no live broker — see
Q19), but the design: partition count sets the parallelism ceiling
(Q1 follow-up), and Spark's own checkpoint-based offset tracking means a
lagging consumer resumes from its last committed offset, not from
scratch or from `latest` (which would silently skip the backlog).

### 11. How would you scale to 100M transactions/day?
Kafka partition count first (read parallelism ceiling), then Spark
shuffle-partition tuning (currently a dev-scale default of 4, explicitly
isolated to one config line for exactly this reason), then periodic
Delta `OPTIMIZE` for file compaction under high per-day throughput. Full
reasoning at three scale points (100K/1M/5M+) in `docs/spark-streaming.md §10`.

### 12. How do you prevent data leakage in the ML pipeline?
Two distinct failure modes, both guarded and unit-tested with adversarial
cases: feature-level (`.shift()`/rolling with `closed='left'`, verified by
constructing a transaction with a deliberately huge amount and confirming
it does NOT inflate its own prior-average feature) and split-level
(chronological train/val/test split, never `sklearn`'s random default —
`docs/ml.md §3`).

### 13. Why XGBoost for the supervised model?
Trained directly on labeled ground truth, handles the severe class
imbalance via `scale_pos_weight` computed from the TRAINING split only
(computing it globally would leak test-set class balance into training).
Real result: PR-AUC 0.934–0.955 across runs, a 30–40x improvement over
the majority-class baseline.

### 14. Why Isolation Forest, given it scored much worse than XGBoost?
Because it's trained with NO label information — its job in the hybrid
design is catching patterns XGBoost was never trained on, not
outperforming a supervised model on data XGBoost has seen the labels
for. I report its weaker precision (~18%) plainly rather than hiding it —
that's the honest, expected trade-off of an unsupervised signal, not a
failure (`docs/ml.md §4`).

### 15. How do you monitor model drift?
Two layers: `model_validation` (Airflow, `@daily`) reloads the current
production-aliased model and re-evaluates it — honestly caveated as
evaluating against the full current dataset, not genuinely held-out
future data, since there's no live transaction stream here. `model_monitoring`
(`@hourly`) computes real fraud-rate drift against the historical
baseline via direct SQL — verified with an actual run showing a 1.14–1.45x
drift ratio against a 3.0x threshold.

### 16. How would you migrate this architecture to Azure?
Every phase already has a documented "Local vs. Production" mapping:
Postgres→Azure Database for PostgreSQL, Kafka→Event Hubs, Spark→Databricks
+ ADLS Gen2, MLflow→a real tracking server, secrets→Key Vault. Phase 10's
Terraform modules provision exactly this — 8 modules, validated for HCL
syntax and cross-module composition (`terraform-config-inspect` +
`python-hcl2`, since Terraform's own registry was also blocked in this
sandbox), never `terraform apply`'d.

### 17. How would you reduce Azure costs?
Documented per-module in variable comments (e.g. storage replication:
LRS for dev, GRS/ZRS for production) rather than inventing specific
dollar figures — Phase 0 explicitly warns against fabricating unverified
costs. The actual cost-reduction levers: right-sized SKUs per
environment, Databricks cluster auto-termination, Delta file compaction
to avoid the small-file problem, batch-vs-streaming trade-offs sized to
real business latency requirements.

### 18. How would you secure sensitive financial data?
Phase 9's real audit trail: zero hardcoded secrets (grep-verified), 2
real CVEs found and fixed via `pip-audit` (pyarrow, pytest — both
upgraded, full 151-test suite reverified with zero regressions), API key
auth with constant-time comparison, and an honestly-documented Postgres
least-privilege gap (the local `finguard` role has `CREATEDB`, broader
than needed) that Terraform's `postgres` module architecturally fixes
for a fresh deployment rather than retrofitting onto already-verified
local state (`docs/security.md`, `docs/adr/012-terraform-azure-architecture.md §5`).

### 19. What would you change in a production environment?
Run the things that couldn't run here: a real Kafka broker (Docker),
real Delta Lake (Maven access), real Prometheus/Grafana containers, real
`terraform apply`. Beyond infrastructure access: instrument Kafka/Spark
with Prometheus (currently only the API is instrumented — a deliberate
Phase 8 scoping decision, not an oversight), build a shared feature store
to close the training/serving skew gap named since Phase 5, and replace
the single shared API key with OAuth2/JWT or Azure API Management.

### 20. What's the single most interesting bug you found while building this?
The `array[NULL]` bug in `fact_customer_risk.sql` — Postgres's
`array[case ... end]` produces a ONE-element array containing NULL when
the CASE has no ELSE and evaluates false, not an empty array. It's the
same class of mistake as an earlier Spark bug
(`array_remove(arr, None)` never matching, since SQL NULL-equality is
never true) — but the fix is different: Postgres's `array_remove(array,
NULL)` DOES correctly strip NULLs, unlike Spark's. Two bugs, same root
cause (assuming NULL behaves like a normal value in array construction),
opposite-language-specific fixes — a good example of why "I've seen this
pattern before" isn't the same as "the fix transfers automatically."

## Interview Framing Notes

- **When asked what didn't work**: lead with it, don't wait to be asked
  twice. Every phase in this repo has an honest "NOT RUN" or "known
  limitation" section — that's not weakness, it's the signal that
  everything else claimed as working was actually verified.
- **When asked for a number**: give the real one from the docs, with the
  run it came from (`docs/ml.md §4`'s table, not a rounded guess).
- **When asked "why didn't you just—"** about a workaround for a blocked
  dependency (Docker, Maven, Terraform registry): the honest answer is
  "the sandbox's network policy blocks it, confirmed directly" — not a
  clever bypass. That's the right answer in a real regulated-environment
  job too.
