# FinGuard Financial — Real-Time Financial Risk & Fraud Data Platform

## Project Overview
An enterprise-grade, end-to-end Data Engineering platform that simulates a modern financial fraud detection system. The project demonstrates how real-time transactions are ingested, validated, processed, transformed, and served using a production-inspired Lakehouse architecture built with Apache Kafka, Spark Structured Streaming, Delta Lake, dbt, Airflow, MLflow, FastAPI, Docker, Terraform, and Azure.

The platform follows modern Data Engineering best practices, including event-driven streaming, Bronze/Silver/Gold data modeling, data quality validation, checkpointing, schema evolution, observability, CI/CD, infrastructure as code, and machine learning lifecycle management.

## Business Problem
FinGuard processes millions of transactions daily and needs to: detect fraud
in real time, maintain live customer risk scores, surface behavioral
anomalies, decide which transactions to block or review, quantify financial
exposure, and keep its own data/ML pipelines observably healthy. See the
Phase 0 architecture doc below for the full requirements breakdown.
## 🏷️ Technologies & Topics

![Data Engineering](https://img.shields.io/badge/Data%20Engineering-0052CC?style=for-the-badge)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Spark Structured Streaming](https://img.shields.io/badge/Spark-Structured%20Streaming-F47A20?style=for-the-badge)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8?style=for-the-badge)
![Lakehouse](https://img.shields.io/badge/Lakehouse-0A66C2?style=for-the-badge)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Microsoft Azure](https://img.shields.io/badge/Microsoft-Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure Data Engineering](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?style=for-the-badge)
![Streaming](https://img.shields.io/badge/Streaming-FF6F00?style=for-the-badge)
![ETL](https://img.shields.io/badge/ETL-4CAF50?style=for-the-badge)
![Real-Time](https://img.shields.io/badge/Real-Time-FF5722?style=for-the-badge)
![Fraud Detection](https://img.shields.io/badge/Fraud-Detection-C62828?style=for-the-badge)
![Data Platform](https://img.shields.io/badge/Data-Platform-6A1B9A?style=for-the-badge)
## Phase 0 Architecture
```
Synthetic Generator ─▶ Kafka ─▶ Spark Structured Streaming ─▶ Fraud Engine
                                                                    │
                                          Delta Lake (Bronze/Silver/Gold)
                                        │                              │
                                       dbt                    foreachBatch upsert
                                        │                              │
                                    Power BI              PostgreSQL Serving Layer
                                                                       │
                                                                   FastAPI
                                                                       │
                                                                Applications

MLflow tracks/registers models · Airflow orchestrates batch/training/monitoring
(not the streaming path) · Prometheus/Grafana observe every layer.
```
Key decisions: Kafka for streaming (maps to Event Hubs on Azure), Spark
Structured Streaming + Delta Lake for the lakehouse, a hybrid rules +
XGBoost + Isolation Forest fraud engine, `fact_customer_risk` as an SCD
Type 2 table for historical risk reconstruction, and a strict separation
between **analytical storage** (Delta) and **serving storage** (PostgreSQL) —
the API never queries Delta directly. Full rationale lives in `docs/adr/`.

## Phase 1 Foundation
**Status: Phase 1 — Foundation: COMPLETED**

Implemented and verified in this phase:
- Configuration management (`src/common/config.py`) — env-driven, no hardcoded secrets
- Structured JSON logging (`src/common/logging.py`)
- Core entity schemas (`src/common/schemas.py`) — Pydantic-validated, Schema V1
- Synthetic data generator (`src/generator/`) — deterministic, seeded, 8 documented fraud patterns
- PostgreSQL persistence layer (`src/common/db.py`) for generated reference/transaction data
- Docker Compose service for local PostgreSQL, with healthcheck
- Test suite: 26 tests (unit + integration), 91% coverage on `src/`
- Makefile developer workflow

**Not yet implemented** (by design — later phases): Kafka, Spark Streaming,
the fraud engine, Delta Lake, dbt, Airflow, FastAPI, MLflow, Terraform,
Prometheus/Grafana. Phase 1 only builds the foundation these depend on.

## Repository Structure
```
finguard-fraud-platform/
├── src/
│   ├── common/          # config, logging, schemas, db persistence
│   ├── generator/        # synthetic data generator + fraud patterns
│   ├── ml/                 # feature engineering, training, evaluation, MLflow, serving (Phase 5/7)
│   ├── api/                  # FastAPI scoring service (Phase 7), API key auth (Phase 9)
│   └── streaming/
│       ├── producer/, consumer/, common/   # Kafka producer, consumer, envelope, DLQ (Phase 2)
│       └── spark/          # Structured Streaming, Bronze/Silver, Delta (Phase 3)
├── dbt/
│   ├── models/staging/, intermediate/, marts/   # Gold layer star schema (Phase 4)
│   ├── tests/               # singular dbt tests
│   ├── dbt_project.yml, profiles.yml
├── dags/
│   ├── common.py            # shared default_args, env helpers
│   ├── gold_layer_refresh_dag.py, daily_data_quality_dag.py,
│   │   model_training_dag.py, model_validation_dag.py, model_monitoring_dag.py
├── scripts/
│   ├── create_kafka_topics.py   # reproducible topic initialization
│   └── airflow_env.sh             # correct Airflow env vars (see docs/airflow.md §5)
├── infra/
│   ├── docker/init.sql, Dockerfile.api   # Postgres schema (Phase 1); API image (Phase 8/9, non-root, not built)
│   ├── k8s/               # reserved for later phases
│   └── terraform/           # 8 modules + dev environment composition (Phase 10, HCL-validated, not applied)
├── monitoring/
│   ├── prometheus.yml       # scrape config (Phase 8, not executed -- no Docker)
│   └── grafana/               # provisioning + api-observability.json dashboard
├── tests/
│   ├── unit/               # includes real local-Spark tests (test_spark_*.py) and ML tests (test_ml_*.py)
│   ├── integration/       # requires reachable Postgres/Kafka/Delta, auto-skips otherwise; includes test_api.py, test_metrics.py
│   └── dags/                 # DAG integrity tests -- run via the Airflow venv, not the main suite
├── docs/
│   ├── adr/               # architecture decision records
│   ├── kafka.md             # Phase 2 Kafka architecture, semantics, failure scenarios
│   ├── spark-streaming.md    # Phase 3 Spark/Delta architecture, windowing, watermarking
│   ├── dbt.md                 # Phase 4 dbt/Gold layer architecture, source note, provisional tables
│   ├── ml.md                    # Phase 5 ML pipeline, leakage guards, real results
│   ├── airflow.md                 # Phase 6 orchestration, two real bugs found and fixed
│   ├── api.md                       # Phase 7 scoring service, two more real bugs found and fixed
│   ├── observability.md               # Phase 8 metrics, what's verified vs. documented-only
│   ├── security.md                      # Phase 9 secrets audit, CVE fixes, API auth, least-privilege review
│   ├── terraform.md                       # Phase 10 Azure IaC, real HCL validation methodology
│   ├── interview-prep.md                    # Phase 11: 20 technical Q&A, grounded in real project artifacts
│   ├── cv-and-pitch.md                        # Phase 11: CV bullets, GitHub/LinkedIn copy, 30s/2min pitches
│   └── diagrams/architecture.md                 # Phase 11: 5 Mermaid diagrams (system, deployment, sequence, ERD, "NOT RUN" map)
├── docker-compose.yml     # Postgres + Kafka (KRaft) + Prometheus + Grafana + API
├── Makefile
├── requirements.txt
├── .env.example
└── README.md
```

## Technology Stack
Python 3.12, Pydantic / pydantic-settings, Faker, pandas + pyarrow (Parquet),
psycopg3, confluent-kafka, PySpark 3.5 + Delta Lake 3.2, dbt-core + dbt-postgres,
scikit-learn + XGBoost + MLflow, Apache Airflow 2.10 (isolated venv), FastAPI + uvicorn,
Prometheus + Grafana (metrics instrumented and tested; containers not run -- no Docker),
pip-audit (dependency vulnerability scanning), Terraform/azurerm (HCL-validated, not applied -- no registry access),
pytest + pytest-cov, PostgreSQL 16,
Apache Kafka (KRaft mode), Docker Compose, ruff.

## Local Setup
```bash
cp .env.example .env
# edit .env and set a real POSTGRES_PASSWORD

make install     # pip install -r requirements.txt
make up           # docker compose up -d (postgres + kafka), waits for healthchecks
python -m scripts.create_kafka_topics   # create the 6 Kafka topics (idempotent)
make generate      # generate synthetic data (parquet by default)
make test           # run the test suite
make test-cov        # run tests with coverage report
make down             # stop postgres + kafka
```

> **Note on this development environment**: the repository and its Docker
> Compose file were authored and are ready to run with `docker compose up`,
> but the sandbox used to build and validate this code has no Docker daemon
> available. All application logic (schema, generator, persistence layer,
> Kafka envelope/producer/consumer/DLQ/partitioning) was instead validated
> either against a real, natively-installed PostgreSQL 16 instance on the
> same host, or via unit tests that don't require a broker (see
> `docs/kafka.md §9` and the Phase 2 close-out report for the exact
> distinction between what was actually executed and what is implemented
> but unverified against a live Kafka broker).

## Environment Variables
See `.env.example` for the full list. Nothing is hardcoded in code — all
config flows through `src/common/config.py`.

## Generate Synthetic Data
```bash
python -m src.generator.generate --customers 1000 --transactions 10000 --seed 42
# add --persist-db to also load into PostgreSQL
```
Output: `data/raw/{customers,accounts,merchants,transactions}.parquet`

## Run Tests
```bash
pytest -v
pytest -v --cov=src --cov-report=term-missing
```

## Data Model (Phase 1 reference schema)
`customers`, `accounts` (FK → customers), `merchants`, `transactions`
(FK → accounts, customers, merchants). Full DDL in `infra/docker/init.sql`.
This is the Phase 1 reference/dev schema — not yet the streaming serving
layer described in Phase 0 (see `docs/adr/003-postgres-scope-phase1.md`).

## Fraud Patterns
Eight behaviorally-structured (not random) fraud episode generators in
`src/generator/fraud_patterns.py`:

| Pattern | Behavior simulated |
|---|---|
| `LARGE_AMOUNT_OUTLIER` | Transaction ~8–15x the customer's average |
| `IMPOSSIBLE_TRAVEL` | Two transactions in different countries within minutes |
| `TRANSACTION_VELOCITY` | 6–12 rapid small transactions (card testing) |
| `UNUSUAL_MERCHANT_CATEGORY` | High-value purchase at a high-risk category (e.g. jewelry) |
| `UNUSUAL_HOUR` | Transaction at 2–4am local time |
| `MULTI_COUNTRY_BURST` | Transactions across up to 4 countries within an hour |
| `ACCOUNT_TAKEOVER` | New device + new country, small probe followed by a high-value purchase |
| `REPEATED_FAILED_ATTEMPTS` | Several declined attempts followed by one approval |

## Phase 2 — Kafka Streaming Ingestion
**Status: Phase 2 — Kafka Ingestion: COMPLETED (code + unit tests); Kafka integration testing: NOT RUN — Docker/Kafka unavailable in the Claude sandbox.**

### What was implemented
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
Not implemented yet (by design — Phase 3+): Spark Structured Streaming, Delta
Lake, dbt, Airflow, MLflow, FastAPI, Prometheus/Grafana, Terraform.

### Listener architecture
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
Two listeners are required because Kafka's `advertised.listeners` handshake
tells a client where to reconnect for subsequent requests, not just where it
first dialed — a host client bootstrapping to `localhost:9092` but then
being handed `kafka:9092` (unresolvable outside Docker) would fail on every
real request. See `docs/kafka.md §1` for the full explanation, and §9 of
this README for the real bug this distinction caught.

### Topics (full detail in `docs/kafka.md §2`)
`transactions`, `customers`, `accounts`, `merchant-events`, `fraud-alerts`,
`dead-letter-events` — partition counts, replication, cleanup policy,
retention, and key field are all defined once in
`src/streaming/producer/topic_config.py` and created reproducibly by
`scripts/create_kafka_topics.py`.

### Partition/key strategy
`transactions`/`fraud-alerts` are keyed by `customer_id` so all of one
customer's events land on the same partition — Kafka orders *within* a
partition, never globally, so this is what makes per-customer event
ordering possible. Demonstrated without needing a broker by reimplementing
Kafka's own default partitioner in `compute_partition()`
(`src/streaming/producer/topic_config.py`) — verified: `C001` always maps
to partition 5, `C002` always maps to partition 0, out of 6 partitions.

### Producer, consumer, envelope, DLQ
- `FinGuardProducer` (`src/streaming/producer/producer.py`): idempotent
  (`enable.idempotence=True`, `acks=all`), bounded retries, delivery
  callbacks, in-process metrics, graceful shutdown (flush before close).
- `FinGuardConsumer` (`src/streaming/consumer/consumer.py`): manual offset
  commits, committed only after a message is fully handled (processed or
  DLQ'd) — never before.
- `EventEnvelope` (`src/streaming/common/events.py`): every message carries
  `schema_version`, `event_type`, `event_id`, `event_timestamp`, `payload` —
  the mechanism Phase 0's documented V1→V2 transaction schema evolution
  hooks into.
- DLQ: any envelope or payload validation failure is captured as a
  `DeadLetterEvent` (original topic/partition/offset/event_id/error
  type+message/timestamp/original payload) and published to
  `dead-letter-events` before the original offset is committed.

### Delivery semantics
Reliable **at-least-once** delivery with producer-side idempotence
(eliminates broker-side duplicates from the producer's own retries, does
**not** provide end-to-end exactly-once). Duplicates and reprocessing are
resolved downstream via idempotent writes (Postgres `ON CONFLICT
transaction_id DO NOTHING` today, Delta `MERGE` in Phase 4) — not claimed to
be eliminated at the transport layer. Full failure-scenario table (8
interview-relevant scenarios) in `docs/kafka.md §8`.

### Local setup for Kafka specifically
```bash
make up
python -m scripts.create_kafka_topics --bootstrap-servers localhost:9092
python -m src.generator.generate --customers 100 --transactions 1000 --seed 42 --publish-kafka
pytest -v tests/integration/test_kafka_integration.py
```

### Kafka integration testing limitation
```text
Kafka integration testing:
NOT RUN — Docker/Kafka unavailable in the Claude sandbox.
```
The full producer→broker→consumer→DLQ code path is implemented and covered
by `tests/integration/test_kafka_integration.py`, which skips automatically
(not "passes") when no broker is reachable — exactly what happened in this
environment. All Kafka logic that does *not* require a live broker (envelope
serialization, partition-key determinism, payload validation, DLQ
construction, producer configuration, topic-creation logic) was unit tested
for real, with a fake `confluent_kafka.Producer`/`AdminClient` standing in
for the broker. See the Phase 2 close-out report for the full breakdown.

## Phase 3 — Spark Structured Streaming + Delta Lake
**Status: Phase 3 — Spark/Delta: COMPLETED (implementation + real Spark tests); Delta Lake integration testing: NOT RUN — Maven Central unavailable in the Claude sandbox.**

### What was implemented
```
Kafka (transactions topic) → Spark Structured Streaming → Bronze Delta (raw, all event types)
                                                                  │
                                                     (foreachBatch, per micro-batch)
                                                                  ▼
                                            filter → parse → validate → dedup → MERGE
                                                                  │
                                                                  ▼
                                                          Silver Delta (validated, deduplicated)
```
Full entrypoint: `python -m src.streaming.spark.streaming_job`. Not
implemented yet (Phase 4+, by design): Gold layer, dbt, Airflow, MLflow,
FastAPI, Prometheus/Grafana, Terraform.

### Bronze vs. Silver
Bronze preserves everything (any event_type, envelope-parsed, ingestion
metadata added, zero business filtering) so it can be replayed if Silver
logic needs fixing later. Silver filters to `event_type='transaction'`,
parses the payload, validates business rules (required fields, positive
amount, recognized status, non-future timestamp), deduplicates, and
MERGEs. Full detail in `docs/spark-streaming.md §2`.

### Deduplication
Two layers: in-batch (`dropDuplicates` on `transaction_id`) and cross-batch
(Delta `MERGE ... WHEN NOT MATCHED THEN INSERT`, also keyed on
`transaction_id` — not `event_id`, which is a fresh UUID per publish and
would miss real duplicates by design). See `docs/spark-streaming.md §5`.

### Windowing and watermarking — verified with a real streaming query
`tests/unit/test_spark_watermark.py` runs an actual Structured Streaming
query (checkpointed across several `trigger(availableNow=True)` restarts,
not a batch-mode stand-in) proving: an on-time event is counted, and a
late event arriving after its window's watermark has passed is dropped —
not counted, not erred on. Full explanation, including the precise
difference between event time and processing time, in
`docs/spark-streaming.md §3-4`.

### Checkpointing
Local checkpoint directories under `data/checkpoints/{bronze,silver}/transactions`
(configurable via `DeltaSettings`, env-prefixed `DELTA_`). Precise "what
happens if Spark crashes mid-batch" answer, and why this gives
at-least-once-with-idempotent-writes rather than global exactly-once, in
`docs/spark-streaming.md §6`.

### Delta Lake
```text
PySpark transformation tests: VERIFIED (30 tests, real local Spark session)
Watermark tests: VERIFIED (real streaming query, checkpointed across restarts)
Delta Lake write integration: NOT RUN
Reason: Delta Spark connector unavailable because Maven Central is blocked in the sandbox.
```
`bronze.py`/`silver.py` implement the real Delta APIs (`format("delta")`,
`mergeSchema`, `DeltaTable.merge()`); `requirements.txt` pins
`pyspark==3.5.3`/`delta-spark==3.2.1`. Verified directly: `curl -sI
https://repo1.maven.org/maven2/` → `403 host_not_allowed`; Delta's GitHub
releases were checked and only publish Hive-assembly jars, not the core
Spark connector. `tests/integration/test_spark_delta_integration.py` skips
automatically (not "passes") when the connector can't be resolved — same
honesty pattern as Phase 2's Kafka integration tests. Full detail in
`docs/spark-streaming.md §7`.

### Partitioning
Bronze: `event_type`, `ingestion_date`. Silver: `event_date`. `customer_id`
was explicitly considered and rejected for either table — too
high-cardinality, would fragment into many small per-customer files.
Reasoning in `docs/spark-streaming.md §8`.

### Local setup for Spark specifically
```bash
pip install -r requirements.txt   # includes pyspark, delta-spark
make up
python -m scripts.create_kafka_topics
python -m src.generator.generate --customers 100 --transactions 1000 --seed 42 --publish-kafka
python -m src.streaming.spark.streaming_job --topic transactions
```
> Requires both a reachable Kafka broker (Phase 2's limitation) and Maven
> Central access (this phase's limitation) — neither was available in this
> sandbox, so this exact command was not run end-to-end here. Every
> transformation, windowing, watermarking, and schema-evolution function
> WAS run for real against a genuine local Spark session — see
> `docs/spark-streaming.md §11` and the Phase 3 close-out report.

## Phase 4 — dbt + Gold Layer + Data Warehouse
**Status: Phase 4 — dbt/Gold Layer: COMPLETED. dbt run: PASS (14/14 models). dbt test: PASS (68/68 tests) — against real, live-generated data.**

### Architecture note — read this first
dbt targets **Postgres** (the Phase 1 reference tables), not Silver Delta,
because Delta Lake could not be executed in Phase 3 (Maven Central
blocked). This is documented, not hidden — see `docs/dbt.md §2` for exactly
what would change to point this at the real target once Delta is
available. The dbt models/tests themselves are source-agnostic.

```
Postgres (customers, accounts, merchants, transactions)
        ↓
dbt staging (stg_customers, stg_accounts, stg_merchants, stg_transactions)
        ↓
dbt intermediate (int_transactions_enriched)
        ↓
dbt marts / Gold layer
    dim_customer, dim_account, dim_merchant, dim_location, dim_date
    fact_transactions, fact_fraud_alerts*, fact_customer_risk*, fact_account_activity
    (* provisional — see below)
```

### Provisional Gold tables
`fact_fraud_alerts` and `fact_customer_risk` are derived from the
synthetic generator's **ground-truth** fraud labels, not a real fraud
engine (Phase 5 doesn't exist yet). This is explicit in the data itself
(`alert_source = 'GROUND_TRUTH_LABEL'`), not just in comments. Full
reasoning in `docs/dbt.md §4`.

### Real data, real results
```text
dim_customer: 2,000 rows
dim_account: 3,030 rows
dim_merchant: 300 rows
dim_location: 15 rows
dim_date: 91 rows
fact_transactions: 30,000 rows
fact_fraud_alerts: 451 rows
fact_customer_risk: 2,000 rows (80 HIGH / 58 MEDIUM / 1,862 LOW)
fact_account_activity: 3,030 rows
dbt test: 68/68 PASS
```

### A real bug found and fixed
`dbt test` caught 6 duplicate `account_id` values in `fact_account_activity`
— caused by `account_id` numbering restarting at 0 across independent
generator runs in this dev environment, combined with a fact table
trusting a transaction row's own (denormalized, occasionally stale)
`customer_id` instead of the authoritative account→customer mapping.
Fixed both operationally (clean reload) and architecturally
(`int_transactions_enriched` now sources `customer_id` via a join through
`stg_accounts`). Full incident writeup in `docs/dbt.md §5`.

### No dbt packages
`dbt_utils` was deliberately avoided — installing a dbt package requires
network access to `hub.getdbt.com`/GitHub at `dbt deps` time, a second
external-dependency risk on top of Delta's Maven Central issue. Every
place it would have been used (`accepted_range`, `date_spine`) was
implemented with native SQL instead.

### Local setup for dbt specifically
```bash
python -m src.generator.generate --customers 2000 --transactions 30000 --seed 42 --persist-db
cd dbt
set -a && . ../.env && set +a
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
```

## Phase 5 — Machine Learning (Fraud Classification + Anomaly Detection)
**Status: Phase 5 — ML: COMPLETED and RUN END-TO-END. No environment blockers this phase — MLflow's local SQLite backend needs no server or external network.**

### Pipeline
```
Postgres → batch feature engineering (point-in-time correct)
        → CHRONOLOGICAL train/val/test split
        → XGBoost (supervised) + Isolation Forest (unsupervised) + baseline
        → hybrid engine (rules + both models, blended, explainable)
        → MLflow tracking + registry + Candidate→Staging→Production promotion
```
Run with `python -m src.ml.train`. Full detail: `docs/ml.md`.

### Real results (from an actual run — 30,000 transactions, 21K/4.5K/4.5K temporal split)
```text
                  Precision  Recall    F1     ROC-AUC  PR-AUC  FPR
Baseline           0.000     0.000    0.000    0.500   0.026   0.0000
XGBoost            0.938     0.906    0.922    0.985   0.934   0.0016
Isolation Forest   0.182     0.752    0.293    0.882   0.532   0.0901
Hybrid             0.981     0.889    0.933    0.948   0.916   0.0005
```
XGBoost achieves a **35.9x PR-AUC improvement** over the majority-class baseline — the number the promotion workflow actually gates on (auto-promoted to Production in the run this table is from). PR-AUC, not accuracy, is the headline metric — with a 2.6% fraud rate, accuracy alone would hide a model that catches nothing. Full discussion of the honest trade-offs (Isolation Forest's weaker precision, the hybrid's slightly lower recall than XGBoost alone) in `docs/ml.md §4`.

### Leakage guards — verified, not just claimed
Two distinct leakage risks, both guarded against and unit-tested with adversarial cases:
- **Feature-level**: every behavioral feature uses `.shift()`/rolling windows with `closed='left'` so a transaction's own data never leaks into its own features (`tests/unit/test_ml_features.py`).
- **Split-level**: chronological (not random) train/val/test split, so the model is never evaluated on data that occurred before some of its training data (`tests/unit/test_ml_split.py`).

### Two real bugs found and fixed this phase
- A `groupby().apply()` edge case that silently worked on the 2,000-customer production dataset but broke on small test fixtures with exactly one group — fixed with a more robust `groupby().rolling()` form.
- A cross-test-file Postgres state dependency: another test file's fixture truncates the shared `transactions` table, which could silently starve the ML integration tests of data depending on run order. Fixed by making the ML tests generate their own data. Both detailed in `docs/adr/007-machine-learning.md`.

### Local setup for ML specifically
```bash
python -m src.generator.generate --customers 2000 --transactions 30000 --seed 42 --persist-db
python -m src.ml.train
```

## Phase 6 — Airflow Orchestration
**Status: Phase 6 — Airflow: COMPLETED and RUN END-TO-END. All 5 DAGs executed successfully via `airflow dags test` against real infrastructure.**

### DAGs
```text
gold_layer_refresh  (@daily)   generate data → dbt run → dbt test
daily_data_quality  (@daily)   real SQL-computed quality report + gate
model_training       (@weekly)  Phase 5 pipeline; fails if model doesn't beat baseline
model_validation     (@daily)   reload PRODUCTION model, re-evaluate, fail on degradation
model_monitoring     (@hourly)  recent vs. historical fraud-rate drift check
```
No business logic is duplicated into DAG files — every DAG is a thin wrapper around already-tested pipelines (`src.generator.generate`, `dbt run`/`test`, `src.ml.train`/`evaluate`). Full detail: `docs/airflow.md`.

### Real results (from actual `airflow dags test` runs, not descriptions)
```text
gold_layer_refresh:  generate → dbt run (14/14) → dbt test (68/68) — SUCCESS
daily_data_quality:  10,050 records, 100.0% quality score — SUCCESS
model_training:      XGBoost PR-AUC 0.9546, 39.78x baseline, promoted to production — SUCCESS
model_validation:    reloaded production model, PR-AUC 0.9355 — SUCCESS
model_monitoring:    drift ratio 1.14x (< 3.0x threshold) — SUCCESS
```

### Two real bugs found and fixed
1. **Cross-phase dependency conflict**: installing Phase 5's ML stack had silently upgraded `typing_extensions` to a version incompatible with `dbt`'s `mashumaro` dependency — `dbt --version` was broken and nobody had noticed until this phase's `gold_layer_refresh` DAG tried to call it. Fixed by pinning `typing_extensions==4.16.0`, verified against the full 120-test suite.
2. **`AIRFLOW_HOME` named `airflow` inside the repo shadowed the real package**: Python's implicit namespace packages turned that directory into a fake, empty `airflow` module for anything with the repo root on `sys.path` — including this project's own test suite. Compounded by some Airflow log paths not fully respecting `AIRFLOW_HOME` and silently recreating the same directory. Fixed by relocating `AIRFLOW_HOME` outside the repo and setting every log path explicitly (`scripts/airflow_env.sh`). Full incident writeup: `docs/adr/008-airflow-orchestration.md`.

### Why Airflow runs in its own virtualenv
To avoid destabilizing the accumulated Phases 1-5 dependency set (exactly the risk that caused bug #1 above) — `apache-airflow==2.10.5` lives in `/home/claude/airflow_venv`, separate from the main `requirements.txt` environment.

### Local setup for Airflow specifically
```bash
export POSTGRES_PASSWORD=<your password>
. scripts/airflow_env.sh
/home/claude/airflow_venv/bin/airflow db migrate
/home/claude/airflow_venv/bin/airflow dags test gold_layer_refresh 2026-01-01
# or: make airflow-dag-test DAG=gold_layer_refresh DATE=2026-01-01
```

## Phase 7 — FastAPI Scoring Service
**Status: Phase 7 — API: COMPLETED and tested end-to-end for real (14/14 tests, via FastAPI's TestClient) against live Postgres and the live MLflow production model.**

### Endpoints
```text
GET  /health                          database + model-load status
POST /score-transaction                real-time fraud scoring, explainable
GET  /customers/{customer_id}/risk     current risk state (Gold layer)
GET  /transactions/{transaction_id}    transaction lookup
GET  /alerts                           recent fraud alerts, filterable
```
All five match the Phase 0 spec exactly. Every handler is thin — validates via Pydantic, delegates to `src/ml/serving.py`/`src/api/repository.py`, no business logic in the route file.

### Real-time scoring, not a training-time shortcut
A new transaction is appended to that customer's real prior history (queried fresh from Postgres) and run through the **exact same** `engineer_features()` function Phase 5's training pipeline uses — the concrete fix for the training/serving skew this project has flagged since Phase 5. Verified against real data:
```text
$50,000 transaction (customer's real average ~$61) → HIGH risk, "transaction amount 818.2x customer average"
$55 transaction, correct home country → LOW risk, "no individual risk factors triggered"
```

### Two more real bugs found and fixed
1. **Series dtype collapse**: extracting a feature row as `pd.Series` collapses all columns to `object` dtype (a Series can only hold one dtype), which XGBoost's `predict()` rejects. Fixed by building the model input from a dict with explicit per-column casting — regression-tested directly.
2. **`array[NULL]` in Postgres ≠ empty array**: `GET /customers/{id}/risk` crashed on its first real request — `fact_customer_risk.sql`'s array construction produced `{NULL}` instead of `{}` when no risk reasons applied. Same bug class as Phase 3's Spark issue, different (correct-for-Postgres) fix: `array_remove(array[...], NULL)`.

A third issue — an earlier edit to `src/ml/data.py` had silently deleted `load_training_data`'s own `def` line while adding a new function — was caught by rerunning the **full** regression suite, not just this phase's new tests, and fixed by deduplicating the two functions' shared SQL. Full detail on all three: `docs/api.md` and `docs/adr/009-fastapi-scoring-service.md`.

### Testing approach
A backgrounded `uvicorn` process was tried and confirmed killed when its shell command ended — this sandbox doesn't support persistent background processes. Used FastAPI's `TestClient` instead (in-process, the standard tool for this, not a workaround) — same posture as Phase 6's `airflow dags test`.

### Local setup for the API specifically
```bash
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
curl -X POST http://localhost:8000/score-transaction -H "Content-Type: application/json" -d '{
  "transaction_id": "TX1", "account_id": "ACCT0000000", "customer_id": "CUST0000000",
  "merchant_id": "MERCH000000", "amount": 50000, "country": "XX", "device_id": "DEV_NEW"
}'
```

## Phase 8 — Observability (Prometheus/Grafana)
**Status: Phase 8 — Observability: COMPLETED. FastAPI metrics: VERIFIED end-to-end (8/8 tests). Prometheus/Grafana containers: NOT RUN (no Docker in this sandbox).**

### What's real vs. documented-only
```text
GET /metrics (FastAPI, Prometheus text format):     VERIFIED — real requests, real counter/histogram values
Prometheus scraping /metrics:                        NOT RUN — no Docker daemon
Grafana rendering the dashboard:                      NOT RUN — no Docker daemon
```
Consistent with the Kafka/Delta pattern from Phases 2–3: the code that *can* run here was actually run and tested; the containers that would consume it are documented, reviewed, and syntax-validated, not executed.

### Metrics
Generic HTTP metrics (`prometheus-fastapi-instrumentator`) plus domain-specific counters (`src/api/metrics.py`): scoring requests, scoring latency histogram, scoring errors by type, risk-level distribution, customer-risk lookup hit rate, alerts listed. All confirmed live in one real run:
```text
finguard_scoring_requests_total 1.0
finguard_scoring_latency_seconds_sum 0.6307195379999939
finguard_risk_level_total{risk_level="LOW"} 1.0
finguard_customer_risk_lookups_total{found="True"} 1.0
finguard_alerts_listed_total 3.0
```

### One real dashboard, not several unverifiable ones
Only the API layer has live, tested metrics in this sandbox — Kafka/Spark/Airflow aren't instrumented for Prometheus yet, and building dashboard panels for metrics that don't exist would violate this project's "no fake results" principle just as much as fabricating a number. `monitoring/grafana/dashboards/api-observability.json` (6 panels, syntax-validated) covers only what's real.

### A second cross-test isolation bug found and fixed
The same class of bug found in Phase 5 (a shared `TRUNCATE` fixture silently starving other tests of data depending on run order) recurred for the new API/metrics tests. Generalized the Phase 5 fix into a shared `tests/integration/conftest.py` fixture so every integration test needing real reference data is now order-independent by construction, not by individual test-by-test luck.

### Local setup
```bash
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
curl http://localhost:8000/metrics
# Full stack (requires Docker, not run here): docker compose up -d prometheus grafana api
```

## Phase 9 — Security & Secrets Management
**Status: Phase 9 — Security: COMPLETED. Real audits run: secrets grep (clean), dependency scan (2 CVEs found and fixed), Postgres privilege review (1 finding, documented). API key auth implemented and tested (16 tests).**

### Real findings, not just policy documentation
```text
Secrets grep across the whole repo:     0 hardcoded credentials found
pip-audit (dependency scan):            2 real CVEs found (pyarrow, pytest) — both upgraded, re-scanned clean
Postgres role privileges (\du):         finguard role has CREATEDB — broader than needed, documented gap
Full test suite after CVE upgrades:     144 → 151 passed, 0 regressions
```

### CVEs found and fixed
```text
pyarrow 18.0.0 → 23.0.1   (PYSEC-2026-113, Use-After-Free in Arrow C++ IPC pre-buffering)
pytest 8.3.3 → 9.0.3        (PYSEC-2026-1845, predictable /tmp dir, local privilege issue)
```
Both upgraded rather than accepted as low-risk, and only kept after the full 144-test suite passed with zero regressions — an unverified patch is its own risk.

### API key authentication
`X-API-Key` header, constant-time comparison, required on every endpoint touching customer/transaction/financial data. `/health` and `/metrics` deliberately stay public — verified directly with a test that a configured key still lets `/health` through with none provided. No key configured → allowed through with a logged warning (local-dev-only posture, matching the same pattern already used for Kafka's local unauthenticated mode).

### Honest gaps, not hidden ones
- Postgres `finguard` role has `CREATEDB` (granted in Phase 6 for local convenience) — documented as a real least-privilege gap with the correct production fix (separate app vs. migration roles), not fixed here to avoid destabilizing already-verified earlier-phase state.
- Non-root Docker container fixed (`infra/docker/Dockerfile.api`) but unbuilt — no Docker daemon in this sandbox.
- Single shared API key, not OAuth2/JWT — documented production target: Azure API Management.

### Local setup
```bash
pip-audit -r requirements.txt --desc
export SECURITY_API_KEY=your-local-dev-key   # optional; unset = auth disabled with a warning
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
curl -H "X-API-Key: your-local-dev-key" http://localhost:8000/alerts
```

## Phase 10 — Terraform / Azure Deployment Architecture
**Status: Phase 10 — Terraform: COMPLETED. Real HCL syntax + cross-module composition validation run (9/9 directories valid, 0 composition problems). `terraform init`/`plan`/`apply` NOT RUN — both HashiCorp's release server and the Terraform Registry are network-blocked in this sandbox.**

### Real validation, without the terraform binary itself
```text
curl -sI https://releases.hashicorp.com/terraform/   → 403 host_not_allowed
curl -sI https://registry.terraform.io/                → 403 host_not_allowed
```
Neither the `terraform` CLI nor the `azurerm` provider it needs could be installed the normal way — a harder version of the Maven Central (Phase 3) and Docker (Phase 2+) limitations. Instead: `terraform-config-inspect` (HashiCorp's own shallow HCL parser, apt-installable, no registry needed) + `python-hcl2` (an independent parser) validated all 8 modules and the `dev` environment with **zero diagnostics**. A custom script (`scripts/check_terraform_composition.py`) additionally cross-checked every module call's variables and every output reference against what each module actually declares — catching the class of mistake neither parser alone can see, since neither resolves cross-module references.
```bash
make terraform-validate
# ALL MODULES VALID
# Module composition valid: all variables/outputs referenced correctly.
```

### 8 modules, one environment composition
`resource_group`, `networking` (VNet + Databricks-delegated subnets + deny-by-default NSG), `storage` (ADLS Gen2, bronze/silver/gold/checkpoints), `key_vault` (read-only app access policy), `container_registry` (non-root image target, managed-identity pull), `postgres` (Flexible Server, separate app/Airflow databases), `databricks` (VNet-injected), `monitoring` (Log Analytics + App Insights + Azure Monitor managed Prometheus) — composed in `environments/dev/main.tf`. Every module maps directly to a local component documented across Phases 1–9.

### Carrying the Phase 9 least-privilege gap forward — fixed, not just noted again
The `postgres` module provisions separate `finguard` and `airflow` databases on a fresh server — the right foundation for the least-privilege role split Phase 9 found but deliberately didn't retrofit onto the already-running local sandbox. The actual GRANT SQL is flagged as follow-up work (would need a `postgresql` provider), not silently assumed done.

### Local setup
```bash
bash scripts/validate_terraform.sh
python3 scripts/check_terraform_composition.py
# Real deployment (requires registry access this sandbox doesn't have):
# cd infra/terraform/environments/dev && terraform init && terraform plan
```

## Current Status
Phase 1 — Foundation: **COMPLETED**.
Phase 2 — Kafka Ingestion: **COMPLETED** (implementation + unit tests); **Kafka integration NOT RUN** (no Docker in this sandbox).
Phase 3 — Spark Structured Streaming + Delta Lake: **COMPLETED** (implementation + real Spark tests); **Delta Lake integration NOT RUN** (Maven Central unavailable in this sandbox).
Phase 4 — dbt + Gold Layer: **COMPLETED** — dbt run and dbt test both actually executed and passing (14/14 models, 68/68 tests) against Postgres, with the Delta-vs-Postgres source substitution explicitly documented.
Phase 5 — Machine Learning: **COMPLETED** and run end-to-end for real — XGBoost/Isolation Forest/hybrid trained, evaluated, tracked in MLflow, and promoted to Production, all against live data.
Phase 6 — Airflow Orchestration: **COMPLETED** and run end-to-end for real — all 5 DAGs executed successfully via `airflow dags test`, two real cross-environment bugs found and fixed along the way.
Phase 7 — FastAPI Scoring Service: **COMPLETED** and tested end-to-end for real — all 5 endpoints working against live data, three more real bugs found and fixed.
Phase 8 — Observability: **COMPLETED** — FastAPI metrics verified end-to-end (8/8 tests); Prometheus/Grafana containers documented and syntax-validated, not run (no Docker).
Phase 9 — Security & Secrets Management: **COMPLETED** — real secrets/dependency/privilege audits run, 2 CVEs fixed, API auth implemented and tested (16 tests), gaps documented honestly rather than hidden.
## Phase 11 — Documentation & Final Project Assembly
**Status: Phase 11 — Final Assembly: COMPLETED. Project complete (11/11 phases).**

### What this phase added
- **5 Mermaid architecture diagrams** (`docs/diagrams/architecture.md`): end-to-end system, local-vs-production deployment mapping, a real sequence diagram of the actual scoring request flow, the Gold layer star schema as an ERD, and a "what NOT RUN actually means" diagram pairing every environment-blocked component with what was independently verified in its place.
- **20-question interview prep guide** (`docs/interview-prep.md`), each answer citing a specific file/metric/test count in this repo rather than a generic description.
- **CV bullets, GitHub/LinkedIn copy, and 30-second/2-minute pitches** (`docs/cv-and-pitch.md`), built only from real, sourced numbers from this project's own docs — no rounding up.

### One more real finding, caught during final regression
Running the final full-suite pass surfaced 3 new CVEs in `cryptography` (a transitive dependency Phase 9's original audit didn't have pinned, and therefore couldn't reproducibly scan). Investigated and mostly fixed: upgraded `mlflow` 3.14.0→3.15.1 to relax its version cap, pinned `cryptography==49.0.0` (fixes 2 of 3), and verified zero code-path exposure to the one remaining, unfixable-without-a-newer-mlflow CVE via a direct grep — not an assumption. Documented as an honest addendum to `docs/security.md §9`, not a silent retroactive edit of Phase 9's original findings.

### Final numbers (from the last full regression run of the whole project)
```text
Tests: 151 passed, 0 failed, 6 skipped (Kafka/Delta integration — documented infra limitations)
Coverage: 88%
Ruff: PASS
pip-audit: 1 known vulnerability (documented accepted risk, zero verified exposure)
dbt: 14/14 models, 68/68 tests
Terraform: 9/9 directories HCL-valid, 0 composition problems
```
---

---

## 👤 Author

<div align="center">

**Khawla BOUGHATTAS**
*Data Engineering & Decision Support Systems · ENET'COM Sfax · Class of 2026*

[![GitHub](https://img.shields.io/badge/GitHub-khaoula--boughattas-181717?style=for-the-badge&logo=github)](https://github.com/khaoula-boughattas)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/khaoula-boughattas-983597295/)

</div>

---

