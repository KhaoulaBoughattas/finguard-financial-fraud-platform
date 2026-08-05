# CV, GitHub, LinkedIn & Interview Pitch Material

Every metric below is sourced from a specific doc/test-run in this repo,
listed alongside it — nothing here is rounded up or invented for effect.

## Project Title
**FinGuard Financial — Real-Time Fraud Detection & Risk Data Platform**

## Technology List
Python, Apache Kafka, Apache Spark (Structured Streaming), Delta Lake,
PostgreSQL, dbt, Apache Airflow, XGBoost, Isolation Forest, MLflow,
FastAPI, Prometheus, Grafana, Terraform, Docker, GitHub Actions-ready CI
patterns, Azure (target architecture).

## 3 ATS-Friendly CV Bullets

> **Designed and built an 11-phase, production-modeled data engineering
> platform** (Kafka → Spark Structured Streaming → Delta Lake → dbt →
> XGBoost/Isolation Forest hybrid fraud model → FastAPI → Airflow →
> Terraform/Azure), achieving 151 passing automated tests across unit,
> integration, and infrastructure-validation layers with 88% code
> coverage. *(source: final regression run, this repo's README "Current
> Status")*

> **Trained and deployed a hybrid fraud-detection model** (XGBoost +
> Isolation Forest + business rules) achieving a 30–40x PR-AUC
> improvement over baseline (0.934–0.955 vs. ~0.02 for a majority-class
> baseline) on a severely imbalanced dataset (~2% fraud rate), with
> leakage-safe point-in-time feature engineering verified by adversarial
> unit tests. *(source: docs/ml.md §4)*

> **Identified and fixed 10+ real cross-environment bugs** during
> development — including a Postgres NULL-array construction bug, an
> MLflow 3.x model-URI compatibility break, a Python namespace-package
> collision that silently shadowed a real library, and 2 CVEs found via
> `pip-audit` — each diagnosed to root cause and verified fixed against
> the full test suite, not just patched and assumed working. *(source:
> ADRs 005, 007, 008, 009, 011 across this repo)*

## GitHub Description
> A production-modeled, end-to-end fraud detection data platform —
> streaming ingestion (Kafka/Spark/Delta), a dbt-modeled analytics
> warehouse, a hybrid ML fraud engine (XGBoost + Isolation Forest) served
> via FastAPI, orchestrated with Airflow, instrumented with Prometheus,
> and provisioned via Terraform for Azure. Built with an explicit
> "no fake results" discipline: every phase's README section states
> plainly what was executed and verified vs. documented as a production
> target, with real test counts and real metrics throughout.

## LinkedIn Project Description
> I built FinGuard Financial, an 11-phase fraud-detection data platform,
> to go deep on the full modern data engineering stack end-to-end:
> real-time ingestion (Kafka, Spark Structured Streaming), a lakehouse
> (Delta Lake, Bronze/Silver/Gold), an analytics warehouse (dbt, 68
> passing data tests), a hybrid ML fraud model (XGBoost + Isolation
> Forest, 30–40x PR-AUC improvement over baseline), a scoring API
> (FastAPI), orchestration (Airflow), observability (Prometheus/Grafana),
> security hardening (dependency scanning, API auth, least-privilege
> review), and infrastructure-as-code (Terraform/Azure). What I'm most
> proud of isn't any single component — it's the discipline of verifying
> everything I could and being precise about what I couldn't (a few
> components needed infrastructure my dev environment didn't have), which
> mirrors how I'd want to work on a real production team.

## 30-Second Pitch
"FinGuard is an end-to-end fraud detection platform I built to
demonstrate the full modern data stack — Kafka and Spark for real-time
ingestion, Delta Lake and dbt for the analytics layer, a hybrid XGBoost
and Isolation Forest model for fraud scoring, served through FastAPI and
orchestrated with Airflow, with Terraform for the Azure deployment
architecture. The model achieves a 30 to 40x improvement over baseline on
a severely imbalanced dataset, verified with leakage-safe, point-in-time
feature engineering. Across 151 tests, I found and fixed real bugs at
nearly every layer — which is honestly the part I learned the most from."

## 2-Minute Pitch
"FinGuard is a fraud detection platform for a fictional financial
institution, built as an 11-phase project to demonstrate real,
production-grade data engineering practices rather than a toy pipeline.

The ingestion layer uses Kafka in KRaft mode with a documented
partition/key strategy — customer_id keying so Kafka's per-partition
ordering guarantee becomes per-customer ordering, which matters for
correct behavioral feature computation downstream. Spark Structured
Streaming reads that into a Bronze/Silver Delta Lake architecture, with
real watermarking and checkpointing — I actually ran a live streaming
query across multiple checkpointed restarts to prove late-arriving events
get dropped correctly after their window's watermark passes, rather than
just asserting that in a comment.

On top of that, dbt builds a star-schema analytics warehouse — 9 models,
68 passing data tests, including one real bug I found and fixed where a
Postgres array-construction pattern silently produced a one-element array
containing NULL instead of an empty array.

The ML layer trains an XGBoost classifier and an Isolation Forest
anomaly detector, blended with business rules into an explainable hybrid
score. I was deliberate about avoiding data leakage — both feature-level,
using point-in-time-correct rolling windows verified with adversarial
tests, and split-level, using a chronological train/test split instead of
a random one, since financial transactions are a time series. The model
achieves a 30 to 40x improvement in PR-AUC over a majority-class baseline
on a roughly 2% fraud rate — and I report the weaker unsupervised model's
results honestly too, since that's the accurate picture, not just the
number that looks best.

That model is served through a FastAPI scoring endpoint, orchestrated by
five Airflow DAGs covering data quality, training, validation, and
monitoring, instrumented with Prometheus metrics, hardened with API-key
auth and a real dependency vulnerability scan that found and fixed two
CVEs, and provisioned via eight Terraform modules for an Azure
deployment.

The thing I'd want a hiring team to take away isn't any single
component — it's that every phase's documentation states plainly what I
actually ran and verified versus what I could only document as the
production target, because a few components — Docker, Delta's Maven
dependency, Terraform's registry — needed infrastructure my sandbox
didn't have. I'd rather show you exactly where the real edges are than
claim a clean sweep."
