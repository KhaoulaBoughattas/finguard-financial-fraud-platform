# ADR 003: PostgreSQL Scope in Phase 1

## Context
The Phase 0 architecture defines PostgreSQL as the **serving layer**, populated
by Spark via `foreachBatch` upserts once streaming and the fraud engine exist
(Phase 3/7). In Phase 1 there is no Kafka, no Spark, and no fraud engine yet —
only a synthetic data generator producing batches of customers, accounts,
merchants and transactions.

## Decision
Phase 1 uses PostgreSQL to persist the **generator's reference and transaction
output** (`customers`, `accounts`, `merchants`, `transactions` tables) so that
generated data is queryable, relationally valid (foreign keys enforced), and
testable end-to-end. This is intentionally a *different* use of Postgres than
the streaming serving layer — it is dev/test data loading, not the
`current_customer_risk` / `recent_alerts` / live-feature tables the API will
read from in later phases.

## Alternatives
- **Skip Postgres in Phase 1, keep everything in Parquet**: simpler, but would
  leave foreign-key/referential-integrity assumptions unverified until much
  later, and wouldn't let us build/test the DB access layer (`src/common/db.py`)
  incrementally.
- **Build the full serving-layer schema now**: premature — those tables only
  make sense once there's a streaming write path populating them.

## Trade-offs
Using the same database for two different purposes (dev-data loading now,
serving layer later) means the schema will need to evolve in Phase 3/7 to add
the streaming-specific tables (`current_customer_risk`, `recent_alerts`,
etc.) alongside what's here. That's an accepted, explicit trade-off — the
alternative (a second local database) adds operational complexity Phase 1
doesn't need.

## Consequences
- `infra/docker/init.sql` defines only the four Phase 1 tables.
- The `--persist-db` generator flag is opt-in; Parquet output remains the
  default so Postgres is never a hard dependency just to generate data.
- Phase 3/7 will extend this schema, not replace it.
