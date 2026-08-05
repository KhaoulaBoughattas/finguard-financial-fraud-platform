# dbt + Gold Layer (Phase 4)

## 1. What this phase builds

```
Postgres (Phase 1 reference tables)          [LOCAL/SANDBOX SOURCE -- see §2]
        │
        ▼
dbt staging (stg_customers, stg_accounts, stg_merchants, stg_transactions)
        │  light renaming/casting only
        ▼
dbt intermediate (int_transactions_enriched)
        │  joins, denormalization -- computed once, reused by every mart
        ▼
dbt marts (Gold layer)
    dim_customer, dim_account, dim_merchant, dim_location, dim_date
    fact_transactions, fact_fraud_alerts, fact_customer_risk, fact_account_activity
```

Run with: `cd dbt && dbt run --profiles-dir .` (see §6 for env setup).
Tested with: `dbt test --profiles-dir .` -- **68/68 tests passing** against
real, live-generated data (2,000 customers, 30,000 transactions).

## 2. Local/sandbox source note -- read this before extending the models

**In the target architecture (Phase 0), dbt reads from the Silver Delta
table** (Phase 3), via Databricks SQL or a Delta-aware warehouse. **That
could not be executed in this sandbox** -- Delta Lake's Spark connector is
only distributed via Maven Central, which this sandbox's network policy
blocks (see the Phase 3 close-out report and `docs/spark-streaming.md §7`
for the full investigation).

So Phase 4's `sources.yml` instead points at the **Postgres reference
tables from Phase 1** (`customers`, `accounts`, `merchants`,
`transactions`, populated by `python -m src.generator.generate
--persist-db`) -- the only end-to-end materialized dataset available in
this environment. This is a deliberate, documented substitution, not a
silent architecture change: the dbt *models themselves* (staging →
intermediate → marts, the SQL logic, the tests) are exactly what would run
against Silver Delta too. **The only thing that would change to point this
at the real target** is `models/staging/_sources.yml`'s source
definition (and the profile/connection, if the warehouse also changes from
Postgres to a Delta-aware target) -- none of the staging/intermediate/mart
SQL references anything Postgres-specific in a way that would need
rewriting.

## 3. Why Postgres works as a stand-in here specifically

Phase 1's Postgres tables hold the *same* transaction data (with the same
`is_fraud`/`fraud_pattern` ground-truth labels) that would eventually flow
through Kafka → Spark → Silver Delta. Using it lets Phase 4's dbt models,
tests, and Gold-layer shape be built and genuinely validated now, rather
than blocked entirely by the Delta limitation -- while keeping the gap
explicit rather than pretending Delta was used.

## 4. Provisional Gold tables -- read before treating these as "real" fraud output

**`fact_fraud_alerts` and `fact_customer_risk` are explicitly provisional**,
documented as such in both the model SQL and `_marts_models.yml`:

- `fact_fraud_alerts` is derived from the synthetic generator's
  **ground-truth** `is_fraud`/`fraud_pattern` labels
  (`src/generator/fraud_patterns.py`) -- real data, correctly modeled, but
  a stand-in for what Phase 5's actual hybrid rules + XGBoost + Isolation
  Forest fraud engine will produce. `alert_source = 'GROUND_TRUTH_LABEL'`
  makes this explicit in the data itself, not just in comments.
- `fact_customer_risk` implements the documented SCD Type 2 **shape**
  (`customer_risk_key`, `effective_from`, `effective_to`, `is_current`) from
  the Phase 0 architecture amendments, but currently produces exactly
  **one** row per customer -- because this dbt run only has one batch of
  historical data, not a real time-series of successive risk-state changes
  streamed in by a live fraud engine. `risk_score` here is a transparent,
  simple heuristic (a customer's own observed fraud rate) -- explicitly NOT
  the real hybrid model's output.

When Phase 5 exists: `fact_fraud_alerts` and `fact_customer_risk` should be
repointed at the fraud engine's actual output (via the Postgres serving
layer or Silver Delta, per Phase 0's serving-layer design), and the
`unique(customer_id)` test on `fact_customer_risk` (currently passing
because there's no history yet) should be changed to only apply to
`is_current=true` rows once real history exists -- this is called out
directly in that test's own comment in `_marts_models.yml`.

## 5. Data-quality bug found and fixed during this phase

`dbt test` initially failed: `unique_fact_account_activity_account_id` --
6 duplicate `account_id` values. Root cause: this sandbox's Postgres had
accumulated data from multiple independent `--persist-db` generator runs
across earlier sessions; the generator's `account_id` numbering restarts
at 0 each invocation, so the same `account_id` string had legitimately
been assigned to different `customer_id`s across different runs, and
`int_transactions_enriched` was trusting each transaction row's own
embedded `customer_id` rather than the authoritative account→customer
mapping.

**Fix, both operational and architectural**:
1. Truncated and did one clean generator run for this demo (removes the
   accumulated cross-run collision for now).
2. **`int_transactions_enriched` now sources `customer_id` via a join
   through `stg_accounts`** (which has a real primary key on
   `account_id`), not from the transaction row's own copy of that foreign
   key. This is the correct fix regardless of the specific dev-data quirk
   that surfaced it -- a fact table should never trust a denormalized
   foreign key over the dimension that actually owns that relationship.

This is exactly the kind of thing `dbt test` is supposed to catch, and it
did.

## 6. Local Setup

```bash
pip install -r requirements.txt   # includes dbt-core==1.12.0, dbt-postgres==1.11.0
cp .env.example .env
# set POSTGRES_PASSWORD

make up   # or a native Postgres install, as used to validate this phase (see README)
python -m src.generator.generate --customers 2000 --transactions 30000 --seed 42 --persist-db

cd dbt
set -a && . ../.env && set +a   # dbt's profiles.yml reads the SAME POSTGRES_* env vars
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
```

`profiles.yml` deliberately reuses `POSTGRES_HOST`/`POSTGRES_PORT`/
`POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` -- the same variables
`src/common/config.py` uses -- rather than inventing `DBT_`-prefixed
duplicates. One source of truth for "which Postgres, which credentials."

## 7. Why no `dbt_utils` package

`dbt_utils` (for `accepted_range`, `date_spine`, etc.) was deliberately
avoided. Installing a dbt package requires `dbt deps`, which fetches from
`hub.getdbt.com`/GitHub at install time -- another external dependency this
sandbox might not reach, mirroring the Maven Central lesson from Phase 3.
Everything `dbt_utils` would have provided here was implemented with
native SQL instead: a singular test
(`tests/assert_transaction_amounts_positive.sql`) for the range check, and
Postgres's native `generate_series` for the `dim_date` spine. This keeps
the project's dependency footprint minimal and avoids a second
network-availability risk on top of the one already documented for Delta.

## 8. Data Model Reference

| Table | Grain | Notes |
|---|---|---|
| `dim_customer` | one row per customer | |
| `dim_account` | one row per account | FK to `dim_customer` |
| `dim_merchant` | one row per merchant | |
| `dim_location` | one row per distinct country code | Deliberately simple -- no city/region hierarchy, since the source data doesn't have one |
| `dim_date` | one row per calendar day in the observed transaction range | Generated via `generate_series`, not a package macro |
| `fact_transactions` | one row per transaction | Central fact table |
| `fact_fraud_alerts` | one row per fraud-flagged transaction | **Provisional** -- see §4 |
| `fact_customer_risk` | one row per customer risk STATE (SCD2 shape) | **Provisional, single-snapshot** -- see §4 |
| `fact_account_activity` | one row per account | Aggregated summary metrics |
