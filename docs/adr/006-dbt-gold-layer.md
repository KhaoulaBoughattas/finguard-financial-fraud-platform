# ADR 006: dbt + Gold Layer

## Context
Phase 0 specifies dbt (staging/intermediate/marts) building a Bronze→
Silver→Gold lakehouse, with Gold as a star schema (facts + dimensions)
feeding Power BI. Phase 4 had to decide: what source dbt actually reads
from given Phase 3's Delta limitation, how to handle Gold tables
(`fact_fraud_alerts`, `fact_customer_risk`) that depend on a fraud engine
that doesn't exist yet (Phase 5), and how to avoid repeating Phase 3's
external-dependency surprise.

## Decision: dbt targets Postgres, not Delta, in this environment
Delta Lake could not be executed in Phase 3 (Maven Central blocked). Rather
than leave Phase 4 entirely blocked on that same limitation, dbt is pointed
at the Phase 1 Postgres reference tables -- real, materialized,
end-to-end-testable data in this sandbox. This is documented explicitly in
`sources.yml` and `docs/dbt.md §2`, not silently substituted: the staging/
intermediate/mart SQL itself is source-agnostic (nothing references
Postgres-specific syntax that Silver Delta wouldn't also support via
dbt-spark/Databricks), so repointing `sources.yml` at the real target later
is a small, isolated change, not a rewrite.

**Alternative considered**: leave Phase 4 entirely blocked/undone until
Delta becomes available. Rejected -- the dbt layer, tests, and Gold schema
are independently valuable and verifiable against Postgres, and building
them now means Phase 5 has a real Gold layer to build on rather than
waiting on an external network-policy change with no clear timeline.

## Decision: no dbt packages (no `dbt_utils`)
`dbt deps` fetches packages from `hub.getdbt.com`/GitHub over the network
at install time -- a second potential network-availability risk on top of
the one Delta already exposed in Phase 3. Every place `dbt_utils` would
normally be reached for (`accepted_range`, `date_spine`) was implemented
with native SQL instead (a singular test, Postgres's `generate_series`).
This was a direct lesson carried over from the Delta/Maven Central
discovery: don't add a dependency on an unverified external service when a
few lines of native SQL do the same job.

## Decision: fact_fraud_alerts and fact_customer_risk are explicitly
provisional, sourced from ground-truth labels
Phase 0's Gold layer includes fraud-alert and customer-risk facts that, in
the full architecture, are populated by Phase 5's fraud engine. Building
Phase 4 in the documented dependency order (before Phase 5 exists) meant a
real choice: skip these two tables entirely, or build them from the best
data actually available. Chose to build them from the synthetic
generator's ground-truth `is_fraud`/`fraud_pattern` labels -- real,
non-fabricated data -- with unambiguous labeling (`alert_source =
'GROUND_TRUTH_LABEL'` as an actual column value, not just a comment) so
nobody downstream mistakes this for real fraud-model output. **Alternative
considered**: fabricate placeholder risk scores with no data quality
control. Rejected outright -- this would violate the project's "no fake
results" principle; a clearly-labeled ground-truth derivation is honest,
a fabricated model score would not be.

## Decision: fact_customer_risk implements the SCD2 shape now, with only
one row per customer
The Phase 0 architecture amendments specify `effective_from`,
`effective_to`, `is_current` for historical risk-state reconstruction.
Built the correct column shape and grain now (so Phase 5 can start writing
successive rows into an already-correct table structure), while being
explicit that there's no real history yet -- a `unique(customer_id)` test
was added with an inline comment stating it must change scope (to
`is_current=true` rows only) once real multi-row history exists. This is a
deliberate half-step: building the target shape without pretending it's
already doing what only Phase 5's streaming engine can actually produce.

## Decision: customer_id sourced from the account dimension, not the
transaction's own copy
Found as a real bug during this phase (see below), then generalized into a
standing rule: `int_transactions_enriched` derives `customer_id` via
`stg_accounts` (which has a real primary key on `account_id`), never
trusts a fact row's own denormalized foreign key. This is the correct
pattern regardless of the specific bug that surfaced it.

## Bug found and fixed: account_id reuse across independent generator runs
`dbt test` failed on `unique_fact_account_activity_account_id` (6
duplicates). Root cause: this sandbox's Postgres had accumulated data from
multiple independent `--persist-db` runs across earlier sessions; the
generator's `account_id` numbering restarts at 0 each invocation, so the
same `account_id` string ended up legitimately associated with different
customers across runs. Fixed both operationally (clean single reload for
this demo) and architecturally (customer_id sourced from the dimension, as
above) -- see `docs/dbt.md §5` for the full incident writeup. This is
exactly the class of error `dbt test` exists to catch, and did.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| dbt source | Postgres (Phase 1 reference tables) | Silver Delta (Phase 3), via Databricks SQL/dbt-spark |
| Warehouse for Gold | Postgres (`public_marts` schema) | Delta Gold tables (or a dedicated warehouse fed from Delta) |
| Package management | None (native SQL only) | Same recommendation -- keep dependencies minimal regardless of environment |
| fact_fraud_alerts / fact_customer_risk source | Ground-truth generator labels | Phase 5 fraud engine's real output |

## Known Limitations
- Gold layer is built against Postgres, not Silver Delta -- see decision
  above; the gap is documented, not hidden.
- `fact_fraud_alerts`/`fact_customer_risk` are provisional, ground-truth-
  derived, not real fraud-model output -- see decision above.
- `fact_customer_risk` has no real historical state yet (single snapshot).
- No Power BI dashboard was built against this Gold layer in this phase
  (Phase 0 documents the dashboard spec; wiring an actual BI tool is not
  yet scoped to a specific phase in this project's plan).

## Consequences
- Any future phase reading "the Gold layer" should read from
  `public_marts.*` in Postgres for now, and switch to Delta Gold tables
  only once Phase 3's Delta limitation is resolved -- `docs/dbt.md §2` is
  the canonical place documenting exactly what would need to change.
- Phase 5, when it builds the real fraud engine, must repoint
  `fact_fraud_alerts`/`fact_customer_risk` at real model output and update
  the `fact_customer_risk` uniqueness test scope, both called out
  explicitly in `docs/dbt.md §4` and inline comments in `_marts_models.yml`.
