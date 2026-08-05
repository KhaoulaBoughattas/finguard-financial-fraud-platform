# ADR 009: FastAPI Scoring Service

## Context
Phase 0 specifies four HTTP endpoints (health, customer risk, transaction
lookup, alerts) plus a scoring endpoint, with Pydantic validation and
structured error handling. Phase 7 had to decide: how real-time feature
computation reuses (or doesn't reuse) Phase 5's training-time feature
engineering, how the API's serving-layer reads relate to the Phase 0/4
Gold layer, and how to test an HTTP service in a sandbox that can't sustain
a persistent server process.

## Decision: real-time feature computation reuses engineer_features()
directly, not a reimplementation
A new, not-yet-persisted transaction is appended to that customer's real
prior history (queried fresh from Postgres, strictly before the new
transaction's timestamp) and run through the SAME `engineer_features()`
function Phase 5's training pipeline uses. **Alternative considered**:
hand-write simplified serving-time feature logic (faster to reason about
in isolation). Rejected -- any divergence between training-time and
serving-time feature math is exactly the kind of silent bug that produces
a model that scores well in evaluation and badly in production; sharing
the actual function eliminates that entire class of bug by construction,
at the cost of a live database round-trip per scoring request (acceptable
at this project's scale, flagged as a real trade-off in `docs/api.md §2`
and `docs/ml.md §2`'s training/serving-skew discussion -- not eliminated,
narrowed to "same feature math, different feature-store mechanism").

## Decision: GET endpoints read from the Gold layer, not recomputed
inline
`/customers/{id}/risk` and `/alerts` query `public_marts.fact_customer_risk`
/`fact_fraud_alerts` (Phase 4's dbt Gold layer) directly -- consistent
with the Phase 0 serving-layer principle that the API reads from
analytical/serving tables it doesn't own the write path for, rather than
recomputing business logic in a route handler. This also means these two
endpoints inherit Phase 4's already-documented caveat: `fact_fraud_alerts`
and `fact_customer_risk` are ground-truth-derived placeholders, not real
fraud-engine output yet (`docs/dbt.md §4`) -- the API surfaces that data
honestly rather than hiding the provenance.

## Decision: model loaded once, cached in-process
Reloading from the MLflow registry on every request would add latency and
registry load for a model that changes only when Phase 6's
`model_training`/`model_validation` DAGs promote a new version. Cached
after first load; `clear_model_cache()` exists for tests and as a future
ops-endpoint hook, not currently exposed over HTTP -- a real deployment
would add a `/admin/reload-model` endpoint or a scheduled restart tied to
Phase 6's promotion events, not built here since there's no live traffic
to justify it yet.

## Decision: FastAPI's TestClient, not a running uvicorn process, for
testing
A backgrounded `uvicorn` process was tried and confirmed killed when its
originating shell command ended -- this sandbox does not support
persistent background processes across tool invocations. `TestClient`
(in-process, synchronous, no real socket) is the standard, recommended way
to test a FastAPI app regardless of that constraint -- not a workaround,
the normal tool for this job. Same posture as Phase 6's `airflow dags
test`: validate real behavior without needing a long-running daemon.

## Bug found and fixed: Series dtype collapse breaks XGBoost's dtype check
Extracting one feature row as a `pd.Series` (from `.iloc[-1]` on a
DataFrame with mixed float/int/bool columns) collapses every value to
`object` dtype, since a Series can only hold one dtype -- XGBoost's
`predict()` rejects the resulting DataFrame even though every individual
value is valid. Fixed with `_feature_row_to_model_input()`, building the
one-row frame from a dict (preserving per-column type inference) with
explicit bool/numeric casting. Regression-tested by constructing the
exact object-dtype Series the bug produced.

## Bug found and fixed: `array[NULL]` in Postgres is a one-element array,
not empty
`GET /customers/{id}/risk` crashed on its first real request against a
customer with no triggered risk reasons -- `fact_customer_risk.sql`
(Phase 4) built `risk_reasons` as `array[case when ... end]`; when the
CASE has no ELSE and evaluates to NULL, `array[NULL]` in Postgres produces
`{NULL}` (a 1-element array containing NULL), not `{}` (empty). This is
the same class of bug as Phase 3's Spark `array_remove(arr, None)` issue,
but Postgres's `array_remove(array, NULL)` DOES correctly strip NULL
elements (unlike Spark's null-equality semantics) -- fixed by wrapping the
array construction in `array_remove(..., NULL)`, rebuilt via `dbt run
--select fact_customer_risk`, verified against the live API afterward, not
just assumed fixed from reading the SQL.

## Bug found and fixed: a str_replace edit silently deleted a function
definition
While adding `load_customer_history` to `src/ml/data.py`, an edit matched
and replaced the `def load_training_data(...):` line itself, leaving that
function's entire body (docstring, query, connection block) orphaned
below with no `def` line -- syntactically valid Python (no error until
import), but `load_training_data` became unimportable, silently breaking
every module that depended on it (`src/ml/train.py`, and by extension the
Phase 5 training integration tests). Caught by running the full test
suite after the Phase 7 changes, not assumed clean because Phase 7's own
new tests passed. Fixed by rewriting the file with both functions sharing
a single `_SELECT_JOIN` query fragment -- deduplicating the near-identical
SQL is also what removes the copy-paste surface that caused the original
mistake, not just a fix for this one instance of it.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Server process | `TestClient`, no persistent daemon | Real `uvicorn`/`gunicorn` workers behind a load balancer |
| Auth | None | API gateway / auth middleware (TLS + token-based, matching the Kafka TLS+SASL pattern already documented) |
| Feature computation | Live DB round-trip per request | A feature store (see `docs/ml.md §2`) for lower-latency, training-aligned serving |
| Model reload | Process restart | Scheduled reload or an admin endpoint tied to Phase 6 promotion events |

## Known Limitations
- No authentication/authorization or rate limiting -- documented gap, not
  silently omitted.
- No persistent server process run in this sandbox -- validated via
  `TestClient`, not sustained real-traffic operation.
- Isolation Forest isn't served (only XGBoost is registry-managed) -- the
  hybrid engine degrades gracefully rather than requiring it.
- Real-time feature computation does one DB round-trip per request (full
  customer history reload) -- acceptable at this project's scale, a real
  latency concern at production transaction volumes (see `docs/ml.md §2`'s
  feature-store discussion for the actual fix).

## Consequences
- Any future phase (Prometheus/Grafana observability, Terraform/Azure
  deployment) instrumenting or deploying this service should treat
  `src/api/main.py`'s route handlers as the stable HTTP contract and
  `src/ml/serving.py`/`src/api/repository.py` as the layer to instrument,
  not duplicate logic into new files.
- `fact_customer_risk.sql`'s `array_remove(..., NULL)` pattern should be
  the template for any future dbt model building a nullable-CASE array --
  the bare `array[case ... end]` form is now a known footgun in this
  codebase.
