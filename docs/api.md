# FastAPI Scoring Service (Phase 7)

## 1. Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Database + model-load status |
| POST | `/score-transaction` | Real-time fraud scoring for a new transaction |
| GET | `/customers/{customer_id}/risk` | Current risk state (Gold layer, Phase 4) |
| GET | `/transactions/{transaction_id}` | Transaction lookup |
| GET | `/alerts` | Recent fraud alerts, optional `customer_id` filter, bounded `limit` |

All five match the Phase 0 specification exactly. Every route handler is
thin -- validate input via Pydantic, call `src/ml/serving.py` or
`src/api/repository.py`, map the result to an HTTP response. No business
logic lives in `src/api/main.py`.

## 2. Real-time scoring: how it actually works

`POST /score-transaction` does NOT just run the model on the request body
-- amount and country alone aren't enough to compute the behavioral
features (`customer_txn_count_last_24h`, `amount_to_avg_ratio`, etc.) the
model was trained on. Instead:

1. Look up the referenced customer/account/merchant (404 if any don't
   exist -- `UnknownCustomerError`).
2. Load that customer's REAL prior transaction history from Postgres,
   filtered strictly before the new transaction's timestamp
   (`load_customer_history`, added this phase).
3. Append the new transaction as one more row and run it through the
   EXACT SAME `engineer_features()` function training uses (Phase 5).
4. Load the `production`-aliased XGBoost model from MLflow (cached
   in-process after first load) and predict.
5. Run the hybrid rules+model engine (`src/ml/hybrid.py`, Phase 5)
   unchanged, producing a risk score, level, and explainable reasons.

This is the concrete, working answer to the training/serving skew this
project has flagged since Phase 5 (`docs/ml.md §2`): the feature
MATH is identical between training and serving (same function, same
code), even though the mechanism (batch pandas both times, not a live
feature store) is still not what a high-throughput production system
would use.

## 3. Two Real Bugs Found and Fixed This Phase

### Bug 1: Series dtype collapse breaks XGBoost's dtype check
`feature_row[FEATURE_COLUMNS].to_frame().T` looks like a reasonable way
to turn one row into a one-row DataFrame for `model.predict()` -- but a
pandas Series can only hold ONE dtype. A feature row mixes floats, ints,
and booleans; extracting it as a Series collapses every value to
`object`, and XGBoost's dtype validation rejects the whole frame even
though each individual value is fine. Fixed with
`_feature_row_to_model_input()`, which builds the one-row DataFrame from
a dict (preserving per-column type inference) and explicitly casts bool
vs. numeric columns. Regression-tested directly in
`tests/unit/test_ml_serving.py` by constructing the exact object-dtype
Series the bug produced and asserting the fix recovers proper dtypes.

### Bug 2: `array[NULL]` in Postgres isn't an empty array
`GET /customers/{id}/risk` crashed with a Pydantic validation error the
first time it hit a customer with `risk_reasons = []` intended. The dbt
model (`fact_customer_risk.sql`, Phase 4) built the array as
`array[case when ... then ... end]` -- when the CASE has no ELSE and the
condition is false, it evaluates to NULL, and `array[NULL]` in Postgres
produces a ONE-element array containing NULL (`{NULL}`), not an empty
array. This is the same class of bug as Phase 3's Spark
`array_remove(arr, None)` issue (found and fixed then too) -- but the fix
is different here: Postgres's `array_remove(array, NULL)` DOES correctly
strip NULL elements (unlike Spark's, where NULL never equality-matches).
Fixed with `array_remove(array[...], NULL)` in `fact_customer_risk.sql`,
rebuilt via `dbt run --select fact_customer_risk`, and re-verified against
the live API.

## 4. Error Handling

- **Pydantic validation** (missing fields, negative amount, wrong-length
  country code) → automatic `422` with a field-level error body, before
  any application code runs.
- **Unknown customer/account/merchant** → `404` with a specific message,
  not a generic error or a silently-wrong prediction.
- **Scoring failure** (e.g. no production model registered) → `503`, not
  `500` -- signals "temporarily unavailable," distinct from a client
  error or an unexpected server bug.
- **Any unhandled exception** → a global exception handler logs the real
  exception server-side and returns a generic `500` body -- a raw
  traceback is never leaked to the client.

## 5. Model Loading

The production model is loaded once, lazily, on first use (either the
first `/score-transaction` call or the first `/health` check that probes
it), and cached in-process (`src/ml/serving.py::_model_cache`) --
reloading from the MLflow registry on every request would add
unnecessary latency for a model that only changes when Phase 6's
`model_training`/`model_validation` DAGs promote a new version.
`clear_model_cache()` exists for tests and as a hook for a future "reload
model" ops endpoint, not currently exposed over HTTP.

## 6. Testing

`tests/integration/test_api.py` uses FastAPI's `TestClient` (in-process,
no running server needed) -- a persistent background `uvicorn` process
isn't supported in this sandbox (verified directly: a backgrounded
`uvicorn` process was killed when its originating shell command ended),
and `TestClient` is the standard, recommended way to test a FastAPI app
regardless of that constraint. 14 tests cover: health, successful scoring
with real risk output, large-outlier detection, unknown-customer 404,
three distinct validation-error cases (negative amount, missing field,
malformed country code), customer-risk lookup (found and 404), transaction
lookup (found and 404), and alert listing (bounded, filtered, over-limit
rejected). All run against live Postgres and the live MLflow registry --
not mocked.

## 7. What Phase 7 Does NOT Do

- No authentication/authorization on any endpoint -- documented as a gap,
  matching the Phase 0 architecture's "production would use TLS + SASL +
  ACLs" pattern already established for Kafka; the equivalent here would
  be an API gateway or auth middleware, not built in this phase.
- No rate limiting.
- No live `uvicorn` server was run as a persistent process in this
  sandbox -- verified via `TestClient`, matching the pattern used for
  Airflow's `dags test` in Phase 6 (validate real behavior without
  needing a long-running daemon this environment can't sustain).
- Isolation Forest is not served -- only the registry-promoted XGBoost
  model is; the hybrid engine degrades gracefully with a neutral
  Isolation Forest score (0.0) rather than requiring a second model
  lookup for a model whose lifecycle Phase 5/6 doesn't manage via the
  registry.
