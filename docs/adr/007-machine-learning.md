# ADR 007: Machine Learning -- Fraud Classification and Anomaly Detection

## Context
Phase 0 specifies a hybrid fraud engine (business rules + XGBoost +
Isolation Forest), MLflow tracking with a promotion workflow, and explicit
warnings against accuracy-only evaluation and data leakage. Phase 5 had to
decide: where features come from (relative to Phase 3's streaming
features), how to guarantee leakage-free training on genuinely temporal
data, how to combine three heterogeneous signals into one explainable
score, and how to run MLflow in a sandbox with no server infrastructure.

## Decision: batch (pandas) feature engineering, separate from Phase 3's
streaming features
Phase 3's Spark Structured Streaming features exist for real-time scoring;
Phase 5 needs point-in-time-correct features for offline training over
historical data. Rather than force one implementation to serve both
purposes, built a separate pandas-based implementation for training. This
is training/serving skew, named and documented (`docs/ml.md §2`) rather
than glossed over -- the honest trade-off is: two implementations to keep
conceptually aligned, in exchange for each being the natural tool for its
own job. **Alternative considered**: build a shared feature store now.
Rejected as premature -- there's no real-time scoring consumer yet
(Phase 7) to share it with, so building the abstraction now would be
speculative.

## Decision: chronological (not random) train/val/test split
`sklearn.model_selection.train_test_split`'s random default was rejected
outright for this data. Financial transactions are a time series; a random
split lets a model train on transactions that happen chronologically after
some of its "test" transactions, which silently inflates evaluation
metrics relative to real deployment. `temporal_split()` sorts by
`tx_timestamp` and splits by time, matching Phase 0's explicit
requirement: "Use appropriate train/test methodology for temporal
financial data."

## Decision: `.shift()`/rolling with `closed='left'`, not naive
`.rolling()`/`.expanding()`
Pandas' default rolling/expanding windows are inclusive of the current
row. Using them directly for behavioral features would leak the
transaction's own amount and fraud outcome into its own feature vector --
a subtle, easy-to-miss mistake that would make a model look far better in
offline evaluation than it could ever perform in production. Verified
directly with unit tests constructing adversarial cases (a transaction
with a deliberately huge amount, checked to NOT inflate its own
prior-average feature) rather than only reasoning about it in comments.

## Decision: three-signal hybrid with a weighted blend and rule-first
explainability
Per Phase 0's explicit requirement ("Do NOT only return a probability.
Make the system explainable"), every scored transaction gets both a
blended numeric score AND a list of human-readable reasons. Business rules
run first and cheaply; the blend weights (0.2 rules / 0.6 XGBoost / 0.2
Isolation Forest) favor the supervised model but keep meaningful weight on
the other two. **Explicitly NOT claimed as an optimally-tuned blend** --
real weights would come from a business cost-benefit analysis, documented
as a stated limitation in `docs/ml.md §7`, not presented as more rigorous
than it is.

## Decision: MLflow with a local SQLite backend, not a filesystem store or
a tracking server
MLflow 3.x deprecated the plain filesystem (`./mlruns`) tracking store.
SQLite (`sqlite:///data/mlflow/mlflow.db`) was chosen over standing up a
real `mlflow server` process: it requires no server, no port, no
additional infrastructure, while still supporting the Model Registry
features the filesystem store does not. This is also, unlike Kafka
(Phase 2) and Delta (Phase 3), a dependency this sandbox could actually
satisfy end-to-end -- verified by running the full pipeline for real, not
just implementing and skipping it.

## Decision: alias-based promotion (Candidate/Staging/Production), not the
older numeric stage API
MLflow's older `transition_model_version_stage` API is deprecated in
favor of registered-model aliases. Implemented the Phase 0-documented
Candidate -> Staging -> Production workflow using
`set_registered_model_alias`, gated on the candidate's test PR-AUC
clearing a minimum multiple of the baseline's PR-AUC -- PR-AUC, not
accuracy or even F1, chosen specifically because it's the metric Phase 0
flags as appropriate for severe class imbalance.

## Bug found and fixed: `groupby().apply()` breaks on single-group input
The initial `customer_txn_count_last_24h` implementation used
`groupby(...).apply(helper_function)`. It worked correctly against the
full 2,000-customer production dataset but raised a `ValueError` against
small unit-test fixtures with only one customer -- pandas'
`groupby.apply()` result-shape inference behaves differently (and
incorrectly, for this use case) when there's exactly one group. Fixed by
switching to `groupby(...).rolling('24h', closed='left').count()` directly
-- a form with no such shape ambiguity regardless of group count, verified
to produce identical results on the production dataset both before and
after the fix.

## Bug found and fixed: cross-test-file Postgres state dependency
`tests/integration/test_ml_training_integration.py` initially trusted
whatever transaction data happened to already be in the shared Postgres
`transactions` table. `tests/integration/test_db_integration.py`'s fixture
`TRUNCATE`s that same table as part of its own setup -- when both files ran
in the same pytest session, the ML integration tests would non-
deterministically fail or misleadingly skip depending on execution order.
Fixed by making the ML integration tests generate their own sufficient
dataset immediately before running, rather than depending on external
state left by other tests -- verified by running both test files in both
orders and confirming identical pass results either way.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| MLflow backend | SQLite file | A real tracking server with a proper database backend |
| Feature source | Batch pandas over Postgres | A feature store shared with the real-time scoring path |
| Model serving | None yet | Phase 7's FastAPI service, loading `models:/finguard-fraud-xgboost@production` |
| Retraining | Manual (`python -m src.ml.train`) | Airflow-scheduled |

## Known Limitations
- No hyperparameter tuning -- reasonable defaults, not a tuning search.
- No model monitoring/drift detection yet.
- `fact_fraud_alerts`/`fact_customer_risk` (Phase 4 Gold layer) are not
  yet repointed at this phase's real model output.
- Business-rule thresholds and hybrid blend weights are starting points,
  not the result of a business cost-benefit analysis.

## Consequences
- Any future phase needing fraud predictions should load the production-
  aliased model via `mlflow_tracking.get_production_model_uri()`, not
  retrain or hardcode a model path.
- `src/ml/features.py`'s `FEATURE_COLUMNS` list is the single source of
  truth for what the model expects -- any future scoring service (Phase 7)
  must compute exactly these features, in a leakage-safe way.
