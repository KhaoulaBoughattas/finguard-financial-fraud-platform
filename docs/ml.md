# Machine Learning: Fraud Detection (Phase 5)

## 1. Pipeline

```
Postgres (transactions, accounts, customers, merchants)
        │
        ▼
src/ml/data.py            -- load_training_data() joins raw tables
        │
        ▼
src/ml/features.py        -- point-in-time-correct batch feature engineering
        │
        ▼
src/ml/split.py           -- CHRONOLOGICAL train/val/test split
        │
        ├──▶ src/ml/train_supervised.py    -- XGBoost (labeled)
        ├──▶ src/ml/train_unsupervised.py  -- Isolation Forest (unlabeled)
        └──▶ src/ml/evaluate.py            -- baseline / XGBoost / IForest / hybrid, all on TEST
        │
        ▼
src/ml/hybrid.py          -- rules + XGBoost + Isolation Forest, blended, explainable
        │
        ▼
src/ml/mlflow_tracking.py -- log run, register model, Candidate -> Staging -> Production
```

Run with `python -m src.ml.train`. No external network or infrastructure
dependency beyond a reachable Postgres -- MLflow uses a local SQLite file,
so unlike Phases 2-3 (Kafka/Delta), **this phase runs and was validated
end-to-end in this sandbox**, not just implemented-and-skipped.

## 2. Why batch features here, not Phase 3's streaming features

Phase 3's `src/streaming/spark/features.py` computes windowed behavioral
features (transaction counts, unique countries, etc.) for real-time
**scoring** via Spark Structured Streaming. This module computes
analogous features for offline **training**, via pandas over historical
data. These are deliberately two separate implementations of similar
logic -- a real, well-known pattern in ML systems called **training/serving
skew**. It's worth being explicit about rather than pretending one
implementation serves both purposes.

What would reduce that skew in a production system: a shared **feature
store** (e.g., a table both the training pipeline and the real-time
scoring path read the same computed feature values from, rather than each
recomputing similar logic independently) -- not implemented here, since it
would be premature complexity without a live scoring path (Phase 7's
FastAPI service) to actually share it with yet.

## 3. Avoiding Data Leakage -- the two distinct failure modes, both guarded against

**Feature-level leakage**: a transaction's own features must never be
computed using information only available AFTER it happened (including
its own values). `engineer_features()` uses `.shift(1)`/rolling windows
with `closed='left'` -- verified directly, not just asserted, in
`tests/unit/test_ml_features.py` (e.g.
`test_prior_average_amount_excludes_current_row` constructs a transaction
with a deliberately huge amount and confirms it does NOT inflate its OWN
prior-average feature).

**Split-level leakage**: even with leakage-free features, evaluating a
model on data that occurred chronologically BEFORE some of its training
data gives an unrealistically optimistic picture, since production never
sees the future. `temporal_split()` sorts by `tx_timestamp` and splits
chronologically (earliest 70% train, next 15% validation, latest 15%
test) -- not `sklearn.model_selection.train_test_split`'s random default.

**A third, quieter leakage risk avoided**: `scale_pos_weight` (XGBoost's
class-imbalance handling) is computed from the TRAINING split's class
balance only, not the full dataset -- computing it globally would leak
test-set label-distribution information into training.

## 4. Real Results (from an actual run -- see the Phase 5 close-out report for the exact command)

Trained on 30,000 real synthetic transactions (2,000 customers), temporally
split 21,000 / 4,500 / 4,500 (train/val/test), test-set fraud rate 2.6%.

| Model | Precision | Recall | F1 | ROC-AUC | PR-AUC | FPR |
|---|---|---|---|---|---|---|
| Baseline (majority class) | 0.00 | 0.00 | 0.00 | 0.50 | 0.026 | 0.00 |
| XGBoost (supervised) | 0.938 | 0.906 | 0.922 | 0.985 | 0.934 | 0.0016 |
| Isolation Forest (unsupervised) | 0.182 | 0.752 | 0.293 | 0.882 | 0.532 | 0.090 |
| Hybrid (rules + both) | 0.981 | 0.889 | 0.933 | 0.948 | 0.916 | 0.0005 |

**PR-AUC, not accuracy, is the headline metric** here (Phase 0's explicit
requirement) -- with a 2.6% positive rate, a trivial always-predict-negative
baseline would score ~97.4% accuracy while catching zero fraud; its PR-AUC
(0.026, essentially the base rate) is what actually exposes that it has no
discriminative power. XGBoost's PR-AUC of 0.934 is a **35.9x improvement**
over that baseline -- the number the promotion workflow (§6) actually gates
on.

**Reading the model comparison honestly**:
- XGBoost alone is the strongest single model -- expected, since it's
  trained directly on the labeled ground truth these particular fraud
  patterns produce.
- Isolation Forest is markedly weaker on precision (18.2%) -- also
  expected and correct to report plainly: it was trained with NO label
  information at all, so it flags anything statistically unusual, not
  specifically "unusual in the way labeled fraud is unusual." Its purpose
  in the hybrid design is catching patterns XGBoost was never trained on,
  not outperforming a supervised model on data XGBoost has seen the
  labels for.
- The hybrid's recall (0.889) is slightly LOWER than XGBoost alone
  (0.906) -- a real, visible trade-off from blending in Isolation Forest's
  noisier signal, traded for meaningfully higher precision (0.981 vs
  0.938) and a lower false-positive rate. Whether that trade-off is
  "worth it" is a business decision (cost of a missed fraud vs. cost of a
  false alarm), not something this project claims to have optimally
  solved -- see §7.

## 5. Feature Importance (from the same run, by XGBoost gain)

```
seconds_since_prior_txn     375.4
is_cross_border             201.7
amount                      196.2
customer_txn_count_prior     23.6
amount_to_avg_ratio          20.7
merchant_risk_score_base     15.8
hour_of_day                  13.7
customer_avg_amount_prior    11.1
customer_txn_count_last_24h   6.3
is_unusual_hour               2.9
```
`seconds_since_prior_txn` dominating makes sense given the generator's
own fraud patterns: `TRANSACTION_VELOCITY` (rapid-fire transactions) and
`ACCOUNT_TAKEOVER` (probe then immediate high-value purchase) both leave a
strong signature in inter-transaction timing -- the model correctly
learned to key on exactly the behavioral signal the synthetic fraud
patterns were designed around.

## 6. MLflow Tracking and Model Registry

Local SQLite backend (`sqlite:///data/mlflow/mlflow.db`) -- MLflow 3.x
deprecated the older plain filesystem store (`./mlruns`) in favor of a
database backend; SQLite keeps this fully local with no server process,
while unlocking Model Registry features the filesystem store doesn't
support. Every run logs: parameters, all four models' full metric sets,
per-feature importance, and the trained model artifacts themselves
(`mlflow.xgboost.log_model`, `mlflow.sklearn.log_model`).

**Promotion workflow** (`src/ml/mlflow_tracking.py`), via MLflow's current
alias-based API (numeric stage transitions are deprecated in recent MLflow
versions):
```
Candidate  (registered, always)
    │
    ▼  (if test PR-AUC >= 5x the baseline's PR-AUC)
Staging
    │
Production
```
The 5x multiplier (`MIN_PR_AUC_IMPROVEMENT_MULTIPLIER` in `train.py`) is a
**starting point for tuning against business-defined cost thresholds**, not
a claim of a principled optimum -- documented as such in the code, not
presented as more rigorous than it is. In the actual run this gated on, the
achieved improvement (35.9x) cleared it comfortably.

## 7. Business-Rule Thresholds and Their Provenance

`RULE_AMOUNT_RATIO_THRESHOLD = 8.0` in `hybrid.py` is deliberately set to
match the synthetic generator's own `LARGE_AMOUNT_OUTLIER` pattern (8-15x
customer average, `src/generator/fraud_patterns.py`) -- the rule is
calibrated against the same threshold the ground truth uses, which is
honest for this synthetic dataset but should be understood as a
demonstration of the mechanism, not a production-ready threshold tuned
against real transaction-amount distributions. The same caveat applies to
`RULE_VELOCITY_THRESHOLD` and the hybrid blend weights
(`rule_weight=0.2, xgb_weight=0.6, iforest_weight=0.2`) -- real values would
come from a business cost-benefit analysis (false-positive review cost vs.
missed-fraud cost), not asserted here.

## 8. What Phase 5 Does NOT Do

- No real-time scoring endpoint (Phase 7's FastAPI service).
- No feature store (see §2) -- training features are recomputed from raw
  Postgres data each run, not read from a shared, versioned source.
- No hyperparameter tuning / cross-validation sweep -- `n_estimators=200,
  max_depth=5, learning_rate=0.1` are reasonable XGBoost defaults for this
  data scale, not the result of a tuning search. Documented as a known gap,
  not hidden.
- No model monitoring / drift detection (Phase 8, per the original project
  plan's Airflow monitoring DAGs).
- `fact_fraud_alerts`/`fact_customer_risk` in the Phase 4 Gold layer are
  NOT yet repointed at this phase's model output -- they remain the
  ground-truth-derived placeholders documented in `docs/dbt.md §4`. Wiring
  them to read from this phase's actual predictions is future work, not
  done here.
