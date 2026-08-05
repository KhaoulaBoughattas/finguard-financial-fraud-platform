-- Grain: one row per transaction flagged as fraudulent.
--
-- PROVISIONAL DATA SOURCE, DOCUMENTED HONESTLY: the Phase 0 architecture's
-- fraud-alerts stream is meant to come from the hybrid rules + XGBoost +
-- Isolation Forest engine built in Phase 5, which does not exist yet. This
-- table is instead derived from the synthetic generator's GROUND-TRUTH
-- is_fraud/fraud_pattern labels (src/generator/fraud_patterns.py) -- real
-- data, not fabricated, but a stand-in for the real fraud engine's output,
-- not a claim that fraud detection has been implemented. Once Phase 5
-- exists, this model's source should become the fraud engine's alert
-- stream (via the serving-layer Postgres tables from Phase 0's
-- architecture amendments), not this ground-truth derivation.
select
    transaction_id as alert_id,
    transaction_id,
    customer_id,
    account_id,
    merchant_id,
    amount,
    tx_timestamp as alert_timestamp,
    event_date,
    fraud_pattern as alert_reason,
    'GROUND_TRUTH_LABEL' as alert_source
from {{ ref('int_transactions_enriched') }}
where is_fraud = true
