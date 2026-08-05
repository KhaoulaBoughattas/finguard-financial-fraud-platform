-- Grain: one row per customer per DISTINCT risk state (SCD Type 2), per
-- the design in docs/adr/ (Phase 0 architecture amendments):
--   customer_risk_key, customer_id, risk_score, risk_level, risk_reasons,
--   effective_from, effective_to, is_current
--
-- PROVISIONAL, DOCUMENTED HONESTLY: this dbt run only has ONE batch of
-- historical data available (the Phase 1 generator's synthetic dataset,
-- persisted once), not a real time-series of successive risk-state
-- changes streamed in by a live fraud engine (Phase 5) via the
-- foreachBatch upsert pattern documented in Phase 0. So this model
-- currently produces exactly one row per customer (their only-ever-known
-- risk state), with is_current=true and effective_to set far in the
-- future -- it correctly implements the SCD2 GRAIN and COLUMN SHAPE, but
-- there is no real history to show yet. Once Phase 5's fraud engine is
-- writing successive risk states over time, this model's source would
-- change to read that changelog and this comment should be removed.
--
-- risk_score here is a simple, transparent heuristic (this customer's
-- observed fraud rate among their own transactions) -- explicitly NOT a
-- claim that this is the hybrid rules+XGBoost+IsolationForest score from
-- Phase 5. It exists so this mart's shape and tests are meaningful now.
with customer_transaction_stats as (
    select
        customer_id,
        count(*) as total_transactions,
        sum(case when is_fraud then 1 else 0 end) as fraud_transactions,
        max(tx_timestamp) as last_transaction_at
    from {{ ref('int_transactions_enriched') }}
    group by customer_id
)
select
    row_number() over (order by customer_id) as customer_risk_key,
    customer_id,
    case
        when total_transactions = 0 then 0.0
        else round(fraud_transactions::numeric / total_transactions, 4)
    end as risk_score,
    case
        when total_transactions = 0 then 'LOW'
        when fraud_transactions::numeric / total_transactions >= 0.10 then 'HIGH'
        when fraud_transactions::numeric / total_transactions >= 0.03 then 'MEDIUM'
        else 'LOW'
    end as risk_level,
    array_remove(
        array[
            case when fraud_transactions > 0
                 then fraud_transactions || ' of ' || total_transactions || ' transactions flagged fraudulent (ground-truth label)'
            end
        ],
        NULL
    ) as risk_reasons,
    last_transaction_at as effective_from,
    timestamp '9999-12-31' as effective_to,
    true as is_current
from customer_transaction_stats
