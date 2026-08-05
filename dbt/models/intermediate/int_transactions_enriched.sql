-- Intermediate: one row per transaction, enriched with denormalized
-- customer/account/merchant attributes needed by multiple downstream
-- marts (fact_transactions, fact_fraud_alerts, fact_customer_risk) --
-- computed once here rather than repeating the same joins in each mart.
--
-- customer_id is deliberately sourced via the ACCOUNT dimension
-- (a.customer_id), not directly from the transaction row (t.customer_id).
-- account_id has a real primary key on stg_accounts, so it authoritatively
-- maps to exactly one customer; a transaction's own embedded customer_id
-- field is redundant with that mapping and, in this dev environment (where
-- the Phase 1 generator has been run multiple independent times against
-- the same Postgres database, and account_id numbering restarts at 0 each
-- run), can legitimately disagree with it for a reused account_id from an
-- earlier run. Trusting the dimension instead of the fact's own copy of a
-- foreign key is the correct fix regardless of this specific dev-data
-- quirk -- it's what caught it (via the unique test on
-- fact_account_activity.account_id failing) and it's good practice either way.
select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.merchant_id,
    t.amount,
    t.tx_timestamp,
    t.transaction_country,
    t.device_id,
    t.status,
    t.is_fraud,
    t.fraud_pattern,
    c.risk_segment as customer_risk_segment,
    c.country as customer_home_country,
    a.account_type,
    a.home_country as account_home_country,
    m.merchant_name,
    m.merchant_category,
    m.risk_score_base as merchant_risk_score_base,
    (t.transaction_country != c.country) as is_cross_border,
    date(t.tx_timestamp) as event_date
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
left join {{ ref('stg_customers') }} c on a.customer_id = c.customer_id
left join {{ ref('stg_merchants') }} m on t.merchant_id = m.merchant_id
