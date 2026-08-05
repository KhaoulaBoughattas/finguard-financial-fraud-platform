-- Grain: one row per account, aggregated activity summary. Supports the
-- "Data Platform Health"/"Customer Risk" Power BI pages' account-level
-- drill-through without needing to re-aggregate fact_transactions on the
-- fly for every report load.
select
    account_id,
    customer_id,
    count(*) as total_transactions,
    sum(amount) as total_amount,
    sum(case when is_fraud then 1 else 0 end) as fraud_transaction_count,
    sum(case when status = 'DECLINED' then 1 else 0 end) as declined_count,
    count(distinct merchant_id) as unique_merchants,
    count(distinct transaction_country) as unique_countries,
    min(tx_timestamp) as first_transaction_at,
    max(tx_timestamp) as last_transaction_at
from {{ ref('int_transactions_enriched') }}
group by account_id, customer_id
