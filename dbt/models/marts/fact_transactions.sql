-- Grain: one row per transaction. The central fact table everything else
-- (fraud alerts, customer risk, account activity) derives from.
select
    transaction_id,
    account_id,
    customer_id,
    merchant_id,
    amount,
    tx_timestamp,
    event_date,
    transaction_country,
    device_id,
    status,
    is_fraud,
    fraud_pattern,
    is_cross_border
from {{ ref('int_transactions_enriched') }}
