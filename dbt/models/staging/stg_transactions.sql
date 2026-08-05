select
    transaction_id,
    account_id,
    customer_id,
    merchant_id,
    amount,
    tx_timestamp,
    country as transaction_country,
    device_id,
    status,
    is_fraud,
    fraud_pattern
from {{ source('finguard_raw', 'transactions') }}
