select
    merchant_id,
    merchant_name,
    category as merchant_category,
    country as merchant_country,
    risk_score_base
from {{ source('finguard_raw', 'merchants') }}
