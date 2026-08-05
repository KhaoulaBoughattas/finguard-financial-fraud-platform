select
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    risk_score_base
from {{ ref('stg_merchants') }}
