select
    customer_id,
    country,
    signup_date,
    risk_segment
from {{ ref('stg_customers') }}
