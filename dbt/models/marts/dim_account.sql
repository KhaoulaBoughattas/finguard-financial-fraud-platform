select
    account_id,
    customer_id,
    account_type,
    opened_at,
    home_country
from {{ ref('stg_accounts') }}
