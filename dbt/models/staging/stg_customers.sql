-- Staging: light renaming/casting only, no business logic. One staging
-- model per source table, 1:1 grain with the source.
select
    customer_id,
    full_name_hash,
    country,
    signup_date,
    risk_segment
from {{ source('finguard_raw', 'customers') }}
