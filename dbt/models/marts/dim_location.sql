-- A small, deliberately simple location dimension: one row per distinct
-- country code observed anywhere in the dataset (transaction, customer
-- home, merchant). Not a full geo-hierarchy (region/city) -- the synthetic
-- generator only produces ISO country codes, so building out a richer
-- hierarchy here would be inventing detail the source data doesn't have.
with countries as (
    select transaction_country as country from {{ ref('stg_transactions') }}
    union
    select country from {{ ref('stg_customers') }}
    union
    select merchant_country as country from {{ ref('stg_merchants') }}
),
distinct_countries as (
    select distinct country from countries
)
select
    row_number() over (order by country) as location_id,
    country
from distinct_countries
