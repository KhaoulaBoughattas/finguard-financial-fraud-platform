-- Date spine covering the observed transaction date range, generated with
-- Postgres's native generate_series (no dbt_utils date_spine dependency --
-- consistent with this project avoiding package installs that need network
-- access, see docs/dbt.md).
with bounds as (
    select
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('int_transactions_enriched') }}
),
spine as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    (extract(dow from date_day) in (0, 6)) as is_weekend
from spine
