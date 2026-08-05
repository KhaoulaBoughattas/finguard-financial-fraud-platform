-- Singular test: fails (returns rows) if any staged transaction has a
-- non-positive amount. A native "accepted_range"-style check without
-- depending on the dbt_utils package (avoided deliberately -- see
-- docs/dbt.md for why this project avoids package installs that would
-- need network access to hub.getdbt.com, mirroring the Maven Central
-- caution from Phase 3).
select transaction_id, amount
from {{ ref('stg_transactions') }}
where amount <= 0
