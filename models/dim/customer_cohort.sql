{{
  config(
    materialized = 'table',
    )
}}
-- definition of cohort for a customer:
-- the first time the customer showed any activity
select
    customer_id,
    min(from_date) as initial_dt
from {{ ref('stg_manual_activity') }}
group by all
