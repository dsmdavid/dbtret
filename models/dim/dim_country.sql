{{
  config(
    materialized = 'table',
    )
}}

select distinct customer_country as country
from {{ ref('stg_manual_customers') }}
