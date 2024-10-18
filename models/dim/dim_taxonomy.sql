{{
  config(
    materialized = 'table',
    )
}}

select distinct taxonomy_business_category_group
from {{ ref('stg_manual_acq_orders') }}