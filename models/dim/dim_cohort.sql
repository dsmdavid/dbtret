{{
  config(
    materialized = 'table',
    )
}}
select
    initial_dt,
    customer_country,
    taxonomy_business_category_group,
    cohort_sk,
    current_cohort_size as initial_cohort_size

from {{ ref('product_cohorted') }}
where initial_dt = current_dt
