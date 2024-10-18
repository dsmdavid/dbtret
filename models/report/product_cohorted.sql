{{
  config(
    materialized = 'table',
    )
}}

{%- set flags = [
    'is_new',
    'is_upsell',
    'is_downsell',
    'is_churn',
    'is_reactivate'
    ] -%}

with enriched as (
    select
        customer_day.*,
        customer_cohort.initial_dt,
        ccountry.customer_country,
        acq_orders.taxonomy_business_category_group
    from {{ ref('customer_day') }} as customer_day
    left join
        {{ ref('customer_cohort') }} as customer_cohort
        on customer_day.customer_id = customer_cohort.customer_id
    left join
        {{ ref('stg_manual_customers') }} as ccountry
        on customer_day.customer_id = ccountry.customer_id
    left join
        {{ ref('stg_manual_acq_orders') }} as acq_orders
        on customer_day.customer_id = acq_orders.customer_id
),

-- roll up, remove customer_grain
grouped as (
    select
        initial_dt,
        customer_country,
        taxonomy_business_category_group,
        current_dt,
        datediff('day', initial_dt, current_dt) as cohorted_days,
        {{ dbt_utils.generate_surrogate_key([
            'initial_dt',
            'customer_country',
            'taxonomy_business_category_group'
        ])}} as cohort_sk,
        {{ dbt_utils.generate_surrogate_key([
            'cohort_sk',
            'current_dt'
            ])}} as product_cohorted_sk,
        count(distinct customer_id) as current_cohort_size,
        sum(n_subscriptions) as current_total_subscriptions,
        {% for flag in flags %}
            sum(iff({{ flag }}, 1, 0))
                as customer_{{ flag|replace("is_", "") }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from enriched
    group by all
)

select * from grouped
