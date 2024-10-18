{{
  config(
    materialized = 'table',
    )
}}

/*
Type of activities
- New customer: a previous non-customer starts a subscription
- Upsell/cross-sell (will be combined as there's no way to know):
     a current customer increases the number of subscriptions
- Downsell: a current customer decreases the number of subscriptions,
    but still maintains at least one.
- Churn: a customer that previously had at least one subscription now has none
- Reactivation: a previously churned customer starts a subscription
    (original cohort remains as it was)
*/

{#
Goal:
    - to have a table with grain customer and day (for days of interest-- all covering the subscription plus one)
    - with total number of subscriptions on the day
    - and flags for any of the activities above
#}
with raw_data as (
    select * from  {{ ref('stg_manual_activity') }}
            {# where customer_id =1134406 #}
),
-- generate the array to obtain all dates spanning the subscription
days_added as (
    select
        *,
        datediff('day', from_date, to_dt+1) as days, 
        -- assume from_date, to_dt is a close-open interval [from_date, to_dt) 
        -- if from_date = to_dt, active for that day
        -- no difference with from_date = to_dt - 1
        -- not sure how to interpret from_date = to_dt
        array_generate_range(0, days) as arr
    from raw_data
),

expanded_calendar as (
    select
        days_added.customer_id,
        days_added.subscription_id,
        days_added.from_date,
        days_added.to_dt,
        coalesce(arr_exp.value, 0) as days_to_add,
        dateadd('day', days_to_add, days_added.from_date) as current_dt
    from days_added,
        lateral flatten(days_added.arr, OUTER => TRUE) as arr_exp
    union all
    {# this adds one extra row per subscription with current_dt
        one day after the to_dt
    #}
    select
        customer_id,
        NULL as subscription_id,
        NULL as from_date,
        NULL as to_dt,
        NULL as days_to_add,
        dateadd('day', 1, to_dt) as current_dt
    from days_added
),

customer_dt as (
    select
        customer_id,
        current_dt,
        count(distinct subscription_id) as n_subscriptions
    from expanded_calendar
    group by all
    order by customer_id, current_dt
)

select
    customer_id,
    current_dt,
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'current_dt'
        ]) }} as customer_day_sk,
    n_subscriptions,
    lag(n_subscriptions)
        over (partition by customer_id order by current_dt)
        as n_subscriptions_prev_day,
    n_subscriptions_prev_day is NULL as is_new,
    (not is_new) and n_subscriptions_prev_day < n_subscriptions as is_upsell,
    (not is_new) and (n_subscriptions_prev_day > n_subscriptions) and (n_subscriptions != 0) as is_downsell,
    (not is_new) and n_subscriptions = 0 and not is_downsell as is_churn,
    (not is_new)
    and n_subscriptions_prev_day = 0
    and n_subscriptions > 0 as is_reactivate
from customer_dt
