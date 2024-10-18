with source as (
      select * from {{ source('manual', 'activity') }}
),
renamed as (
    select
        {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("SUBSCRIPTION_ID") }},
        {{ adapter.quote("FROM_DATE") }},
        {{ adapter.quote("TO_DATE") }} as to_dt

    from source
)
select * from renamed