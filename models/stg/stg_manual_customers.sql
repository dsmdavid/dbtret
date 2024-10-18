with source as (
      select * from {{ source('manual', 'customers') }}
),
renamed as (
    select
        {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("CUSTOMER_COUNTRY") }}

    from source
)
select * from renamed
  