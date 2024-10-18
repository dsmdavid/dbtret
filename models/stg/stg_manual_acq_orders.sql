with source as (
      select * from {{ source('manual', 'acq_orders') }}
),
renamed as (
    select
        {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("TAXONOMY_BUSINESS_CATEGORY_GROUP") }}

    from source
)
select * from renamed
  