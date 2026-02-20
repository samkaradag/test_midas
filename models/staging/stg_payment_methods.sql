{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'payment_methods'],
    description='Cleaned and deduplicated payment methods. Consolidates payment method types.'
  )
}}

with source_data as (
  select
    payment_method_id,
    customer_id,
    method_type,
    is_default,
    created_at,
    updated_at,
    row_number() over (partition by payment_method_id order by created_at) as rn
  from {{ source('raw_data', 'payment_methods') }}
  where _ab_cdc_deleted_at is null
),

deduplicated as (
  select
    payment_method_id,
    customer_id,
    -- Consolidate method types
    case 
      when method_type = 'credit_card' then 'card'
      else lower(method_type)
    end as method_type,
    is_default,
    created_at,
    updated_at
  from source_data
  where rn = 1
)

select * from deduplicated