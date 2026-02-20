{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['dimension', 'customers'],
    description='Customer dimension with surrogate and natural keys. Includes customer attributes and active status.'
  )
}}

with customers as (
  select
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    created_at,
    updated_at
  from {{ ref('stg_customers') }}
),

-- Generate surrogate key
with_surrogate_key as (
  select
    md5(customer_id) as customer_key,
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    case 
      when kyc_status = 'VERIFIED' then true 
      else false 
    end as is_active,
    created_at,
    updated_at,
    current_timestamp() as dbt_loaded_at
  from customers
)

select * from with_surrogate_key