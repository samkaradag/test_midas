{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'customers'],
    description='Cleaned and deduplicated customer data. Removes test data and standardizes KYC status.'
  )
}}

with source_data as (
  select
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    created_at,
    updated_at,
    row_number() over (partition by customer_id order by created_at) as rn
  from {{ source('raw_data', 'customers') }}
  where 
    -- Remove test data
    customer_type != 'samet'
    -- Remove soft-deleted records
    and _ab_cdc_deleted_at is null
),

deduplicated as (
  select
    customer_id,
    customer_type,
    email,
    phone_number,
    -- Standardize KYC status
    case 
      when kyc_status in ('ok', 'done', 'yes') then 'VERIFIED'
      when kyc_status is null then 'UNKNOWN'
      else upper(kyc_status)
    end as kyc_status,
    created_at,
    updated_at
  from source_data
  where rn = 1
)

select * from deduplicated