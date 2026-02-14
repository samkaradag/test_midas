-- stg_fees.sql
-- Purpose: Clean and deduplicate fee data
-- Removes test data (fee_type = 'Test Fee')
-- Validates transaction_id foreign key
-- Input: raw fees table (359K rows)
-- Output: clean unique fees

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fees'],
    description='Cleaned fees with test data removal'
) }}

with source_data as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        fee_type,
        created_at,
        _airbyte_extracted_at,
        row_number() over (partition by fee_id order by created_at asc) as rn
    from {{ source('raw_customers', 'fees') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each fee_id
deduplicated as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        fee_type,
        created_at
    from source_data
    where rn = 1
),

-- Remove test data
remove_test_data as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        fee_type,
        created_at
    from deduplicated
    where fee_type != 'Test Fee'
      and lower(fee_type) not like '%test%'
),

-- Validate and clean fees
validate_fees as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        case 
            when lower(fee_type) = 'transaction_fee' then 'TRANSACTION_FEE'
            when lower(fee_type) = 'service_fee' then 'SERVICE_FEE'
            when lower(fee_type) = 'processing_fee' then 'PROCESSING_FEE'
            when lower(fee_type) = 'monthly_fee' then 'MONTHLY_FEE'
            when lower(fee_type) = 'penalty_fee' then 'PENALTY_FEE'
            else upper(fee_type)
        end as fee_type,
        created_at,
        current_timestamp() as dbt_updated_at,
        case 
            when transaction_id is null then false
            when amount <= 0 then false
            else true
        end as is_valid_fee
    from remove_test_data
),

-- Validate transaction FK
validate_fk as (
    select
        f.fee_id,
        f.transaction_id,
        f.amount,
        f.currency,
        f.fee_type,
        f.created_at,
        f.dbt_updated_at,
        f.is_valid_fee,
        case when st.transaction_id is not null then true else false end as transaction_exists
    from validate_fees f
    left join {{ ref('stg_transactions') }} st
        on f.transaction_id = st.transaction_id
)

select
    fee_id,
    transaction_id,
    amount,
    currency,
    fee_type,
    created_at,
    dbt_updated_at
from validate_fk
where is_valid_fee = true
  and transaction_exists = true