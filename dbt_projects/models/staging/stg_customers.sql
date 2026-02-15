-- stg_customers.sql
-- Purpose: Clean and deduplicate customer data
-- Removes test data, standardizes KYC status, and removes duplicates
-- Input: raw customers table (441K rows)
-- Output: clean unique customers (34 unique customer_ids)

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'customers'],
    description='Cleaned customer data with deduplication and test data removal'
) }}

with source_data as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        kyc_status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by customer_id order by created_at asc) as rn
    from {{ source('raw_customers', 'customers') }}
    where _ab_cdc_deleted_at is null  -- Exclude soft-deleted records
),

-- Remove duplicates: keep first record by created_at for each customer_id
deduplicated as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        kyc_status,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Remove test data (customer_type = 'samet')
remove_test_data as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        kyc_status,
        created_at,
        updated_at
    from deduplicated
    where customer_type != 'samet'
),

-- Standardize KYC status: map {ok, done, yes} → VERIFIED
standardize_kyc as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        case 
            when lower(kyc_status) in ('ok', 'done', 'yes') then 'VERIFIED'
            when lower(kyc_status) = 'pending' then 'PENDING'
            when lower(kyc_status) = 'rejected' then 'REJECTED'
            else 'UNKNOWN'
        end as kyc_status,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at
    from remove_test_data
)

select * from standardize_kyc