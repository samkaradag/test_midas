-- dim_customers.sql
-- Purpose: Customer dimension table for star schema
-- Creates surrogate keys and enriches customer attributes
-- Input: stg_customers (cleaned customer data)
-- Output: customer dimension with SCD Type 1

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'dimension', 'customers'],
    description='Customer dimension with surrogate keys and attributes'
) }}

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

-- Add surrogate key and derived attributes
enrich_customers as (
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
        end as is_kyc_verified,
        case 
            when updated_at >= current_timestamp() - interval 90 day then true 
            else false 
        end as is_active,
        created_at,
        updated_at,
        current_timestamp() as dbt_loaded_at
    from customers
)

select * from enrich_customers