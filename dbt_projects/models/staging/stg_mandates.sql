-- stg_mandates.sql
-- Purpose: Clean and deduplicate mandate data
-- Validates customer_id foreign key
-- Input: raw mandates table (342K rows)
-- Output: clean unique mandates

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'mandates'],
    description='Cleaned mandates with FK validation'
) }}

with source_data as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by mandate_id order by created_at asc) as rn
    from {{ source('raw_customers', 'mandates') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each mandate_id
deduplicated as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        status,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Validate and clean mandates
validate_mandates as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        case 
            when lower(status) in ('active', 'enabled') then 'ACTIVE'
            when lower(status) in ('inactive', 'disabled', 'cancelled') then 'INACTIVE'
            when lower(status) = 'expired' then 'EXPIRED'
            when lower(status) = 'pending' then 'PENDING'
            else 'UNKNOWN'
        end as status,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at,
        case 
            when customer_id is null then false
            else true
        end as is_valid_mandate
    from deduplicated
),

-- Validate customer FK
validate_fk as (
    select
        m.mandate_id,
        m.customer_id,
        m.payment_method_id,
        m.reference,
        m.status,
        m.created_at,
        m.updated_at,
        m.dbt_updated_at,
        m.is_valid_mandate,
        case when sc.customer_id is not null then true else false end as customer_exists
    from validate_mandates m
    left join {{ ref('stg_customers') }} sc
        on m.customer_id = sc.customer_id
)

select
    mandate_id,
    customer_id,
    payment_method_id,
    reference,
    status,
    created_at,
    updated_at,
    dbt_updated_at
from validate_fk
where is_valid_mandate = true
  and customer_exists = true