-- stg_payment_methods.sql
-- Purpose: Clean and deduplicate payment methods
-- Consolidates payment method types (credit_card → card)
-- Validates customer FK references
-- Input: raw payment_methods table (359K rows)
-- Output: clean unique payment methods

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'payment_methods'],
    description='Cleaned payment methods with type consolidation'
) }}

with source_data as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by payment_method_id order by created_at asc) as rn
    from {{ source('raw_customers', 'payment_methods') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each payment_method_id
deduplicated as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Consolidate payment method types
consolidate_types as (
    select
        payment_method_id,
        customer_id,
        case 
            when lower(method_type) in ('credit_card', 'creditcard', 'credit card') then 'card'
            when lower(method_type) in ('debit_card', 'debitcard', 'debit card') then 'card'
            when lower(method_type) = 'bank_transfer' then 'bank_transfer'
            when lower(method_type) = 'wallet' then 'wallet'
            when lower(method_type) = 'check' then 'check'
            else lower(method_type)
        end as method_type,
        coalesce(is_default, false) as is_default,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at
    from deduplicated
),

-- Validate customer FK exists
validate_fk as (
    select
        pm.payment_method_id,
        pm.customer_id,
        pm.method_type,
        pm.is_default,
        pm.created_at,
        pm.updated_at,
        pm.dbt_updated_at,
        case when sc.customer_id is not null then true else false end as customer_exists
    from consolidate_types pm
    left join {{ ref('stg_customers') }} sc
        on pm.customer_id = sc.customer_id
)

select
    payment_method_id,
    customer_id,
    method_type,
    is_default,
    created_at,
    updated_at,
    dbt_updated_at
from validate_fk
-- Only include records with valid customer FK (optional: can be changed to warning in tests)
where customer_exists = true