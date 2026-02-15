-- stg_refunds.sql
-- Purpose: Clean and deduplicate refund data
-- Validates original_transaction_id foreign key
-- Input: raw refunds table (205K rows)
-- Output: clean unique refunds

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'refunds'],
    description='Cleaned refunds with FK validation'
) }}

with source_data as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by refund_id order by created_at asc) as rn
    from {{ source('raw_customers', 'refunds') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each refund_id
deduplicated as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Validate and clean refunds
validate_refunds as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        reason,
        case 
            when lower(status) in ('pending', 'processing') then 'PENDING'
            when lower(status) in ('completed', 'success', 'done') then 'COMPLETED'
            when lower(status) in ('failed', 'declined') then 'FAILED'
            when lower(status) = 'cancelled' then 'CANCELLED'
            else 'UNKNOWN'
        end as status,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at,
        case 
            when original_transaction_id is null then false
            when amount <= 0 then false
            else true
        end as is_valid_refund
    from deduplicated
),

-- Validate transaction FK
validate_fk as (
    select
        r.refund_id,
        r.original_transaction_id,
        r.amount,
        r.currency,
        r.reason,
        r.status,
        r.created_at,
        r.updated_at,
        r.dbt_updated_at,
        r.is_valid_refund,
        case when st.transaction_id is not null then true else false end as transaction_exists
    from validate_refunds r
    left join {{ ref('stg_transactions') }} st
        on r.original_transaction_id = st.transaction_id
)

select
    refund_id,
    original_transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at,
    updated_at,
    dbt_updated_at
from validate_fk
where is_valid_refund = true
  and transaction_exists = true