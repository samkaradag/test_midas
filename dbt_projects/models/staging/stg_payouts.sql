-- stg_payouts.sql
-- Purpose: Clean and deduplicate payout data
-- Validates recipient_customer_id foreign key
-- Input: raw payouts table (359K rows)
-- Output: clean unique payouts

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'payouts'],
    description='Cleaned payouts with FK validation'
) }}

with source_data as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        status,
        scheduled_at,
        executed_at,
        created_at,
        _airbyte_extracted_at,
        row_number() over (partition by payout_id order by created_at asc) as rn
    from {{ source('raw_customers', 'payouts') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each payout_id
deduplicated as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        status,
        scheduled_at,
        executed_at,
        created_at
    from source_data
    where rn = 1
),

-- Validate and clean payouts
validate_payouts as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        case 
            when lower(status) in ('scheduled', 'pending') then 'SCHEDULED'
            when lower(status) in ('processing', 'in_progress') then 'PROCESSING'
            when lower(status) in ('completed', 'success', 'done') then 'COMPLETED'
            when lower(status) in ('failed', 'declined') then 'FAILED'
            when lower(status) = 'cancelled' then 'CANCELLED'
            else 'UNKNOWN'
        end as status,
        scheduled_at,
        executed_at,
        created_at,
        current_timestamp() as dbt_updated_at,
        case 
            when recipient_customer_id is null then false
            when amount <= 0 then false
            else true
        end as is_valid_payout
    from deduplicated
),

-- Validate customer FK
validate_fk as (
    select
        p.payout_id,
        p.recipient_customer_id,
        p.amount,
        p.currency,
        p.status,
        p.scheduled_at,
        p.executed_at,
        p.created_at,
        p.dbt_updated_at,
        p.is_valid_payout,
        case when sc.customer_id is not null then true else false end as customer_exists
    from validate_payouts p
    left join {{ ref('stg_customers') }} sc
        on p.recipient_customer_id = sc.customer_id
)

select
    payout_id,
    recipient_customer_id,
    amount,
    currency,
    status,
    scheduled_at,
    executed_at,
    created_at,
    dbt_updated_at
from validate_fk
where is_valid_payout = true
  and customer_exists = true