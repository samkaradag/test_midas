-- stg_disputes.sql
-- Purpose: Clean and deduplicate dispute data
-- Removes test data (reason = 'Test Dispute')
-- Validates transaction_id foreign key
-- Input: raw disputes table (205K rows)
-- Output: clean unique disputes

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'disputes'],
    description='Cleaned disputes with test data removal'
) }}

with source_data as (
    select
        dispute_id,
        transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by dispute_id order by created_at asc) as rn
    from {{ source('raw_customers', 'disputes') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each dispute_id
deduplicated as (
    select
        dispute_id,
        transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Remove test data
remove_test_data as (
    select
        dispute_id,
        transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at
    from deduplicated
    where reason != 'Test Dispute'
      and lower(reason) not like '%test%'
),

-- Validate and clean disputes
validate_disputes as (
    select
        dispute_id,
        transaction_id,
        amount,
        currency,
        reason,
        case 
            when lower(status) in ('open', 'pending') then 'OPEN'
            when lower(status) in ('resolved', 'closed', 'won') then 'RESOLVED'
            when lower(status) in ('lost', 'chargeback') then 'LOST'
            else 'UNKNOWN'
        end as status,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at,
        case 
            when transaction_id is null then false
            when amount <= 0 then false
            else true
        end as is_valid_dispute
    from remove_test_data
),

-- Validate transaction FK
validate_fk as (
    select
        d.dispute_id,
        d.transaction_id,
        d.amount,
        d.currency,
        d.reason,
        d.status,
        d.created_at,
        d.updated_at,
        d.dbt_updated_at,
        d.is_valid_dispute,
        case when st.transaction_id is not null then true else false end as transaction_exists
    from validate_disputes d
    left join {{ ref('stg_transactions') }} st
        on d.transaction_id = st.transaction_id
)

select
    dispute_id,
    transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at,
    updated_at,
    dbt_updated_at
from validate_fk
where is_valid_dispute = true
  and transaction_exists = true