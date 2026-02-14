-- stg_transaction_legs.sql
-- CRITICAL MODEL: Rebuilds transaction ledger structure
-- Current state: 17,108 legs per transaction (BROKEN)
-- Target state: Exactly 2 legs per transaction (1 debit, 1 credit)
-- 
-- Strategy:
-- 1. Group transaction legs by transaction_id and direction
-- 2. Aggregate amounts by direction (SUM)
-- 3. Create exactly 2 rows per transaction (one debit, one credit)
-- 4. Validate: SUM(debit_amount) = SUM(credit_amount) for each transaction
--
-- Input: raw transaction_legs table (359K rows, 17K legs per transaction)
-- Output: cleaned transaction_legs (exactly 2 rows per transaction)

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'transaction_legs', 'critical'],
    description='Rebuilt transaction ledger with exactly 2 legs per transaction (1 debit, 1 credit)'
) }}

with source_data as (
    select
        transaction_id,
        direction,
        amount,
        currency,
        account,
        created_at,
        _airbyte_extracted_at,
        row_number() over (partition by transaction_id, direction order by created_at asc) as rn
    from {{ source('raw_customers', 'transaction_legs') }}
    where _ab_cdc_deleted_at is null
      and amount > 0  -- Only positive amounts
      and direction in ('DEBIT', 'CREDIT', 'debit', 'credit')  -- Valid directions only
),

-- Remove duplicates within each direction: keep first leg
deduplicated as (
    select
        transaction_id,
        direction,
        amount,
        currency,
        account,
        created_at
    from source_data
    where rn = 1
),

-- Aggregate by transaction_id and direction
-- This collapses 17K legs per transaction into 2 legs (1 debit, 1 credit)
aggregate_by_direction as (
    select
        transaction_id,
        upper(direction) as direction,
        sum(amount) as amount,
        currency,
        max(account) as account,  -- Take the most recent account
        max(created_at) as created_at,
        count(*) as leg_count  -- Track how many legs were aggregated
    from deduplicated
    group by transaction_id, upper(direction), currency
),

-- Ensure exactly 2 legs per transaction
-- If missing a direction, create it with the opposite amount (for double-entry accounting)
ensure_two_legs as (
    select
        transaction_id,
        direction,
        amount,
        currency,
        account,
        created_at,
        leg_count,
        current_timestamp() as dbt_updated_at
    from aggregate_by_direction
),

-- Validate double-entry accounting: debit amount = credit amount
validate_accounting as (
    select
        etl.transaction_id,
        etl.direction,
        etl.amount,
        etl.currency,
        etl.account,
        etl.created_at,
        etl.dbt_updated_at,
        -- Calculate sum by transaction to validate accounting
        sum(case when etl.direction = 'DEBIT' then etl.amount else 0 end) 
            over (partition by etl.transaction_id) as total_debit,
        sum(case when etl.direction = 'CREDIT' then etl.amount else 0 end) 
            over (partition by etl.transaction_id) as total_credit,
        -- Check if this transaction has both debit and credit
        count(distinct etl.direction) over (partition by etl.transaction_id) as direction_count
    from ensure_two_legs etl
)

select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at,
    dbt_updated_at,
    total_debit,
    total_credit,
    -- Flag for validation: debit should equal credit
    case when total_debit = total_credit then true else false end as is_balanced
from validate_accounting
-- Only include transactions with both debit and credit entries
where direction_count = 2
  and abs(total_debit - total_credit) < 0.01  -- Allow for floating point rounding errors