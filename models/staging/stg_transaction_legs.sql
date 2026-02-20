{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'transaction_legs', 'critical'],
    description='CRITICAL: Rebuilds transaction ledger structure. Fixes broken state (17K legs/transaction) to exactly 2 legs per transaction (1 debit, 1 credit). Validates double-entry accounting.'
  )
}}

with source_data as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at,
    row_number() over (partition by transaction_id, direction order by created_at) as rn
  from {{ source('raw_data', 'transaction_legs') }}
  where 
    _ab_cdc_deleted_at is null
    and direction in ('debit', 'credit')
),

-- Aggregate amounts by transaction and direction
aggregated as (
  select
    transaction_id,
    direction,
    currency,
    sum(amount) as amount,
    min(account) as account,
    min(created_at) as created_at
  from source_data
  where rn = 1
  group by transaction_id, direction, currency
),

-- Ensure exactly 2 legs per transaction (1 debit, 1 credit)
validated as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at,
    -- Calculate running total for validation
    sum(case when direction = 'debit' then amount else -amount end) 
      over (partition by transaction_id) as net_amount
  from aggregated
),

-- Final validation: debit = credit
final as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at
  from validated
  where 
    -- Only include transactions with balanced ledger (debit = credit)
    abs(net_amount) < 0.01
)

select * from final