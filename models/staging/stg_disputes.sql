{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'disputes'],
    description='Cleaned and deduplicated disputes. Removes test data and validates foreign keys.'
  )
}}

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
    row_number() over (partition by dispute_id order by created_at) as rn
  from {{ source('raw_data', 'disputes') }}
  where 
    _ab_cdc_deleted_at is null
    -- Remove test disputes
    and reason != 'Test Dispute'
),

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
  where 
    rn = 1
    and amount > 0
    and status in ('open', 'resolved', 'lost', 'won')
),

-- Validate foreign keys
with_fk_validation as (
  select
    d.dispute_id,
    d.transaction_id,
    d.amount,
    d.currency,
    d.reason,
    d.status,
    d.created_at,
    d.updated_at,
    case when t.transaction_id is not null then true else false end as fk_valid
  from deduplicated d
  left join {{ ref('stg_transactions') }} t 
    on d.transaction_id = t.transaction_id
)

select
  dispute_id,
  transaction_id,
  amount,
  currency,
  reason,
  status,
  created_at,
  updated_at
from with_fk_validation
where fk_valid = true