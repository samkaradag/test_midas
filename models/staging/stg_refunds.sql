{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'refunds'],
    description='Cleaned and deduplicated refunds. Validates foreign key references.'
  )
}}

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
    row_number() over (partition by refund_id order by created_at) as rn
  from {{ source('raw_data', 'refunds') }}
  where _ab_cdc_deleted_at is null
),

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
  where 
    rn = 1
    and amount > 0
    and status in ('pending', 'completed', 'failed', 'cancelled')
),

-- Validate foreign keys
with_fk_validation as (
  select
    r.refund_id,
    r.original_transaction_id,
    r.amount,
    r.currency,
    r.reason,
    r.status,
    r.created_at,
    r.updated_at,
    case when t.transaction_id is not null then true else false end as fk_valid
  from deduplicated r
  left join {{ ref('stg_transactions') }} t 
    on r.original_transaction_id = t.transaction_id
)

select
  refund_id,
  original_transaction_id,
  amount,
  currency,
  reason,
  status,
  created_at,
  updated_at
from with_fk_validation
where fk_valid = true