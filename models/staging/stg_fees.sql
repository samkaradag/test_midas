{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'fees'],
    description='Cleaned and deduplicated fees. Removes test data and validates foreign keys.'
  )
}}

with source_data as (
  select
    fee_id,
    transaction_id,
    amount,
    currency,
    fee_type,
    created_at,
    row_number() over (partition by fee_id order by created_at) as rn
  from {{ source('raw_data', 'fees') }}
  where 
    _ab_cdc_deleted_at is null
    -- Remove test fees
    and fee_type != 'Test Fee'
),

deduplicated as (
  select
    fee_id,
    transaction_id,
    amount,
    currency,
    fee_type,
    created_at
  from source_data
  where 
    rn = 1
    and amount > 0
),

-- Validate foreign keys
with_fk_validation as (
  select
    f.fee_id,
    f.transaction_id,
    f.amount,
    f.currency,
    f.fee_type,
    f.created_at,
    case when t.transaction_id is not null then true else false end as fk_valid
  from deduplicated f
  left join {{ ref('stg_transactions') }} t 
    on f.transaction_id = t.transaction_id
)

select
  fee_id,
  transaction_id,
  amount,
  currency,
  fee_type,
  created_at
from with_fk_validation
where fk_valid = true