{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'payouts'],
    description='Cleaned and deduplicated payouts. Validates recipient customer foreign keys.'
  )
}}

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
    row_number() over (partition by payout_id order by created_at) as rn
  from {{ source('raw_data', 'payouts') }}
  where _ab_cdc_deleted_at is null
),

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
  where 
    rn = 1
    and amount > 0
    and status in ('pending', 'completed', 'failed', 'cancelled')
),

-- Validate foreign keys
with_fk_validation as (
  select
    p.payout_id,
    p.recipient_customer_id,
    p.amount,
    p.currency,
    p.status,
    p.scheduled_at,
    p.executed_at,
    p.created_at,
    case when c.customer_id is not null then true else false end as fk_valid
  from deduplicated p
  left join {{ ref('stg_customers') }} c 
    on p.recipient_customer_id = c.customer_id
)

select
  payout_id,
  recipient_customer_id,
  amount,
  currency,
  status,
  scheduled_at,
  executed_at,
  created_at
from with_fk_validation
where fk_valid = true