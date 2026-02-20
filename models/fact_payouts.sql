{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['fact', 'payouts'],
    description='Payout fact table with dimensional foreign keys and payout details.'
  )
}}

with payouts as (
  select
    payout_id,
    recipient_customer_id,
    amount,
    currency,
    status,
    scheduled_at,
    executed_at,
    created_at
  from {{ ref('stg_payouts') }}
),

-- Join with dimensions
with_keys as (
  select
    md5(p.payout_id) as payout_key,
    p.payout_id,
    dd.date_key,
    dc.customer_key as recipient_customer_key,
    p.amount as payout_amount,
    p.currency,
    p.status as payout_status,
    p.scheduled_at,
    p.executed_at,
    p.created_at,
    current_timestamp() as dbt_loaded_at
  from payouts p
  left join {{ ref('dim_date') }} dd 
    on cast(dd.date as date) = cast(p.created_at as date)
  left join {{ ref('dim_customers') }} dc 
    on p.recipient_customer_id = dc.customer_id
)

select * from with_keys