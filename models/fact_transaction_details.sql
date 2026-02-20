{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['fact', 'transaction_details'],
    description='Transaction details fact table aggregating refunds, disputes, and fees by transaction.'
  )
}}

with transactions as (
  select
    transaction_id,
    amount
  from {{ ref('stg_transactions') }}
),

-- Aggregate refunds by transaction
refunds_agg as (
  select
    original_transaction_id as transaction_id,
    sum(amount) as refund_amount,
    count(*) as refund_count
  from {{ ref('stg_refunds') }}
  group by original_transaction_id
),

-- Aggregate disputes by transaction
disputes_agg as (
  select
    transaction_id,
    sum(amount) as dispute_amount,
    count(*) as dispute_count
  from {{ ref('stg_disputes') }}
  group by transaction_id
),

-- Aggregate fees by transaction
fees_agg as (
  select
    transaction_id,
    sum(amount) as fee_amount,
    count(*) as fee_count
  from {{ ref('stg_fees') }}
  group by transaction_id
),

-- Combine all aggregations
combined as (
  select
    md5(t.transaction_id) as transaction_key,
    t.transaction_id,
    coalesce(r.refund_amount, 0) as refund_amount,
    coalesce(d.dispute_amount, 0) as dispute_amount,
    coalesce(f.fee_amount, 0) as fee_amount,
    coalesce(r.refund_count, 0) as refund_count,
    coalesce(d.dispute_count, 0) as dispute_count,
    coalesce(f.fee_count, 0) as fee_count,
    t.amount - coalesce(r.refund_amount, 0) - coalesce(f.fee_amount, 0) as net_transaction_amount,
    current_timestamp() as dbt_loaded_at
  from transactions t
  left join refunds_agg r on t.transaction_id = r.transaction_id
  left join disputes_agg d on t.transaction_id = d.transaction_id
  left join fees_agg f on t.transaction_id = f.transaction_id
)

select * from combined