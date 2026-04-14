{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'fraud', 'transactions'],
    description='Transactions enriched with fraud indicator from disputes. Joins stg_transactions with stg_disputes to add is_fraud flag.'
  )
}}

with transactions as (
  select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    reference,
    created_at,
    updated_at
  from {{ ref('stg_transactions') }}
),

disputes as (
  select
    transaction_id,
    dispute_id,
    amount as dispute_amount,
    reason as dispute_reason,
    status as dispute_status
  from {{ ref('stg_disputes') }}
),

enriched as (
  select
    t.transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    t.payment_method_id,
    t.amount,
    t.currency,
    t.status,
    t.reference,
    case when d.dispute_id is not null then 1 else 0 end as is_fraud,
    d.dispute_id,
    d.dispute_amount,
    d.dispute_reason,
    d.dispute_status,
    t.created_at,
    t.updated_at
  from transactions t
  left join disputes d on t.transaction_id = d.transaction_id
)

select * from enriched