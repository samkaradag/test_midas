{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fraud', 'customer_features'],
    description='Customer transaction history aggregates for ML feature engineering. Computes transaction counts, totals, averages, and temporal features per customer.'
  )
}}

with transactions as (
  select
    debtor_customer_id as customer_id,
    transaction_id,
    amount,
    status,
    is_fraud,
    created_at
  from {{ ref('stg_transactions_fraud') }}
),

customer_stats as (
  select
    customer_id,
    count(distinct transaction_id) as total_transactions,
    count(distinct case when is_fraud = 1 then transaction_id end) as fraud_transactions,
    count(distinct case when is_fraud = 0 then transaction_id end) as legitimate_transactions,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount,
    stddev(amount) as stddev_amount,
    count(distinct case when status = 'completed' then transaction_id end) as completed_transactions,
    count(distinct case when status = 'failed' then transaction_id end) as failed_transactions,
    count(distinct case when status = 'pending' then transaction_id end) as pending_transactions,
    min(created_at) as first_transaction_date,
    max(created_at) as last_transaction_date,
    date_diff(max(created_at), min(created_at), day) as days_active,
    round(count(distinct case when is_fraud = 1 then transaction_id end) * 100.0 / 
          nullif(count(distinct transaction_id), 0), 2) as fraud_rate_pct
  from transactions
  group by customer_id
)

select
  customer_id,
  total_transactions,
  fraud_transactions,
  legitimate_transactions,
  total_amount,
  avg_amount,
  min_amount,
  max_amount,
  stddev_amount,
  completed_transactions,
  failed_transactions,
  pending_transactions,
  first_transaction_date,
  last_transaction_date,
  days_active,
  fraud_rate_pct,
  current_timestamp() as dbt_created_at
from customer_stats