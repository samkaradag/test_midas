{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fraud', 'ml_features'],
    description='ML-ready fraud features derived from transactions, customer history, and payment methods. Includes behavioral, temporal, and monetary features.'
  )
}}

with transactions_fraud as (
  select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    is_fraud,
    created_at
  from {{ ref('stg_transactions_fraud') }}
),

customer_history as (
  select
    customer_id,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    avg_amount,
    stddev_amount,
    completed_transactions,
    failed_transactions,
    days_active
  from {{ ref('stg_customer_transaction_history') }}
),

payment_methods as (
  select
    payment_method_id,
    method_type,
    is_default
  from {{ ref('stg_payment_methods') }}
),

customers as (
  select
    customer_id,
    kyc_status
  from {{ ref('stg_customers') }}
),

features as (
  select
    t.transaction_id,
    t.debtor_customer_id,
    t.creditor_customer_id,
    t.payment_method_id,
    
    -- Target variable
    t.is_fraud as fraud_label,
    
    -- Transaction features
    t.amount as transaction_amount,
    t.currency,
    t.status as transaction_status,
    
    -- Temporal features
    extract(hour from t.created_at) as transaction_hour,
    extract(dayofweek from t.created_at) as transaction_day_of_week,
    extract(date from t.created_at) as transaction_date,
    
    -- Customer behavior features
    ch.total_transactions as customer_lifetime_transactions,
    ch.fraud_rate_pct as customer_fraud_rate_pct,
    ch.avg_amount as customer_avg_transaction_amount,
    ch.stddev_amount as customer_stddev_transaction_amount,
    ch.completed_transactions as customer_completed_transactions,
    ch.failed_transactions as customer_failed_transactions,
    ch.days_active as customer_days_active,
    
    -- Amount deviation features
    case 
      when ch.avg_amount > 0 then round((t.amount - ch.avg_amount) / nullif(ch.stddev_amount, 0), 2)
      else 0
    end as amount_zscore,
    
    case 
      when t.amount > ch.max_amount then 1 else 0 
    end as is_max_amount_exceeded,
    
    -- Payment method features
    pm.method_type,
    pm.is_default as payment_method_is_default,
    
    -- Customer KYC features
    c.kyc_status,
    case when c.kyc_status = 'VERIFIED' then 1 else 0 end as is_kyc_verified,
    
    -- Risk indicators
    case 
      when ch.fraud_rate_pct > 10 then 'high_risk'
      when ch.fraud_rate_pct > 5 then 'medium_risk'
      else 'low_risk'
    end as customer_risk_segment,
    
    t.created_at
  from transactions_fraud t
  left join customer_history ch on t.debtor_customer_id = ch.customer_id
  left join payment_methods pm on t.payment_method_id = pm.payment_method_id
  left join customers c on t.debtor_customer_id = c.customer_id
)

select * from features