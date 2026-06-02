{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fraud', 'transactions'],
    description='Enriched transactions with fraud labels, customer, and payment method details'
) }}

with stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

stg_disputes as (
    select * from {{ ref('stg_disputes') }}
),

stg_customers as (
    select * from {{ ref('stg_customers') }}
),

stg_payment_methods as (
    select * from {{ ref('stg_payment_methods') }}
),

transactions_with_fraud as (
    select
        t.transaction_id,
        t.debtor_customer_id,
        t.creditor_customer_id,
        t.payment_method_id,
        t.amount,
        t.currency,
        t.status as transaction_status,
        t.reference,
        t.created_at as transaction_created_at,
        t.updated_at as transaction_updated_at,
        case when d.dispute_id is not null then 1 else 0 end as is_fraud,
        d.dispute_id,
        d.amount as dispute_amount,
        d.reason as dispute_reason,
        d.status as dispute_status,
        d.created_at as dispute_created_at
    from stg_transactions t
    left join stg_disputes d on t.transaction_id = d.transaction_id
),

enriched_with_customer as (
    select
        txf.*,
        c.customer_id,
        c.customer_type,
        c.kyc_status,
        c.country,
        c.created_at as customer_created_at
    from transactions_with_fraud txf
    left join stg_customers c on txf.debtor_customer_id = c.customer_id
),

enriched_with_payment_method as (
    select
        ewc.*,
        pm.method_type,
        pm.is_default as payment_method_is_default,
        pm.created_at as payment_method_created_at
    from enriched_with_customer ewc
    left join stg_payment_methods pm on ewc.payment_method_id = pm.payment_method_id
)

select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    customer_id,
    amount,
    currency,
    transaction_status,
    is_fraud,
    dispute_id,
    dispute_amount,
    dispute_reason,
    dispute_status,
    reference,
    customer_type,
    kyc_status,
    country,
    method_type,
    payment_method_is_default,
    transaction_created_at,
    transaction_updated_at,
    dispute_created_at,
    customer_created_at,
    payment_method_created_at,
    current_timestamp() as dbt_created_at
from enriched_with_payment_method