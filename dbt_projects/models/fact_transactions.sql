-- fact_transactions.sql
-- Purpose: Transaction fact table for star schema
-- Core fact table with FKs to dimensions
-- Input: stg_transactions, dim_customers, dim_payment_methods, dim_date
-- Output: transaction facts with dimensional keys

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'fact', 'transactions'],
    description='Transaction fact table with dimensional keys'
) }}

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

-- Join with dimensions
enrich_transactions as (
    select
        md5(transaction_id) as transaction_key,
        t.transaction_id,
        format_date('%Y%m%d', date(t.created_at)) as date_key,
        dc_debtor.customer_key as debtor_customer_key,
        dc_creditor.customer_key as creditor_customer_key,
        dpm.payment_method_key,
        t.amount as transaction_amount,
        t.currency,
        t.status as transaction_status,
        t.reference,
        t.created_at,
        t.updated_at,
        current_timestamp() as dbt_loaded_at
    from transactions t
    left join {{ ref('dim_customers') }} dc_debtor
        on t.debtor_customer_id = dc_debtor.customer_id
    left join {{ ref('dim_customers') }} dc_creditor
        on t.creditor_customer_id = dc_creditor.customer_id
    left join {{ ref('dim_payment_methods') }} dpm
        on t.payment_method_id = dpm.payment_method_id
)

select * from enrich_transactions