-- mart_customer_metrics.sql
-- Purpose: Customer-level analytics and engagement metrics
-- Provides comprehensive customer behavior and value analysis
-- Input: fact_transactions, fact_transaction_details, dim_customers, dim_date
-- Output: Customer-level KPIs for segmentation and analysis

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'metrics', 'customers'],
    description='Customer-level metrics including transaction history, CLV, and activity status'
) }}

with customer_transactions as (
    select
        dc.customer_key,
        dc.customer_id,
        dc.customer_type,
        dc.kyc_status,
        dc.is_kyc_verified,
        dc.created_at as customer_created_at,
        ft.transaction_id,
        ft.transaction_amount,
        ft.transaction_status,
        ftd.net_transaction_amount,
        ftd.refund_amount,
        ftd.dispute_amount,
        ftd.fee_amount,
        ft.created_at as transaction_date,
        dd.calendar_date as transaction_calendar_date
    from {{ ref('dim_customers') }} dc
    left join {{ ref('fact_transactions') }} ft
        on dc.customer_key = ft.debtor_customer_key
    left join {{ ref('fact_transaction_details') }} ftd
        on ft.transaction_key = ftd.transaction_key
    left join {{ ref('dim_date') }} dd
        on ft.date_key = dd.date_key
),

customer_aggregates as (
    select
        customer_key,
        customer_id,
        customer_type,
        kyc_status,
        is_kyc_verified,
        customer_created_at,
        
        -- Transaction counts
        count(distinct transaction_id) as total_transactions_count,
        sum(case when transaction_status = 'COMPLETED' then 1 else 0 end) as completed_transactions_count,
        sum(case when transaction_status = 'FAILED' then 1 else 0 end) as failed_transactions_count,
        
        -- Transaction amounts
        sum(transaction_amount) as total_transaction_amount,
        sum(case when transaction_status = 'COMPLETED' then transaction_amount else 0 end) as total_completed_amount,
        avg(case when transaction_status = 'COMPLETED' then transaction_amount end) as avg_completed_transaction_amount,
        avg(transaction_amount) as avg_transaction_amount,
        
        -- Refunds and disputes
        sum(coalesce(refund_amount, 0)) as total_refund_amount,
        sum(coalesce(dispute_amount, 0)) as total_dispute_amount,
        sum(coalesce(fee_amount, 0)) as total_fee_amount,
        
        -- Net CLV
        sum(coalesce(net_transaction_amount, 0)) as customer_lifetime_value,
        
        -- Activity metrics
        max(transaction_calendar_date) as last_transaction_date,
        date_diff(current_date(), max(transaction_calendar_date), day) as days_since_last_transaction,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
    from customer_transactions
    group by customer_key, customer_id, customer_type, kyc_status, is_kyc_verified, customer_created_at
),

enrich_activity_status as (
    select
        *,
        case
            when days_since_last_transaction <= 30 then true
            when last_transaction_date is null then false
            else false
        end as is_active_customer_30d,
        case
            when days_since_last_transaction <= 90 then true
            when last_transaction_date is null then false
            else false
        end as is_active_customer_90d
    from customer_aggregates
)

select * from enrich_activity_status
order by customer_lifetime_value desc