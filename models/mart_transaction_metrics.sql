-- mart_transaction_metrics.sql
-- Purpose: Daily transaction KPIs and metrics
-- Provides comprehensive transaction analytics by date
-- Input: fact_transactions, dim_date
-- Output: Daily transaction metrics for monitoring and reporting

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'metrics', 'transactions', 'daily'],
    description='Daily transaction KPIs including volume, value, success rates, and pending counts'
) }}

with transactions as (
    select
        ft.date_key,
        dd.calendar_date,
        dd.year,
        dd.month,
        dd.quarter,
        dd.week,
        ft.transaction_id,
        ft.transaction_amount,
        ft.currency,
        ft.transaction_status
    from {{ ref('fact_transactions') }} ft
    left join {{ ref('dim_date') }} dd
        on ft.date_key = dd.date_key
),

daily_metrics as (
    select
        date_key,
        calendar_date,
        year,
        month,
        quarter,
        week,
        -- Transaction counts
        count(distinct transaction_id) as transaction_count,
        sum(case when transaction_status = 'COMPLETED' then 1 else 0 end) as completed_transaction_count,
        sum(case when transaction_status = 'FAILED' then 1 else 0 end) as failed_transaction_count,
        sum(case when transaction_status = 'PENDING' then 1 else 0 end) as pending_transaction_count,
        sum(case when transaction_status = 'CANCELLED' then 1 else 0 end) as cancelled_transaction_count,
        
        -- Transaction amounts
        sum(case when transaction_status = 'COMPLETED' then transaction_amount else 0 end) as total_completed_amount,
        sum(transaction_amount) as total_transaction_amount,
        avg(case when transaction_status = 'COMPLETED' then transaction_amount end) as avg_completed_amount,
        avg(transaction_amount) as avg_transaction_amount,
        min(transaction_amount) as min_transaction_amount,
        max(transaction_amount) as max_transaction_amount,
        
        -- Success/failure rates
        round(
            safe_divide(
                sum(case when transaction_status = 'COMPLETED' then 1 else 0 end),
                count(distinct transaction_id)
            ) * 100,
            2
        ) as transaction_success_rate,
        round(
            safe_divide(
                sum(case when transaction_status = 'FAILED' then 1 else 0 end),
                count(distinct transaction_id)
            ) * 100,
            2
        ) as transaction_failure_rate,
        round(
            safe_divide(
                sum(case when transaction_status = 'PENDING' then 1 else 0 end),
                count(distinct transaction_id)
            ) * 100,
            2
        ) as transaction_pending_rate,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
    from transactions
    group by date_key, calendar_date, year, month, quarter, week
)

select * from daily_metrics
order by calendar_date desc