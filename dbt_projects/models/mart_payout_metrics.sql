-- mart_payout_metrics.sql
-- Purpose: Payout and settlement metrics
-- Tracks payout volume, timing, and settlement performance
-- Input: fact_payouts, dim_customers, dim_date
-- Output: Payout settlement and performance metrics

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'metrics', 'payouts', 'settlement'],
    description='Payout metrics including volume, success rates, and settlement timing'
) }}

with payout_data as (
    select
        fp.payout_key,
        fp.payout_id,
        fp.recipient_customer_key,
        dc.customer_id,
        dc.customer_type,
        fp.payout_amount,
        fp.currency,
        fp.payout_status,
        fp.scheduled_at,
        fp.executed_at,
        fp.created_at,
        dd.calendar_date as payout_date,
        case
            when fp.executed_at is not null
            then date_diff(date(fp.executed_at), date(fp.scheduled_at), day)
            else null
        end as days_to_execute
    from {{ ref('fact_payouts') }} fp
    left join {{ ref('dim_customers') }} dc
        on fp.recipient_customer_key = dc.customer_key
    left join {{ ref('dim_date') }} dd
        on fp.date_key = dd.date_key
),

payout_summary as (
    select
        -- Payout counts
        count(distinct payout_id) as total_payouts_count,
        sum(case when payout_status = 'COMPLETED' then 1 else 0 end) as completed_payouts_count,
        sum(case when payout_status = 'SCHEDULED' then 1 else 0 end) as scheduled_payouts_count,
        sum(case when payout_status = 'PROCESSING' then 1 else 0 end) as processing_payouts_count,
        sum(case when payout_status = 'FAILED' then 1 else 0 end) as failed_payouts_count,
        sum(case when payout_status = 'CANCELLED' then 1 else 0 end) as cancelled_payouts_count,
        
        -- Payout amounts
        sum(payout_amount) as total_payout_amount,
        sum(case when payout_status = 'COMPLETED' then payout_amount else 0 end) as completed_payout_amount,
        sum(case when payout_status in ('SCHEDULED', 'PROCESSING') then payout_amount else 0 end) as pending_payout_amount,
        avg(payout_amount) as avg_payout_amount,
        max(payout_amount) as max_payout_amount,
        min(payout_amount) as min_payout_amount,
        
        -- Success metrics
        round(
            safe_divide(
                sum(case when payout_status = 'COMPLETED' then 1 else 0 end),
                count(distinct payout_id)
            ) * 100,
            2
        ) as payout_success_rate,
        round(
            safe_divide(
                sum(case when payout_status = 'FAILED' then 1 else 0 end),
                count(distinct payout_id)
            ) * 100,
            2
        ) as payout_failure_rate,
        
        -- Timing metrics
        round(avg(days_to_execute), 2) as avg_days_to_execute,
        max(days_to_execute) as max_days_to_execute,
        min(days_to_execute) as min_days_to_execute,
        
        current_timestamp() as dbt_loaded_at
    from payout_data
),

customer_payout_frequency as (
    select
        customer_id,
        customer_type,
        count(distinct payout_id) as customer_payout_count,
        sum(payout_amount) as customer_total_payout_amount,
        avg(payout_amount) as customer_avg_payout_amount,
        max(payout_date) as customer_last_payout_date
    from payout_data
    where customer_id is not null
    group by customer_id, customer_type
)

select
    ps.total_payouts_count,
    ps.completed_payouts_count,
    ps.scheduled_payouts_count,
    ps.processing_payouts_count,
    ps.failed_payouts_count,
    ps.cancelled_payouts_count,
    ps.total_payout_amount,
    ps.completed_payout_amount,
    ps.pending_payout_amount,
    ps.avg_payout_amount,
    ps.max_payout_amount,
    ps.min_payout_amount,
    ps.payout_success_rate,
    ps.payout_failure_rate,
    ps.avg_days_to_execute,
    ps.max_days_to_execute,
    ps.min_days_to_execute,
    (select count(distinct customer_id) from customer_payout_frequency) as customers_with_payouts,
    (select avg(customer_payout_count) from customer_payout_frequency) as avg_payouts_per_customer,
    ps.dbt_loaded_at
from payout_summary ps