-- mart_refund_dispute_metrics.sql
-- Purpose: Risk and quality metrics for refunds and disputes
-- Tracks chargeback risk, refund rates, and dispute resolution
-- Input: stg_refunds, stg_disputes, fact_transactions, fact_transaction_details
-- Output: Risk and quality metrics for compliance and monitoring

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'metrics', 'risk', 'quality'],
    description='Refund and dispute metrics including rates, amounts, and resolution tracking'
) }}

with transaction_summary as (
    select
        count(distinct transaction_id) as total_transactions,
        sum(transaction_amount) as total_transaction_volume
    from {{ ref('fact_transactions') }}
),

refund_metrics as (
    select
        count(distinct refund_id) as total_refunds_count,
        sum(amount) as total_refund_amount,
        avg(amount) as avg_refund_amount,
        max(amount) as max_refund_amount,
        min(amount) as min_refund_amount
    from {{ ref('stg_refunds') }}
),

dispute_metrics as (
    select
        count(distinct dispute_id) as total_disputes_count,
        sum(amount) as total_dispute_amount,
        avg(amount) as avg_dispute_amount,
        max(amount) as max_dispute_amount,
        min(amount) as min_dispute_amount
    from {{ ref('stg_disputes') }}
),

dispute_reasons as (
    select
        reason as dispute_reason,
        count(distinct dispute_id) as dispute_count,
        sum(amount) as dispute_amount,
        round(
            safe_divide(
                count(distinct dispute_id),
                (select total_disputes_count from dispute_metrics)
            ) * 100,
            2
        ) as reason_percentage
    from {{ ref('stg_disputes') }}
    where reason is not null
    group by reason
    order by dispute_count desc
),

combined_metrics as (
    select
        -- Refund metrics
        (select total_refunds_count from refund_metrics) as total_refunds_count,
        (select total_refund_amount from refund_metrics) as total_refund_amount,
        (select avg_refund_amount from refund_metrics) as avg_refund_amount,
        (select max_refund_amount from refund_metrics) as max_refund_amount,
        (select min_refund_amount from refund_metrics) as min_refund_amount,
        
        -- Dispute metrics
        (select total_disputes_count from dispute_metrics) as total_disputes_count,
        (select total_dispute_amount from dispute_metrics) as total_dispute_amount,
        (select avg_dispute_amount from dispute_metrics) as avg_dispute_amount,
        (select max_dispute_amount from dispute_metrics) as max_dispute_amount,
        (select min_dispute_amount from dispute_metrics) as min_dispute_amount,
        
        -- Rates
        round(
            safe_divide(
                (select total_refunds_count from refund_metrics),
                (select total_transactions from transaction_summary)
            ) * 100,
            2
        ) as refund_rate_percent,
        round(
            safe_divide(
                (select total_disputes_count from dispute_metrics),
                (select total_transactions from transaction_summary)
            ) * 100,
            2
        ) as dispute_rate_percent,
        
        -- Combined risk
        round(
            safe_divide(
                (select total_refunds_count from refund_metrics) + (select total_disputes_count from dispute_metrics),
                (select total_transactions from transaction_summary)
            ) * 100,
            2
        ) as combined_risk_rate_percent,
        
        -- Amounts as % of volume
        round(
            safe_divide(
                (select total_refund_amount from refund_metrics),
                (select total_transaction_volume from transaction_summary)
            ) * 100,
            2
        ) as refund_amount_percent_of_volume,
        round(
            safe_divide(
                (select total_dispute_amount from dispute_metrics),
                (select total_transaction_volume from transaction_summary)
            ) * 100,
            2
        ) as dispute_amount_percent_of_volume,
        
        current_timestamp() as dbt_loaded_at
    from transaction_summary
)

select * from combined_metrics