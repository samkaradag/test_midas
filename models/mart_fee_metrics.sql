-- mart_fee_metrics.sql
-- Purpose: Fee and revenue metrics
-- Tracks fee collection, types, and revenue impact
-- Input: stg_fees, fact_transactions, fact_transaction_details
-- Output: Fee revenue and margin metrics

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'metrics', 'revenue', 'fees'],
    description='Fee metrics including collection, types, and revenue margins'
) }}

with transaction_summary as (
    select
        count(distinct transaction_id) as total_transactions,
        sum(transaction_amount) as total_transaction_amount
    from {{ ref('fact_transactions') }}
),

fee_data as (
    select
        fee_id,
        transaction_id,
        amount as fee_amount,
        currency,
        fee_type,
        created_at
    from {{ ref('stg_fees') }}
),

transaction_fees as (
    select
        ft.transaction_id,
        ft.transaction_amount,
        coalesce(sum(sf.fee_amount), 0) as total_fees_for_transaction
    from {{ ref('fact_transactions') }} ft
    left join fee_data sf
        on ft.transaction_id = sf.transaction_id
    group by ft.transaction_id, ft.transaction_amount
),

fee_by_type as (
    select
        fee_type,
        count(distinct fee_id) as fee_count,
        sum(fee_amount) as fee_amount_total,
        avg(fee_amount) as avg_fee_amount,
        max(fee_amount) as max_fee_amount,
        min(fee_amount) as min_fee_amount
    from fee_data
    group by fee_type
),

combined_metrics as (
    select
        -- Total fees
        (select sum(fee_amount) from fee_data) as total_fees_collected,
        (select count(distinct fee_id) from fee_data) as total_fee_records,
        
        -- Fees by type
        sum(case when fee_type = 'TRANSACTION_FEE' then fee_amount else 0 end) as transaction_fees,
        sum(case when fee_type = 'SERVICE_FEE' then fee_amount else 0 end) as service_fees,
        sum(case when fee_type = 'PROCESSING_FEE' then fee_amount else 0 end) as processing_fees,
        sum(case when fee_type = 'MONTHLY_FEE' then fee_amount else 0 end) as monthly_fees,
        sum(case when fee_type = 'PENALTY_FEE' then fee_amount else 0 end) as penalty_fees,
        
        -- Average metrics
        round(
            safe_divide(
                (select sum(fee_amount) from fee_data),
                (select total_transactions from transaction_summary)
            ),
            2
        ) as avg_fee_per_transaction,
        
        -- Margin metrics
        round(
            safe_divide(
                (select sum(fee_amount) from fee_data),
                (select total_transaction_amount from transaction_summary)
            ) * 100,
            2
        ) as fee_margin_percent,
        
        -- Fee distribution
        round(
            safe_divide(
                sum(case when fee_type = 'TRANSACTION_FEE' then fee_amount else 0 end),
                (select sum(fee_amount) from fee_data)
            ) * 100,
            2
        ) as transaction_fee_percent_of_total,
        round(
            safe_divide(
                sum(case when fee_type = 'SERVICE_FEE' then fee_amount else 0 end),
                (select sum(fee_amount) from fee_data)
            ) * 100,
            2
        ) as service_fee_percent_of_total,
        
        current_timestamp() as dbt_loaded_at
    from fee_data
)

select * from combined_metrics