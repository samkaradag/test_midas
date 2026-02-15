-- fact_transaction_details.sql
-- Purpose: Detailed transaction facts including refunds, disputes, and fees
-- Aggregates related transactions details by transaction_id
-- Input: stg_refunds, stg_disputes, stg_fees, fact_transactions
-- Output: aggregated transaction details

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'fact', 'transaction_details'],
    description='Transaction details with aggregated refunds, disputes, and fees'
) }}

with transactions as (
    select
        transaction_key,
        transaction_id,
        transaction_amount
    from {{ ref('fact_transactions') }}
),

-- Aggregate refunds by transaction
refunds_agg as (
    select
        original_transaction_id as transaction_id,
        sum(amount) as refund_amount,
        count(*) as refund_count
    from {{ ref('stg_refunds') }}
    group by original_transaction_id
),

-- Aggregate disputes by transaction
disputes_agg as (
    select
        transaction_id,
        sum(amount) as dispute_amount,
        count(*) as dispute_count
    from {{ ref('stg_disputes') }}
    group by transaction_id
),

-- Aggregate fees by transaction
fees_agg as (
    select
        transaction_id,
        sum(amount) as fee_amount,
        count(*) as fee_count
    from {{ ref('stg_fees') }}
    group by transaction_id
),

-- Combine all details
combine_details as (
    select
        t.transaction_key,
        t.transaction_id,
        t.transaction_amount,
        coalesce(r.refund_amount, 0) as refund_amount,
        coalesce(d.dispute_amount, 0) as dispute_amount,
        coalesce(f.fee_amount, 0) as fee_amount,
        coalesce(r.refund_count, 0) as refund_count,
        coalesce(d.dispute_count, 0) as dispute_count,
        coalesce(f.fee_count, 0) as fee_count,
        -- Calculate net transaction amount: amount - refunds - fees
        t.transaction_amount 
            - coalesce(r.refund_amount, 0) 
            - coalesce(f.fee_amount, 0) as net_transaction_amount,
        current_timestamp() as dbt_loaded_at
    from transactions t
    left join refunds_agg r
        on t.transaction_id = r.transaction_id
    left join disputes_agg d
        on t.transaction_id = d.transaction_id
    left join fees_agg f
        on t.transaction_id = f.transaction_id
)

select * from combine_details
order by transaction_id