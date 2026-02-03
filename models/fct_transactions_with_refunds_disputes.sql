{{ config(
    materialized='table',
    description='Denormalized transaction fact table with aggregated refunds and disputes. One row per transaction with refund and dispute metrics.',
    meta={
        'owner': 'data_engineering',
        'layer': 'facts',
        'grain': 'one row per transaction',
        'denormalization_type': 'enriched_with_refunds_disputes'
    }
) }}

with transactions as (
    select
        transaction_id,
        reference,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        transaction_date
    from {{ ref('stg_transactions') }}
),

refunds_agg as (
    select
        original_transaction_id,
        count(*) as total_refunds_count,
        count(case when status = 'COMPLETED' then 1 end) as completed_refunds_count,
        count(case when status = 'PENDING' then 1 end) as pending_refunds_count,
        sum(amount) as total_refunded_amount,
        max(amount) as max_refund_amount,
        min(amount) as min_refund_amount,
        array_agg(distinct reason) as refund_reasons,
        array_agg(distinct status) as refund_statuses,
        max(updated_at) as last_refund_update
    from {{ ref('stg_refunds') }}
    where has_data_quality_issues = false
    group by original_transaction_id
),

disputes_agg as (
    select
        transaction_id,
        count(*) as total_disputes_count,
        count(case when status = 'OPEN' then 1 end) as open_disputes_count,
        count(case when status = 'RESOLVED' then 1 end) as resolved_disputes_count,
        sum(amount) as total_disputed_amount,
        max(amount) as max_dispute_amount,
        min(amount) as min_dispute_amount,
        array_agg(distinct reason) as dispute_reasons,
        array_agg(distinct status) as dispute_statuses,
        max(updated_at) as last_dispute_update
    from {{ ref('stg_disputes') }}
    where has_data_quality_issues = false
    group by transaction_id
),

joined as (
    select
        t.transaction_id,
        t.reference,
        t.debtor_customer_id,
        t.creditor_customer_id,
        t.payment_method_id,
        t.amount as transaction_amount,
        t.currency,
        t.status as transaction_status,
        t.created_at,
        t.updated_at,
        t._airbyte_extracted_at,
        t.transaction_date,
        -- Refund aggregates
        coalesce(ra.total_refunds_count, 0) as total_refunds_count,
        coalesce(ra.completed_refunds_count, 0) as completed_refunds_count,
        coalesce(ra.pending_refunds_count, 0) as pending_refunds_count,
        coalesce(ra.total_refunded_amount, 0) as total_refunded_amount,
        ra.max_refund_amount,
        ra.min_refund_amount,
        ra.refund_reasons,
        ra.refund_statuses,
        ra.last_refund_update,
        -- Dispute aggregates
        coalesce(da.total_disputes_count, 0) as total_disputes_count,
        coalesce(da.open_disputes_count, 0) as open_disputes_count,
        coalesce(da.resolved_disputes_count, 0) as resolved_disputes_count,
        coalesce(da.total_disputed_amount, 0) as total_disputed_amount,
        da.max_dispute_amount,
        da.min_dispute_amount,
        da.dispute_reasons,
        da.dispute_statuses,
        da.last_dispute_update,
        -- Calculated fields
        t.amount - coalesce(ra.total_refunded_amount, 0) as net_transaction_amount,
        case
            when coalesce(ra.total_refunds_count, 0) > 0 then true
            else false
        end as has_refunds,
        case
            when coalesce(da.total_disputes_count, 0) > 0 then true
            else false
        end as has_disputes,
        case
            when coalesce(ra.total_refunds_count, 0) > 0 
                or coalesce(da.total_disputes_count, 0) > 0 
            then true
            else false
        end as has_refunds_or_disputes,
        case
            when coalesce(da.open_disputes_count, 0) > 0 then 'DISPUTED'
            when coalesce(ra.pending_refunds_count, 0) > 0 then 'PENDING_REFUND'
            when coalesce(ra.completed_refunds_count, 0) > 0 then 'REFUNDED'
            else 'CLEAN'
        end as transaction_risk_status,
        current_timestamp() as dbt_loaded_at
    from transactions t
    left join refunds_agg ra on t.transaction_id = ra.original_transaction_id
    left join disputes_agg da on t.transaction_id = da.transaction_id
)

select * from joined