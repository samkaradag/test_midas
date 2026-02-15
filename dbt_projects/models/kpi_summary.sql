-- kpi_summary.sql
-- Purpose: Executive KPI dashboard with all key metrics
-- Single view combining all major business metrics for reporting
-- Input: All mart_* models
-- Output: Comprehensive KPI dashboard for executives and stakeholders

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'kpi', 'dashboard', 'executive'],
    description='Executive KPI dashboard combining all key business metrics in one view'
) }}

with latest_transaction_metrics as (
    select
        transaction_count,
        completed_transaction_count,
        failed_transaction_count,
        pending_transaction_count,
        total_transaction_amount,
        avg_transaction_amount,
        transaction_success_rate,
        transaction_failure_rate
    from {{ ref('mart_transaction_metrics') }}
    order by calendar_date desc
    limit 1
),

customer_summary as (
    select
        count(distinct customer_id) as total_customers,
        sum(case when is_active_customer_30d = true then 1 else 0 end) as active_customers_30d,
        sum(case when is_active_customer_90d = true then 1 else 0 end) as active_customers_90d,
        sum(case when is_kyc_verified = true then 1 else 0 end) as kyc_verified_customers,
        sum(customer_lifetime_value) as total_customer_lifetime_value,
        avg(customer_lifetime_value) as avg_customer_lifetime_value,
        max(customer_lifetime_value) as max_customer_lifetime_value,
        count(case when total_transactions_count = 0 then 1 end) as inactive_customers
    from {{ ref('mart_customer_metrics') }}
),

risk_metrics as (
    select
        total_refunds_count,
        total_refund_amount,
        refund_rate_percent,
        total_disputes_count,
        total_dispute_amount,
        dispute_rate_percent,
        combined_risk_rate_percent,
        avg_refund_amount,
        avg_dispute_amount
    from {{ ref('mart_refund_dispute_metrics') }}
),

fee_metrics as (
    select
        total_fees_collected,
        transaction_fees,
        service_fees,
        processing_fees,
        monthly_fees,
        penalty_fees,
        avg_fee_per_transaction,
        fee_margin_percent
    from {{ ref('mart_fee_metrics') }}
),

payout_metrics as (
    select
        total_payouts_count,
        completed_payouts_count,
        pending_payout_amount,
        total_payout_amount,
        avg_payout_amount,
        payout_success_rate,
        avg_days_to_execute,
        customers_with_payouts,
        avg_payouts_per_customer
    from {{ ref('mart_payout_metrics') }}
),

combined_kpis as (
    select
        -- Transaction KPIs
        ltm.transaction_count,
        ltm.completed_transaction_count,
        ltm.failed_transaction_count,
        ltm.pending_transaction_count,
        ltm.total_transaction_amount,
        ltm.avg_transaction_amount,
        ltm.transaction_success_rate,
        ltm.transaction_failure_rate,
        
        -- Customer KPIs
        cs.total_customers,
        cs.active_customers_30d,
        cs.active_customers_90d,
        cs.kyc_verified_customers,
        cs.inactive_customers,
        cs.total_customer_lifetime_value,
        cs.avg_customer_lifetime_value,
        round(safe_divide(cs.active_customers_30d, cs.total_customers) * 100, 2) as customer_activation_rate_30d,
        
        -- Risk KPIs
        rm.total_refunds_count,
        rm.total_refund_amount,
        rm.refund_rate_percent,
        rm.total_disputes_count,
        rm.total_dispute_amount,
        rm.dispute_rate_percent,
        rm.combined_risk_rate_percent,
        
        -- Revenue KPIs
        fm.total_fees_collected,
        fm.fee_margin_percent,
        fm.avg_fee_per_transaction,
        round(safe_divide(fm.total_fees_collected, ltm.total_transaction_amount) * 100, 2) as fee_to_transaction_ratio_percent,
        
        -- Payout KPIs
        pm.total_payouts_count,
        pm.completed_payouts_count,
        pm.payout_success_rate,
        pm.total_payout_amount,
        pm.avg_payout_amount,
        pm.pending_payout_amount,
        pm.avg_days_to_execute,
        
        -- Derived metrics
        round(safe_divide(ltm.total_transaction_amount, cs.total_customers), 2) as revenue_per_customer,
        round(safe_divide(fm.total_fees_collected, cs.total_customers), 2) as fee_revenue_per_customer,
        round(
            safe_divide(
                ltm.total_transaction_amount - rm.total_refund_amount,
                ltm.total_transaction_amount
            ) * 100,
            2
        ) as net_transaction_rate_percent,
        
        -- Metadata
        current_timestamp() as report_generated_at,
        current_date() as report_date
    from latest_transaction_metrics ltm
    cross join customer_summary cs
    cross join risk_metrics rm
    cross join fee_metrics fm
    cross join payout_metrics pm
)

select * from combined_kpis