{{ config(
    materialized='table',
    description='Denormalized transaction fact table with all related customer, payment method, and transaction-related details. One row per transaction with enriched context.',
    meta={
        'owner': 'data_engineering',
        'layer': 'marts',
        'grain': 'one row per transaction with enriched details'
    }
) }}

with transactions as (
    select
        transaction_id,
        reference,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount as transaction_amount,
        currency as transaction_currency,
        status as transaction_status,
        created_at as transaction_created_at,
        updated_at as transaction_updated_at,
        transaction_date,
        has_data_quality_issues as transaction_has_quality_issues
    from {{ ref('stg_transactions') }}
),

debtor_customer as (
    select
        customer_id as debtor_customer_id,
        email as debtor_email,
        customer_type as debtor_customer_type,
        kyc_status as debtor_kyc_status,
        risk_profile as debtor_risk_profile,
        phone_number as debtor_phone,
        address as debtor_address,
        created_at as debtor_created_at
    from {{ ref('stg_customers') }}
),

creditor_customer as (
    select
        customer_id as creditor_customer_id,
        email as creditor_email,
        customer_type as creditor_customer_type,
        kyc_status as creditor_kyc_status,
        risk_profile as creditor_risk_profile,
        phone_number as creditor_phone,
        address as creditor_address,
        created_at as creditor_created_at
    from {{ ref('stg_customers') }}
),

payment_method as (
    select
        payment_method_id,
        method_type,
        details as payment_method_details,
        is_default as payment_method_is_default,
        created_at as payment_method_created_at
    from {{ ref('stg_payment_methods') }}
),

fees_agg as (
    select
        transaction_id,
        count(*) as fee_count,
        sum(amount) as total_fees,
        min(created_at) as first_fee_date,
        max(created_at) as last_fee_date,
        string_agg(distinct fee_type, ', ') as fee_types
    from {{ ref('stg_fees') }}
    group by transaction_id
),

refunds_agg as (
    select
        original_transaction_id as transaction_id,
        count(*) as refund_count,
        sum(amount) as total_refunded,
        min(created_at) as first_refund_date,
        max(created_at) as last_refund_date,
        string_agg(distinct reason, ', ') as refund_reasons
    from {{ ref('stg_refunds') }}
    group by original_transaction_id
),

disputes_agg as (
    select
        transaction_id,
        count(*) as dispute_count,
        sum(amount) as total_disputed,
        min(created_at) as first_dispute_date,
        max(created_at) as last_dispute_date,
        string_agg(distinct reason, ', ') as dispute_reasons,
        string_agg(distinct status, ', ') as dispute_statuses
    from {{ ref('stg_disputes') }}
    group by transaction_id
),

final as (
    select
        t.transaction_id,
        t.reference,
        t.debtor_customer_id,
        t.creditor_customer_id,
        t.payment_method_id,
        t.transaction_amount,
        t.transaction_currency,
        t.transaction_status,
        t.transaction_date,
        t.transaction_created_at,
        t.transaction_updated_at,
        -- Debtor Customer Details
        dc.debtor_email,
        dc.debtor_customer_type,
        dc.debtor_kyc_status,
        dc.debtor_risk_profile,
        dc.debtor_phone,
        dc.debtor_address,
        dc.debtor_created_at,
        -- Creditor Customer Details
        cc.creditor_email,
        cc.creditor_customer_type,
        cc.creditor_kyc_status,
        cc.creditor_risk_profile,
        cc.creditor_phone,
        cc.creditor_address,
        cc.creditor_created_at,
        -- Payment Method Details
        pm.method_type,
        pm.payment_method_details,
        pm.payment_method_is_default,
        pm.payment_method_created_at,
        -- Fee Details
        coalesce(fa.fee_count, 0) as fee_count,
        coalesce(fa.total_fees, 0) as total_fees,
        fa.fee_types,
        fa.first_fee_date,
        fa.last_fee_date,
        -- Refund Details
        coalesce(ra.refund_count, 0) as refund_count,
        coalesce(ra.total_refunded, 0) as total_refunded,
        ra.refund_reasons,
        ra.first_refund_date,
        ra.last_refund_date,
        -- Dispute Details
        coalesce(da.dispute_count, 0) as dispute_count,
        coalesce(da.total_disputed, 0) as total_disputed,
        da.dispute_reasons,
        da.dispute_statuses,
        da.first_dispute_date,
        da.last_dispute_date,
        -- Calculated Fields
        case when ra.refund_count > 0 then true else false end as has_refunds,
        case when da.dispute_count > 0 then true else false end as has_disputes,
        case when fa.fee_count > 0 then true else false end as has_fees,
        t.transaction_amount - coalesce(fa.total_fees, 0) as net_transaction_amount,
        current_timestamp() as dbt_loaded_at
    from transactions t
    left join debtor_customer dc on t.debtor_customer_id = dc.debtor_customer_id
    left join creditor_customer cc on t.creditor_customer_id = cc.creditor_customer_id
    left join payment_method pm on t.payment_method_id = pm.payment_method_id
    left join fees_agg fa on t.transaction_id = fa.transaction_id
    left join refunds_agg ra on t.transaction_id = ra.transaction_id
    left join disputes_agg da on t.transaction_id = da.transaction_id
)

select * from final