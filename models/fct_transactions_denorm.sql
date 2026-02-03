{{ config(
    materialized='table',
    description='Denormalized transaction fact table with enriched customer, payment method, and fee data. One row per transaction with aggregated fees and associated customer details.',
    meta={
        'owner': 'data_engineering',
        'layer': 'facts',
        'grain': 'one row per transaction',
        'denormalization_type': 'enriched_with_aggregates'
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
        transaction_date,
        has_data_quality_issues as txn_has_data_quality_issues
    from {{ ref('stg_transactions') }}
),

debtor_customers as (
    select
        customer_id as debtor_id,
        email as debtor_email,
        customer_type as debtor_customer_type,
        kyc_status as debtor_kyc_status,
        risk_profile as debtor_risk_profile,
        phone_number as debtor_phone,
        address as debtor_address
    from {{ ref('stg_customers') }}
),

creditor_customers as (
    select
        customer_id as creditor_id,
        email as creditor_email,
        customer_type as creditor_customer_type,
        kyc_status as creditor_kyc_status,
        risk_profile as creditor_risk_profile,
        phone_number as creditor_phone,
        address as creditor_address
    from {{ ref('stg_customers') }}
),

payment_methods as (
    select
        payment_method_id,
        customer_id as pm_customer_id,
        method_type,
        is_default as is_default_payment_method
    from {{ ref('stg_payment_methods') }}
),

fees_agg as (
    select
        transaction_id,
        count(*) as total_fees_count,
        sum(amount) as total_fees_amount,
        max(amount) as max_fee_amount,
        min(amount) as min_fee_amount,
        array_agg(distinct fee_type) as fee_types
    from {{ ref('stg_fees') }}
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
        t.status,
        t.created_at,
        t.updated_at,
        t._airbyte_extracted_at,
        t.transaction_date,
        -- Debtor customer fields
        dc.debtor_email,
        dc.debtor_customer_type,
        dc.debtor_kyc_status,
        dc.debtor_risk_profile,
        dc.debtor_phone,
        dc.debtor_address,
        -- Creditor customer fields
        cc.creditor_email,
        cc.creditor_customer_type,
        cc.creditor_kyc_status,
        cc.creditor_risk_profile,
        cc.creditor_phone,
        cc.creditor_address,
        -- Payment method fields
        pm.method_type,
        pm.is_default_payment_method,
        -- Aggregated fees
        coalesce(fa.total_fees_count, 0) as total_fees_count,
        coalesce(fa.total_fees_amount, 0) as total_fees_amount,
        fa.max_fee_amount,
        fa.min_fee_amount,
        fa.fee_types,
        -- Calculated fields
        t.amount + coalesce(fa.total_fees_amount, 0) as total_amount_with_fees,
        case
            when t.amount > 10000 then 'HIGH'
            when t.amount > 1000 then 'MEDIUM'
            else 'LOW'
        end as transaction_size_category,
        case
            when dc.debtor_risk_profile = 'CRITICAL' or cc.creditor_risk_profile = 'CRITICAL' then true
            else false
        end as involves_critical_risk_customer,
        case
            when t.txn_has_data_quality_issues = true or dc.debtor_id is null or cc.creditor_id is null then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from transactions t
    left join debtor_customers dc on t.debtor_customer_id = dc.debtor_id
    left join creditor_customers cc on t.creditor_customer_id = cc.creditor_id
    left join payment_methods pm on t.payment_method_id = pm.payment_method_id
    left join fees_agg fa on t.transaction_id = fa.transaction_id
)

select * from joined