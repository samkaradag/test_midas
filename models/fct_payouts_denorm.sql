{{ config(
    materialized='table',
    description='Denormalized payout fact table with enriched recipient customer data. One row per payout with complete customer profile.',
    meta={
        'owner': 'data_engineering',
        'layer': 'facts',
        'grain': 'one row per payout',
        'denormalization_type': 'enriched_with_customer'
    }
) }}

with payouts as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        status,
        scheduled_at,
        executed_at,
        created_at,
        _airbyte_extracted_at,
        payout_date,
        scheduled_date,
        executed_date,
        has_data_quality_issues as payout_has_data_quality_issues
    from {{ ref('stg_payouts') }}
),

recipients as (
    select
        customer_id,
        email as recipient_email,
        customer_type as recipient_customer_type,
        kyc_status as recipient_kyc_status,
        risk_profile as recipient_risk_profile,
        phone_number as recipient_phone,
        address as recipient_address,
        created_at as customer_created_at,
        updated_at as customer_updated_at
    from {{ ref('stg_customers') }}
),

joined as (
    select
        p.payout_id,
        p.recipient_customer_id,
        p.amount,
        p.currency,
        p.status,
        p.scheduled_at,
        p.executed_at,
        p.created_at,
        p._airbyte_extracted_at,
        p.payout_date,
        p.scheduled_date,
        p.executed_date,
        -- Recipient customer fields
        r.recipient_email,
        r.recipient_customer_type,
        r.recipient_kyc_status,
        r.recipient_risk_profile,
        r.recipient_phone,
        r.recipient_address,
        r.customer_created_at,
        r.customer_updated_at,
        -- Calculated fields
        case
            when p.executed_at is not null then datediff(day, p.scheduled_at, p.executed_at)
            else datediff(day, p.scheduled_at, current_timestamp())
        end as days_to_execute,
        case
            when p.executed_at is not null then 'EXECUTED'
            when p.scheduled_at <= current_timestamp() then 'OVERDUE'
            else 'PENDING'
        end as payout_status_category,
        case
            when p.status = 'COMPLETED' and p.executed_at is not null then true
            else false
        end as is_successfully_executed,
        case
            when r.recipient_risk_profile in ('HIGH', 'CRITICAL') then true
            else false
        end as recipient_is_high_risk,
        case
            when r.recipient_kyc_status = 'VERIFIED' then true
            else false
        end as recipient_is_kyc_verified,
        case
            when p.payout_has_data_quality_issues = true 
                or r.customer_id is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from payouts p
    left join recipients r on p.recipient_customer_id = r.customer_id
)

select * from joined