{{ config(
    materialized='table',
    description='Denormalized payout fact table with recipient customer details. One row per payout.',
    meta={
        'owner': 'data_engineering',
        'layer': 'marts',
        'grain': 'one row per payout with enriched customer details'
    }
) }}

with payouts as (
    select
        payout_id,
        recipient_customer_id,
        amount as payout_amount,
        currency as payout_currency,
        status as payout_status,
        scheduled_at,
        executed_at,
        created_at as payout_created_at,
        payout_date,
        scheduled_date,
        executed_date,
        has_data_quality_issues as payout_has_quality_issues
    from {{ ref('stg_payouts') }}
),

recipient_customer as (
    select
        customer_id,
        email,
        customer_type,
        kyc_status,
        risk_profile,
        phone_number,
        address,
        created_at as customer_created_at
    from {{ ref('stg_customers') }}
),

final as (
    select
        p.payout_id,
        p.recipient_customer_id,
        p.payout_amount,
        p.payout_currency,
        p.payout_status,
        p.payout_date,
        p.scheduled_date,
        p.executed_date,
        p.payout_created_at,
        p.scheduled_at,
        p.executed_at,
        -- Recipient Customer Details
        rc.email as recipient_email,
        rc.customer_type as recipient_customer_type,
        rc.kyc_status as recipient_kyc_status,
        rc.risk_profile as recipient_risk_profile,
        rc.phone_number as recipient_phone,
        rc.address as recipient_address,
        rc.customer_created_at as recipient_customer_created_at,
        -- Calculated Fields
        case 
            when p.payout_status = 'EXECUTED' then true 
            else false 
        end as is_executed,
        case 
            when p.executed_at is not null 
            then datediff(day, p.scheduled_date, p.executed_date) 
            else null 
        end as days_to_execute,
        current_timestamp() as dbt_loaded_at
    from payouts p
    left join recipient_customer rc on p.recipient_customer_id = rc.customer_id
)

select * from final