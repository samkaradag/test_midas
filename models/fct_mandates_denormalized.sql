{{ config(
    materialized='table',
    description='Denormalized mandate fact table with customer and payment method details. One row per mandate.',
    meta={
        'owner': 'data_engineering',
        'layer': 'marts',
        'grain': 'one row per mandate with enriched details'
    }
) }}

with mandates as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        status as mandate_status,
        created_at as mandate_created_at,
        updated_at as mandate_updated_at,
        mandate_date,
        has_data_quality_issues as mandate_has_quality_issues
    from {{ ref('stg_mandates') }}
),

customer as (
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

payment_method as (
    select
        payment_method_id,
        customer_id as pm_customer_id,
        method_type,
        details as payment_method_details,
        is_default as payment_method_is_default,
        created_at as payment_method_created_at
    from {{ ref('stg_payment_methods') }}
),

final as (
    select
        m.mandate_id,
        m.customer_id,
        m.payment_method_id,
        m.reference,
        m.mandate_status,
        m.mandate_date,
        m.mandate_created_at,
        m.mandate_updated_at,
        -- Customer Details
        c.email,
        c.customer_type,
        c.kyc_status,
        c.risk_profile,
        c.phone_number,
        c.address,
        c.customer_created_at,
        -- Payment Method Details
        pm.method_type,
        pm.payment_method_details,
        pm.payment_method_is_default,
        pm.payment_method_created_at,
        -- Calculated Fields
        case 
            when m.mandate_status = 'ACTIVE' then true 
            else false 
        end as is_active_mandate,
        current_timestamp() as dbt_loaded_at
    from mandates m
    left join customer c on m.customer_id = c.customer_id
    left join payment_method pm on m.payment_method_id = pm.payment_method_id
)

select * from final