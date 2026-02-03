{{ config(
    materialized='table',
    description='Denormalized customer 360 view with aggregated payment methods and mandates. One row per customer with complete customer profile and associated payment/mandate data.',
    meta={
        'owner': 'data_engineering',
        'layer': 'facts',
        'grain': 'one row per customer',
        'denormalization_type': 'customer_enriched'
    }
) }}

with customers as (
    select
        customer_id,
        email,
        customer_type,
        kyc_status,
        risk_profile,
        phone_number,
        address,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        has_valid_type,
        has_kyc_status,
        has_risk_profile,
        has_data_quality_issues as customer_has_data_quality_issues
    from {{ ref('stg_customers') }}
),

payment_methods_agg as (
    select
        customer_id,
        count(*) as total_payment_methods,
        count(case when is_default = true then 1 end) as default_payment_method_count,
        array_agg(distinct method_type) as payment_method_types,
        max(updated_at) as last_payment_method_update
    from {{ ref('stg_payment_methods') }}
    where has_data_quality_issues = false
    group by customer_id
),

mandates_agg as (
    select
        customer_id,
        count(*) as total_mandates,
        count(case when status = 'ACTIVE' then 1 end) as active_mandates_count,
        count(case when status = 'INACTIVE' then 1 end) as inactive_mandates_count,
        array_agg(distinct status) as mandate_statuses,
        max(updated_at) as last_mandate_update
    from {{ ref('stg_mandates') }}
    where has_data_quality_issues = false
    group by customer_id
),

joined as (
    select
        c.customer_id,
        c.email,
        c.customer_type,
        c.kyc_status,
        c.risk_profile,
        c.phone_number,
        c.address,
        c.created_at,
        c.updated_at,
        c._airbyte_extracted_at,
        -- Payment method aggregates
        coalesce(pma.total_payment_methods, 0) as total_payment_methods,
        coalesce(pma.default_payment_method_count, 0) as default_payment_method_count,
        pma.payment_method_types,
        pma.last_payment_method_update,
        -- Mandate aggregates
        coalesce(ma.total_mandates, 0) as total_mandates,
        coalesce(ma.active_mandates_count, 0) as active_mandates_count,
        coalesce(ma.inactive_mandates_count, 0) as inactive_mandates_count,
        ma.mandate_statuses,
        ma.last_mandate_update,
        -- Calculated fields
        case
            when c.kyc_status = 'VERIFIED' then true
            else false
        end as is_kyc_verified,
        case
            when c.risk_profile in ('HIGH', 'CRITICAL') then true
            else false
        end as is_high_risk,
        case
            when c.customer_has_data_quality_issues = true 
                or c.kyc_status is null 
                or c.risk_profile is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from customers c
    left join payment_methods_agg pma on c.customer_id = pma.customer_id
    left join mandates_agg ma on c.customer_id = ma.customer_id
)

select * from joined