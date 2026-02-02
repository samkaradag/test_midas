{{ config(
    materialized='table',
    description='Cleaned and deduplicated customer master data. Removes CDC duplicates by keeping latest record per customer_id.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per customer'
    }
) }}

with source_data as (
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
        _ab_cdc_deleted_at,
        row_number() over (partition by customer_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'customers') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
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
        case when customer_type is null then false else true end as has_valid_type,
        case when kyc_status is null then false else true end as has_kyc_status,
        case when risk_profile is null then false else true end as has_risk_profile,
        case when email is null or customer_id is null then true else false end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

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
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated