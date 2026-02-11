{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'customers']
) }}

with raw_customers as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        _airbyte_data,
        -- Parse JSON data
        json_extract_scalar(_airbyte_data, '$.customer_id') as customer_id,
        json_extract_scalar(_airbyte_data, '$.customer_type') as customer_type,
        json_extract_scalar(_airbyte_data, '$.email') as email,
        json_extract_scalar(_airbyte_data, '$.phone_number') as phone_number,
        json_extract_scalar(_airbyte_data, '$.address') as address,
        json_extract_scalar(_airbyte_data, '$.kyc_status') as kyc_status,
        json_extract_scalar(_airbyte_data, '$.risk_profile') as risk_profile,
        timestamp(json_extract_scalar(_airbyte_data, '$.created_at')) as created_at,
        timestamp(json_extract_scalar(_airbyte_data, '$.updated_at')) as updated_at
    from {{ source('raw_customers', 'airbyte_raw_customers') }}
),

cleaned_customers as (
    select
        customer_id,
        customer_type,
        lower(trim(email)) as email,
        phone_number,
        address,
        kyc_status,
        risk_profile,
        created_at,
        updated_at,
        current_timestamp() as dbt_loaded_at
    from raw_customers
    where customer_id is not null
)

select * from cleaned_customers