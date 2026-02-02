{{ config(
    materialized='table',
    description='Cleaned and deduplicated mandate data. Removes CDC duplicates and validates customer/payment method relationships.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per mandate'
    }
) }}

with source_data as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _ab_cdc_deleted_at,
        row_number() over (partition by mandate_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'mandates') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        mandate_id,
        customer_id,
        payment_method_id,
        reference,
        upper(coalesce(status, 'UNKNOWN')) as status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        cast(created_at as date) as mandate_date,
        case
            when customer_id is null then false
            else true
        end as has_valid_customer,
        case
            when payment_method_id is null then false
            else true
        end as has_valid_payment_method,
        case
            when status is null or status = '' then false
            else true
        end as has_valid_status,
        case
            when mandate_id is null 
                or customer_id is null 
                or payment_method_id is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

select
    mandate_id,
    customer_id,
    payment_method_id,
    reference,
    status,
    created_at,
    updated_at,
    _airbyte_extracted_at,
    mandate_date,
    has_valid_customer,
    has_valid_payment_method,
    has_valid_status,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated