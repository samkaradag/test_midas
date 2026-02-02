{{ config(
    materialized='table',
    description='Cleaned and deduplicated payment method data. Removes CDC duplicates and validates relationships.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per payment method'
    }
) }}

with source_data as (
    select
        payment_method_id,
        customer_id,
        method_type,
        details,
        is_default,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _ab_cdc_deleted_at,
        row_number() over (partition by payment_method_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'payment_methods') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        payment_method_id,
        customer_id,
        upper(method_type) as method_type,
        details,
        coalesce(is_default, false) as is_default,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        case
            when method_type is null then false
            else true
        end as has_valid_method_type,
        case
            when customer_id is null then false
            else true
        end as has_valid_customer,
        case
            when payment_method_id is null 
                or customer_id is null 
                or method_type is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

select
    payment_method_id,
    customer_id,
    method_type,
    details,
    is_default,
    created_at,
    updated_at,
    _airbyte_extracted_at,
    has_valid_method_type,
    has_valid_customer,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated