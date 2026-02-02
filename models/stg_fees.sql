{{ config(
    materialized='table',
    description='Cleaned and deduplicated fee data. Removes CDC duplicates and validates amounts.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per fee'
    }
) }}

with source_data as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        fee_type,
        created_at,
        _airbyte_extracted_at,
        _ab_cdc_deleted_at,
        row_number() over (partition by fee_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'fees') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        upper(coalesce(fee_type, 'UNKNOWN')) as fee_type,
        created_at,
        _airbyte_extracted_at,
        cast(created_at as date) as fee_date,
        case
            when amount is null or amount < 0 then false
            else true
        end as has_valid_amount,
        case
            when currency is null then false
            else true
        end as has_valid_currency,
        case
            when fee_id is null 
                or transaction_id is null 
                or amount is null 
                or amount < 0
                or currency is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

select
    fee_id,
    transaction_id,
    amount,
    currency,
    fee_type,
    created_at,
    _airbyte_extracted_at,
    fee_date,
    has_valid_amount,
    has_valid_currency,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated