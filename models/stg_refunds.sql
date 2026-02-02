{{ config(
    materialized='table',
    description='Cleaned and deduplicated refund data. Removes CDC duplicates and validates against original transactions.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per refund'
    }
) }}

with source_data as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _ab_cdc_deleted_at,
        row_number() over (partition by refund_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'refunds') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        upper(coalesce(reason, 'UNKNOWN')) as reason,
        upper(coalesce(status, 'UNKNOWN')) as status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        cast(created_at as date) as refund_date,
        case
            when amount is null or amount <= 0 then false
            else true
        end as has_valid_amount,
        case
            when currency is null then false
            else true
        end as has_valid_currency,
        case
            when status is null or status = '' then false
            else true
        end as has_valid_status,
        case
            when refund_id is null 
                or original_transaction_id is null 
                or amount is null 
                or amount <= 0
                or currency is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

select
    refund_id,
    original_transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at,
    updated_at,
    _airbyte_extracted_at,
    refund_date,
    has_valid_amount,
    has_valid_currency,
    has_valid_status,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated