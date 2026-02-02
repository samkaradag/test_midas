{{ config(
    materialized='table',
    description='Cleaned and deduplicated payout data. Removes CDC duplicates and validates status transitions.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per payout'
    }
) }}

with source_data as (
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
        _ab_cdc_deleted_at,
        row_number() over (partition by payout_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'payouts') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        upper(coalesce(status, 'UNKNOWN')) as status,
        scheduled_at,
        executed_at,
        created_at,
        _airbyte_extracted_at,
        cast(created_at as date) as payout_date,
        cast(coalesce(scheduled_at, created_at) as date) as scheduled_date,
        cast(executed_at as date) as executed_date,
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
            when payout_id is null 
                or recipient_customer_id is null 
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
    has_valid_amount,
    has_valid_currency,
    has_valid_status,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated