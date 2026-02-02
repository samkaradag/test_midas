{{ config(
    materialized='table',
    description='Cleaned and deduplicated transaction data. Removes CDC duplicates and validates transaction amounts.',
    meta={
        'owner': 'data_engineering',
        'layer': 'staging',
        'grain': 'one row per transaction'
    }
) }}

with source_data as (
    select
        transaction_id,
        reference,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _ab_cdc_deleted_at,
        row_number() over (partition by transaction_id order by _airbyte_extracted_at desc) as rn
    from {{ source('payments_raw', 'transactions') }}
    where _ab_cdc_deleted_at is null
),

deduplicated as (
    select
        transaction_id,
        reference,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        cast(created_at as date) as transaction_date,
        case
            when amount is null or amount <= 0 then false
            else true
        end as has_valid_amount,
        case
            when currency is null then false
            else true
        end as has_valid_currency,
        case
            when status is null then false
            else true
        end as has_valid_status,
        case
            when transaction_id is null 
                or debtor_customer_id is null 
                or creditor_customer_id is null 
                or amount is null 
                or amount <= 0
                or currency is null
                or status is null
            then true
            else false
        end as has_data_quality_issues,
        current_timestamp() as dbt_loaded_at
    from source_data
    where rn = 1
)

select
    transaction_id,
    reference,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    created_at,
    updated_at,
    _airbyte_extracted_at,
    transaction_date,
    has_valid_amount,
    has_valid_currency,
    has_valid_status,
    has_data_quality_issues,
    dbt_loaded_at
from deduplicated