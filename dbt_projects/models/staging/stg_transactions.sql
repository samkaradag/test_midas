-- stg_transactions.sql
-- Purpose: Clean and deduplicate transaction data
-- Validates transaction statuses and customer IDs
-- Ensures debtor_customer_id ≠ creditor_customer_id
-- Input: raw transactions table (359K rows)
-- Output: clean unique transactions

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'transactions'],
    description='Cleaned transactions with validation and deduplication'
) }}

with source_data as (
    select
        transaction_id,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        reference,
        created_at,
        updated_at,
        _airbyte_extracted_at,
        row_number() over (partition by transaction_id order by created_at asc) as rn
    from {{ source('raw_customers', 'transactions') }}
    where _ab_cdc_deleted_at is null
),

-- Remove duplicates: keep first record by created_at for each transaction_id
deduplicated as (
    select
        transaction_id,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        status,
        reference,
        created_at,
        updated_at
    from source_data
    where rn = 1
),

-- Validate and clean transactions
validate_transactions as (
    select
        transaction_id,
        debtor_customer_id,
        creditor_customer_id,
        payment_method_id,
        amount,
        currency,
        case 
            when lower(status) in ('pending', 'processing') then 'PENDING'
            when lower(status) in ('completed', 'success', 'done') then 'COMPLETED'
            when lower(status) in ('failed', 'declined') then 'FAILED'
            when lower(status) = 'cancelled' then 'CANCELLED'
            when lower(status) = 'refunded' then 'REFUNDED'
            else 'UNKNOWN'
        end as status,
        reference,
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at,
        -- Flags for validation
        case 
            when debtor_customer_id is null then false
            when creditor_customer_id is null then false
            when debtor_customer_id = creditor_customer_id then false  -- Same customer cannot be debtor and creditor
            when amount <= 0 then false  -- Amount must be positive
            else true
        end as is_valid_transaction
    from deduplicated
),

-- Join with cleaned customers to validate FK
validate_fk as (
    select
        t.transaction_id,
        t.debtor_customer_id,
        t.creditor_customer_id,
        t.payment_method_id,
        t.amount,
        t.currency,
        t.status,
        t.reference,
        t.created_at,
        t.updated_at,
        t.dbt_updated_at,
        t.is_valid_transaction,
        case when sc_debtor.customer_id is not null then true else false end as debtor_exists,
        case when sc_creditor.customer_id is not null then true else false end as creditor_exists
    from validate_transactions t
    left join {{ ref('stg_customers') }} sc_debtor
        on t.debtor_customer_id = sc_debtor.customer_id
    left join {{ ref('stg_customers') }} sc_creditor
        on t.creditor_customer_id = sc_creditor.customer_id
)

select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    reference,
    created_at,
    updated_at,
    dbt_updated_at
from validate_fk
where is_valid_transaction = true
  and debtor_exists = true
  and creditor_exists = true