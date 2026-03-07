{%
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'transactions'],
    description='Cleaned and deduplicated transactions. Validates transaction integrity.'
  )
%}

with source_data as (
  select
    transaction_id,
    debtor_cus_id as debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    reference,
    created_at,
    updated_at,
    row_number() over (partition by transaction_id order by created_at) as rn
  from {{ source('raw_data', 'transactions') }}
  where _ab_cdc_deleted_at is null
),

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
  where 
    rn = 1
    -- Validate status
    and status in ('pending', 'completed', 'failed', 'cancelled')
    -- Ensure debtor and creditor are different
    and debtor_customer_id != creditor_customer_id
    -- Ensure amount is positive
    and amount > 0
)

select * from deduplicated