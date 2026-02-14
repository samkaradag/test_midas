-- dim_payment_methods.sql
-- Purpose: Payment methods dimension table for star schema
-- Creates surrogate keys and links to customer dimension
-- Input: stg_payment_methods (cleaned payment method data)
-- Output: payment methods dimension

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'dimension', 'payment_methods'],
    description='Payment methods dimension with surrogate keys'
) }}

with payment_methods as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        created_at,
        updated_at
    from {{ ref('stg_payment_methods') }}
),

-- Join with customer dimension to get customer_key
enrich_payment_methods as (
    select
        {{ dbt_utils.generate_surrogate_key(['payment_method_id']) }} as payment_method_key,
        payment_method_id,
        dc.customer_key,
        pm.customer_id,
        pm.method_type,
        pm.is_default,
        pm.created_at,
        pm.updated_at,
        current_timestamp() as dbt_loaded_at
    from payment_methods pm
    left join {{ ref('dim_customers') }} dc
        on pm.customer_id = dc.customer_id
)

select * from enrich_payment_methods