{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['dimension', 'payment_methods'],
    description='Payment methods dimension with customer foreign key.'
  )
}}

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
with_customer_key as (
  select
    md5(pm.payment_method_id) as payment_method_key,
    pm.payment_method_id,
    dc.customer_key,
    pm.method_type,
    pm.is_default,
    pm.created_at,
    pm.updated_at,
    current_timestamp() as dbt_loaded_at
  from payment_methods pm
  left join {{ ref('dim_customers') }} dc 
    on pm.customer_id = dc.customer_id
)

select * from with_customer_key