{{
  config(
    materialized='view',
    schema='payments_v1',
    tags=['staging', 'mandates'],
    description='Cleaned and deduplicated mandates. Validates customer foreign keys.'
  )
}}

with source_data as (
  select
    mandate_id,
    customer_id,
    payment_method_id,
    reference,
    status,
    created_at,
    updated_at,
    row_number() over (partition by mandate_id order by created_at) as rn
  from {{ source('raw_data', 'mandates') }}
  where _ab_cdc_deleted_at is null
),

deduplicated as (
  select
    mandate_id,
    customer_id,
    payment_method_id,
    reference,
    status,
    created_at,
    updated_at
  from source_data
  where 
    rn = 1
    and status in ('active', 'inactive', 'cancelled')
),

-- Validate foreign keys
with_fk_validation as (
  select
    m.mandate_id,
    m.customer_id,
    m.payment_method_id,
    m.reference,
    m.status,
    m.created_at,
    m.updated_at,
    case when c.customer_id is not null then true else false end as customer_fk_valid
  from deduplicated m
  left join {{ ref('stg_customers') }} c 
    on m.customer_id = c.customer_id
)

select
  mandate_id,
  customer_id,
  payment_method_id,
  reference,
  status,
  created_at,
  updated_at
from with_fk_validation
where customer_fk_valid = true