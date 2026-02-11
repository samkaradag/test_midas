{{ config(
    materialized='incremental',
    schema='payments_v1',
    unique_key='customer_id',
    on_schema_change='fail',
    tags=['dimension', 'customers']
) }}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

customer_with_key as (
    select
        row_number() over (order by customer_id) as customer_key,
        customer_id,
        customer_type,
        email,
        phone_number,
        address,
        kyc_status,
        risk_profile,
        created_at,
        updated_at,
        current_timestamp() as effective_date,
        cast(null as timestamp) as end_date,
        true as is_current
    from stg_customers

    {% if execute %}
        {% if not run_started_at %}
            -- Initial load
            where 1=1
        {% else %}
            -- Incremental load - only new or changed records
            where updated_at >= (select max(updated_at) from {{ this }})
        {% endif %}
    {% endif %}
)

select * from customer_with_key