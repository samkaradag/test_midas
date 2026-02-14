-- fact_payouts.sql
-- Purpose: Payout fact table for star schema
-- Fact table for payouts with FKs to dimensions
-- Input: stg_payouts, dim_customers, dim_date
-- Output: payout facts with dimensional keys

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'fact', 'payouts'],
    description='Payout fact table with dimensional keys'
) }}

with payouts as (
    select
        payout_id,
        recipient_customer_id,
        amount,
        currency,
        status,
        scheduled_at,
        executed_at,
        created_at
    from {{ ref('stg_payouts') }}
),

-- Join with dimensions
enrich_payouts as (
    select
        {{ dbt_utils.generate_surrogate_key(['payout_id']) }} as payout_key,
        p.payout_id,
        format_date('%Y%m%d', date(p.created_at)) as date_key,
        dc.customer_key as recipient_customer_key,
        p.amount as payout_amount,
        p.currency,
        p.status as payout_status,
        p.scheduled_at,
        p.executed_at,
        p.created_at,
        current_timestamp() as dbt_loaded_at
    from payouts p
    left join {{ ref('dim_customers') }} dc
        on p.recipient_customer_id = dc.customer_id
)

select * from enrich_payouts