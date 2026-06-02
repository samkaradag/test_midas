{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fraud', 'payment_features'],
    description='Payment method profiles with age, type, and historical dispute rate for risk assessment'
) }}

with stg_payment_methods as (
    select * from {{ ref('stg_payment_methods') }}
),

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

stg_disputes as (
    select * from {{ ref('stg_disputes') }}
),

payment_method_deduped as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        created_at,
        updated_at,
        row_number() over (partition by payment_method_id order by updated_at desc) as rn
    from stg_payment_methods
    where payment_method_id is not null
),

payment_method_clean as (
    select
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        created_at,
        updated_at
    from payment_method_deduped
    where rn = 1
),

payment_method_transaction_stats as (
    select
        t.payment_method_id,
        count(distinct t.transaction_id) as total_transactions,
        sum(case when d.dispute_id is not null then 1 else 0 end) as dispute_count,
        count(distinct case when d.dispute_id is not null then d.dispute_id end) as unique_disputes,
        min(t.created_at) as first_transaction_date,
        max(t.created_at) as last_transaction_date,
        date_diff(max(t.created_at), min(t.created_at), day) as method_age_days,
        count(distinct t.debtor_customer_id) as unique_customers_used,
        round(
            safe_divide(
                count(distinct case when d.dispute_id is not null then d.dispute_id end),
                count(distinct t.transaction_id)
            ) * 100,
            2
        ) as dispute_rate_pct,
        round(
            safe_divide(
                sum(case when d.dispute_id is not null then t.amount else 0 end),
                sum(t.amount)
            ) * 100,
            2
        ) as disputed_amount_pct
    from stg_transactions t
    left join stg_disputes d on t.transaction_id = d.transaction_id
    where t.payment_method_id is not null
    group by t.payment_method_id
),

payment_method_risk_segment as (
    select
        payment_method_id,
        dispute_rate_pct,
        case
            when dispute_rate_pct > 15 then 'high_risk'
            when dispute_rate_pct > 8 then 'medium_risk'
            else 'low_risk'
        end as risk_segment
    from payment_method_transaction_stats
),

final as (
    select
        pmc.payment_method_id,
        pmc.customer_id,
        pmc.method_type,
        pmc.is_default,
        pmts.total_transactions,
        pmts.dispute_count,
        pmts.unique_disputes,
        pmts.dispute_rate_pct,
        pmts.disputed_amount_pct,
        pmts.method_age_days,
        pmts.unique_customers_used,
        pmts.first_transaction_date,
        pmts.last_transaction_date,
        pmrs.risk_segment,
        pmc.created_at as payment_method_created_at,
        pmc.updated_at as payment_method_updated_at,
        current_timestamp() as dbt_created_at
    from payment_method_clean pmc
    left join payment_method_transaction_stats pmts on pmc.payment_method_id = pmts.payment_method_id
    left join payment_method_risk_segment pmrs on pmc.payment_method_id = pmrs.payment_method_id
)

select * from final