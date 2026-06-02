{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging', 'fraud', 'customer_features'],
    description='Deduplicated and enriched customer profiles with KYC status and risk scores'
) }}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

stg_disputes as (
    select * from {{ ref('stg_disputes') }}
),

customer_deduped as (
    select
        customer_id,
        customer_type,
        kyc_status,
        country,
        created_at,
        updated_at,
        row_number() over (partition by customer_id order by updated_at desc) as rn
    from stg_customers
    where customer_id is not null
),

customer_clean as (
    select
        customer_id,
        customer_type,
        kyc_status,
        country,
        created_at,
        updated_at
    from customer_deduped
    where rn = 1
),

customer_transaction_stats as (
    select
        t.debtor_customer_id as customer_id,
        count(distinct t.transaction_id) as total_transactions,
        sum(case when d.dispute_id is not null then 1 else 0 end) as dispute_count,
        count(distinct case when d.dispute_id is not null then d.dispute_id end) as unique_disputes,
        min(t.created_at) as first_transaction_date,
        max(t.created_at) as last_transaction_date,
        datediff(day, min(t.created_at), max(t.created_at)) as account_age_days,
        round(
            safe_divide(
                count(distinct case when d.dispute_id is not null then d.dispute_id end),
                count(distinct t.transaction_id)
            ) * 100,
            2
        ) as dispute_rate_pct
    from stg_transactions t
    left join stg_disputes d on t.transaction_id = d.transaction_id
    group by t.debtor_customer_id
),

customer_risk_segment as (
    select
        customer_id,
        dispute_rate_pct,
        case
            when dispute_rate_pct > 10 then 'high_risk'
            when dispute_rate_pct > 5 then 'medium_risk'
            else 'low_risk'
        end as risk_segment
    from customer_transaction_stats
),

final as (
    select
        cc.customer_id,
        cc.customer_type,
        cc.kyc_status,
        case when cc.kyc_status = 'VERIFIED' then 1 else 0 end as is_kyc_verified,
        cc.country,
        cts.total_transactions,
        cts.dispute_count,
        cts.unique_disputes,
        cts.dispute_rate_pct,
        cts.account_age_days,
        cts.first_transaction_date,
        cts.last_transaction_date,
        crs.risk_segment,
        cc.created_at as customer_created_at,
        cc.updated_at as customer_updated_at,
        current_timestamp() as dbt_created_at
    from customer_clean cc
    left join customer_transaction_stats cts on cc.customer_id = cts.customer_id
    left join customer_risk_segment crs on cc.customer_id = crs.customer_id
)

select * from final