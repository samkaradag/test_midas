
  
    

    create or replace table `prd-dagen`.`payments_v1_payments_v1`.`fact_payouts`
      
    
    

    OPTIONS()
    as (
      -- fact_payouts.sql
-- Purpose: Payout fact table for star schema
-- Fact table for payouts with FKs to dimensions
-- Input: stg_payouts, dim_customers, dim_date
-- Output: payout facts with dimensional keys



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
    from `prd-dagen`.`payments_v1_payments_v1`.`stg_payouts`
),

-- Join with dimensions
enrich_payouts as (
    select
        md5(payout_id) as payout_key,
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
    left join `prd-dagen`.`payments_v1_payments_v1`.`dim_customers` dc
        on p.recipient_customer_id = dc.customer_id
)

select * from enrich_payouts
    );
  