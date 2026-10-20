
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    transaction_id,
    transaction_created_at,
    transaction_updated_at,
    latest_refund_at,
    latest_dispute_at,
    latest_fee_at,
    'future timestamp detected' as failure_reason
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where transaction_created_at > current_timestamp()
   or transaction_updated_at > current_timestamp()
   or latest_refund_at > current_timestamp()
   or latest_dispute_at > current_timestamp()
   or latest_fee_at > current_timestamp()
  
  
      
    ) dbt_internal_test