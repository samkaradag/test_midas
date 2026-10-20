
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    transaction_id,
    transaction_amount,
    completed_refund_amount,
    completed_refund_amount - transaction_amount as excess_refund_amount,
    'completed refunds exceed transaction amount' as failure_reason
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where completed_refund_amount > transaction_amount + 0.01
  
  
      
    ) dbt_internal_test