
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    transaction_id,
    count(*) as row_count,
    'more than one target row per transaction' as failure_reason
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
group by transaction_id
having transaction_id is null or count(*) != 1
  
  
      
    ) dbt_internal_test