
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fee_count
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where fee_count is null



  
  
      
    ) dbt_internal_test