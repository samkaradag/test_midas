
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_amount
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where transaction_amount is null



  
  
      
    ) dbt_internal_test