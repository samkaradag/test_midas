
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where currency is null



  
  
      
    ) dbt_internal_test