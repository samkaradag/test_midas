
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_method_id
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where payment_method_id is null



  
  
      
    ) dbt_internal_test