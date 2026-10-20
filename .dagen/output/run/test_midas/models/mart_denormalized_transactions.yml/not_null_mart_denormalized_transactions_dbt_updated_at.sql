
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dbt_updated_at
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where dbt_updated_at is null



  
  
      
    ) dbt_internal_test