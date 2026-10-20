
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_leg_count_consistent
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where is_leg_count_consistent is null



  
  
      
    ) dbt_internal_test