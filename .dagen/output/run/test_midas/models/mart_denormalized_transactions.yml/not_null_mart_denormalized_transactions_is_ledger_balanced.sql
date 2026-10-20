
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_ledger_balanced
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where is_ledger_balanced is null



  
  
      
    ) dbt_internal_test