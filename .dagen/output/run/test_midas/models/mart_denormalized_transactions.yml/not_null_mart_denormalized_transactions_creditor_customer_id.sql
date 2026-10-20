
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select creditor_customer_id
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where creditor_customer_id is null



  
  
      
    ) dbt_internal_test