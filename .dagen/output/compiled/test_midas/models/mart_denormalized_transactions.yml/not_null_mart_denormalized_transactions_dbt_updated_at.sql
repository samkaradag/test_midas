
    
    



select dbt_updated_at
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where dbt_updated_at is null


