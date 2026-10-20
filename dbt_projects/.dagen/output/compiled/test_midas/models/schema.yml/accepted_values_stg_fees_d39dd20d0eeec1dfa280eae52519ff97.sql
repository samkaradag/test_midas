
    
    

with all_values as (

    select
        fee_type as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_fees`
    group by fee_type

)

select *
from all_values
where value_field not in (
    'TRANSACTION_FEE','SERVICE_FEE','PROCESSING_FEE','MONTHLY_FEE','PENALTY_FEE'
)


