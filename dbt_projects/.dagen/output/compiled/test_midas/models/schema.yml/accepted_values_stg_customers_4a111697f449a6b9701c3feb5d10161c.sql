
    
    

with all_values as (

    select
        kyc_status as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_customers`
    group by kyc_status

)

select *
from all_values
where value_field not in (
    'VERIFIED','PENDING','REJECTED','UNKNOWN'
)


