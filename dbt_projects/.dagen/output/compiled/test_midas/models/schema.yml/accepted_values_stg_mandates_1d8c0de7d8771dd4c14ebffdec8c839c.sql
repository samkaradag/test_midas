
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_mandates`
    group by status

)

select *
from all_values
where value_field not in (
    'ACTIVE','INACTIVE','EXPIRED','PENDING','UNKNOWN'
)


