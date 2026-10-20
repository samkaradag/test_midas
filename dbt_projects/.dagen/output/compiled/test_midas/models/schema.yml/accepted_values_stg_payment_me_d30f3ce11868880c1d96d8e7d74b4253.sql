
    
    

with all_values as (

    select
        method_type as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_payment_methods`
    group by method_type

)

select *
from all_values
where value_field not in (
    'card','bank_transfer','wallet','check'
)


