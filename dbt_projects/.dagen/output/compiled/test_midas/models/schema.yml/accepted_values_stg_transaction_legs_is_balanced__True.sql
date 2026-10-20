
    
    

with all_values as (

    select
        is_balanced as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_transaction_legs`
    group by is_balanced

)

select *
from all_values
where value_field not in (
    'True'
)


