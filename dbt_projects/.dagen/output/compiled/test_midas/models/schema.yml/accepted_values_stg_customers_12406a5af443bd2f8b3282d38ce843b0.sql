
    
    

with all_values as (

    select
        customer_type as value_field,
        count(*) as n_records

    from `prd-dagen`.`payments_v1_payments_v1`.`stg_customers`
    group by customer_type

)

select *
from all_values
where value_field not in (
    'individual','business'
)


