
    
    

with child as (
    select recipient_customer_key as from_field
    from `prd-dagen`.`payments_v1_payments_v1`.`fact_payouts`
    where recipient_customer_key is not null
),

parent as (
    select customer_key as to_field
    from `prd-dagen`.`payments_v1_payments_v1`.`dim_customers`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


