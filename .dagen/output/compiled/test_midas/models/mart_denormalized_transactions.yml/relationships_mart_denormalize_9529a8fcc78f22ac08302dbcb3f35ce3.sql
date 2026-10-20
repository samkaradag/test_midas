
    
    

with child as (
    select payment_method_id as from_field
    from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
    where payment_method_id is not null
),

parent as (
    select payment_method_id as to_field
    from `prd-dagen`.`payments_v1`.`payment_methods`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


