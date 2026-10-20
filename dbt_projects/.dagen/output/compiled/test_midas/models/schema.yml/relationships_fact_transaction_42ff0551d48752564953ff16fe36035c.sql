
    
    

with child as (
    select transaction_key as from_field
    from `prd-dagen`.`payments_v1_payments_v1`.`fact_transaction_details`
    where transaction_key is not null
),

parent as (
    select transaction_key as to_field
    from `prd-dagen`.`payments_v1_payments_v1`.`fact_transactions`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


