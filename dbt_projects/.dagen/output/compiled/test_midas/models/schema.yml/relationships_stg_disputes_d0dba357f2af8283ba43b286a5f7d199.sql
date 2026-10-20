
    
    

with child as (
    select transaction_id as from_field
    from `prd-dagen`.`payments_v1_payments_v1`.`stg_disputes`
    where transaction_id is not null
),

parent as (
    select transaction_id as to_field
    from `prd-dagen`.`payments_v1_payments_v1`.`stg_transactions`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


