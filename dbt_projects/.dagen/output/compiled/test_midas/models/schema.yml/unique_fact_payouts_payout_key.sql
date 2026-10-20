
    
    

with dbt_test__target as (

  select payout_key as unique_field
  from `prd-dagen`.`payments_v1_payments_v1`.`fact_payouts`
  where payout_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


