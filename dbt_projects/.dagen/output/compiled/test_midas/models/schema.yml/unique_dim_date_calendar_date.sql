
    
    

with dbt_test__target as (

  select calendar_date as unique_field
  from `prd-dagen`.`payments_v1_payments_v1`.`dim_date`
  where calendar_date is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


