
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with ranked_source as (
    select
        transaction_id,
        _ab_cdc_deleted_at,
        row_number() over (
            partition by transaction_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from `prd-dagen`.`payments_v1`.`transactions`
    where transaction_id is not null
),
expected as (
    select count(*) as expected_count
    from ranked_source
    where version_rank = 1
      and _ab_cdc_deleted_at is null
),
actual as (
    select count(*) as actual_count
    from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
)
select
    expected_count,
    actual_count,
    actual_count - expected_count as count_difference,
    'target row count differs from latest current source transaction count' as failure_reason
from expected
cross join actual
where expected_count != actual_count
  
  
      
    ) dbt_internal_test