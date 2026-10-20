
    
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
    select transaction_id
    from ranked_source
    where version_rank = 1
      and _ab_cdc_deleted_at is null
),
actual as (
    select transaction_id
    from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
),
reconciled as (
    select
        coalesce(expected.transaction_id, actual.transaction_id) as transaction_id,
        expected.transaction_id is null as unexpected_target_row,
        actual.transaction_id is null as missing_target_row
    from expected
    full outer join actual using (transaction_id)
)
select
    transaction_id,
    unexpected_target_row,
    missing_target_row,
    'target transaction keys do not reconcile to latest current source keys' as failure_reason
from reconciled
where unexpected_target_row or missing_target_row
  
  
      
    ) dbt_internal_test