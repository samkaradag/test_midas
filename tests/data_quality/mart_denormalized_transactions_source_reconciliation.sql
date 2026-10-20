-- dagen:failed_job_id: 2590
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
    from {{ source('raw_data', 'transactions') }}
    where transaction_id is not null
),
expected as (
    select transaction_id
    from ranked_source
    where version_rank = 1
      and _ab_cdc_deleted_at is null
),
actual as (
    select transaction_id from {{ ref('mart_denormalized_transactions_serving') }}
    union all
    select transaction_id from {{ ref('mart_denormalized_transactions_errors') }}
),
reconciled as (
    select
        coalesce(expected.transaction_id, actual.transaction_id) as transaction_id,
        expected.transaction_id is null as unexpected_output_row,
        actual.transaction_id is null as missing_output_row
    from expected
    full outer join actual using (transaction_id)
)
select
    transaction_id,
    unexpected_output_row,
    missing_output_row,
    'serving plus errors do not reconcile to latest current source keys' as failure_reason
from reconciled
where unexpected_output_row or missing_output_row
