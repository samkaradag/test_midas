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
    select count(*) as expected_count
    from ranked_source
    where version_rank = 1
      and _ab_cdc_deleted_at is null
),
serving as (
    select count(*) as serving_count
    from {{ ref('mart_denormalized_transactions_serving') }}
),
errors as (
    select count(*) as error_count
    from {{ ref('mart_denormalized_transactions_errors') }}
)
select
    expected_count,
    serving_count,
    error_count,
    serving_count + error_count as actual_count,
    serving_count + error_count - expected_count as count_difference,
    'serving plus errors count differs from latest current source transaction count' as failure_reason
from expected
cross join serving
cross join errors
where expected_count != serving_count + error_count
