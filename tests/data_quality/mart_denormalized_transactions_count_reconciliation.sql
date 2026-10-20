with expected as (
    select count(distinct transaction_id) as expected_count
    from {{ source('raw_data', 'transactions') }}
    where _ab_cdc_deleted_at is null
      and transaction_id is not null
),
actual as (
    select count(*) as actual_count
    from {{ ref('mart_denormalized_transactions') }}
)
select
    expected_count,
    actual_count,
    actual_count - expected_count as count_difference,
    'target row count differs from deduplicated current source transaction count' as failure_reason
from expected
cross join actual
where expected_count != actual_count
