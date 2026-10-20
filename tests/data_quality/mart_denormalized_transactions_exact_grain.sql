-- dagen:failed_job_id: 2590
select
    transaction_id,
    count(*) as row_count,
    'more than one target row per transaction' as failure_reason
from {{ ref('mart_denormalized_transactions_serving') }}
group by transaction_id
having transaction_id is null or count(*) != 1
