-- dagen:failed_job_id: 2590
select
    transaction_id,
    rejection_reasons,
    'rejected transaction must have at least one durable reason' as failure_reason
from {{ ref('mart_denormalized_transactions_errors') }}
where rejection_reasons is null or trim(rejection_reasons) = ''
