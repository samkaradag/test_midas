-- dagen:failed_job_id: 2590
select
    transaction_id,
    transaction_amount,
    completed_refund_amount,
    completed_refund_amount - transaction_amount as excess_refund_amount,
    'completed refunds exceed transaction amount' as failure_reason
from {{ ref('mart_denormalized_transactions_serving') }}
where completed_refund_amount > transaction_amount + 0.01
