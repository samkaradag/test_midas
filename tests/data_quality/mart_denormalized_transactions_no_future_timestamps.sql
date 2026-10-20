select
    transaction_id,
    transaction_created_at,
    transaction_updated_at,
    latest_refund_at,
    latest_dispute_at,
    latest_fee_at,
    'future timestamp detected' as failure_reason
from {{ ref('mart_denormalized_transactions') }}
where transaction_created_at > current_timestamp()
   or transaction_updated_at > current_timestamp()
   or latest_refund_at > current_timestamp()
   or latest_dispute_at > current_timestamp()
   or latest_fee_at > current_timestamp()
