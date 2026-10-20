select
    transaction_id,
    is_nonnegative_transaction_amount,
    has_distinct_parties,
    is_valid_transaction_status,
    is_refund_currency_valid,
    is_dispute_currency_valid,
    is_fee_currency_valid,
    is_leg_currency_valid,
    is_refund_within_transaction_amount,
    'one or more transaction business validation flags failed' as failure_reason
from {{ ref('mart_denormalized_transactions') }}
where not coalesce(is_nonnegative_transaction_amount, false)
   or not coalesce(has_distinct_parties, false)
   or not coalesce(is_valid_transaction_status, false)
   or not coalesce(is_refund_currency_valid, false)
   or not coalesce(is_dispute_currency_valid, false)
   or not coalesce(is_fee_currency_valid, false)
   or not coalesce(is_leg_currency_valid, false)
   or not coalesce(is_refund_within_transaction_amount, false)
