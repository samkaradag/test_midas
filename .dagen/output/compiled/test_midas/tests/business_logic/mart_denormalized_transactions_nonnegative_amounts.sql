select
    transaction_id,
    transaction_amount,
    total_refund_amount,
    completed_refund_amount,
    total_dispute_amount,
    total_fee_amount,
    total_debit_amount,
    total_credit_amount,
    'negative financial amount' as failure_reason
from `prd-dagen`.payments_v1_payments_v1.`mart_denormalized_transactions`
where transaction_amount < 0
   or total_refund_amount < 0
   or completed_refund_amount < 0
   or total_dispute_amount < 0
   or total_fee_amount < 0
   or total_debit_amount < 0
   or total_credit_amount < 0