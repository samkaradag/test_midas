select
    transaction_id,
    leg_count,
    debit_leg_count,
    credit_leg_count,
    total_debit_amount,
    total_credit_amount,
    ledger_net_amount,
    'ledger debit and credit totals are not balanced' as failure_reason
from {{ ref('mart_denormalized_transactions') }}
where leg_count > 0
  and abs(ledger_net_amount) > 0.01
