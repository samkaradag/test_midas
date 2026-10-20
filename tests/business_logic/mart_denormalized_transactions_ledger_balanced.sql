select
    transaction_id,
    leg_count,
    debit_leg_count,
    credit_leg_count,
    has_ledger,
    is_leg_count_consistent,
    is_ledger_balanced,
    total_debit_amount,
    total_credit_amount,
    ledger_net_amount,
    case
        when not has_ledger then 'transaction has no current ledger'
        when leg_count != debit_leg_count + credit_leg_count then 'ledger contains unclassified directions or inconsistent counts'
        else 'ledger debit and credit totals are not balanced'
    end as failure_reason
from {{ ref('mart_denormalized_transactions') }}
where not has_ledger
   or not is_leg_count_consistent
   or not is_ledger_balanced
   or leg_count != debit_leg_count + credit_leg_count
   or abs(ledger_net_amount) > 0.01
