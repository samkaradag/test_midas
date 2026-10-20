-- dagen:failed_job_id: 2590
{{ config(
    materialized='table',
    alias='mart_denormalized_transactions_errors',
    tags=['mart', 'transactions', 'daily', 'business_validation', 'errors']
) }}

with classified as (
    select
        all_rows.*,
        all_rows.transaction_amount >= 0
          and all_rows.total_refund_amount >= 0
          and all_rows.completed_refund_amount >= 0
          and all_rows.total_dispute_amount >= 0
          and all_rows.total_fee_amount >= 0
          and all_rows.total_debit_amount >= 0
          and all_rows.total_credit_amount >= 0 as has_nonnegative_financial_amounts,
        coalesce(all_rows.transaction_created_at <= current_timestamp(), false)
          and coalesce(all_rows.transaction_updated_at <= current_timestamp(), true)
          and coalesce(all_rows.latest_refund_at <= current_timestamp(), true)
          and coalesce(all_rows.latest_dispute_at <= current_timestamp(), true)
          and coalesce(all_rows.latest_fee_at <= current_timestamp(), true) as has_valid_timestamps
    from {{ ref('mart_denormalized_transactions') }} all_rows
),
with_reasons as (
    select
        classified.*,
        array_to_string(
            array(
                select reason
                from unnest([
                    if(not has_nonnegative_financial_amounts, 'NEGATIVE_FINANCIAL_AMOUNT', null),
                    if(not has_valid_timestamps, 'FUTURE_TIMESTAMP', null),
                    if(not coalesce(has_distinct_parties, false), 'SAME_DEBTOR_AND_CREDITOR', null),
                    if(not coalesce(is_valid_transaction_status, false), 'INVALID_TRANSACTION_STATUS', null),
                    if(not coalesce(is_refund_currency_valid, false), 'REFUND_CURRENCY_MISMATCH', null),
                    if(not coalesce(is_dispute_currency_valid, false), 'DISPUTE_CURRENCY_MISMATCH', null),
                    if(not coalesce(is_fee_currency_valid, false), 'FEE_CURRENCY_MISMATCH', null),
                    if(not coalesce(is_leg_currency_valid, false), 'LEG_CURRENCY_MISMATCH', null),
                    if(not coalesce(is_refund_within_transaction_amount, false), 'REFUND_EXCEEDS_TRANSACTION', null),
                    if(not coalesce(has_ledger, false), 'MISSING_LEDGER', null),
                    if(not coalesce(is_leg_count_consistent, false), 'INCONSISTENT_LEG_COUNT', null),
                    if(not coalesce(is_ledger_balanced, false), 'UNBALANCED_LEDGER', null)
                ]) reason
                where reason is not null
            ),
            '|'
        ) as rejection_reasons,
        current_timestamp() as rejected_at
    from classified
)
select *
from with_reasons
where rejection_reasons != ''
