-- dagen:failed_job_id: 2590
{{ config(
    materialized='table',
    alias='mart_denormalized_transactions',
    tags=['mart', 'transactions', 'daily', 'business_validation', 'serving']
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
)
select *
from classified
where has_nonnegative_financial_amounts
  and has_valid_timestamps
  and coalesce(has_distinct_parties, false)
  and coalesce(is_valid_transaction_status, false)
  and coalesce(is_refund_currency_valid, false)
  and coalesce(is_dispute_currency_valid, false)
  and coalesce(is_fee_currency_valid, false)
  and coalesce(is_leg_currency_valid, false)
  and coalesce(is_refund_within_transaction_amount, false)
  and coalesce(has_ledger, false)
  and coalesce(is_leg_count_consistent, false)
  and coalesce(is_ledger_balanced, false)
