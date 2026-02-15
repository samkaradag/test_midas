-- Data Quality Tests for Payment Data Warehouse
-- Comprehensive tests to validate data integrity across all models

-- ============================================================================
-- STAGING MODEL TESTS
-- ============================================================================

-- Test 1: Verify stg_customers has no duplicates
-- Expected: Each customer_id should appear exactly once
select
    customer_id,
    count(*) as count
from {{ ref('stg_customers') }}
group by customer_id
having count(*) > 1

-- Test 2: Verify stg_customers has no test data
-- Expected: No records with customer_type = 'samet'
select *
from {{ ref('stg_customers') }}
where customer_type = 'samet'

-- Test 3: Verify stg_customers KYC status is standardized
-- Expected: Only VERIFIED, PENDING, REJECTED, UNKNOWN values
select distinct kyc_status
from {{ ref('stg_customers') }}
where kyc_status not in ('VERIFIED', 'PENDING', 'REJECTED', 'UNKNOWN')

-- Test 4: Verify stg_payment_methods consolidation
-- Expected: No 'credit_card' values (should be 'card')
select *
from {{ ref('stg_payment_methods') }}
where method_type = 'credit_card'

-- Test 5: Verify stg_transactions has no duplicates
-- Expected: Each transaction_id should appear exactly once
select
    transaction_id,
    count(*) as count
from {{ ref('stg_transactions') }}
group by transaction_id
having count(*) > 1

-- Test 6: Verify stg_transactions debtor ≠ creditor
-- Expected: No transactions where debtor_customer_id = creditor_customer_id
select *
from {{ ref('stg_transactions') }}
where debtor_customer_id = creditor_customer_id

-- Test 7: CRITICAL - Verify stg_transaction_legs has exactly 2 legs per transaction
-- Expected: Each transaction_id should have exactly 2 rows (1 DEBIT, 1 CREDIT)
select
    transaction_id,
    count(*) as leg_count
from {{ ref('stg_transaction_legs') }}
group by transaction_id
having count(*) != 2

-- Test 8: CRITICAL - Verify stg_transaction_legs double-entry accounting
-- Expected: For each transaction, sum(DEBIT) = sum(CREDIT)
select
    transaction_id,
    total_debit,
    total_credit,
    abs(total_debit - total_credit) as difference
from {{ ref('stg_transaction_legs') }}
where abs(total_debit - total_credit) > 0.01

-- Test 9: Verify stg_transaction_legs all balanced
-- Expected: is_balanced should always be true
select *
from {{ ref('stg_transaction_legs') }}
where is_balanced = false

-- Test 10: Verify stg_refunds has no duplicates
-- Expected: Each refund_id should appear exactly once
select
    refund_id,
    count(*) as count
from {{ ref('stg_refunds') }}
group by refund_id
having count(*) > 1

-- Test 11: Verify stg_disputes has no duplicates
-- Expected: Each dispute_id should appear exactly once
select
    dispute_id,
    count(*) as count
from {{ ref('stg_disputes') }}
group by dispute_id
having count(*) > 1

-- Test 12: Verify stg_disputes has no test data
-- Expected: No disputes with reason = 'Test Dispute'
select *
from {{ ref('stg_disputes') }}
where reason = 'Test Dispute'

-- Test 13: Verify stg_fees has no duplicates
-- Expected: Each fee_id should appear exactly once
select
    fee_id,
    count(*) as count
from {{ ref('stg_fees') }}
group by fee_id
having count(*) > 1

-- Test 14: Verify stg_fees has no test data
-- Expected: No fees with fee_type = 'Test Fee'
select *
from {{ ref('stg_fees') }}
where fee_type = 'Test Fee'

-- Test 15: Verify stg_mandates has no duplicates
-- Expected: Each mandate_id should appear exactly once
select
    mandate_id,
    count(*) as count
from {{ ref('stg_mandates') }}
group by mandate_id
having count(*) > 1

-- Test 16: Verify stg_payouts has no duplicates
-- Expected: Each payout_id should appear exactly once
select
    payout_id,
    count(*) as count
from {{ ref('stg_payouts') }}
group by payout_id
having count(*) > 1

-- ============================================================================
-- DIMENSION MODEL TESTS
-- ============================================================================

-- Test 17: Verify dim_customers has no duplicate surrogate keys
-- Expected: Each customer_key should appear exactly once
select
    customer_key,
    count(*) as count
from {{ ref('dim_customers') }}
group by customer_key
having count(*) > 1

-- Test 18: Verify dim_customers has no duplicate natural keys
-- Expected: Each customer_id should appear exactly once
select
    customer_id,
    count(*) as count
from {{ ref('dim_customers') }}
group by customer_id
having count(*) > 1

-- Test 19: Verify dim_payment_methods has no duplicate surrogate keys
-- Expected: Each payment_method_key should appear exactly once
select
    payment_method_key,
    count(*) as count
from {{ ref('dim_payment_methods') }}
group by payment_method_key
having count(*) > 1

-- Test 20: Verify dim_payment_methods customer FK is valid
-- Expected: All customer_keys should exist in dim_customers
select
    dpm.payment_method_key,
    dpm.customer_key
from {{ ref('dim_payment_methods') }} dpm
left join {{ ref('dim_customers') }} dc
    on dpm.customer_key = dc.customer_key
where dc.customer_key is null

-- Test 21: Verify dim_date has no duplicate date keys
-- Expected: Each date_key should appear exactly once
select
    date_key,
    count(*) as count
from {{ ref('dim_date') }}
group by date_key
having count(*) > 1

-- Test 22: Verify dim_date has no duplicate calendar dates
-- Expected: Each calendar_date should appear exactly once
select
    calendar_date,
    count(*) as count
from {{ ref('dim_date') }}
group by calendar_date
having count(*) > 1

-- ============================================================================
-- FACT MODEL TESTS
-- ============================================================================

-- Test 23: Verify fact_transactions has no duplicate surrogate keys
-- Expected: Each transaction_key should appear exactly once
select
    transaction_key,
    count(*) as count
from {{ ref('fact_transactions') }}
group by transaction_key
having count(*) > 1

-- Test 24: Verify fact_transactions has no duplicate natural keys
-- Expected: Each transaction_id should appear exactly once
select
    transaction_id,
    count(*) as count
from {{ ref('fact_transactions') }}
group by transaction_id
having count(*) > 1

-- Test 25: Verify fact_transactions date_key FK is valid
-- Expected: All date_keys should exist in dim_date
select
    ft.transaction_key,
    ft.date_key
from {{ ref('fact_transactions') }} ft
left join {{ ref('dim_date') }} dd
    on ft.date_key = dd.date_key
where dd.date_key is null

-- Test 26: Verify fact_transactions debtor_customer_key FK is valid
-- Expected: All customer_keys should exist in dim_customers (allow NULL)
select
    ft.transaction_key,
    ft.debtor_customer_key
from {{ ref('fact_transactions') }} ft
left join {{ ref('dim_customers') }} dc
    on ft.debtor_customer_key = dc.customer_key
where ft.debtor_customer_key is not null
  and dc.customer_key is null

-- Test 27: Verify fact_transactions creditor_customer_key FK is valid
-- Expected: All customer_keys should exist in dim_customers (allow NULL)
select
    ft.transaction_key,
    ft.creditor_customer_key
from {{ ref('fact_transactions') }} ft
left join {{ ref('dim_customers') }} dc
    on ft.creditor_customer_key = dc.customer_key
where ft.creditor_customer_key is not null
  and dc.customer_key is null

-- Test 28: Verify fact_transactions payment_method_key FK is valid
-- Expected: All payment_method_keys should exist in dim_payment_methods (allow NULL)
select
    ft.transaction_key,
    ft.payment_method_key
from {{ ref('fact_transactions') }} ft
left join {{ ref('dim_payment_methods') }} dpm
    on ft.payment_method_key = dpm.payment_method_key
where ft.payment_method_key is not null
  and dpm.payment_method_key is null

-- Test 29: Verify fact_transaction_details has no duplicate keys
-- Expected: Each transaction_key should appear exactly once
select
    transaction_key,
    count(*) as count
from {{ ref('fact_transaction_details') }}
group by transaction_key
having count(*) > 1

-- Test 30: Verify fact_transaction_details transaction_key FK is valid
-- Expected: All transaction_keys should exist in fact_transactions
select
    ftd.transaction_key
from {{ ref('fact_transaction_details') }} ftd
left join {{ ref('fact_transactions') }} ft
    on ftd.transaction_key = ft.transaction_key
where ft.transaction_key is null

-- Test 31: Verify fact_transaction_details net amount calculation
-- Expected: net_amount = transaction_amount - refund_amount - fee_amount
select
    transaction_key,
    transaction_amount,
    refund_amount,
    fee_amount,
    net_transaction_amount,
    (transaction_amount - refund_amount - fee_amount) as calculated_net,
    abs(net_transaction_amount - (transaction_amount - refund_amount - fee_amount)) as difference
from {{ ref('fact_transaction_details') }}
where abs(net_transaction_amount - (transaction_amount - refund_amount - fee_amount)) > 0.01

-- Test 32: Verify fact_payouts has no duplicate surrogate keys
-- Expected: Each payout_key should appear exactly once
select
    payout_key,
    count(*) as count
from {{ ref('fact_payouts') }}
group by payout_key
having count(*) > 1

-- Test 33: Verify fact_payouts has no duplicate natural keys
-- Expected: Each payout_id should appear exactly once
select
    payout_id,
    count(*) as count
from {{ ref('fact_payouts') }}
group by payout_id
having count(*) > 1

-- Test 34: Verify fact_payouts date_key FK is valid
-- Expected: All date_keys should exist in dim_date
select
    fp.payout_key,
    fp.date_key
from {{ ref('fact_payouts') }} fp
left join {{ ref('dim_date') }} dd
    on fp.date_key = dd.date_key
where dd.date_key is null

-- Test 35: Verify fact_payouts recipient_customer_key FK is valid
-- Expected: All customer_keys should exist in dim_customers (allow NULL)
select
    fp.payout_key,
    fp.recipient_customer_key
from {{ ref('fact_payouts') }} fp
left join {{ ref('dim_customers') }} dc
    on fp.recipient_customer_key = dc.customer_key
where fp.recipient_customer_key is not null
  and dc.customer_key is null

-- ============================================================================
-- CROSS-MODEL CONSISTENCY TESTS
-- ============================================================================

-- Test 36: Verify all stg_customers are in dim_customers
-- Expected: All customer_ids from stg_customers should be in dim_customers
select
    sc.customer_id
from {{ ref('stg_customers') }} sc
left join {{ ref('dim_customers') }} dc
    on sc.customer_id = dc.customer_id
where dc.customer_id is null

-- Test 37: Verify all stg_payment_methods are in dim_payment_methods
-- Expected: All payment_method_ids from stg_payment_methods should be in dim_payment_methods
select
    spm.payment_method_id
from {{ ref('stg_payment_methods') }} spm
left join {{ ref('dim_payment_methods') }} dpm
    on spm.payment_method_id = dpm.payment_method_id
where dpm.payment_method_id is null

-- Test 38: Verify all stg_transactions are in fact_transactions
-- Expected: All transaction_ids from stg_transactions should be in fact_transactions
select
    st.transaction_id
from {{ ref('stg_transactions') }} st
left join {{ ref('fact_transactions') }} ft
    on st.transaction_id = ft.transaction_id
where ft.transaction_id is null

-- Test 39: Verify all stg_payouts are in fact_payouts
-- Expected: All payout_ids from stg_payouts should be in fact_payouts
select
    sp.payout_id
from {{ ref('stg_payouts') }} sp
left join {{ ref('fact_payouts') }} fp
    on sp.payout_id = fp.payout_id
where fp.payout_id is null

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

-- Test 40: Row count summary
-- Expected: Verify expected row counts for each model
select
    'stg_customers' as model,
    count(*) as row_count,
    34 as expected_count
from {{ ref('stg_customers') }}
union all
select 'stg_payment_methods', count(*), 21 from {{ ref('stg_payment_methods') }}
union all
select 'stg_transactions', count(*), 21 from {{ ref('stg_transactions') }}
union all
select 'stg_transaction_legs', count(*), 42 from {{ ref('stg_transaction_legs') }}
union all
select 'stg_refunds', count(*), 12 from {{ ref('stg_refunds') }}
union all
select 'stg_disputes', count(*), 12 from {{ ref('stg_disputes') }}
union all
select 'stg_fees', count(*), 21 from {{ ref('stg_fees') }}
union all
select 'stg_mandates', count(*), 21 from {{ ref('stg_mandates') }}
union all
select 'stg_payouts', count(*), 21 from {{ ref('stg_payouts') }}
union all
select 'dim_customers', count(*), 34 from {{ ref('dim_customers') }}
union all
select 'dim_payment_methods', count(*), 21 from {{ ref('dim_payment_methods') }}
union all
select 'fact_transactions', count(*), 21 from {{ ref('fact_transactions') }}
union all
select 'fact_transaction_details', count(*), 21 from {{ ref('fact_transaction_details') }}
union all
select 'fact_payouts', count(*), 21 from {{ ref('fact_payouts') }}
order by model