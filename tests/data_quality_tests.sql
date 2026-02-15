-- Data Quality Tests for Payment Data Warehouse
-- Comprehensive SQL-based tests to validate data integrity across all models

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

-- Test 2: Verify stg_payment_methods consolidation
-- Expected: No 'credit_card' values (should be 'card')
select *
from {{ ref('stg_payment_methods') }}
where method_type = 'credit_card'

-- Test 3: Verify stg_transactions has no duplicates
-- Expected: Each transaction_id should appear exactly once
select
    transaction_id,
    count(*) as count
from {{ ref('stg_transactions') }}
group by transaction_id
having count(*) > 1

-- Test 4: Verify stg_transactions debtor ≠ creditor
-- Expected: No transactions where debtor_customer_id = creditor_customer_id
select *
from {{ ref('stg_transactions') }}
where debtor_customer_id = creditor_customer_id

-- Test 5: Verify stg_refunds has no duplicates
-- Expected: Each refund_id should appear exactly once
select
    refund_id,
    count(*) as count
from {{ ref('stg_refunds') }}
group by refund_id
having count(*) > 1

-- Test 6: Verify stg_disputes has no duplicates
-- Expected: Each dispute_id should appear exactly once
select
    dispute_id,
    count(*) as count
from {{ ref('stg_disputes') }}
group by dispute_id
having count(*) > 1

-- Test 7: Verify stg_fees has no duplicates
-- Expected: Each fee_id should appear exactly once
select
    fee_id,
    count(*) as count
from {{ ref('stg_fees') }}
group by fee_id
having count(*) > 1

-- Test 8: Verify stg_mandates has no duplicates
-- Expected: Each mandate_id should appear exactly once
select
    mandate_id,
    count(*) as count
from {{ ref('stg_mandates') }}
group by mandate_id
having count(*) > 1

-- Test 9: Verify stg_payouts has no duplicates
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

-- Test 10: Verify dim_customers has no duplicate surrogate keys
-- Expected: Each customer_key should appear exactly once
select
    customer_key,
    count(*) as count
from {{ ref('dim_customers') }}
group by customer_key
having count(*) > 1

-- Test 11: Verify dim_customers has no duplicate natural keys
-- Expected: Each customer_id should appear exactly once
select
    customer_id,
    count(*) as count
from {{ ref('dim_customers') }}
group by customer_id
having count(*) > 1

-- Test 12: Verify dim_payment_methods has no duplicate surrogate keys
-- Expected: Each payment_method_key should appear exactly once
select
    payment_method_key,
    count(*) as count
from {{ ref('dim_payment_methods') }}
group by payment_method_key
having count(*) > 1

-- Test 13: Verify dim_date has no duplicate date keys
-- Expected: Each date_key should appear exactly once
select
    date_key,
    count(*) as count
from {{ ref('dim_date') }}
group by date_key
having count(*) > 1

-- ============================================================================
-- FACT MODEL TESTS
-- ============================================================================

-- Test 14: Verify fact_transactions has no duplicate surrogate keys
-- Expected: Each transaction_key should appear exactly once
select
    transaction_key,
    count(*) as count
from {{ ref('fact_transactions') }}
group by transaction_key
having count(*) > 1

-- Test 15: Verify fact_transactions has no duplicate natural keys
-- Expected: Each transaction_id should appear exactly once
select
    transaction_id,
    count(*) as count
from {{ ref('fact_transactions') }}
group by transaction_id
having count(*) > 1

-- Test 16: Verify fact_transaction_details has no duplicate keys
-- Expected: Each transaction_key should appear exactly once
select
    transaction_key,
    count(*) as count
from {{ ref('fact_transaction_details') }}
group by transaction_key
having count(*) > 1

-- Test 17: Verify fact_payouts has no duplicate surrogate keys
-- Expected: Each payout_key should appear exactly once
select
    payout_key,
    count(*) as count
from {{ ref('fact_payouts') }}
group by payout_key
having count(*) > 1

-- Test 18: Verify fact_payouts has no duplicate natural keys
-- Expected: Each payout_id should appear exactly once
select
    payout_id,
    count(*) as count
from {{ ref('fact_payouts') }}
group by payout_id
having count(*) > 1