# SQL Examples & Key Patterns

This document provides detailed SQL examples and patterns used in the test_midas project.

---

## 🎯 Core Patterns

### 1. Deduplication Pattern

**Use Case**: Remove duplicate records, keeping the first by a specified order

```sql
with source_data as (
  select
    customer_id,
    customer_type,
    email,
    created_at,
    row_number() over (partition by customer_id order by created_at) as rn
  from raw_customers
),

deduplicated as (
  select
    customer_id,
    customer_type,
    email,
    created_at
  from source_data
  where rn = 1  -- Keep only the first occurrence
)

select * from deduplicated
```

**Key Points**:
- `ROW_NUMBER()` assigns sequential numbers to rows within each partition
- `PARTITION BY customer_id` groups rows by the key field
- `ORDER BY created_at` determines the order (first record kept)
- `WHERE rn = 1` filters to keep only the first occurrence

**Applied In**:
- stg_customers, stg_payment_methods, stg_transactions, stg_refunds, stg_disputes, stg_fees, stg_mandates, stg_payouts

---

### 2. Categorical Standardization Pattern

**Use Case**: Map multiple values to a single standardized value

```sql
with source_data as (
  select
    customer_id,
    kyc_status,
    created_at
  from raw_customers
),

standardized as (
  select
    customer_id,
    case 
      when kyc_status in ('ok', 'done', 'yes') then 'VERIFIED'
      when kyc_status is null then 'UNKNOWN'
      else upper(kyc_status)
    end as kyc_status,
    created_at
  from source_data
)

select * from standardized
```

**Key Points**:
- Use `CASE WHEN` for conditional logic
- Group related values together with `IN`
- Handle nulls explicitly
- Use `UPPER()` for consistent casing

**Applied In**:
- stg_customers (KYC status)
- stg_payment_methods (payment method types)

---

### 3. Foreign Key Validation Pattern

**Use Case**: Validate that referenced records exist in parent table

```sql
with refunds as (
  select
    refund_id,
    original_transaction_id,
    amount
  from staging_refunds
),

-- Validate foreign keys
with_fk_validation as (
  select
    r.refund_id,
    r.original_transaction_id,
    r.amount,
    case when t.transaction_id is not null then true else false end as fk_valid
  from refunds r
  left join stg_transactions t 
    on r.original_transaction_id = t.transaction_id
)

select
  refund_id,
  original_transaction_id,
  amount
from with_fk_validation
where fk_valid = true  -- Only include valid references
```

**Key Points**:
- Use `LEFT JOIN` to check for existence
- Create a validation flag with `CASE WHEN`
- Filter to valid records only
- Eliminates orphaned records

**Applied In**:
- stg_refunds (validates transaction_id)
- stg_disputes (validates transaction_id)
- stg_fees (validates transaction_id)
- stg_mandates (validates customer_id)
- stg_payouts (validates customer_id)

---

### 4. Aggregation Pattern

**Use Case**: Aggregate related records into a summary

```sql
with transactions as (
  select transaction_id, amount from stg_transactions
),

refunds_agg as (
  select
    original_transaction_id as transaction_id,
    sum(amount) as refund_amount,
    count(*) as refund_count
  from stg_refunds
  group by original_transaction_id
),

combined as (
  select
    t.transaction_id,
    coalesce(r.refund_amount, 0) as refund_amount,
    coalesce(r.refund_count, 0) as refund_count,
    t.amount - coalesce(r.refund_amount, 0) as net_amount
  from transactions t
  left join refunds_agg r on t.transaction_id = r.transaction_id
)

select * from combined
```

**Key Points**:
- Use `GROUP BY` to aggregate
- Use `SUM()` for totals and `COUNT()` for counts
- Use `COALESCE()` to handle nulls (missing refunds)
- Calculate derived fields (net_amount)

**Applied In**:
- fact_transaction_details (aggregates refunds, disputes, fees)

---

### 5. Surrogate Key Generation Pattern

**Use Case**: Create a surrogate key using MD5 hash

```sql
with customers as (
  select
    customer_id,
    customer_type,
    email
  from stg_customers
),

with_surrogate_key as (
  select
    md5(customer_id) as customer_key,  -- Surrogate key
    customer_id,                        -- Natural key
    customer_type,
    email
  from customers
)

select * from with_surrogate_key
```

**Key Points**:
- `MD5()` creates a consistent hash of the natural key
- Surrogate key enables efficient joins
- Keep natural key for auditability
- Hash is deterministic (same input = same hash)

**Applied In**:
- dim_customers (customer_key)
- dim_payment_methods (payment_method_key)
- fact_transactions (transaction_key)
- fact_payouts (payout_key)

---

### 6. Dimensional Join Pattern

**Use Case**: Join fact table with multiple dimensions

```sql
with transactions as (
  select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    created_at
  from stg_transactions
),

with_keys as (
  select
    md5(t.transaction_id) as transaction_key,
    t.transaction_id,
    dd.date_key,
    dc_debtor.customer_key as debtor_customer_key,
    dc_creditor.customer_key as creditor_customer_key,
    dpm.payment_method_key,
    t.amount as transaction_amount,
    t.created_at
  from transactions t
  left join dim_date dd 
    on cast(dd.date as date) = cast(t.created_at as date)
  left join dim_customers dc_debtor 
    on t.debtor_customer_id = dc_debtor.customer_id
  left join dim_customers dc_creditor 
    on t.creditor_customer_id = dc_creditor.customer_id
  left join dim_payment_methods dpm 
    on t.payment_method_id = dpm.payment_method_id
)

select * from with_keys
```

**Key Points**:
- Use `LEFT JOIN` to preserve all fact records
- Join on natural keys (customer_id, payment_method_id)
- Create date_key from timestamp
- Use aliases for clarity (dc_debtor, dc_creditor)

**Applied In**:
- fact_transactions (joins all dimensions)
- fact_payouts (joins customer and date dimensions)

---

### 7. Date Dimension Pattern

**Use Case**: Create a date dimension with attributes

```sql
with date_spine as (
  select
    cast(date_value as date) as date_value
  from (
    select
      date_add('2025-07-26', interval cast(row_number() over (order by 1) - 1 as int64) day) as date_value
    from (
      select 1 union all select 2 union all select 3 union all select 4 union all select 5
      -- ... repeat to 44
    )
  )
),

date_attributes as (
  select
    format_date('%Y%m%d', date_value) as date_key,
    date_value as date,
    extract(year from date_value) as year,
    extract(month from date_value) as month,
    extract(day from date_value) as day,
    extract(quarter from date_value) as quarter,
    extract(week from date_value) as week_of_year,
    extract(dayofweek from date_value) as day_of_week,
    case 
      when extract(dayofweek from date_value) in (1, 7) then true 
      else false 
    end as is_weekend,
    format_date('%A', date_value) as day_name,
    format_date('%B', date_value) as month_name
  from date_spine
)

select * from date_attributes
```

**Key Points**:
- Use `DATE_ADD()` with intervals to generate date series
- `FORMAT_DATE()` creates formatted strings
- `EXTRACT()` pulls date components
- `CASE WHEN` identifies weekends
- Covers specific date range (2025-07-26 to 2025-09-08)

**Applied In**:
- dim_date (complete date dimension)

---

### 8. Double-Entry Accounting Validation Pattern

**Use Case**: Validate that debits equal credits

```sql
with source_data as (
  select
    transaction_id,
    direction,
    amount,
    row_number() over (partition by transaction_id, direction order by created_at) as rn
  from transaction_legs
),

aggregated as (
  select
    transaction_id,
    direction,
    sum(amount) as amount
  from source_data
  where rn = 1
  group by transaction_id, direction
),

validated as (
  select
    transaction_id,
    direction,
    amount,
    sum(case when direction = 'debit' then amount else -amount end) 
      over (partition by transaction_id) as net_amount
  from aggregated
),

final as (
  select
    transaction_id,
    direction,
    amount
  from validated
  where 
    -- Only include transactions with balanced ledger (debit = credit)
    abs(net_amount) < 0.01  -- Allow for rounding
)

select * from final
```

**Key Points**:
- Aggregate by transaction and direction
- Calculate net amount (debits positive, credits negative)
- Validate balance is near zero (allow 0.01 for rounding)
- Ensures double-entry accounting integrity

**Applied In**:
- stg_transaction_legs (CRITICAL validation)

---

## 🔍 Real-World Examples

### Example 1: Customer Cleaning (stg_customers)

```sql
-- Raw data: 441,409 rows with duplicates and test data
-- Clean data: 34 rows with standardized values

with source_data as (
  select
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    created_at,
    updated_at,
    row_number() over (partition by customer_id order by created_at) as rn
  from `prd-dagen`.`payments_v1`.`customers`
  where 
    customer_type != 'samet'  -- Remove test data
    and _ab_cdc_deleted_at is null  -- Remove soft-deleted
),

deduplicated as (
  select
    customer_id,
    customer_type,
    email,
    phone_number,
    case 
      when kyc_status in ('ok', 'done', 'yes') then 'VERIFIED'
      when kyc_status is null then 'UNKNOWN'
      else upper(kyc_status)
    end as kyc_status,
    created_at,
    updated_at
  from source_data
  where rn = 1
)

select * from deduplicated
```

**Transformations**:
1. Remove test customers (customer_type='samet')
2. Remove soft-deleted records
3. Deduplicate by customer_id (keep first by created_at)
4. Standardize KYC status
5. Keep all relevant attributes

---

### Example 2: Transaction Ledger Rebuild (stg_transaction_legs)

```sql
-- Raw data: 359,268 rows with 17,108 legs per transaction (BROKEN)
-- Clean data: 42 rows with exactly 2 legs per transaction (FIXED)

with source_data as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at,
    row_number() over (partition by transaction_id, direction order by created_at) as rn
  from `prd-dagen`.`payments_v1`.`transaction_legs`
  where 
    _ab_cdc_deleted_at is null
    and direction in ('debit', 'credit')
),

-- Step 1: Aggregate amounts by transaction and direction
aggregated as (
  select
    transaction_id,
    direction,
    currency,
    sum(amount) as amount,
    min(account) as account,
    min(created_at) as created_at
  from source_data
  where rn = 1
  group by transaction_id, direction, currency
),

-- Step 2: Calculate net amount for validation
validated as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at,
    sum(case when direction = 'debit' then amount else -amount end) 
      over (partition by transaction_id) as net_amount
  from aggregated
),

-- Step 3: Filter to balanced transactions only
final as (
  select
    transaction_id,
    direction,
    amount,
    currency,
    account,
    created_at
  from validated
  where abs(net_amount) < 0.01  -- Debit = Credit
)

select * from final
```

**Transformations**:
1. Filter to valid directions (debit, credit)
2. Aggregate amounts by transaction and direction
3. Calculate net amount for validation
4. Keep only balanced transactions
5. Result: Exactly 2 legs per transaction

---

### Example 3: Fact Table with Aggregations (fact_transaction_details)

```sql
-- Aggregates refunds, disputes, and fees by transaction

with transactions as (
  select
    transaction_id,
    amount
  from stg_transactions
),

refunds_agg as (
  select
    original_transaction_id as transaction_id,
    sum(amount) as refund_amount,
    count(*) as refund_count
  from stg_refunds
  group by original_transaction_id
),

disputes_agg as (
  select
    transaction_id,
    sum(amount) as dispute_amount,
    count(*) as dispute_count
  from stg_disputes
  group by transaction_id
),

fees_agg as (
  select
    transaction_id,
    sum(amount) as fee_amount,
    count(*) as fee_count
  from stg_fees
  group by transaction_id
),

combined as (
  select
    md5(t.transaction_id) as transaction_key,
    t.transaction_id,
    coalesce(r.refund_amount, 0) as refund_amount,
    coalesce(d.dispute_amount, 0) as dispute_amount,
    coalesce(f.fee_amount, 0) as fee_amount,
    coalesce(r.refund_count, 0) as refund_count,
    coalesce(d.dispute_count, 0) as dispute_count,
    coalesce(f.fee_count, 0) as fee_count,
    t.amount - coalesce(r.refund_amount, 0) - coalesce(f.fee_amount, 0) as net_transaction_amount
  from transactions t
  left join refunds_agg r on t.transaction_id = r.transaction_id
  left join disputes_agg d on t.transaction_id = d.transaction_id
  left join fees_agg f on t.transaction_id = f.transaction_id
)

select * from combined
```

**Transformations**:
1. Aggregate refunds by transaction
2. Aggregate disputes by transaction
3. Aggregate fees by transaction
4. Left join all aggregations (preserve all transactions)
5. Calculate net amount

---

## 📊 Query Performance Tips

### 1. Use Approximate Aggregates for Large Datasets
```sql
-- Instead of COUNT(DISTINCT customer_id)
select approx_count_distinct(customer_id)
from large_table
```

### 2. Filter Early
```sql
-- Bad: Filter after join
select * from raw_data r
left join dim_customers d on r.customer_id = d.customer_id
where r.created_at > '2025-01-01'

-- Good: Filter before join
select * from (
  select * from raw_data
  where created_at > '2025-01-01'
) r
left join dim_customers d on r.customer_id = d.customer_id
```

### 3. Use Appropriate Data Types
```sql
-- Use NUMERIC for money (not FLOAT)
amount NUMERIC,

-- Use DATE for dates (not TIMESTAMP)
created_date DATE,

-- Use STRING for IDs (not INT)
customer_id STRING
```

### 4. Partition Large Tables
```sql
-- Partition by date for faster queries
create or replace table fact_transactions
partition by cast(created_at as date)
as select * from stg_transactions
```

---

## 🧪 Testing Queries

### Test: Verify Deduplication
```sql
select customer_id, count(*) as cnt
from stg_customers
group by customer_id
having cnt > 1
```
Expected: 0 rows (all unique)

### Test: Verify Foreign Keys
```sql
select count(*)
from stg_refunds r
left join stg_transactions t on r.original_transaction_id = t.transaction_id
where t.transaction_id is null
```
Expected: 0 rows (all valid FKs)

### Test: Verify Ledger Balance
```sql
select 
  transaction_id,
  sum(case when direction = 'debit' then amount else -amount end) as net
from stg_transaction_legs
group by transaction_id
having abs(net) > 0.01
```
Expected: 0 rows (all balanced)

### Test: Verify KYC Standardization
```sql
select distinct kyc_status
from stg_customers
order by kyc_status
```
Expected: VERIFIED, UNKNOWN, PENDING, REJECTED

---

## 🔗 Additional Resources

- **dbt Documentation**: https://docs.getdbt.com/
- **BigQuery SQL Reference**: https://cloud.google.com/bigquery/docs/reference/standard-sql
- **Window Functions**: https://cloud.google.com/bigquery/docs/window-functions
- **Date Functions**: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions

---

**Last Updated**: 2026-02-19  
**Project**: test_midas v2.0.0