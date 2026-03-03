# DBT Best Practices Improvement Recommendations

## test_midas Project - Action Items

**Assessment Date:** 2026-03-03  
**Current Compliance Score:** 95.7/100  
**Potential Score (with recommendations):** 98.5/100  

---

## Priority Matrix

```
            HIGH IMPACT
                  ▲
                  │
    MEDIUM        │  CUSTOM TESTS
    EFFORT        │  TEST SEVERITY
                  │
    LOW EFFORT    │  FACT NAMING
                  │  INT LAYER
                  └──────────────────────────────►
                    LOW IMPACT
```

---

## 1. MEDIUM PRIORITY: Custom Tests for Business Logic

**Current Status:** ⚠️ Gap  
**Impact:** Medium (adds data quality validation)  
**Effort:** Medium (2-4 hours)  
**Complexity:** Medium  

### Problem
The project uses only standard tests (unique, not_null, relationships, accepted_values). Complex business logic is not validated:
- Double-entry accounting (transaction_legs)
- Amount non-negativity
- Date range validation
- Aggregate-level referential integrity

### Solution

#### 1.1 Double-Entry Accounting Validation

**File:** `models/staging/stg_transaction_legs.sql`

Add a custom test to validate that debits equal credits per transaction:

```sql
-- tests/test_transaction_legs_balanced.sql
-- Purpose: Validate double-entry accounting - debits must equal credits

select
  transaction_id,
  sum(case when direction = 'debit' then amount else 0 end) as total_debits,
  sum(case when direction = 'credit' then amount else 0 end) as total_credits
from {{ ref('stg_transaction_legs') }}
group by transaction_id
having 
  abs(total_debits - total_credits) > 0.01  -- Allow 1 cent variance for rounding
```

**Add to schema.yml:**
```yaml
stg_transaction_legs:
  tests:
    - test_transaction_legs_balanced
```

**Expected Results:**
- ✅ Validates double-entry accounting integrity
- ✅ Detects data quality issues early
- ✅ Prevents invalid ledger entries in downstream models

#### 1.2 Amount Non-Negativity Validation

**File:** Add to `models/schema.yml`

```yaml
stg_transactions:
  columns:
    - name: amount
      tests:
        - not_null
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 0
            max_value: 999999999

stg_refunds:
  columns:
    - name: amount
      tests:
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 0

stg_fees:
  columns:
    - name: amount
      tests:
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 0
```

**Expected Results:**
- ✅ Prevents negative amounts
- ✅ Detects data entry errors
- ✅ Improves financial data quality

#### 1.3 Date Range Validation

**File:** Create `tests/test_dates_in_valid_range.sql`

```sql
-- Purpose: Validate all dates are within expected range
select
  transaction_id,
  created_at
from {{ ref('stg_transactions') }}
where 
  created_at < '2025-07-01'  -- Before project start
  or created_at > current_timestamp() + interval 1 day  -- Future dates
```

**Expected Results:**
- ✅ Validates date range
- ✅ Detects data anomalies
- ✅ Prevents future-dated records

### Implementation Steps

1. **Install dbt-expectations package** (if not already installed):
```yaml
# packages.yml
packages:
  - package: calogica/dbt-expectations
    version: 0.8.0
```

2. **Run dbt deps:**
```bash
dbt deps
```

3. **Add custom tests** to `tests/` directory

4. **Update schema.yml** with new test configurations

5. **Run tests:**
```bash
dbt test
```

6. **Commit changes:**
```bash
git add tests/ models/schema.yml packages.yml
git commit -m "Add custom tests for business logic validation"
git push
```

---

## 2. LOW PRIORITY: Test Severity Levels

**Current Status:** ⚠️ Gap  
**Impact:** Low (improves CI/CD integration)  
**Effort:** Low (1-2 hours)  
**Complexity:** Low  

### Problem
All tests are treated equally. Critical tests should fail the run, while warnings should only alert.

### Solution

Add severity levels to critical tests:

```yaml
# models/schema.yml

stg_customers:
  columns:
    - name: customer_id
      tests:
        - not_null:
            severity: error  # Fail the dbt run
        - unique:
            severity: error

stg_transactions:
  columns:
    - name: transaction_id
      tests:
        - not_null:
            severity: error
        - unique:
            severity: error
    - name: amount
      tests:
        - not_null:
            severity: error
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 0
            severity: warn  # Only warn, don't fail

dim_customers:
  columns:
    - name: customer_key
      tests:
        - not_null:
            severity: error
        - unique:
            severity: error

fact_transactions:
  columns:
    - name: transaction_key
      tests:
        - not_null:
            severity: error
        - unique:
            severity: error
```

### Implementation Steps

1. **Identify critical tests:**
   - Primary key tests → `severity: error`
   - Foreign key tests → `severity: error`
   - Not null on critical fields → `severity: error`
   - Data quality warnings → `severity: warn`

2. **Update schema.yml** with severity levels

3. **Test locally:**
```bash
dbt test
dbt test --select tag:critical  # Run only critical tests
```

4. **Commit changes:**
```bash
git add models/schema.yml
git commit -m "Add severity levels to tests (error/warn)"
git push
```

### CI/CD Integration

Use in dbt Cloud or GitHub Actions:
```yaml
# .github/workflows/dbt-test.yml
- name: Run dbt tests
  run: |
    dbt test --fail-fast  # Fail on first error-severity test
```

---

## 3. LOW PRIORITY: Fact Table Naming Convention

**Current Status:** ⚠️ Minor deviation  
**Impact:** Low (style/consistency)  
**Effort:** Medium (refactoring required)  
**Complexity:** Medium  

### Problem
Project uses `fact_*` prefix instead of standard dbt-labs `fct_*` prefix.

### Current Names
- `fact_transactions` → `fct_transactions`
- `fact_transaction_details` → `fct_transaction_details`
- `fact_payouts` → `fct_payouts`

### Solution

This is optional and only for strict adherence to dbt-labs standards. Both conventions are widely accepted.

**If you decide to rename:**

1. **Create migration branch:**
```bash
git checkout -b refactor/rename-fact-tables
```

2. **Rename files:**
```bash
mv models/fact_transactions.sql models/fct_transactions.sql
mv models/fact_transaction_details.sql models/fct_transaction_details.sql
mv models/fact_payouts.sql models/fct_payouts.sql
```

3. **Update references in schema.yml:**
```yaml
# Before
- name: fact_transactions

# After
- name: fct_transactions
```

4. **Update references in dependent models:**
```sql
-- fact_transaction_details.sql
from {{ ref('fct_transactions') }}  # Update ref

-- fact_payouts.sql
from {{ ref('fct_transactions') }}  # Update ref
```

5. **Update dbt_project.yml:**
```yaml
# Before
fact_transactions:
  +materialized: table

# After
fct_transactions:
  +materialized: table
```

6. **Test:**
```bash
dbt parse
dbt compile
dbt test
```

7. **Commit and push:**
```bash
git add models/ dbt_project.yml models/schema.yml
git commit -m "Refactor: Rename fact tables to use fct_ prefix"
git push origin refactor/rename-fact-tables
```

8. **Create Pull Request** for review

**Impact Assessment:**
- ✅ Improves naming consistency
- ⚠️ Requires downstream updates
- ⚠️ Breaking change for BI tools (need to update queries)

---

## 4. LOW PRIORITY: Add Intermediate Layer (Optional)

**Current Status:** ⚠️ Design choice  
**Impact:** Low (improves readability for complex logic)  
**Effort:** High (significant refactoring)  
**Complexity:** High  

### Problem
Current 3-layer architecture works well, but some models have complex joins that could be simplified with an intermediate layer.

### Solution

Only implement if:
- Models become too complex to understand
- Multiple models share the same complex joins
- Need to test intermediate business logic

### Example: Intermediate Model for Transactions with Details

```sql
-- models/intermediate/int_transactions_with_details.sql
-- Purpose: Join transactions with refunds, disputes, fees
-- This is complex logic that could be reused

with transactions as (
  select * from {{ ref('stg_transactions') }}
),

refunds as (
  select
    original_transaction_id,
    sum(amount) as total_refunds,
    count(*) as refund_count
  from {{ ref('stg_refunds') }}
  group by original_transaction_id
),

disputes as (
  select
    transaction_id,
    sum(amount) as total_disputes,
    count(*) as dispute_count
  from {{ ref('stg_disputes') }}
  group by transaction_id
),

fees as (
  select
    transaction_id,
    sum(amount) as total_fees,
    count(*) as fee_count
  from {{ ref('stg_fees') }}
  group by transaction_id
)

select
  t.*,
  coalesce(r.total_refunds, 0) as refund_amount,
  coalesce(d.total_disputes, 0) as dispute_amount,
  coalesce(f.total_fees, 0) as fee_amount,
  coalesce(r.refund_count, 0) as refund_count,
  coalesce(d.dispute_count, 0) as dispute_count,
  coalesce(f.fee_count, 0) as fee_count
from transactions t
left join refunds r on t.transaction_id = r.original_transaction_id
left join disputes d on t.transaction_id = d.transaction_id
left join fees f on t.transaction_id = f.transaction_id
```

**When to implement:**
- If fact_transaction_details becomes too complex
- If multiple downstream models need this logic
- Only then create `models/intermediate/int_*` models

---

## 5. OPTIONAL: Incremental Models

**Current Status:** ⚠️ Design choice  
**Impact:** Low (performance optimization)  
**Effort:** High (requires state management)  
**Complexity:** High  

### Problem
All staging and dimension models are full refreshes. For large tables, incremental models improve performance.

### Solution (Only if needed for performance)

```sql
-- models/staging/stg_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail',
) }}

with source_data as (
  select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    payment_method_id,
    amount,
    currency,
    status,
    reference,
    created_at,
    updated_at
  from {{ source('raw_data', 'transactions') }}
  
  {% if execute and execute_macros == true %}
    {% if var('full_refresh', False) %}
      -- Full refresh
    {% else %}
      -- Incremental load - only new/updated records
      where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
  {% endif %}
)

select * from source_data
```

**When to implement:**
- Only if staging models take > 5 minutes
- Only if source tables have > 1M rows
- Only after profiling confirms bottleneck

---

## 6. DOCUMENTATION: Add Test Descriptions

**Current Status:** ⚠️ Gap  
**Impact:** Low (improves maintainability)  
**Effort:** Low (1-2 hours)  
**Complexity:** Low  

### Problem
Tests don't have descriptions explaining their purpose.

### Solution

Add descriptions to complex tests:

```yaml
stg_transactions:
  columns:
    - name: transaction_id
      tests:
        - not_null:
            description: "Every transaction must have an ID"
        - unique:
            description: "Transaction IDs must be unique to prevent duplicates"
    - name: amount
      tests:
        - not_null:
            description: "Amount is required for all transactions"
        - dbt_expectations.expect_column_values_to_be_between:
            min_value: 0
            max_value: 999999999
            description: "Amounts must be positive and within reasonable bounds (0 to $999M)"
```

---

## Implementation Timeline

### Week 1 (Immediate)
- [ ] Add test severity levels (1-2 hours)
- [ ] Add test descriptions (1-2 hours)

### Week 2-3 (Soon)
- [ ] Implement custom tests for business logic (4-6 hours)
  - Double-entry accounting validation
  - Amount non-negativity
  - Date range validation

### Week 4+ (Later)
- [ ] Fact table naming refactor (optional, 3-4 hours)
- [ ] Intermediate layer (only if needed, 8-12 hours)
- [ ] Incremental models (only if needed, 8-12 hours)

---

## Success Metrics

### After Implementing Recommendations:

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Compliance Score | 95.7 | 98.5 | ↑ +2.8 |
| Custom Tests | 0 | 3+ | ↑ |
| Test Severity | None | 100% critical marked | ↑ |
| Test Documentation | Partial | 100% | ↑ |
| Fact Naming | fact_ | fct_ | ↑ Optional |

---

## Code Examples Repository

### Example 1: Adding Custom Test
```sql
-- tests/test_transaction_legs_balanced.sql
{{ config(severity = 'error') }}

select
  transaction_id,
  sum(case when direction = 'debit' then amount else 0 end) as debits,
  sum(case when direction = 'credit' then amount else 0 end) as credits
from {{ ref('stg_transaction_legs') }}
group by transaction_id
having abs(debits - credits) > 0.01
```

### Example 2: Adding Severity
```yaml
stg_customers:
  columns:
    - name: customer_id
      tests:
        - not_null:
            severity: error
        - unique:
            severity: error
```

### Example 3: Adding Description
```yaml
stg_transactions:
  columns:
    - name: amount
      tests:
        - not_null:
            description: "Transaction amount is required"
```

---

## Questions & Support

For questions about implementing these recommendations:

1. **Custom Tests:** Refer to dbt documentation on custom tests
2. **Test Severity:** See dbt Cloud test configuration docs
3. **Refactoring:** Use `dbt parse` to validate changes
4. **CI/CD:** Integrate with GitHub Actions or dbt Cloud

---

## Final Notes

✅ **Current state is production-ready**  
✅ **Recommendations are optional enhancements**  
✅ **Focus on custom tests for maximum impact**  

The project demonstrates excellent DBT practices. These recommendations are for incremental improvement, not critical fixes.

---

**Report Generated:** 2026-03-03  
**Next Review:** 2026-06-03 (Quarterly)