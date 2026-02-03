# Denormalization Strategy - Staging Tables

## Overview
Four denormalized fact tables have been created to improve query performance by reducing join complexity and pre-computing common aggregations.

---

## 📊 Denormalized Models

### 1. **fct_transactions_denorm**
**Purpose:** Complete transaction view with enriched customer and payment method data

**Grain:** One row per transaction

**Source Tables:**
- `stg_transactions` (base)
- `stg_customers` (debtor & creditor)
- `stg_payment_methods` (payment details)
- `stg_fees` (aggregated)

**Key Features:**
- ✅ Debtor & creditor customer profiles inline
- ✅ Payment method type and default flag
- ✅ Pre-calculated fee aggregates:
  - `total_fees_count`: Number of fees
  - `total_fees_amount`: Sum of all fees
  - `max_fee_amount` / `min_fee_amount`: Fee range
  - `fee_types`: Array of distinct fee types
- ✅ Calculated fields:
  - `total_amount_with_fees`: Transaction + fees
  - `transaction_size_category`: LOW/MEDIUM/HIGH based on amount
  - `involves_critical_risk_customer`: Risk flag for either party
- ✅ Data quality flags

**Use Cases:**
- Transaction reporting with customer context
- Fee analysis and reconciliation
- Risk assessment dashboards
- High-value transaction monitoring

---

### 2. **fct_customers_360**
**Purpose:** Complete customer view with aggregated payment methods and mandates

**Grain:** One row per customer

**Source Tables:**
- `stg_customers` (base)
- `stg_payment_methods` (aggregated)
- `stg_mandates` (aggregated)

**Key Features:**
- ✅ Customer profile with KYC and risk data
- ✅ Payment method aggregates:
  - `total_payment_methods`: Count of payment methods
  - `default_payment_method_count`: Count of defaults
  - `payment_method_types`: Array of method types
  - `last_payment_method_update`: Last update timestamp
- ✅ Mandate aggregates:
  - `total_mandates`: Count of mandates
  - `active_mandates_count` / `inactive_mandates_count`: Breakdown
  - `mandate_statuses`: Array of statuses
  - `last_mandate_update`: Last update timestamp
- ✅ Calculated fields:
  - `is_kyc_verified`: Boolean flag
  - `is_high_risk`: Flag for HIGH/CRITICAL risk profiles
- ✅ Data quality flags

**Use Cases:**
- Customer master data reporting
- KYC compliance dashboards
- Payment method management
- Mandate lifecycle tracking
- Customer risk profiling

---

### 3. **fct_transactions_with_refunds_disputes**
**Purpose:** Transaction view with refund and dispute metrics

**Grain:** One row per transaction

**Source Tables:**
- `stg_transactions` (base)
- `stg_refunds` (aggregated)
- `stg_disputes` (aggregated)

**Key Features:**
- ✅ Refund aggregates:
  - `total_refunds_count`: Number of refunds
  - `completed_refunds_count` / `pending_refunds_count`: Breakdown
  - `total_refunded_amount`: Sum of refunds
  - `max_refund_amount` / `min_refund_amount`: Range
  - `refund_reasons`: Array of reasons
  - `refund_statuses`: Array of statuses
  - `last_refund_update`: Last update timestamp
- ✅ Dispute aggregates:
  - `total_disputes_count`: Number of disputes
  - `open_disputes_count` / `resolved_disputes_count`: Breakdown
  - `total_disputed_amount`: Sum of disputed amounts
  - `max_dispute_amount` / `min_dispute_amount`: Range
  - `dispute_reasons`: Array of reasons
  - `dispute_statuses`: Array of statuses
  - `last_dispute_update`: Last update timestamp
- ✅ Calculated fields:
  - `net_transaction_amount`: Amount - refunds
  - `has_refunds` / `has_disputes`: Boolean flags
  - `has_refunds_or_disputes`: Combined flag
  - `transaction_risk_status`: DISPUTED/PENDING_REFUND/REFUNDED/CLEAN

**Use Cases:**
- Transaction quality monitoring
- Refund analysis and trends
- Dispute tracking and resolution
- Financial reconciliation
- Chargeback risk assessment

---

### 4. **fct_payouts_denorm**
**Purpose:** Payout view with recipient customer enrichment

**Grain:** One row per payout

**Source Tables:**
- `stg_payouts` (base)
- `stg_customers` (recipient)

**Key Features:**
- ✅ Recipient customer profile inline
- ✅ Recipient KYC and risk data
- ✅ Calculated fields:
  - `days_to_execute`: Days from scheduled to executed
  - `payout_status_category`: EXECUTED/OVERDUE/PENDING
  - `is_successfully_executed`: Completion flag
  - `recipient_is_high_risk`: Risk flag
  - `recipient_is_kyc_verified`: KYC flag
- ✅ Data quality flags

**Use Cases:**
- Payout tracking and monitoring
- Recipient compliance verification
- Settlement analysis
- Payment SLA monitoring
- Recipient risk profiling

---

## 🏗️ Schema Configuration

Models are organized by layer in `dbt_project.yml`:

```yaml
models:
  test_midas:
    staging:
      +schema: test_midas_staging    # stg_* models
    facts:
      +schema: test_midas_facts      # fct_* models
      +materialized: table
```

---

## 📈 Performance Benefits

| Aspect | Benefit |
|--------|---------|
| **Query Simplicity** | Reduce 3-5 joins to single table select |
| **Query Speed** | Pre-aggregated data eliminates group-by operations |
| **Consistency** | Single source of truth for common metrics |
| **Storage** | Minimal overhead; fact tables are denormalized views |
| **Maintenance** | Centralized logic for customer/payment data enrichment |

---

## 🔄 Data Flow

```
Raw Sources (Airbyte CDC)
    ↓
Staging Layer (stg_*)
    ├─ stg_customers
    ├─ stg_transactions
    ├─ stg_payment_methods
    ├─ stg_fees
    ├─ stg_refunds
    ├─ stg_disputes
    ├─ stg_payouts
    └─ stg_mandates
    ↓
Fact Layer (fct_*)
    ├─ fct_transactions_denorm
    ├─ fct_customers_360
    ├─ fct_transactions_with_refunds_disputes
    └─ fct_payouts_denorm
    ↓
Downstream Analytics & Reports
```

---

## 📝 Documentation

- **Schema definitions:** `models/schema.yml` (staging tables)
- **Fact definitions:** `models/facts_schema.yml` (denormalized tables)
- **Source definitions:** `models/sources.yml` (raw Airbyte tables)

---

## 🚀 Next Steps

1. **Run the models:** Execute `dbt run --models fct_*` to build fact tables
2. **Test the models:** Run `dbt test --models fct_*` for data quality checks
3. **Generate docs:** Run `dbt docs generate` to update documentation
4. **Monitor performance:** Compare query times before/after denormalization
5. **Extend:** Add more denormalized views as needed for specific use cases

---

## ⚠️ Important Notes

- All fact tables are materialized as **tables** for optimal query performance
- Aggregations filter out records with `has_data_quality_issues = true`
- Left joins preserve all base records (transactions, customers, payouts)
- Array aggregations may be database-specific (BigQuery: `ARRAY_AGG`)
- Timestamp fields include `dbt_loaded_at` for lineage tracking