# Payment Data Warehouse - DBT Project

## 📋 Project Overview

This is a comprehensive DBT project for building a production-ready data warehouse for payment system data. It implements a complete ELT (Extract, Load, Transform) pipeline with 15 models organized in 3 layers:

1. **Staging Layer** (9 models) - Data cleaning and deduplication
2. **Dimension Layer** (3 models) - Star schema dimensions
3. **Fact Layer** (3 models) - Star schema facts

### Key Statistics
- **Raw Data**: 1.9M+ records across 9 tables
- **Data Quality Issues Fixed**: 
  - 441K customer records → 34 unique customers (deduplication)
  - 17,108 transaction legs per transaction → 2 legs per transaction (CRITICAL FIX)
  - Test data removal and standardization
- **Models**: 15 total (9 staging + 3 dimensions + 3 facts)
- **Tests**: 100+ data quality tests
- **Documentation**: Comprehensive YAML documentation for all models

---

## 🏗️ Architecture

### Layer 1: Staging Models (Data Cleaning)

Raw data from Airbyte ingestion is cleaned and deduplicated:

```
Raw Tables (Airbyte)
├── customers (441K rows)
├── payment_methods (359K rows)
├── transactions (359K rows)
├── transaction_legs (359K rows - CRITICAL ISSUE)
├── refunds (205K rows)
├── disputes (205K rows)
├── fees (359K rows)
├── mandates (342K rows)
└── payouts (359K rows)

↓ CLEANING & DEDUPLICATION ↓

Staging Tables
├── stg_customers (34 rows)
├── stg_payment_methods (21 rows)
├── stg_transactions (21 rows)
├── stg_transaction_legs (42 rows - REBUILT with 2 legs/transaction)
├── stg_refunds (12 rows)
├── stg_disputes (12 rows)
├── stg_fees (21 rows)
├── stg_mandates (21 rows)
└── stg_payouts (21 rows)
```

#### Data Quality Issues Fixed

| Issue | Raw Data | Clean Data | Fix |
|-------|----------|-----------|-----|
| **Duplicate Customers** | 441,409 rows | 34 unique | Deduplicate by customer_id, keep first by created_at |
| **Test Data (Customers)** | Mixed with production | Removed | Filter out customer_type='samet' |
| **KYC Status Inconsistency** | 6 different statuses | Standardized | Map {ok, done, yes} → VERIFIED |
| **Payment Method Types** | credit_card & card mixed | Consolidated | Map credit_card → card |
| **Transaction Legs** | 17,963 legs per transaction | 2 legs per transaction | Aggregate by direction, rebuild ledger |
| **Test Disputes** | Mixed with production | Removed | Filter out reason='Test Dispute' |
| **Test Fees** | Mixed with production | Removed | Filter out fee_type='Test Fee' |
| **Foreign Keys** | Unvalidated | Validated | Check all FKs exist in cleaned tables |

### Layer 2: Dimension Models (Star Schema)

Cleaned data is transformed into dimensions with surrogate keys:

```
Staging Tables → Dimensions
├── stg_customers → dim_customers (34 rows)
│   └── Surrogate key: customer_key (hashed)
│   └── Attributes: customer_type, email, phone_number, kyc_status
│   └── Flags: is_kyc_verified, is_active
│
├── stg_payment_methods → dim_payment_methods (21 rows)
│   └── Surrogate key: payment_method_key (hashed)
│   └── FK to dim_customers
│   └── Attributes: method_type, is_default
│
└── stg_transactions (date range) → dim_date (730 rows)
    └── Surrogate key: date_key (YYYYMMDD)
    └── Attributes: year, month, day, quarter, week, day_of_week, is_weekend
```

### Layer 3: Fact Models (Star Schema)

Clean data is transformed into facts with dimensional keys:

```
Staging Tables → Facts
├── stg_transactions → fact_transactions (21 rows)
│   └── Surrogate key: transaction_key (hashed)
│   └── FKs: date_key, debtor_customer_key, creditor_customer_key, payment_method_key
│   └── Measures: transaction_amount
│
├── stg_refunds, stg_disputes, stg_fees → fact_transaction_details (21 rows)
│   └── FK to fact_transactions
│   └── Aggregates: refund_amount, dispute_amount, fee_amount
│   └── Derived: net_transaction_amount
│
└── stg_payouts → fact_payouts (21 rows)
    └── Surrogate key: payout_key (hashed)
    └── FKs: date_key, recipient_customer_key
    └── Measures: payout_amount
```

---

## 📊 Model Details

### Staging Models (Layer 1)

#### 1. stg_customers
- **Purpose**: Clean customer data
- **Input**: customers (441K rows)
- **Output**: 34 unique customers
- **Transformations**:
  - Deduplicate by customer_id (keep first by created_at)
  - Remove test data (customer_type = 'samet')
  - Standardize KYC status (ok/done/yes → VERIFIED)
- **Key Columns**: customer_id, customer_type, email, phone_number, kyc_status

#### 2. stg_payment_methods
- **Purpose**: Clean payment method data
- **Input**: payment_methods (359K rows)
- **Output**: 21 unique payment methods
- **Transformations**:
  - Deduplicate by payment_method_id
  - Consolidate payment types (credit_card → card)
  - Validate customer FK
- **Key Columns**: payment_method_id, customer_id, method_type, is_default

#### 3. stg_transactions
- **Purpose**: Clean transaction data
- **Input**: transactions (359K rows)
- **Output**: 21 valid transactions
- **Transformations**:
  - Deduplicate by transaction_id
  - Validate transaction status values
  - Ensure debtor_customer_id ≠ creditor_customer_id
  - Validate both customer FKs
- **Key Columns**: transaction_id, debtor_customer_id, creditor_customer_id, amount, status

#### 4. stg_transaction_legs ⚠️ CRITICAL
- **Purpose**: Rebuild transaction ledger structure
- **Input**: transaction_legs (359K rows, 17,963 legs per transaction)
- **Output**: 42 rows (21 transactions × 2 legs)
- **Transformations**:
  - **CRITICAL**: Aggregate legs by transaction_id and direction
  - Collapse 17,963 legs per transaction → exactly 2 legs per transaction
  - Create exactly 2 rows per transaction (1 DEBIT, 1 CREDIT)
  - Validate double-entry accounting (debit amount = credit amount)
- **Key Columns**: transaction_id, direction, amount, is_balanced
- **Validation**: Ensures debit amount = credit amount for each transaction

#### 5. stg_refunds
- **Purpose**: Clean refund data
- **Input**: refunds (205K rows)
- **Output**: 12 valid refunds
- **Transformations**:
  - Deduplicate by refund_id
  - Validate original_transaction_id FK
- **Key Columns**: refund_id, original_transaction_id, amount, reason, status

#### 6. stg_disputes
- **Purpose**: Clean dispute data
- **Input**: disputes (205K rows)
- **Output**: 12 valid disputes
- **Transformations**:
  - Deduplicate by dispute_id
  - Remove test data (reason = 'Test Dispute')
  - Validate transaction_id FK
- **Key Columns**: dispute_id, transaction_id, amount, reason, status

#### 7. stg_fees
- **Purpose**: Clean fee data
- **Input**: fees (359K rows)
- **Output**: 21 valid fees
- **Transformations**:
  - Deduplicate by fee_id
  - Remove test data (fee_type = 'Test Fee')
  - Validate transaction_id FK
- **Key Columns**: fee_id, transaction_id, amount, fee_type

#### 8. stg_mandates
- **Purpose**: Clean mandate data
- **Input**: mandates (342K rows)
- **Output**: 21 valid mandates
- **Transformations**:
  - Deduplicate by mandate_id
  - Validate customer_id FK
- **Key Columns**: mandate_id, customer_id, reference, status

#### 9. stg_payouts
- **Purpose**: Clean payout data
- **Input**: payouts (359K rows)
- **Output**: 21 valid payouts
- **Transformations**:
  - Deduplicate by payout_id
  - Validate recipient_customer_id FK
- **Key Columns**: payout_id, recipient_customer_id, amount, status

### Dimension Models (Layer 2)

#### 10. dim_customers
- **Purpose**: Customer dimension with surrogate keys
- **Grain**: One row per unique customer
- **Row Count**: 34 customers
- **Keys**: 
  - Surrogate key: customer_key (hashed MD5)
  - Natural key: customer_id
- **Attributes**: customer_type, email, phone_number, kyc_status
- **Flags**: is_kyc_verified, is_active (updated in last 90 days)

#### 11. dim_payment_methods
- **Purpose**: Payment methods dimension
- **Grain**: One row per unique payment method
- **Row Count**: 21 payment methods
- **Keys**:
  - Surrogate key: payment_method_key (hashed MD5)
  - Natural key: payment_method_id
  - FK: customer_key → dim_customers
- **Attributes**: method_type, is_default

#### 12. dim_date
- **Purpose**: Date dimension for time-based analysis
- **Grain**: One row per calendar day
- **Row Count**: 730 days (covers full transaction date range)
- **Keys**: 
  - Surrogate key: date_key (YYYYMMDD format)
  - Natural key: calendar_date
- **Attributes**: year, month, day, quarter, week, day_of_week, is_weekend
- **Names**: day_name (Monday, Tuesday, etc.), month_name (January, February, etc.)

### Fact Models (Layer 3)

#### 13. fact_transactions
- **Purpose**: Core transaction fact table
- **Grain**: One row per transaction
- **Row Count**: 21 transactions
- **Keys**:
  - Surrogate key: transaction_key (hashed MD5)
  - Natural key: transaction_id
  - FKs: date_key, debtor_customer_key, creditor_customer_key, payment_method_key
- **Measures**: transaction_amount, currency
- **Attributes**: transaction_status, reference

#### 14. fact_transaction_details
- **Purpose**: Aggregated transaction details (refunds, disputes, fees)
- **Grain**: One row per transaction (one-to-one with fact_transactions)
- **Row Count**: 21 transactions
- **Keys**:
  - FK: transaction_key → fact_transactions
- **Measures**: 
  - refund_amount, dispute_amount, fee_amount
  - refund_count, dispute_count, fee_count
  - net_transaction_amount (amount - refunds - fees)

#### 15. fact_payouts
- **Purpose**: Payout fact table
- **Grain**: One row per payout
- **Row Count**: 21 payouts
- **Keys**:
  - Surrogate key: payout_key (hashed MD5)
  - Natural key: payout_id
  - FKs: date_key, recipient_customer_key
- **Measures**: payout_amount, currency
- **Attributes**: payout_status, scheduled_at, executed_at

---

## 🚀 Getting Started

### Prerequisites
- dbt >= 1.0.0
- BigQuery connection configured
- Service account with appropriate permissions

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/samkaradag/test_midas.git
cd test_midas
```

2. **Install dependencies**
```bash
dbt deps
```

3. **Configure profiles.yml**
```bash
# Already configured with BigQuery connection
# Verify connection details in profiles.yml
```

4. **Run dbt tests**
```bash
dbt test
```

5. **Run the full project**
```bash
dbt run
```

### Running Specific Models

```bash
# Run only staging models
dbt run --select tag:staging

# Run only dimensions
dbt run --select tag:dimension

# Run only facts
dbt run --select tag:fact

# Run a specific model
dbt run --select stg_customers

# Run a model and its dependencies
dbt run --select +stg_customers
```

### Running Tests

```bash
# Run all tests
dbt test

# Run tests for a specific model
dbt test --select stg_customers

# Run only tests with a specific tag
dbt test --select tag:staging
```

---

## 📈 Data Quality & Testing

### Test Coverage

The project includes 100+ data quality tests across all models:

#### Staging Models
- **Uniqueness Tests**: Ensure no duplicate natural keys
- **Not Null Tests**: Verify required fields are populated
- **Referential Integrity Tests**: Validate foreign keys
- **Accepted Values Tests**: Verify categorical values
- **Custom Tests**: Data-specific validations

#### Dimension Models
- **Surrogate Key Uniqueness**: Ensure unique surrogate keys
- **Foreign Key Relationships**: Validate dimension FKs
- **Data Type Tests**: Verify correct data types

#### Fact Models
- **Foreign Key Relationships**: Validate all dimensional FKs
- **Measure Validation**: Ensure positive amounts where appropriate
- **Grain Validation**: Verify correct grain (one-to-one relationships)

### Running Data Quality Checks

```bash
# Run all tests
dbt test

# Run tests with detailed output
dbt test --debug

# Run tests and store failures
dbt test --store-failures
```

---

## 📚 Documentation

### Model Documentation

All models are documented in `models/schema.yml`:
- Model descriptions with business context
- Column definitions with data types
- Data quality tests
- Lineage information

### Source Documentation

All raw sources are documented in `models/sources/raw_sources.yml`:
- Table descriptions
- Column definitions
- Data quality issues noted

### Generate Documentation

```bash
# Generate documentation
dbt docs generate

# Serve documentation locally
dbt docs serve
```

---

## 🔄 Materialization Strategy

### Staging Models
- **Materialization**: Table
- **Rationale**: Staging models are reused by multiple downstream models
- **Refresh**: Full refresh on each run

### Dimension Models
- **Materialization**: Table
- **Rationale**: Dimensions are frequently joined; tables provide better performance
- **Refresh**: Full refresh (SCD Type 1)

### Fact Models
- **Materialization**: Table
- **Rationale**: Facts are large tables; full tables provide best query performance
- **Refresh**: Full refresh (can be changed to incremental for production)

---

## 🔍 Lineage & Dependencies

### Model Dependencies

```
Raw Tables
  ├── customers
  ├── payment_methods
  ├── transactions
  ├── transaction_legs
  ├── refunds
  ├── disputes
  ├── fees
  ├── mandates
  └── payouts

↓ STAGING LAYER ↓

  stg_customers
    ├─→ stg_payment_methods
    ├─→ stg_transactions
    └─→ stg_mandates, stg_payouts
  
  stg_transactions
    ├─→ stg_transaction_legs
    ├─→ stg_refunds
    ├─→ stg_disputes
    └─→ stg_fees

↓ DIMENSION LAYER ↓

  dim_customers (from stg_customers)
  dim_payment_methods (from stg_payment_methods + dim_customers)
  dim_date (from stg_transactions)

↓ FACT LAYER ↓

  fact_transactions (from stg_transactions + dimensions)
  fact_transaction_details (from stg_refunds, stg_disputes, stg_fees + fact_transactions)
  fact_payouts (from stg_payouts + dimensions)
```

### Viewing Lineage

```bash
# Generate lineage diagram
dbt docs generate

# View in dbt Cloud or local server
dbt docs serve
```

---

## 🛠️ Customization & Extension

### Adding New Models

1. Create a new SQL file in `models/staging/` or appropriate directory
2. Add model configuration in the file header:
```sql
{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['staging'],
    description='Your model description'
) }}
```

3. Add documentation in `models/schema.yml`
4. Add tests for data quality

### Modifying Existing Models

1. Edit the SQL file
2. Update documentation if needed
3. Run tests to verify changes:
```bash
dbt test --select <model_name>
```

### Changing Materialization

Update `dbt_project.yml`:
```yaml
models:
  test_midas:
    <model_name>:
      materialized: view  # or incremental, ephemeral
```

---

## 📊 Performance Considerations

### Query Optimization
- **Staging Models**: Use efficient GROUP BY and window functions
- **Dimensions**: Index on natural keys (customer_id, payment_method_id, etc.)
- **Facts**: Index on foreign keys and date_key for efficient joins

### Incremental Loading (Future Enhancement)
Current models use full refresh. For production, consider:
```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

select * from {{ source(...) }}
{% if execute %}
    where created_at >= (select max(created_at) from {{ this }})
{% endif %}
```

---

## 🔐 Security & Data Governance

### Service Account Permissions
Required BigQuery permissions:
- `bigquery.datasets.get`
- `bigquery.tables.create`
- `bigquery.tables.update`
- `bigquery.tables.delete`
- `bigquery.tables.get`
- `bigquery.tables.list`
- `bigquery.datasets.update`

### Data Classification
- **Raw Data**: Sensitive (PII, financial data)
- **Staging Data**: Sensitive (contains PII)
- **Dimensional Data**: Sensitive (contains PII)
- **Fact Data**: Sensitive (contains financial data)

### Masking & Anonymization
- Email addresses and phone numbers are stored but should be masked in analytics
- Payment method details are masked in source data
- Consider implementing column-level security for sensitive fields

---

## 📝 Changelog

### Version 1.0.0 (Initial Release)
- ✅ 9 staging models for data cleaning
- ✅ 3 dimension models for star schema
- ✅ 3 fact models for analytics
- ✅ Comprehensive YAML documentation
- ✅ 100+ data quality tests
- ✅ CRITICAL: Transaction legs rebuild (17K → 2 legs/transaction)

---

## 🤝 Contributing

### Development Workflow

1. Create a feature branch
2. Make changes to models/tests
3. Run tests locally:
```bash
dbt test
dbt run
```
4. Commit changes with descriptive messages
5. Create pull request

### Code Standards
- Use lowercase for SQL keywords
- Use meaningful model names (stg_*, dim_*, fact_*)
- Document all models and columns
- Include tests for all new models
- Follow dbt style guide

---

## 📞 Support & Questions

For questions or issues:
1. Check the documentation in `models/schema.yml`
2. Review the model SQL files for implementation details
3. Check dbt documentation: https://docs.getdbt.com/
4. Review test results for data quality issues

---

## 📄 License

This project is part of the payment data warehouse initiative.

---

## 🙏 Acknowledgments

Built with:
- **dbt** - Data transformation framework
- **BigQuery** - Cloud data warehouse
- **Airbyte** - Data integration platform

---

**Last Updated**: 2026-02-14
**Maintainer**: Data Engineering Team
**Status**: Production Ready