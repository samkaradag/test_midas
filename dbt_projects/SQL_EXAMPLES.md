# SQL Examples & Query Reference

This document provides SQL examples for querying the data warehouse and understanding the model structure.

---

## 📊 Staging Model Queries

### 1. Customer Analysis

#### Get all cleaned customers
```sql
select
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    created_at,
    updated_at
from prd-dagen.payments_v1.stg_customers
order by created_at desc
```

#### Count customers by type
```sql
select
    customer_type,
    count(*) as customer_count
from prd-dagen.payments_v1.stg_customers
group by customer_type
order by customer_count desc
```

#### Count customers by KYC status
```sql
select
    kyc_status,
    count(*) as customer_count
from prd-dagen.payments_v1.stg_customers
group by kyc_status
order by customer_count desc
```

#### Find customers without verified KYC
```sql
select
    customer_id,
    email,
    kyc_status
from prd-dagen.payments_v1.stg_customers
where kyc_status != 'VERIFIED'
order by created_at
```

### 2. Payment Method Analysis

#### List all payment methods by customer
```sql
select
    spm.customer_id,
    spm.payment_method_id,
    spm.method_type,
    spm.is_default,
    spm.created_at
from prd-dagen.payments_v1.stg_payment_methods spm
order by spm.customer_id, spm.created_at
```

#### Count payment methods by type
```sql
select
    method_type,
    count(*) as method_count
from prd-dagen.payments_v1.stg_payment_methods
group by method_type
order by method_count desc
```

#### Find default payment methods per customer
```sql
select
    customer_id,
    payment_method_id,
    method_type
from prd-dagen.payments_v1.stg_payment_methods
where is_default = true
```

### 3. Transaction Analysis

#### Get all transactions with amounts
```sql
select
    transaction_id,
    debtor_customer_id,
    creditor_customer_id,
    amount,
    currency,
    status,
    created_at
from prd-dagen.payments_v1.stg_transactions
order by created_at desc
```

#### Transaction count by status
```sql
select
    status,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from prd-dagen.payments_v1.stg_transactions
group by status
order by transaction_count desc
```

#### Total transaction volume
```sql
select
    count(*) as total_transactions,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount
from prd-dagen.payments_v1.stg_transactions
```

#### Transactions by customer
```sql
select
    debtor_customer_id,
    count(*) as transaction_count,
    sum(amount) as total_sent,
    avg(amount) as avg_sent
from prd-dagen.payments_v1.stg_transactions
group by debtor_customer_id
order by total_sent desc
```

### 4. Transaction Legs Analysis (CRITICAL)

#### Verify transaction legs structure
```sql
select
    transaction_id,
    direction,
    amount,
    total_debit,
    total_credit,
    is_balanced,
    count(*) as leg_count
from prd-dagen.payments_v1.stg_transaction_legs
group by transaction_id, direction, amount, total_debit, total_credit, is_balanced
order by transaction_id
```

#### Verify double-entry accounting
```sql
select
    transaction_id,
    total_debit,
    total_credit,
    abs(total_debit - total_credit) as difference,
    case 
        when total_debit = total_credit then 'BALANCED'
        else 'UNBALANCED'
    end as accounting_status
from prd-dagen.payments_v1.stg_transaction_legs
group by transaction_id, total_debit, total_credit
order by difference desc
```

#### Confirm exactly 2 legs per transaction
```sql
select
    transaction_id,
    count(distinct direction) as direction_count,
    count(*) as total_legs
from prd-dagen.payments_v1.stg_transaction_legs
group by transaction_id
having count(*) != 2 or count(distinct direction) != 2
```

### 5. Refunds Analysis

#### Get all refunds
```sql
select
    refund_id,
    original_transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at
from prd-dagen.payments_v1.stg_refunds
order by created_at desc
```

#### Refund statistics
```sql
select
    count(*) as total_refunds,
    sum(amount) as total_refund_amount,
    avg(amount) as avg_refund_amount
from prd-dagen.payments_v1.stg_refunds
```

#### Refunds by status
```sql
select
    status,
    count(*) as refund_count,
    sum(amount) as total_amount
from prd-dagen.payments_v1.stg_refunds
group by status
order by refund_count desc
```

### 6. Disputes Analysis

#### Get all disputes
```sql
select
    dispute_id,
    transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at
from prd-dagen.payments_v1.stg_disputes
order by created_at desc
```

#### Dispute statistics
```sql
select
    count(*) as total_disputes,
    sum(amount) as total_dispute_amount,
    avg(amount) as avg_dispute_amount
from prd-dagen.payments_v1.stg_disputes
```

#### Disputes by status
```sql
select
    status,
    count(*) as dispute_count,
    sum(amount) as total_amount
from prd-dagen.payments_v1.stg_disputes
group by status
order by dispute_count desc
```

### 7. Fees Analysis

#### Get all fees
```sql
select
    fee_id,
    transaction_id,
    amount,
    currency,
    fee_type,
    created_at
from prd-dagen.payments_v1.stg_fees
order by created_at desc
```

#### Fee statistics
```sql
select
    count(*) as total_fees,
    sum(amount) as total_fee_amount,
    avg(amount) as avg_fee_amount
from prd-dagen.payments_v1.stg_fees
```

#### Fees by type
```sql
select
    fee_type,
    count(*) as fee_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from prd-dagen.payments_v1.stg_fees
group by fee_type
order by total_amount desc
```

---

## 📈 Dimension Model Queries

### 8. Customer Dimension Analysis

#### Get customer dimension with all attributes
```sql
select
    customer_key,
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    is_kyc_verified,
    is_active,
    created_at,
    updated_at
from prd-dagen.payments_v1.dim_customers
order by created_at desc
```

#### Find active customers
```sql
select
    customer_id,
    customer_type,
    email,
    kyc_status
from prd-dagen.payments_v1.dim_customers
where is_active = true
order by customer_id
```

#### Find KYC verified customers
```sql
select
    customer_id,
    customer_type,
    email
from prd-dagen.payments_v1.dim_customers
where is_kyc_verified = true
order by customer_id
```

### 9. Payment Methods Dimension Analysis

#### Get payment methods with customer info
```sql
select
    dpm.payment_method_key,
    dpm.payment_method_id,
    dpm.customer_id,
    dpm.method_type,
    dpm.is_default,
    dc.customer_type,
    dc.email
from prd-dagen.payments_v1.dim_payment_methods dpm
left join prd-dagen.payments_v1.dim_customers dc
    on dpm.customer_key = dc.customer_key
order by dpm.customer_id, dpm.created_at
```

#### Find default payment methods
```sql
select
    customer_id,
    payment_method_id,
    method_type
from prd-dagen.payments_v1.dim_payment_methods
where is_default = true
```

### 10. Date Dimension Analysis

#### Get date dimension for specific date range
```sql
select
    date_key,
    calendar_date,
    year,
    month,
    day,
    quarter,
    day_name,
    month_name,
    is_weekend
from prd-dagen.payments_v1.dim_date
where year = 2025
order by calendar_date
```

#### Get all weekdays
```sql
select
    date_key,
    calendar_date,
    day_name
from prd-dagen.payments_v1.dim_date
where is_weekend = false
order by calendar_date
```

#### Get all weekends
```sql
select
    date_key,
    calendar_date,
    day_name
from prd-dagen.payments_v1.dim_date
where is_weekend = true
order by calendar_date
```

---

## 💰 Fact Model Queries

### 11. Transaction Fact Analysis

#### Get all transactions with dimensional context
```sql
select
    ft.transaction_key,
    ft.transaction_id,
    dd.calendar_date,
    dc_debtor.customer_id as debtor_id,
    dc_creditor.customer_id as creditor_id,
    dpm.method_type,
    ft.transaction_amount,
    ft.currency,
    ft.transaction_status
from prd-dagen.payments_v1.fact_transactions ft
left join prd-dagen.payments_v1.dim_date dd
    on ft.date_key = dd.date_key
left join prd-dagen.payments_v1.dim_customers dc_debtor
    on ft.debtor_customer_key = dc_debtor.customer_key
left join prd-dagen.payments_v1.dim_customers dc_creditor
    on ft.creditor_customer_key = dc_creditor.customer_key
left join prd-dagen.payments_v1.dim_payment_methods dpm
    on ft.payment_method_key = dpm.payment_method_key
order by dd.calendar_date desc
```

#### Transaction volume by date
```sql
select
    dd.calendar_date,
    dd.day_name,
    count(*) as transaction_count,
    sum(ft.transaction_amount) as total_amount,
    avg(ft.transaction_amount) as avg_amount
from prd-dagen.payments_v1.fact_transactions ft
left join prd-dagen.payments_v1.dim_date dd
    on ft.date_key = dd.date_key
group by dd.calendar_date, dd.day_name
order by dd.calendar_date desc
```

#### Transaction volume by payment method
```sql
select
    dpm.method_type,
    count(*) as transaction_count,
    sum(ft.transaction_amount) as total_amount,
    avg(ft.transaction_amount) as avg_amount
from prd-dagen.payments_v1.fact_transactions ft
left join prd-dagen.payments_v1.dim_payment_methods dpm
    on ft.payment_method_key = dpm.payment_method_key
group by dpm.method_type
order by total_amount desc
```

#### Transaction volume by customer
```sql
select
    dc.customer_id,
    dc.customer_type,
    count(*) as transaction_count,
    sum(ft.transaction_amount) as total_sent,
    avg(ft.transaction_amount) as avg_sent
from prd-dagen.payments_v1.fact_transactions ft
left join prd-dagen.payments_v1.dim_customers dc
    on ft.debtor_customer_key = dc.customer_key
group by dc.customer_id, dc.customer_type
order by total_sent desc
```

### 12. Transaction Details Analysis

#### Get transaction details with refunds, disputes, fees
```sql
select
    ftd.transaction_id,
    ftd.transaction_amount,
    ftd.refund_amount,
    ftd.dispute_amount,
    ftd.fee_amount,
    ftd.net_transaction_amount,
    ftd.refund_count,
    ftd.dispute_count,
    ftd.fee_count
from prd-dagen.payments_v1.fact_transaction_details ftd
order by ftd.transaction_id
```

#### Find transactions with refunds
```sql
select
    transaction_id,
    transaction_amount,
    refund_amount,
    (refund_amount / transaction_amount * 100) as refund_percentage,
    net_transaction_amount
from prd-dagen.payments_v1.fact_transaction_details
where refund_amount > 0
order by refund_amount desc
```

#### Find transactions with disputes
```sql
select
    transaction_id,
    transaction_amount,
    dispute_amount,
    (dispute_amount / transaction_amount * 100) as dispute_percentage
from prd-dagen.payments_v1.fact_transaction_details
where dispute_amount > 0
order by dispute_amount desc
```

#### Find transactions with fees
```sql
select
    transaction_id,
    transaction_amount,
    fee_amount,
    (fee_amount / transaction_amount * 100) as fee_percentage,
    net_transaction_amount
from prd-dagen.payments_v1.fact_transaction_details
where fee_amount > 0
order by fee_amount desc
```

#### Calculate net transaction impact
```sql
select
    count(*) as total_transactions,
    sum(transaction_amount) as gross_amount,
    sum(refund_amount) as total_refunds,
    sum(fee_amount) as total_fees,
    sum(net_transaction_amount) as net_amount,
    (sum(refund_amount) / sum(transaction_amount) * 100) as refund_rate,
    (sum(fee_amount) / sum(transaction_amount) * 100) as fee_rate
from prd-dagen.payments_v1.fact_transaction_details
```

### 13. Payout Fact Analysis

#### Get all payouts with dimensional context
```sql
select
    fp.payout_key,
    fp.payout_id,
    dd.calendar_date,
    dc.customer_id,
    dc.customer_type,
    fp.payout_amount,
    fp.currency,
    fp.payout_status,
    fp.scheduled_at,
    fp.executed_at
from prd-dagen.payments_v1.fact_payouts fp
left join prd-dagen.payments_v1.dim_date dd
    on fp.date_key = dd.date_key
left join prd-dagen.payments_v1.dim_customers dc
    on fp.recipient_customer_key = dc.customer_key
order by dd.calendar_date desc
```

#### Payout volume by date
```sql
select
    dd.calendar_date,
    count(*) as payout_count,
    sum(fp.payout_amount) as total_payout,
    avg(fp.payout_amount) as avg_payout
from prd-dagen.payments_v1.fact_payouts fp
left join prd-dagen.payments_v1.dim_date dd
    on fp.date_key = dd.date_key
group by dd.calendar_date
order by dd.calendar_date desc
```

#### Payout volume by customer
```sql
select
    dc.customer_id,
    dc.customer_type,
    count(*) as payout_count,
    sum(fp.payout_amount) as total_payout,
    avg(fp.payout_amount) as avg_payout
from prd-dagen.payments_v1.fact_payouts fp
left join prd-dagen.payments_v1.dim_customers dc
    on fp.recipient_customer_key = dc.customer_key
group by dc.customer_id, dc.customer_type
order by total_payout desc
```

#### Payout volume by status
```sql
select
    payout_status,
    count(*) as payout_count,
    sum(payout_amount) as total_amount
from prd-dagen.payments_v1.fact_payouts
group by payout_status
order by payout_count desc
```

---

## 🔗 Cross-Model Analysis

### 14. Customer Lifecycle Analysis

#### Customer activity summary
```sql
select
    dc.customer_id,
    dc.customer_type,
    dc.kyc_status,
    dc.is_active,
    count(distinct ft.transaction_key) as transaction_count,
    sum(ft.transaction_amount) as total_sent,
    count(distinct fp.payout_key) as payout_count,
    sum(fp.payout_amount) as total_payouts
from prd-dagen.payments_v1.dim_customers dc
left join prd-dagen.payments_v1.fact_transactions ft
    on dc.customer_key = ft.debtor_customer_key
left join prd-dagen.payments_v1.fact_payouts fp
    on dc.customer_key = fp.recipient_customer_key
group by dc.customer_id, dc.customer_type, dc.kyc_status, dc.is_active
order by total_sent desc
```

### 15. Monthly Revenue Analysis

#### Monthly transaction volume and net revenue
```sql
select
    dd.year,
    dd.month,
    dd.month_name,
    count(distinct ftd.transaction_id) as transaction_count,
    sum(ftd.transaction_amount) as gross_revenue,
    sum(ftd.refund_amount) as refunds,
    sum(ftd.fee_amount) as fees,
    sum(ftd.net_transaction_amount) as net_revenue
from prd-dagen.payments_v1.fact_transaction_details ftd
left join prd-dagen.payments_v1.fact_transactions ft
    on ftd.transaction_key = ft.transaction_key
left join prd-dagen.payments_v1.dim_date dd
    on ft.date_key = dd.date_key
group by dd.year, dd.month, dd.month_name
order by dd.year, dd.month
```

---

## 🎯 Key Metrics

### Customer Metrics
```sql
select
    count(distinct customer_id) as total_customers,
    sum(case when is_active = true then 1 else 0 end) as active_customers,
    sum(case when is_kyc_verified = true then 1 else 0 end) as verified_customers
from prd-dagen.payments_v1.dim_customers
```

### Transaction Metrics
```sql
select
    count(*) as total_transactions,
    sum(transaction_amount) as total_volume,
    avg(transaction_amount) as avg_transaction,
    min(transaction_amount) as min_transaction,
    max(transaction_amount) as max_transaction
from prd-dagen.payments_v1.fact_transactions
```

### Payment Method Metrics
```sql
select
    count(distinct payment_method_id) as total_methods,
    count(distinct customer_id) as customers_with_methods,
    sum(case when is_default = true then 1 else 0 end) as default_methods
from prd-dagen.payments_v1.dim_payment_methods
```

---

## 📝 Notes

- All queries use the `prd-dagen` project and `payments_v1` dataset
- Adjust date ranges as needed for your analysis
- Consider adding WHERE clauses to filter by date range for performance
- Use LIMIT clause for large result sets during exploration

---

**Last Updated**: 2026-02-14