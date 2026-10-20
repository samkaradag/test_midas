{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['mart', 'transactions', 'daily', 'business_validation']
) }}

with transactions_ranked as (
    select
        transaction_id,
        reference,
        amount,
        payment_method_id,
        debtor_customer_id,
        creditor_customer_id,
        currency,
        status,
        created_at,
        updated_at,
        row_number() over (
            partition by transaction_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'transactions') }}
    where _ab_cdc_deleted_at is null
      and transaction_id is not null
),
transactions_current as (
    select * except(version_rank)
    from transactions_ranked
    where version_rank = 1
),
customers_ranked as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        address,
        kyc_status,
        risk_profile,
        created_at,
        updated_at,
        row_number() over (
            partition by customer_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'customers') }}
    where _ab_cdc_deleted_at is null
      and customer_id is not null
),
customers_current as (
    select * except(version_rank)
    from customers_ranked
    where version_rank = 1
),
payment_methods_ranked as (
    select
        payment_method_id,
        customer_id,
        method_type,
        details,
        is_default,
        created_at,
        updated_at,
        row_number() over (
            partition by payment_method_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'payment_methods') }}
    where _ab_cdc_deleted_at is null
      and payment_method_id is not null
),
payment_methods_current as (
    select * except(version_rank)
    from payment_methods_ranked
    where version_rank = 1
),
refunds_ranked as (
    select
        refund_id,
        original_transaction_id as transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at,
        row_number() over (
            partition by refund_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'refunds') }}
    where _ab_cdc_deleted_at is null
      and refund_id is not null
),
refunds_by_transaction as (
    select
        transaction_id,
        count(*) as refund_count,
        sum(amount) as total_refund_amount,
        sum(if(lower(status) = 'completed', amount, cast(0 as numeric))) as completed_refund_amount,
        max(coalesce(updated_at, created_at)) as latest_refund_at,
        count(distinct currency) = 1 as has_single_refund_currency,
        any_value(currency having max coalesce(updated_at, created_at)) as refund_currency
    from refunds_ranked
    where version_rank = 1
      and transaction_id is not null
    group by transaction_id
),
disputes_ranked as (
    select
        dispute_id,
        transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at,
        row_number() over (
            partition by dispute_id
            order by
                coalesce(updated_at, created_at) desc,
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'disputes') }}
    where _ab_cdc_deleted_at is null
      and dispute_id is not null
),
disputes_by_transaction as (
    select
        transaction_id,
        count(*) as dispute_count,
        countif(lower(status) = 'open') as open_dispute_count,
        sum(amount) as total_dispute_amount,
        max(coalesce(updated_at, created_at)) as latest_dispute_at,
        count(distinct currency) = 1 as has_single_dispute_currency,
        any_value(currency having max coalesce(updated_at, created_at)) as dispute_currency
    from disputes_ranked
    where version_rank = 1
      and transaction_id is not null
    group by transaction_id
),
fees_ranked as (
    select
        fee_id,
        transaction_id,
        amount,
        currency,
        fee_type,
        created_at,
        row_number() over (
            partition by fee_id
            order by
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'fees') }}
    where _ab_cdc_deleted_at is null
      and fee_id is not null
),
fees_by_transaction as (
    select
        transaction_id,
        count(*) as fee_count,
        sum(amount) as total_fee_amount,
        max(created_at) as latest_fee_at,
        count(distinct currency) = 1 as has_single_fee_currency,
        any_value(currency having max created_at) as fee_currency
    from fees_ranked
    where version_rank = 1
      and transaction_id is not null
    group by transaction_id
),
legs_ranked as (
    select
        leg_id,
        transaction_id,
        direction,
        amount,
        currency,
        account,
        created_at,
        row_number() over (
            partition by leg_id
            order by
                _ab_cdc_lsn desc,
                _airbyte_extracted_at desc,
                _airbyte_raw_id desc
        ) as version_rank
    from {{ source('raw_data', 'transaction_legs') }}
    where _ab_cdc_deleted_at is null
      and leg_id is not null
),
legs_by_transaction as (
    select
        transaction_id,
        count(*) as leg_count,
        countif(lower(direction) = 'debit') as debit_leg_count,
        countif(lower(direction) = 'credit') as credit_leg_count,
        sum(if(lower(direction) = 'debit', amount, cast(0 as numeric))) as total_debit_amount,
        sum(if(lower(direction) = 'credit', amount, cast(0 as numeric))) as total_credit_amount,
        sum(if(lower(direction) = 'debit', amount, cast(0 as numeric)))
          - sum(if(lower(direction) = 'credit', amount, cast(0 as numeric))) as ledger_net_amount,
        abs(
            sum(if(lower(direction) = 'debit', amount, cast(0 as numeric)))
              - sum(if(lower(direction) = 'credit', amount, cast(0 as numeric)))
        ) <= 0.01 as is_ledger_balanced,
        count(distinct currency) = 1 as has_single_leg_currency,
        any_value(currency having max created_at) as leg_currency
    from legs_ranked
    where version_rank = 1
      and transaction_id is not null
    group by transaction_id
)
select
    tx.transaction_id,
    tx.reference,
    tx.amount as transaction_amount,
    tx.currency,
    tx.status as transaction_status,
    tx.created_at as transaction_created_at,
    tx.updated_at as transaction_updated_at,

    tx.debtor_customer_id,
    debtor.customer_type as debtor_customer_type,
    debtor.email as debtor_email,
    debtor.phone_number as debtor_phone_number,
    debtor.address as debtor_address,
    debtor.kyc_status as debtor_kyc_status,
    debtor.risk_profile as debtor_risk_profile,

    tx.creditor_customer_id,
    creditor.customer_type as creditor_customer_type,
    creditor.email as creditor_email,
    creditor.phone_number as creditor_phone_number,
    creditor.address as creditor_address,
    creditor.kyc_status as creditor_kyc_status,
    creditor.risk_profile as creditor_risk_profile,

    tx.payment_method_id,
    payment_method.customer_id as payment_method_customer_id,
    payment_method.method_type as payment_method_type,
    payment_method.details as payment_method_details,
    payment_method.is_default as payment_method_is_default,

    coalesce(refunds.refund_count, 0) as refund_count,
    coalesce(refunds.total_refund_amount, cast(0 as numeric)) as total_refund_amount,
    coalesce(refunds.completed_refund_amount, cast(0 as numeric)) as completed_refund_amount,
    refunds.latest_refund_at,

    coalesce(disputes.dispute_count, 0) as dispute_count,
    coalesce(disputes.open_dispute_count, 0) as open_dispute_count,
    coalesce(disputes.total_dispute_amount, cast(0 as numeric)) as total_dispute_amount,
    disputes.latest_dispute_at,

    coalesce(fees.fee_count, 0) as fee_count,
    coalesce(fees.total_fee_amount, cast(0 as numeric)) as total_fee_amount,
    fees.latest_fee_at,

    coalesce(legs.leg_count, 0) as leg_count,
    coalesce(legs.debit_leg_count, 0) as debit_leg_count,
    coalesce(legs.credit_leg_count, 0) as credit_leg_count,
    coalesce(legs.total_debit_amount, cast(0 as numeric)) as total_debit_amount,
    coalesce(legs.total_credit_amount, cast(0 as numeric)) as total_credit_amount,
    coalesce(legs.ledger_net_amount, cast(0 as numeric)) as ledger_net_amount,
    coalesce(legs.is_ledger_balanced, true) as is_ledger_balanced,

    tx.amount
      - coalesce(refunds.completed_refund_amount, cast(0 as numeric))
      - coalesce(fees.total_fee_amount, cast(0 as numeric)) as net_transaction_amount,

    tx.amount >= 0 as is_nonnegative_transaction_amount,
    tx.debtor_customer_id != tx.creditor_customer_id as has_distinct_parties,
    lower(tx.status) in ('pending', 'completed', 'failed', 'cancelled') as is_valid_transaction_status,
    coalesce(refunds.has_single_refund_currency and refunds.refund_currency = tx.currency, true) as is_refund_currency_valid,
    coalesce(disputes.has_single_dispute_currency and disputes.dispute_currency = tx.currency, true) as is_dispute_currency_valid,
    coalesce(fees.has_single_fee_currency and fees.fee_currency = tx.currency, true) as is_fee_currency_valid,
    coalesce(legs.has_single_leg_currency and legs.leg_currency = tx.currency, true) as is_leg_currency_valid,
    coalesce(refunds.completed_refund_amount, cast(0 as numeric)) <= tx.amount as is_refund_within_transaction_amount,
    current_timestamp() as dbt_updated_at
from transactions_current tx
left join customers_current debtor
    on tx.debtor_customer_id = debtor.customer_id
left join customers_current creditor
    on tx.creditor_customer_id = creditor.customer_id
left join payment_methods_current payment_method
    on tx.payment_method_id = payment_method.payment_method_id
left join refunds_by_transaction refunds
    on tx.transaction_id = refunds.transaction_id
left join disputes_by_transaction disputes
    on tx.transaction_id = disputes.transaction_id
left join fees_by_transaction fees
    on tx.transaction_id = fees.transaction_id
left join legs_by_transaction legs
    on tx.transaction_id = legs.transaction_id
