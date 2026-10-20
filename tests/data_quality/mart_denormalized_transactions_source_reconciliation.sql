with expected as (
    select distinct transaction_id
    from {{ source('raw_data', 'transactions') }}
    where _ab_cdc_deleted_at is null
      and transaction_id is not null
),
actual as (
    select transaction_id
    from {{ ref('mart_denormalized_transactions') }}
),
reconciled as (
    select
        coalesce(expected.transaction_id, actual.transaction_id) as transaction_id,
        expected.transaction_id is null as unexpected_target_row,
        actual.transaction_id is null as missing_target_row
    from expected
    full outer join actual using (transaction_id)
)
select
    transaction_id,
    unexpected_target_row,
    missing_target_row,
    'target transaction keys do not reconcile to current non-deleted source keys' as failure_reason
from reconciled
where unexpected_target_row or missing_target_row
