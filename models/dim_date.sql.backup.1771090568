-- dim_date.sql
-- Purpose: Date dimension table for star schema
-- Creates a date spine with all relevant date attributes
-- Covers 730 days from min to max date in transaction data
-- Input: stg_transactions (to determine date range)
-- Output: complete date dimension

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'dimension', 'date'],
    description='Date dimension with comprehensive date attributes'
) }}

-- Get the date range from transactions
with date_range as (
    select
        min(date(created_at)) as min_date,
        max(date(created_at)) as max_date
    from {{ ref('stg_transactions') }}
),

-- Generate date spine for all days in range
date_spine as (
    select
        date_add(
            (select min_date from date_range),
            interval cast(row_number() over (order by 1) - 1 as int64) day
        ) as calendar_date
    from (
        select 1 as n union all select 2 union all select 3 union all select 4 union all select 5
        union all select 6 union all select 7 union all select 8 union all select 9 union all select 10
    ) cross join (
        select 1 as n union all select 2 union all select 3 union all select 4 union all select 5
        union all select 6 union all select 7 union all select 8 union all select 9 union all select 10
    ) cross join (
        select 1 as n union all select 2 union all select 3 union all select 4 union all select 5
        union all select 6 union all select 7 union all select 8 union all select 9 union all select 10
    )
    where date_add(
        (select min_date from date_range),
        interval cast(row_number() over (order by 1) - 1 as int64) day
    ) <= (select max_date from date_range)
),

-- Enrich with date attributes
enrich_dates as (
    select
        format_date('%Y%m%d', calendar_date) as date_key,
        calendar_date as calendar_date,
        extract(year from calendar_date) as year,
        extract(month from calendar_date) as month,
        extract(day from calendar_date) as day,
        extract(quarter from calendar_date) as quarter,
        extract(week from calendar_date) as week,
        extract(dayofweek from calendar_date) as day_of_week,
        case 
            when extract(dayofweek from calendar_date) in (1, 7) then true 
            else false 
        end as is_weekend,
        format_date('%A', calendar_date) as day_name,
        format_date('%B', calendar_date) as month_name,
        format_date('%Y-%m', calendar_date) as year_month,
        current_timestamp() as dbt_loaded_at
    from date_spine
)

select * from enrich_dates