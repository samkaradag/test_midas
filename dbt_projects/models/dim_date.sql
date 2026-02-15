-- dim_date.sql
-- Purpose: Date dimension table for star schema
-- Creates a date spine with all relevant date attributes

{{ config(
    materialized='table',
    schema='payments_v1',
    tags=['marts', 'dimension', 'date'],
    description='Date dimension with comprehensive date attributes'
) }}

-- Generate dates using UNNEST and date arithmetic
with date_spine as (
    select
        date_add(date('2024-01-01'), interval offset day) as calendar_date
    from unnest(generate_array(0, 730)) as offset
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
order by calendar_date