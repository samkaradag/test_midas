{{
  config(
    materialized='table',
    schema='payments_v1',
    tags=['dimension', 'date'],
    description='Date dimension covering the entire transaction period with date attributes.'
  )
}}

with date_spine as (
  select
    cast(date_value as date) as date_value
  from (
    select
      date_add('2025-07-26', interval cast(row_number() over (order by 1) - 1 as int64) day) as date_value
    from (
      select 1 union all select 2 union all select 3 union all select 4 union all select 5
      union all select 6 union all select 7 union all select 8 union all select 9 union all select 10
      union all select 11 union all select 12 union all select 13 union all select 14 union all select 15
      union all select 16 union all select 17 union all select 18 union all select 19 union all select 20
      union all select 21 union all select 22 union all select 23 union all select 24 union all select 25
      union all select 26 union all select 27 union all select 28 union all select 29 union all select 30
      union all select 31 union all select 32 union all select 33 union all select 34 union all select 35
      union all select 36 union all select 37 union all select 38 union all select 39 union all select 40
      union all select 41 union all select 42 union all select 43 union all select 44
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
    format_date('%B', date_value) as month_name,
    current_timestamp() as dbt_loaded_at
  from date_spine
)

select * from date_attributes